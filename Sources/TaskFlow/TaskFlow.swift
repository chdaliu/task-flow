//
//  TaskFlow.swift
//
//  A node in a task dependency graph.
//
//  TaskFlow lets you build a directed acyclic graph of tasks, where each task
//  runs only after all of its dependencies have completed. Flowing a root task
//  executes the graph in parallel "layers": dependencies first, dependents later.
//

import Foundation
import Combine

/// A single task in a dependency graph.
///
/// - A task holds references to its dependencies; it only runs once they are done.
/// - It can be configured with a retry limit, an execution timeout, and an expiration age.
/// - Its observable `state` drives waiting, retrying, and shared-run de-duplication.
///
/// Instances are intentionally mutable and are expected to be manipulated only
/// through a `TaskFlowPool` actor, which serializes all state changes.
public class TaskFlow: @unchecked Sendable {
    
    /// Lifecycle state of a task, observed through `@Published`.
    enum State {
        case ready
        case flowing
        case done(timestamp: TimeInterval)
        case error(error: Error?)
        case canceled
        
        /// Whether execution has ended for this state and no further waiting is needed.
        var isTerminal: Bool {
            switch self {
            case .done, .error, .canceled:
                return true
            default:
                return false
            }
        }
        
        /// Whether the task was canceled.
        var isCanceled: Bool {
            if case .canceled = self {
                return true
            }
            return false
        }
    }
    
    /// A task body that reports completion (or failure) through its completion closure.
    /// The completion closure is `@escaping` and `@Sendable`; invoke it with `nil` on
    /// success or the error on failure. The parameter label is optional, so both
    /// `{ completion in ... }` and `{ (completion:) in ... }` call sites work.
    public typealias Handler = (_ completion: @escaping @Sendable (Error?) -> Void) -> Void
    
    @Published
    var state: State = .ready
    
    /// Unique identifier, used by the pool to register and de-duplicate tasks.
    let id: AnyHashable
    
    /// Tasks that must complete before this task runs. Mutable so the graph can be
    /// assembled after construction (also used to build test graphs).
    var dependencies: [TaskFlow]
    
    /// The work performed by this task.
    let handler: Handler
    
    /// Duration after completion after which a cached `.done` result is considered stale
    /// and the task is re-run. `0` means the result never expires.
    public var expiresAfter: TimeInterval = 0
    
    /// When `true`, a completed (`.done`) task is kept in the pool even when cleared;
    /// only `clear(force: true)` removes it. Protection never applies to failed,
    /// canceled, not-yet-run, or currently executing tasks.
    public var isClearProtected: Bool = false
    
    /// Maximum time a single execution may take before it is treated as `.error(.timedOut)`.
    /// `0` disables the per-execution timeout.
    public var executionTimeout: TimeInterval = 0
    
    /// Maximum number of retries after a failure. `0` means no retries.
    public var retryLimit: UInt = 0
    
    /// Number of retries already consumed. Lifetime-capped by `retryLimit`.
    public internal(set) var retryCount: UInt = 0
    
    /// Monotonic token bumped on every run so stale completions can be ignored.
    var runID: UInt64 = 0
    
    public init(id: AnyHashable = UUID().uuidString, dependencies: [TaskFlow] = [], _ handler: @escaping Handler) {
        self.id = id
        self.dependencies = dependencies
        self.handler = handler
    }
    
    /// Convenience initializer for synchronous task bodies.
    public convenience init(id: AnyHashable = UUID().uuidString, dependencies: [TaskFlow] = [], _ handler: @escaping () -> Void) {
        self.init(id: id, dependencies: dependencies) { completion in
            handler()
            completion(nil)
        }
    }
    
    /// Number of active references held by the pool. A shared dependency is only
    /// removed when this count reaches zero.
    var sinkCount: UInt = 0
    
    /// The pool currently managing this task. Weak to avoid retain cycles.
    weak var pool: TaskFlowPool?
}

extension TaskFlow {
    public typealias Completion = @Sendable (_ error: Error?) -> Void
    
    /// Starts executing this task and its dependencies on a pool.
    ///
    /// - Parameters:
    ///   - pool: The pool to run on; defaults to the process-wide `mainPool`.
    ///   - timeout: Whole-flow timeout. If the flow does not finish in time, all
    ///     reachable nodes are canceled and a `.timedOut` error is reported.
    ///   - completion: Invoked with `nil` on success or the error on failure.
    public func flow(on pool: TaskFlowPool? = nil, timeout: TimeInterval = 0, completion: Completion? = nil) {
        let pool = pool ?? mainPool
        Task {
            do {
                try await pool.flow(self, timeout: timeout)
                completion?(nil)
            } catch(let error) {
                completion?(error)
            }
        }
        self.pool = pool
    }
    
    /// Requests cancellation of this task on its current pool, if any.
    /// The pool reference is captured synchronously so the request is not lost
    /// if the pool is later replaced or cleared.
    ///
    /// - Parameter clear: When `true`, the task (and its reachable dependencies) is also
    ///   released from the pool after cancellation, following the usual clear-protection
    ///   rules. Defaults to `false`, so the task stays in the pool until explicitly cleared.
    public func cancel(clear: Bool = false) {
        let pool = self.pool
        guard let pool else {
            return
        }
        Task {
            await pool.cancel(self, clear: clear)
        }
    }
    
    /// Releases this task (and its reachable dependencies) from the pool, canceling it.
    ///
    /// A task is kept in the pool when it is currently executing (`.flowing`), or when
    /// its `.done` result is still within the protection window (`isClearProtected`, or
    /// `expiresAfter` has not elapsed yet). In those cases only the sink count drops and
    /// the cached result stays reusable. Pass `force: true` to release it regardless.
    ///
    /// The pool reference is captured before it is cleared so the async cleanup always
    /// runs against the correct pool; it is only nilled if the task is actually removed.
    public func clear(force: Bool = false) {
        let pool = self.pool
        guard let pool else {
            return
        }
        Task {
            await pool.clear(self, force: force)
        }
    }
}

extension TaskFlow {

    /// Cancels every task registered with one of the given `ids`.
    ///
    /// Mirrors `cancel(clear:)`: only `.ready`/`.flowing` tasks are transitioned
    /// to `.canceled` and kept in the pool. Pass `clear: true` to also release
    /// each task (and its reachable dependency graph) from the pool.
    ///
    /// - Parameters:
    ///   - ids: The ids of the tasks to cancel. Unknown ids are ignored.
    ///   - pool: The pool to act on; defaults to the process-wide `mainPool`.
    ///   - clear: When `true`, also releases the canceled tasks from the pool.
    public static func cancel(ids: [AnyHashable], on pool: TaskFlowPool? = nil, clear: Bool = false) {
        let pool = pool ?? mainPool
        let batch = TaskFlowIDBatch(ids: ids)
        Task {
            await pool.cancel(batch, clear: clear)
        }
    }

    /// Clears every task registered with one of the given `ids`.
    ///
    /// Mirrors `clear(force:)`: a task is released (and its reachable dependency
    /// graph balanced) only when it is not executing and holds no still-protected
    /// cached result, unless `force` bypasses that protection.
    ///
    /// - Parameters:
    ///   - ids: The ids of the tasks to clear. Unknown ids are ignored.
    ///   - pool: The pool to act on; defaults to the process-wide `mainPool`.
    ///   - force: When `true`, releases tasks even if they are still protected.
    public static func clear(ids: [AnyHashable], on pool: TaskFlowPool? = nil, force: Bool = false) {
        let pool = pool ?? mainPool
        let batch = TaskFlowIDBatch(ids: ids)
        Task {
            await pool.clear(batch, force: force)
        }
    }
}
