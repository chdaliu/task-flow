//
//  TaskFlowPool.swift
//
//  Actor that owns the execution engine for TaskFlow.
//
//  A pool:
//  - de-duplicates tasks by `id` so shared dependencies run exactly once,
//  - flattens the dependency graph into ordered execution layers,
//  - runs each layer's tasks concurrently, and
//  - tracks retention (`sinkCount`) so a shared node is released only when
//    every owner has been cleared.
//

import Foundation

/// Serializes all access to the shared task registry and state transitions.
public actor TaskFlowPool {
    /// Tasks registered on this pool, keyed by `TaskFlow.id`.
    fileprivate var pool: [AnyHashable: TaskFlow] = [:]

    /// Creates a new, isolated pool. Pass it to `flow(on:)` to run tasks on it,
    /// or omit the pool argument to use the process-wide default pool.
    public init() {}

    /// Returns the current lifecycle state of the registered task with the given
    /// `id`, or `nil` if no such task is registered. The read is actor-isolated.
    public func state(of id: AnyHashable) -> TaskFlow.State? {
        pool[id]?.state
    }
}

/// Errors surfaced by the pool.
///
/// `@unchecked Sendable` because the cycle trace carries `AnyHashable` ids; like
/// `TaskFlow` itself, these errors are only ever created and consumed on (or
/// thrown from) the pool actor.
public enum TaskFlowError: Error, Equatable, @unchecked Sendable {
    /// The task (or a node in its graph) was canceled.
    case canceled
    /// The whole flow exceeded its timeout.
    case timedOut
    /// An unexpected failure with no underlying error attached.
    case unknown
    /// The dependency graph contains a cycle; `nodes` is the cycle trace,
    /// e.g. `["A", "B", "A"]` meaning A depends on B depends on A.
    case circularDependency(nodes: [AnyHashable])
}

extension TaskFlowError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .canceled:
            return "The task was canceled."
        case .timedOut:
            return "The task flow exceeded its timeout."
        case .unknown:
            return "The task failed with an unknown error."
        case .circularDependency(let nodes):
            return "Circular dependency detected: \(nodes)"
        }
    }
}

/// Carries the outcome of a single run of a task.
///
/// Each run gets a fresh box tagged with the run token so that a completion
/// arriving from an older, superseded run is ignored.
fileprivate final class TaskFlowCompletionBox: @unchecked Sendable {
    let runID: UInt64
    var error: (any Error)?

    init(runID: UInt64) {
        self.runID = runID
    }
}

/// Shuttles a batch of task ids across actor boundaries.
///
/// Task ids are `AnyHashable` and therefore not `Sendable`. The pool serializes
/// every access to the ids it stores, so like `TaskFlow` itself this box may be
/// `@unchecked Sendable`; it is only ever created, handed to the pool, and read
/// on the pool actor. It is an internal transport type — it never appears in the
/// public API surface.
struct TaskFlowIDBatch: @unchecked Sendable {
    let ids: [AnyHashable]
}

/// Internal result of a timed flow race.
fileprivate enum TaskFlowResult {
    case value
    case error(any Error)
    case timedOut
}

extension TaskFlowPool {

    /// Runs a task and its dependency graph, optionally enforcing a whole-flow timeout.
    ///
    /// The graph is validated (cycles throw `.circularDependency` before anything
    /// runs), every node is registered on the pool, then each layer is executed
    /// concurrently. When the flow fails, the ownership it acquired is released, so
    /// a failed run does not leak nodes into the pool or skew shared sink counts.
    ///
    /// - Parameters:
    ///   - task: The root task to run.
    ///   - timeout: Whole-flow timeout. If the flow does not finish in time, all
    ///     reachable nodes are canceled and `.timedOut` is thrown.
    /// - Throws: The flow's failure: an underlying task error, `.canceled`,
    ///   `.timedOut`, or `.circularDependency`.
    public func flow(_ task: TaskFlow, timeout: TimeInterval = 0) async throws {
        guard timeout > 0 else {
            try await runFlow(task)
            return
        }
        // Race the flow against a sleep; the first to finish wins.
        let result: TaskFlowResult = await withTaskGroup(of: TaskFlowResult.self) { group in
            group.addTask {
                do {
                    try await self.runFlow(task)
                    return .value
                } catch {
                    return .error(error)
                }
            }
            group.addTask {
                try? await Task.sleep(for: .seconds(timeout))
                return .timedOut
            }
            guard let first = await group.next() else {
                return .timedOut
            }
            group.cancelAll()
            return first
        }
        switch result {
        case .value:
            return
        case .error(let error):
            throw error
        case .timedOut:
            // Prevent later flows from hanging on nodes left `.flowing` by a timed-out run.
            cancelGraph(task)
            throw TaskFlowError.timedOut
        }
    }

    /// Cancels a task and every node reachable from it (cycle-safe via a visited set).
    func cancelGraph(_ task: TaskFlow) {
        var visited: Set<ObjectIdentifier> = []
        func visit(_ node: TaskFlow) {
            guard visited.insert(ObjectIdentifier(node)).inserted else {
                return
            }
            cancel(node)
            for dependency in node.dependencies {
                visit(dependency)
            }
        }
        visit(task)
    }

    /// Validates the graph, registers its nodes, then executes it layer by layer.
    ///
    /// On failure, the ownership every registered node gained for this flow is
    /// released via `clear`, so a failed run cannot leak retained nodes into the
    /// pool or skew shared `sinkCount`s.
    func runFlow(_ task: TaskFlow) async throws {
        let queues = try layers(of: task)
        var registered: [TaskFlow] = []
        // Register every node up front so shared state is consistent across the whole run.
        for queue in queues {
            for node in queue {
                register(node)
                registered.append(node)
            }
        }
        do {
            for queue in queues {
                guard !task.state.isCanceled, !Task.isCancelled else {
                    throw TaskFlowError.canceled
                }
                // Execute each layer's nodes concurrently, since by construction a layer
                // only depends on nodes from earlier layers.
                try await withThrowingTaskGroup(of: Void.self) { group in
                    for node in queue {
                        group.addTask {
                            try await self.execute(node)
                        }
                    }
                    try await group.waitForAll()
                }
            }
            guard !task.state.isCanceled, !Task.isCancelled else {
                throw TaskFlowError.canceled
            }
        } catch {
            for node in registered {
                // A node left `.flowing` by an aborted flow (e.g. its awaiting task was
                // canceled) would otherwise linger forever, making later flows hang on a
                // terminal state that never arrives. When this flow was its last owner,
                // cancel it before releasing so a re-flow fails fast instead of hanging.
                // Shared nodes (still owned by other flows) are left untouched.
                let target = pool[node.id] ?? node
                if target.sinkCount == 1, case .flowing = target.state {
                    cancel(target)
                }
                clear(node)
            }
            throw error
        }
    }

    /// Flattens the dependency graph into execution layers.
    ///
    /// Each node is assigned the depth of its longest dependency chain: dependencies
    /// get smaller depths, so sorting by depth yields a valid topological execution
    /// order. A node reached again on the current DFS path indicates a cycle, which
    /// is reported as a `.circularDependency` error instead of being executed.
    func layers(of task: TaskFlow) throws -> [[TaskFlow]] {
        var depths: [ObjectIdentifier: Int] = [:]
        var visiting: Set<ObjectIdentifier> = []
        var groups: [Int: [TaskFlow]] = [:]
        var path: [TaskFlow] = []

        func collect(_ node: TaskFlow) throws -> Int {
            let key = ObjectIdentifier(node)
            if let depth = depths[key] {
                return depth
            }
            if visiting.contains(key) {
                let start = path.firstIndex { ObjectIdentifier($0) == key } ?? path.startIndex
                throw TaskFlowError.circularDependency(nodes: path[start...].map(\.id) + [node.id])
            }
            visiting.insert(key)
            path.append(node)
            var depth = 0
            for dependency in node.dependencies {
                depth = max(depth, try collect(dependency) + 1)
            }
            path.removeLast()
            visiting.remove(key)
            depths[key] = depth
            groups[depth, default: []].append(node)
            return depth
        }

        _ = try collect(task)
        return groups.sorted { $0.key < $1.key }.map { $0.value }
    }

    /// Adds a node to the pool (once) and increments its retention count.
    ///
    /// Tasks are de-duplicated by `id`: when a node with the same id is already
    /// registered, the canonical node is kept and the incremented sink count is
    /// attributed to it, so retention bookkeeping stays consistent even when a
    /// caller flows a recreated instance that shares an id.
    func register(_ node: TaskFlow) {
        let canonical = pool[node.id] ?? node
        if pool[node.id] == nil {
            pool[node.id] = node
            node.pool = self
        }
        canonical.sinkCount += 1
    }

    /// Runs a single node, dispatching based on its current state.
    func execute(_ node: TaskFlow) async throws {
        let target = pool[node.id] ?? node
        switch target.state {
        case .ready:
            await run(target)
            try await resolveTerminal(target)
        case .done(let timestamp):
            if isExpired(target, timestamp: timestamp) {
                await run(target)
                try await resolveTerminal(target)
            }
        case .flowing:
            // Another flow is already running this node; wait for it.
            await waitForTerminal(target)
            try await resolveTerminal(target)
        case .error:
            try await retry(target)
        case .canceled:
            throw TaskFlowError.canceled
        }
    }

    /// Starts a single execution: marks the node flowing, invokes its handler,
    /// then waits for the terminal state (optionally bounded by `executionTimeout`).
    func run(_ target: TaskFlow) async {
        setState(target, .flowing)
        target.runID &+= 1
        let box = TaskFlowCompletionBox(runID: target.runID)
        target.handler { [weak self, weak target] error in
            guard let self, let target else {
                return
            }
            box.error = error
            Task { await self.complete(target, box: box) }
        }
        if target.executionTimeout > 0 {
            await waitForTerminal(target, timeout: target.executionTimeout)
        } else {
            await waitForTerminal(target)
        }
    }

    /// Records the result of a run, ignoring completions that are no longer current.
    fileprivate func complete(_ target: TaskFlow, box: TaskFlowCompletionBox) async {
        guard box.runID == target.runID else {
            return
        }
        switch target.state {
        case .flowing:
            if let error = box.error {
                setState(target, .error(error: error))
            } else {
                setState(target, .done(timestamp: Date().timeIntervalSince1970))
            }
        default:
            break
        }
    }

    /// Re-runs a failed node up to `retryLimit` times, then surfaces the underlying error.
    func retry(_ target: TaskFlow) async throws {
        guard target.retryCount < target.retryLimit else {
            if case .error(let error) = target.state {
                throw error ?? TaskFlowError.unknown
            }
            throw TaskFlowError.unknown
        }
        target.retryCount += 1
        setState(target, .ready)
        await run(target)
        try await resolveTerminal(target)
    }

    /// Returns whether a `.done` result is too old to be reused.
    func isExpired(_ target: TaskFlow, timestamp: TimeInterval) -> Bool {
        target.expiresAfter > 0 && Date().timeIntervalSince1970 - timestamp > target.expiresAfter
    }

    /// Suspends until the task reaches a terminal state (or returns immediately if it already has).
    ///
    /// Waiter registration happens on the pool actor (via `waitOnActor`), and only
    /// the actor ever transitions `state` (via `setState`), so waiting never reads
    /// or writes `state`/`waiters` from a non-isolated context.
    ///
    /// The waiter is cancellation-aware: if the awaiting task is canceled, its own
    /// continuation is removed and resumed so it cannot strand a task group's
    /// implicit await or leak a registration.
    func waitForTerminal(_ target: TaskFlow) async {
        guard !target.state.isTerminal else {
            return
        }
        let token = UUID()
        await withTaskCancellationHandler {
            await self.waitOnActor(target, token: token)
        } onCancel: {
            Task { await self.cancelWaiter(target, token: token) }
        }
    }

    /// Registers (or aborts) a waiter on the actor.
    ///
    /// Runs actor-isolated and atomically re-checks terminal state and the waiting
    /// task's cancellation, so a waiter canceled before registration never strands.
    /// The continuation is resumed exactly once: either by the terminal transition
    /// (via `setState`) or by `cancelWaiter`.
    private func waitOnActor(_ target: TaskFlow, token: UUID) async {
        if target.state.isTerminal || Task.isCancelled {
            return
        }
        await withCheckedContinuation(isolation: self) {
            (continuation: CheckedContinuation<Void, Never>) in
            target.waiters[token] = continuation
        }
    }

    /// Removes and resumes the waiter registered under `token`, if any.
    private func cancelWaiter(_ target: TaskFlow, token: UUID) async {
        if let waiter = target.waiters.removeValue(forKey: token) {
            waiter.resume()
        }
    }

    /// Suspends until the task reaches a terminal state or the timeout elapses,
    /// marking the task `.error(.timedOut)` if the timeout wins.
    func waitForTerminal(_ target: TaskFlow, timeout: TimeInterval) async {
        guard !target.state.isTerminal else {
            return
        }
        // Race the terminal wait against a background timer. The timer marks the
        // node `.error(.timedOut)` on the actor (resuming every waiter) instead of
        // racing a task group, so a canceled waiter can never strand the group.
        let timeoutTask = Task {
            try? await Task.sleep(for: .seconds(timeout))
            // Cancellation of the run (or its flow) is not a per-task timeout: once
            // canceled, leave the terminal transition to `cancelGraph`/`cancel` so a
            // timed-out flow cannot mark nodes `.error(.timedOut)` instead of `.canceled`.
            guard !Task.isCancelled else {
                return
            }
            await self.markTimedOut(target)
        }
        await waitForTerminal(target)
        timeoutTask.cancel()
    }

    /// Marks a node `.error(.timedOut)` unless it already reached a terminal state.
    private func markTimedOut(_ target: TaskFlow) async {
        if !target.state.isTerminal {
            setState(target, .error(error: TaskFlowError.timedOut))
        }
    }

    /// Transitions a node to a new state, resuming every waiter when the state is terminal.
    ///
    /// This is the single write path for `state` and must only be called on the
    /// pool actor. Non-terminal transitions leave waiters registered.
    func setState(_ target: TaskFlow, _ newState: TaskFlow.State) {
        target.state = newState
        guard newState.isTerminal else {
            return
        }
        for (_, waiter) in target.waiters {
            waiter.resume()
        }
        target.waiters.removeAll()
    }

    /// Applies retry/expiration policy after a node reached a terminal state.
    func resolveTerminal(_ target: TaskFlow) async throws {
        switch target.state {
        case .done(let timestamp):
            if isExpired(target, timestamp: timestamp) {
                await run(target)
                try await resolveTerminal(target)
            }
        case .error:
            try await retry(target)
        case .canceled:
            throw TaskFlowError.canceled
        default:
            break
        }
    }

    /// Transitions a ready/flowing node to `.canceled`; terminal states are left untouched.
    /// When `clear` is `true`, the node (and its reachable graph) is also released from
    /// the pool, subject to the usual clear-protection rules.
    ///
    /// When a recreated instance sharing the node's `id` is passed, the canonical
    /// registered node is acted on instead.
    public func cancel(_ task: TaskFlow, clear: Bool = false) {
        let target = pool[task.id] ?? task
        switch target.state {
        case .ready, .flowing:
            setState(target, .canceled)
        default:
            break
        }
        if clear {
            self.clear(target)
        }
    }

    /// Releases a task and its reachable graph from the pool.
    ///
    /// Each node's `sinkCount` is decremented once; a shared node is only removed
    /// (and canceled) when the count reaches zero, so other owners are unaffected.
    /// A visited set makes the recursion safe on cyclic graphs.
    ///
    /// A node is kept in the pool when it is executing or holds a still-protected
    /// `.done` result (see `shouldRetain`); its sink count still drops, but it is
    /// neither canceled nor removed. Pass `force: true` to bypass that protection,
    /// including for nodes that are already retained (registered with a zero sink count).
    ///
    /// When a recreated instance sharing the node's `id` is passed, the canonical
    /// registered node is acted on instead.
    public func clear(_ task: TaskFlow, force: Bool = false) {
        let target = pool[task.id] ?? task
        var visited: Set<ObjectIdentifier> = []
        clear(target, force: force, visited: &visited)
    }

    private func clear(_ task: TaskFlow, force: Bool, visited: inout Set<ObjectIdentifier>) {
        guard visited.insert(ObjectIdentifier(task)).inserted else {
            return
        }
        let wasOwned = task.sinkCount > 0
        guard wasOwned || pool[task.id] === task else {
            return
        }
        if wasOwned {
            task.sinkCount -= 1
        }
        if wasOwned, task.sinkCount > 0 {
            return
        }
        // This call released the last reference (or the node was already retained):
        // balance its dependencies so a protected node cannot orphan its subtree.
        let justReleased = wasOwned && task.sinkCount == 0
        if justReleased {
            for dependency in task.dependencies {
                clear(dependency, force: force, visited: &visited)
            }
        }
        let shouldRemove = justReleased || (force && pool[task.id] === task)
        guard shouldRemove else {
            return
        }
        guard force || !shouldRetain(task) else {
            return
        }
        remove(task)
    }

    private func remove(_ task: TaskFlow) {
        cancel(task)
        if pool[task.id] === task {
            pool.removeValue(forKey: task.id)
        }
        task.pool = nil
    }

    /// Returns whether a task must be kept in the pool when its last sink is cleared.
    ///
    /// - `.flowing` tasks are never released while running.
    /// - `.done` tasks are kept while flagged with `isClearProtected`, or while their
    ///   `expiresAfter` validity window has not elapsed.
    /// - `.ready`, `.error`, and `.canceled` tasks are always released.
    ///
    /// The explicit `expiresAfter > 0` is intentional: `isExpired` treats `0` as "never
    /// expires", which would otherwise protect every `.done` task from being cleared.
    func shouldRetain(_ task: TaskFlow) -> Bool {
        switch task.state {
        case .flowing:
            return true
        case .done(let timestamp):
            if task.isClearProtected {
                return true
            }
            return task.expiresAfter > 0 && !isExpired(task, timestamp: timestamp)
        default:
            return false
        }
    }
}

extension TaskFlowPool {

    /// Cancels every registered task whose `id` is in the batch.
    ///
    /// Only nodes that are currently `.ready` or `.flowing` are transitioned to
    /// `.canceled`; other states are left untouched. Unregistered ids are ignored.
    /// When `clear` is `true`, each canceled node (and its reachable graph) is also
    /// released from the pool, subject to the usual clear-protection rules.
    func cancel(_ batch: TaskFlowIDBatch, clear: Bool = false) {
        for id in batch.ids {
            if let task = pool[id] {
                cancel(task, clear: clear)
            }
        }
    }

    /// Clears every registered task whose `id` is in the batch.
    ///
    /// Behaves like `clear(_:force:)` for each id: a node is only removed when its
    /// `sinkCount` reaches zero (or `force` is `true`), so a shared dependency of
    /// several requested roots is released exactly once its last owner is cleared.
    /// Unregistered ids are ignored.
    func clear(_ batch: TaskFlowIDBatch, force: Bool = false) {
        for id in batch.ids {
            if let task = pool[id] {
                clear(task, force: force)
            }
        }
    }
}

/// Process-wide default pool used when `flow(on:)` is called without a pool.
let mainPool: TaskFlowPool = TaskFlowPool()