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
import Combine

/// Serializes all access to the shared task registry and state transitions.
public actor TaskFlowPool {
    /// Tasks registered on this pool, keyed by `TaskFlow.id`.
    fileprivate var pool: [AnyHashable: TaskFlow] = [:]
}

/// Errors surfaced by the pool.
///
/// `@unchecked Sendable` because it carries `AnyHashable` ids; like `TaskFlow`
/// itself, values are only ever created and consumed on the pool actor.
enum TaskFlowError: Error, Equatable, @unchecked Sendable {
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
/// on the pool actor.
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
    func flow(_ task: TaskFlow, timeout: TimeInterval = 0) async throws {
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
    func runFlow(_ task: TaskFlow) async throws {
        let queues = try layers(of: task)
        // Register every node up front so shared state is consistent across the whole run.
        for queue in queues {
            for node in queue {
                register(node)
            }
        }
        for queue in queues {
            guard !task.state.isCanceled else {
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
        guard !task.state.isCanceled else {
            throw TaskFlowError.canceled
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
    func register(_ node: TaskFlow) {
        if pool[node.id] == nil {
            pool[node.id] = node
            node.pool = self
        }
        node.sinkCount += 1
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
        target.state = .flowing
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
                target.state = .error(error: error)
            } else {
                target.state = .done(timestamp: Date().timeIntervalSince1970)
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
        target.state = .ready
        await run(target)
        try await resolveTerminal(target)
    }
    
    /// Returns whether a `.done` result is too old to be reused.
    func isExpired(_ target: TaskFlow, timestamp: TimeInterval) -> Bool {
        target.expiresAfter > 0 && Date().timeIntervalSince1970 - timestamp > target.expiresAfter
    }
    
    /// Suspends until the task reaches a terminal state (or returns immediately if it already has).
    func waitForTerminal(_ target: TaskFlow) async {
        guard !target.state.isTerminal else {
            return
        }
        _ = await target.$state.values.first(where: { $0.isTerminal })
    }
    
    /// Suspends until the task reaches a terminal state or the timeout elapses,
    /// marking the task `.error(.timedOut)` if the timeout wins.
    func waitForTerminal(_ target: TaskFlow, timeout: TimeInterval) async {
        guard !target.state.isTerminal else {
            return
        }
        let timedOut = await withTaskGroup(of: Bool.self) { group in
            group.addTask {
                _ = await target.$state.values.first(where: { $0.isTerminal })
                return false
            }
            group.addTask {
                try? await Task.sleep(for: .seconds(timeout))
                return true
            }
            guard let first = await group.next() else {
                return false
            }
            group.cancelAll()
            return first
        }
        if timedOut, !target.state.isTerminal {
            target.state = .error(error: TaskFlowError.timedOut)
        }
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
    func cancel(_ task: TaskFlow, clear: Bool = false) {
        switch task.state {
        case .ready, .flowing:
            task.state = .canceled
        default:
            break
        }
        if clear {
            self.clear(task)
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
    func clear(_ task: TaskFlow, force: Bool = false) {
        var visited: Set<ObjectIdentifier> = []
        clear(task, force: force, visited: &visited)
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
