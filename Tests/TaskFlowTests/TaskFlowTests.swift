import Foundation
import Testing
@testable import TaskFlow

fileprivate final class TestLog: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String] = []
    func add(_ value: String) {
        lock.lock()
        defer { lock.unlock() }
        values.append(value)
    }
    var all: [String] {
        lock.lock()
        defer { lock.unlock() }
        return values
    }
}

fileprivate enum TestError: Error {
    case some
}

@Test func selfCircularDependencyThrows() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.dependencies.append(a)

    do {
        try await pool.flow(a)
        Issue.record("expected circular dependency error")
    } catch let TaskFlowError.circularDependency(nodes) {
        #expect(nodes == ["A", "A"])
    } catch {
        Issue.record("unexpected error: \(error)")
    }
}

@Test func twoNodeCircularDependencyThrows() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    let b = TaskFlow(id: "B", dependencies: [a]) {}
    a.dependencies.append(b)

    do {
        try await pool.flow(a)
        Issue.record("expected circular dependency error")
    } catch let TaskFlowError.circularDependency(nodes) {
        #expect(nodes == ["A", "B", "A"])
    } catch {
        Issue.record("unexpected error: \(error)")
    }
}

@Test func indirectCircularDependencyThrows() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    let b = TaskFlow(id: "B", dependencies: [a]) {}
    let c = TaskFlow(id: "C", dependencies: [b]) {}
    a.dependencies.append(c)

    do {
        try await pool.flow(c)
        Issue.record("expected circular dependency error")
    } catch let TaskFlowError.circularDependency(nodes) {
        #expect(nodes == ["C", "B", "A", "C"])
    } catch {
        Issue.record("unexpected error: \(error)")
    }
}

@Test func layeringOrder() async throws {
    let pool = TaskFlowPool()
    var log: [String] = []
    let d = TaskFlow(id: "D") { log.append("D") }
    let e = TaskFlow(id: "E") { log.append("E") }
    let c = TaskFlow(id: "C") { log.append("C") }
    let b = TaskFlow(id: "B", dependencies: [d, e]) { log.append("B") }
    let a = TaskFlow(id: "A", dependencies: [b, c]) { log.append("A") }
    
    try await pool.flow(a)
    
    #expect(log.count == 5)
    #expect(Set(log) == ["A", "B", "C", "D", "E"])
    #expect(Set(log.prefix(3)) == ["C", "D", "E"])
    #expect(log[3] == "B")
    #expect(log[4] == "A")
}

@Test func diamondDependencyRunsSharedNodeOnce() async throws {
    let pool = TaskFlowPool()
    var log: [String] = []
    let d = TaskFlow(id: "D") { log.append("D") }
    let b = TaskFlow(id: "B", dependencies: [d]) { log.append("B") }
    let c = TaskFlow(id: "C", dependencies: [d]) { log.append("C") }
    let a = TaskFlow(id: "A", dependencies: [b, c]) { log.append("A") }
    
    try await pool.flow(a)
    
    #expect(log.filter { $0 == "D" }.count == 1)
    #expect(log.firstIndex(of: "D")! < log.firstIndex(of: "B")!)
    #expect(log.firstIndex(of: "D")! < log.firstIndex(of: "C")!)
    #expect(log.firstIndex(of: "B")! < log.firstIndex(of: "A")!)
    #expect(log.firstIndex(of: "C")! < log.firstIndex(of: "A")!)
}

@Test func sharedDependencyRunsOnce() async throws {
    let pool = TaskFlowPool()
    var dRuns = 0
    let d = TaskFlow(id: "D") { dRuns += 1 }
    let a1 = TaskFlow(id: "A1", dependencies: [d]) {}
    let a2 = TaskFlow(id: "A2", dependencies: [d]) {}
    
    try await pool.flow(a1)
    try await pool.flow(a2)
    
    #expect(dRuns == 1)
}

@Test func concurrentFlowsShareDependencyOnce() async throws {
    let pool = TaskFlowPool()
    var dRuns = 0
    let d = TaskFlow(id: "D") { dRuns += 1 }
    let a1 = TaskFlow(id: "A1", dependencies: [d]) {}
    let a2 = TaskFlow(id: "A2", dependencies: [d]) {}
    
    async let f1: Void = pool.flow(a1)
    async let f2: Void = pool.flow(a2)
    _ = try await (f1, f2)
    
    #expect(dRuns == 1)
}

@Test func canceledRootThrows() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.state = .canceled
    
    await #expect(throws: TaskFlowError.self) {
        try await pool.flow(a)
    }
}

@Test func dependenciesArePooledEvenWhenCanceled() async throws {
    let pool = TaskFlowPool()
    var dRuns = 0
    let d = TaskFlow(id: "D") { dRuns += 1 }
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    a.state = .canceled
    
    await #expect(throws: TaskFlowError.self) {
        try await pool.flow(a)
    }
    
    #expect(d.sinkCount == 1)
    await pool.clear(a)
    #expect(dRuns == 0)
}

@Test func sinkCountBalancesAfterClear() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    
    try await pool.flow(a)
    
    #expect(a.sinkCount == 1)
    #expect(d.sinkCount == 1)
    
    await pool.clear(a)
    
    #expect(a.sinkCount == 0)
    #expect(d.sinkCount == 0)
}

@Test func clearTerminatesOnCyclicGraph() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    let b = TaskFlow(id: "B", dependencies: [a]) {}
    a.dependencies.append(b)
    await pool.register(a)
    await pool.register(b)

    await pool.clear(a)

    #expect(a.sinkCount == 0)
    #expect(b.sinkCount == 0)
}

@Test func clearSharedDependencyDoesNotBreakOthers() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    let a1 = TaskFlow(id: "A1", dependencies: [d]) {}
    let a2 = TaskFlow(id: "A2", dependencies: [d]) {}

    try await pool.flow(a1)
    try await pool.flow(a2)
    #expect(d.sinkCount == 2)

    await pool.clear(a1)
    #expect(d.sinkCount == 1)
    var dCanceled = false
    if case .canceled = d.state {
        dCanceled = true
    }
    #expect(!dCanceled)

    let a3 = TaskFlow(id: "A3", dependencies: [d]) {}
    try await pool.flow(a3)
    var a3Done = false
    if case .done = a3.state {
        a3Done = true
    }
    #expect(a3Done)
}

@Test func failedDependencyIsRetried() async throws {
    let pool = TaskFlowPool()
    var log: [String] = []
    let d = TaskFlow(id: "D") { log.append("D") }
    d.state = .error(error: nil)
    d.retryLimit = 1
    let a = TaskFlow(id: "A", dependencies: [d]) { log.append("A") }
    
    try await pool.flow(a)
    
    #expect(log == ["D", "A"])
    #expect(d.retryCount == 1)
}

@Test func expiredDependencyIsRerun() async throws {
    let pool = TaskFlowPool()
    var log: [String] = []
    let d = TaskFlow(id: "D") { log.append("D") }
    d.expiresAfter = 60
    d.state = .done(timestamp: Date().timeIntervalSince1970 - 120)
    let a = TaskFlow(id: "A", dependencies: [d]) { log.append("A") }
    
    try await pool.flow(a)
    
    #expect(log == ["D", "A"])
}

@Test func freshDependencyIsSkipped() async throws {
    let pool = TaskFlowPool()
    var log: [String] = []
    let d = TaskFlow(id: "D") { log.append("D") }
    d.expiresAfter = 60
    d.state = .done(timestamp: Date().timeIntervalSince1970)
    let a = TaskFlow(id: "A", dependencies: [d]) { log.append("A") }
    
    try await pool.flow(a)
    
    #expect(log == ["A"])
}

@Test func flowingDependencyWaits() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    d.state = .flowing
    
    let flow = Task { try await pool.flow(a) }
    try await Task.sleep(for: .milliseconds(100))
    d.state = .done(timestamp: Date().timeIntervalSince1970)
    try await flow.value
    
    var done = false
    if case .done = a.state {
        done = true
    }
    #expect(done)
}

@Test func waitingDoesNotBlockActor() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    d.state = .flowing
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    let x = TaskFlow(id: "X") {}
    
    let waiting = Task { try await pool.flow(a) }
    try await Task.sleep(for: .milliseconds(100))
    
    try await pool.flow(x)
    
    var xDone = false
    if case .done = x.state {
        xDone = true
    }
    #expect(xDone)
    
    d.state = .done(timestamp: Date().timeIntervalSince1970)
    try await waiting.value
}

@Test func asyncHandlerCompletionWaits() async throws {
    let pool = TaskFlowPool()
    let log = TestLog()
    let d = TaskFlow(id: "D") { completion in
        log.add("D-start")
        Task {
            try? await Task.sleep(for: .milliseconds(50))
            log.add("D-end")
            completion(nil)
        }
    }
    let a = TaskFlow(id: "A", dependencies: [d]) { completion in
        log.add("A")
        completion(nil)
    }
    
    try await pool.flow(a)
    
    #expect(log.all == ["D-start", "D-end", "A"])
}

@Test func asyncHandlerErrorRetries() async throws {
    let pool = TaskFlowPool()
    let log = TestLog()
    let d = TaskFlow(id: "D") { completion in
        log.add("run\(log.all.count + 1)")
        if log.all.count == 1 {
            completion(TestError.some)
        } else {
            completion(nil)
        }
    }
    d.retryLimit = 1
    let a = TaskFlow(id: "A", dependencies: [d]) { completion in
        log.add("A")
        completion(nil)
    }
    
    try await pool.flow(a)
    
    #expect(log.all == ["run1", "run2", "A"])
    #expect(d.retryCount == 1)
}

@Test func noRetryByDefault() async throws {
    let pool = TaskFlowPool()
    var runs = 0
    let d = TaskFlow(id: "D") { completion in
        runs += 1
        completion(TestError.some)
    }
    let a = TaskFlow(id: "A", dependencies: [d]) { completion in
        completion(nil)
    }
    
    await #expect(throws: TestError.self) {
        try await pool.flow(a)
    }
    
    #expect(runs == 1)
    #expect(d.retryCount == 0)
}

@Test func retryLimitExhaustedThrows() async throws {
    let pool = TaskFlowPool()
    let log = TestLog()
    let d = TaskFlow(id: "D") { completion in
        log.add("run\(log.all.count + 1)")
        completion(TestError.some)
    }
    d.retryLimit = 2
    let a = TaskFlow(id: "A", dependencies: [d]) { completion in
        completion(nil)
    }
    
    await #expect(throws: TestError.self) {
        try await pool.flow(a)
    }
    
    #expect(log.all == ["run1", "run2", "run3"])
    #expect(d.retryCount == 2)
}

@Test func perTaskExecutionTimeoutRetries() async throws {
    let pool = TaskFlowPool()
    let log = TestLog()
    var attempts = 0
    let d = TaskFlow(id: "D") { completion in
        attempts += 1
        log.add("invoke\(attempts)")
        if attempts == 1 {
            Task {
                try? await Task.sleep(for: .milliseconds(200))
                completion(nil)
            }
        } else {
            completion(nil)
        }
    }
    d.executionTimeout = 0.05
    d.retryLimit = 1
    let a = TaskFlow(id: "A", dependencies: [d]) { completion in
        completion(nil)
    }
    
    try await pool.flow(a)
    
    #expect(log.all == ["invoke1", "invoke2"])
    #expect(d.retryCount == 1)
    var done = false
    if case .done = d.state {
        done = true
    }
    #expect(done)
}

@Test func perTaskTimeoutDoesNotFireWhenFast() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") { completion in
        Task {
            try? await Task.sleep(for: .milliseconds(10))
            completion(nil)
        }
    }
    d.executionTimeout = 0.2
    let a = TaskFlow(id: "A", dependencies: [d]) { completion in
        completion(nil)
    }
    
    try await pool.flow(a)
    
    var done = false
    if case .done = d.state {
        done = true
    }
    #expect(done)
    #expect(d.retryCount == 0)
}

@Test func wholeFlowTimeoutThrows() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") { _ in }
    
    await #expect(throws: TaskFlowError.timedOut) {
        try await pool.flow(a, timeout: 0.05)
    }
}

@Test func wholeFlowTimeoutMarksNodesCanceled() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") { _ in }
    
    await #expect(throws: TaskFlowError.timedOut) {
        try await pool.flow(a, timeout: 0.05)
    }
    
    var canceled = false
    if case .canceled = a.state {
        canceled = true
    }
    #expect(canceled)
    
    await #expect(throws: TaskFlowError.canceled) {
        try await pool.flow(a)
    }
}

@Test func wholeFlowTimeoutAllowsCompletion() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") { completion in
        Task {
            try? await Task.sleep(for: .milliseconds(30))
            completion(nil)
        }
    }
    
    try await pool.flow(a, timeout: 0.2)
    
    var done = false
    if case .done = a.state {
        done = true
    }
    #expect(done)
}

// MARK: - Clear protection

@Test func cancelKeepsTaskByDefault() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    await pool.register(a)
    #expect(a.sinkCount == 1)
    
    await pool.cancel(a)
    
    #expect(a.pool != nil)
    var canceled = false
    if case .canceled = a.state {
        canceled = true
    }
    #expect(canceled)
}

@Test func cancelClearsWhenRequested() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    await pool.register(a)
    #expect(a.sinkCount == 1)
    
    await pool.cancel(a, clear: true)
    
    #expect(a.pool == nil)
    var canceled = false
    if case .canceled = a.state {
        canceled = true
    }
    #expect(canceled)
}

@Test func cancelClearReleasesDependencies() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    try await pool.flow(a)
    #expect(d.sinkCount == 1)
    
    await pool.cancel(a, clear: true)
    
    #expect(d.pool == nil)
}

@Test func flowingTaskIsNotCleared() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.state = .flowing
    await pool.register(a)
    #expect(a.sinkCount == 1)
    
    await pool.clear(a)
    
    #expect(a.sinkCount == 0)
    #expect(a.pool != nil)
    var canceled = false
    if case .canceled = a.state {
        canceled = true
    }
    #expect(!canceled)
}

@Test func failedTaskIsClearedEvenWhenProtected() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.state = .error(error: TestError.some)
    a.isClearProtected = true
    await pool.register(a)
    #expect(a.sinkCount == 1)
    
    await pool.clear(a)
    
    #expect(a.sinkCount == 0)
    #expect(a.pool == nil)
    var error = false
    if case .error = a.state {
        error = true
    }
    #expect(error)
}

@Test func readyTaskIsCleared() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    await pool.register(a)
    #expect(a.sinkCount == 1)
    
    await pool.clear(a)
    
    #expect(a.sinkCount == 0)
    #expect(a.pool == nil)
}

@Test func canceledTaskIsCleared() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.state = .canceled
    await pool.register(a)
    
    await pool.clear(a)
    
    #expect(a.pool == nil)
}

@Test func doneWithoutExpiryIsCleared() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.state = .done(timestamp: Date().timeIntervalSince1970)
    await pool.register(a)
    
    await pool.clear(a)
    
    #expect(a.pool == nil)
}

@Test func freshDoneTaskIsRetainedOnClear() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.expiresAfter = 60
    a.state = .done(timestamp: Date().timeIntervalSince1970)
    await pool.register(a)
    
    await pool.clear(a)
    
    #expect(a.sinkCount == 0)
    #expect(a.pool != nil)
    var canceled = false
    if case .canceled = a.state {
        canceled = true
    }
    #expect(!canceled)
}

@Test func expiredDoneTaskIsCleared() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.expiresAfter = 60
    a.state = .done(timestamp: Date().timeIntervalSince1970 - 120)
    await pool.register(a)
    
    await pool.clear(a)
    
    #expect(a.pool == nil)
    var done = false
    if case .done = a.state {
        done = true
    }
    #expect(done)
}

@Test func protectedDoneTaskRetainedEvenAfterExpiry() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.expiresAfter = 60
    a.isClearProtected = true
    a.state = .done(timestamp: Date().timeIntervalSince1970 - 120)
    await pool.register(a)
    
    await pool.clear(a)
    #expect(a.sinkCount == 0)
    #expect(a.pool != nil)
    
    await pool.clear(a, force: true)
    #expect(a.pool == nil)
}

@Test func retainedNodeIsForceRemovableWithoutReRegistering() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    d.expiresAfter = 60
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    try await pool.flow(a)
    d.state = .done(timestamp: Date().timeIntervalSince1970)
    
    await pool.clear(a)
    #expect(d.sinkCount == 0)
    #expect(d.pool != nil)
    
    await pool.clear(d, force: true)
    #expect(d.pool == nil)
}

@Test func clearRetainedNodeStillBalancesDependencies() async throws {
    let pool = TaskFlowPool()
    let c = TaskFlow(id: "C") {}
    let b = TaskFlow(id: "B", dependencies: [c]) {}
    let a = TaskFlow(id: "A", dependencies: [b]) {}
    try await pool.flow(a)
    b.expiresAfter = 60
    
    await pool.clear(a)
    
    #expect(b.pool != nil)
    #expect(b.sinkCount == 0)
    #expect(c.pool == nil)
    #expect(c.sinkCount == 0)
}

@Test func reOwnedRetainedNodeBalancesExactly() async throws {
    let pool = TaskFlowPool()
    let c = TaskFlow(id: "C") {}
    let b = TaskFlow(id: "B", dependencies: [c]) {}
    b.expiresAfter = 60
    let a1 = TaskFlow(id: "A1", dependencies: [b]) {}
    try await pool.flow(a1)
    
    await pool.clear(a1)
    #expect(b.sinkCount == 0)
    #expect(c.sinkCount == 0)
    #expect(c.pool == nil)
    
    let a2 = TaskFlow(id: "A2", dependencies: [b]) {}
    try await pool.flow(a2)
    #expect(b.sinkCount == 1)
    #expect(c.sinkCount == 1)
    
    await pool.clear(a2)
    #expect(b.sinkCount == 0)
    #expect(c.sinkCount == 0)
    #expect(b.pool != nil)
    #expect(c.pool == nil)
}

@Test func retainedFreshResultIsReused() async throws {
    let pool = TaskFlowPool()
    var runs = 0
    let d = TaskFlow(id: "D") { runs += 1 }
    d.expiresAfter = 60
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    
    try await pool.flow(a)
    #expect(runs == 1)
    #expect(d.sinkCount == 1)
    
    await pool.clear(a)
    #expect(d.sinkCount == 0)
    #expect(d.pool != nil)
    
    let b = TaskFlow(id: "B", dependencies: [d]) {}
    try await pool.flow(b)
    #expect(runs == 1)
    var bDone = false
    if case .done = b.state {
        bDone = true
    }
    #expect(bDone)
}

// MARK: - Static cancel/clear by id

func isCanceled(_ task: TaskFlow) -> Bool {
    if case .canceled = task.state {
        return true
    }
    return false
}

@Test func cancelByIDsMarksTasksCanceled() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    let b = TaskFlow(id: "B") {}
    await pool.register(a)
    await pool.register(b)

    await pool.cancel(TaskFlowIDBatch(ids: ["A", "B"]))

    #expect(isCanceled(a))
    #expect(isCanceled(b))
    #expect(a.pool != nil)
    #expect(b.pool != nil)
}

@Test func cancelByIDsKeepsUnmatchedTasks() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    let b = TaskFlow(id: "B") {}
    await pool.register(a)
    await pool.register(b)

    await pool.cancel(TaskFlowIDBatch(ids: ["A"]))

    #expect(isCanceled(a))
    #expect(!isCanceled(b))
}

@Test func cancelByIDsIgnoresUnknownIDs() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    await pool.register(a)

    await pool.cancel(TaskFlowIDBatch(ids: ["NOPE"]))

    #expect(!isCanceled(a))
}

@Test func cancelByIDsWithClearReleasesDependencies() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    let a = TaskFlow(id: "A", dependencies: [d]) {}
    try await pool.flow(a)
    #expect(d.sinkCount == 1)

    await pool.cancel(TaskFlowIDBatch(ids: ["A"]), clear: true)

    #expect(a.pool == nil)
    #expect(d.pool == nil)
}

@Test func clearByIDsReleasesTasks() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    await pool.register(a)

    await pool.clear(TaskFlowIDBatch(ids: ["A"]))

    #expect(a.sinkCount == 0)
    #expect(a.pool == nil)
}

@Test func clearByIDsBalancesSharedDependencyOnce() async throws {
    let pool = TaskFlowPool()
    let d = TaskFlow(id: "D") {}
    let a1 = TaskFlow(id: "A1", dependencies: [d]) {}
    let a2 = TaskFlow(id: "A2", dependencies: [d]) {}
    try await pool.flow(a1)
    try await pool.flow(a2)
    #expect(d.sinkCount == 2)

    await pool.clear(TaskFlowIDBatch(ids: ["A1", "A2"]))

    #expect(a1.sinkCount == 0)
    #expect(a2.sinkCount == 0)
    #expect(d.sinkCount == 0)
    #expect(d.pool == nil)
}

@Test func clearByIDsRespectsProtectionUntilForce() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.expiresAfter = 60
    a.isClearProtected = true
    a.state = .done(timestamp: Date().timeIntervalSince1970)
    await pool.register(a)

    await pool.clear(TaskFlowIDBatch(ids: ["A"]))
    #expect(a.sinkCount == 0)
    #expect(a.pool != nil)

    await pool.clear(TaskFlowIDBatch(ids: ["A"]), force: true)
    #expect(a.pool == nil)
}

@Test func staticCancelDefaultsToMainPool() async throws {
    let a = TaskFlow(id: "static-cancel") {}
    await mainPool.register(a)

    TaskFlow.cancel(ids: ["static-cancel"])

    let deadline = Date().addingTimeInterval(1)
    while !isCanceled(a) && Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    #expect(isCanceled(a))
    #expect(a.pool != nil)

    await mainPool.clear(TaskFlowIDBatch(ids: ["static-cancel"]), force: true)
}

@Test func staticClearDefaultsToMainPool() async throws {
    let a = TaskFlow(id: "static-clear") {}
    await mainPool.register(a)

    TaskFlow.clear(ids: ["static-clear"])

    let deadline = Date().addingTimeInterval(1)
    while a.pool != nil && Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    #expect(a.pool == nil)
}

@Test func cancelByIDsIsNoOpOnCompletedTask() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.state = .done(timestamp: Date().timeIntervalSince1970)
    await pool.register(a)

    await pool.cancel(TaskFlowIDBatch(ids: ["A"]))

    var done = false
    if case .done = a.state {
        done = true
    }
    #expect(done)
    #expect(a.pool != nil)
}

@Test func clearByIDsKeepsFlowingTaskRetained() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.state = .flowing
    await pool.register(a)
    #expect(a.sinkCount == 1)

    await pool.clear(TaskFlowIDBatch(ids: ["A"]))

    #expect(a.sinkCount == 0)
    #expect(a.pool != nil)
    #expect(!isCanceled(a))
}

@Test func clearByIDsForceReleasesProtectedTask() async throws {
    let pool = TaskFlowPool()
    let a = TaskFlow(id: "A") {}
    a.expiresAfter = 60
    a.isClearProtected = true
    a.state = .done(timestamp: Date().timeIntervalSince1970)
    await pool.register(a)

    await pool.clear(TaskFlowIDBatch(ids: ["A"]))
    #expect(a.pool != nil)

    await pool.clear(TaskFlowIDBatch(ids: ["A"]), force: true)
    #expect(a.pool == nil)
}

@Test func staticCancelBySingleID() async throws {
    let a = TaskFlow(id: "static-single-cancel") {}
    await mainPool.register(a)

    TaskFlow.cancel(id: "static-single-cancel")

    let deadline = Date().addingTimeInterval(1)
    while !isCanceled(a) && Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    #expect(isCanceled(a))

    await mainPool.clear(TaskFlowIDBatch(ids: ["static-single-cancel"]), force: true)
}

@Test func staticClearBySingleID() async throws {
    let a = TaskFlow(id: "static-single-clear") {}
    await mainPool.register(a)

    TaskFlow.clear(id: "static-single-clear")

    let deadline = Date().addingTimeInterval(1)
    while a.pool != nil && Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    #expect(a.pool == nil)
}

@Test func staticCancelCompletionIsInvoked() async throws {
    let a = TaskFlow(id: "static-completion-cancel") {}
    await mainPool.register(a)

    let finished = await withCheckedContinuation { continuation in
        TaskFlow.cancel(ids: ["static-completion-cancel"]) {
            continuation.resume()
        }
    }
    _ = finished
    #expect(isCanceled(a))

    await mainPool.clear(TaskFlowIDBatch(ids: ["static-completion-cancel"]), force: true)
}

@Test func staticClearCompletionIsInvoked() async throws {
    let a = TaskFlow(id: "static-completion-clear") {}
    await mainPool.register(a)

    await withCheckedContinuation { continuation in
        TaskFlow.clear(ids: ["static-completion-clear"]) {
            continuation.resume()
        }
    }
    #expect(a.pool == nil)
}
