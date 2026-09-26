# TaskFlow

A lightweight, Swift-native task orchestration library that models your work as a **dependency graph**. Each task runs only after its dependencies complete, shared dependencies run exactly once, and independent tasks execute in parallel.

Built on Swift Concurrency (actors, structured concurrency).

## Features

- **Dependency-based execution** — express work as a DAG and let TaskFlow figure out the order.
- **Layered parallelism** — all ready tasks in a layer run concurrently.
- **Shared-dependency de-duplication** — a node shared by many tasks runs once and its result is reused.
- **Early cycle detection** — circular dependencies throw `TaskFlowError.circularDependency` before anything runs.
- **Retries** — configure a retry limit for failing tasks.
- **Per-task execution timeout** — abort a stuck task and treat it as failed.
- **Whole-flow timeout** — bound the entire flow; timed-out nodes are canceled so later flows fail fast instead of hanging.
- **Result expiration** — cached results are re-run after `expiresAfter`.
- **Cancellation** — cancel a single task or an entire reachable graph.
- **Cleanup** — `clear()` releases a task and its dependencies from the pool, keeping executing tasks and still-fresh cached results (unless `force: true`).
- **Async handlers** — `TaskFlow(operation:)` wraps an `async throws` body as the task.
- **Awaitable cleanup** — `cancelAndWait()`/`clearAndWait()` (and static `cancelAndWait(ids:)`/`clearAndWait(ids:)`) wait until the pool has processed the request.
- **Shared default pool** — `TaskFlowPool.shared` backs `flow()` when no pool is passed.

## Requirements

- Swift 6.0+
- iOS 16+ / macOS 13+

## Installation

### Swift Package Manager

Add TaskFlow as a dependency in your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/chdaliu/task-flow", from: "0.1.0"),
]
```

Or add it through Xcode: **File → Add Package Dependencies…**

### CocoaPods

Add TaskFlow to your `Podfile`:

```ruby
pod 'TaskFlow'
```

Then run `pod install`.

The package source lives in the [`Sources/TaskFlow/`](../Sources/TaskFlow/) directory.

## Usage

### 1. A simple task

```swift
import TaskFlow

let greet = TaskFlow(id: "greet") { completion in
    print("Hello, world!")
    completion(nil)
}

greet.flow { error in
    // error is nil on success
}
```

`flow(on:timeout:completion:)` runs the task and its dependencies on a pool. The
pool argument defaults to the process-wide default pool, so a bare `greet.flow { }`
works out of the box. You can also create your own `TaskFlowPool` to isolate
groups of flows:

```swift
let pool = TaskFlowPool()
greet.flow(on: pool) { error in
    // runs on `pool` instead of the shared main pool
}
```

An `async` form is also available and throws the flow's error on failure:

```swift
try await greet.flow()
let pool = TaskFlowPool()
try await greet.flow(on: pool, timeout: 10)
```

### 2. Task dependencies

```swift
let fetchUser = TaskFlow(id: "fetch-user") { completion in
    /* ... */
    completion(nil)
}
let fetchPosts = TaskFlow(id: "fetch-posts") { completion in
    /* ... */
    completion(nil)
}
let render = TaskFlow(id: "render", dependencies: [fetchUser, fetchPosts]) { completion in
    // runs only after both fetches complete
    completion(nil)
}
```

Tasks in the same layer run concurrently; dependencies always complete first.

### 3. Asynchronous handlers

```swift
let download = TaskFlow(id: "download") { completion in
    URLSession.shared.dataTask(with: url) { data, _, error in
        completion(error)
    }.resume()
}
```

You can also write the body directly as an `async` operation. It runs off the
pool's actor, and thrown errors participate in retries like any other failure:

```swift
let download = TaskFlow(id: "download") {
    let (data, _) = try await URLSession.shared.data(from: url)
    // process data
}
```

> **Synchronous task bodies run on the pool's actor.** Keep them short — a
> long-running or blocking synchronous body (the `{ ... }` convenience form) would
> stall every flow sharing that pool. For real work, use the completion-based form
> above (optionally with `executionTimeout`) or the `async` operation form.

### 4. Retry and timeout

```swift
let fragile = TaskFlow(id: "fragile") { completion in
    completion(SomeError())
}
fragile.retryLimit = 3          // retry up to 3 times
fragile.executionTimeout = 5    // abort a single run after 5 seconds
```

The retry budget belongs to a run chain: every fresh run (including an expired
re-run in a later flow) gets `retryLimit` retries again.

### 5. Whole-flow timeout

```swift
greet.flow(timeout: 10) { error in
    // error == .timedOut if the whole flow exceeded 10 seconds
}
```

### 6. Cached results and expiration

```swift
let config = TaskFlow(id: "config") { completion in
    /* ... */
    completion(nil)
}
config.expiresAfter = 60 // cached for 1 minute, re-runs afterwards
```

### 7. Cancel and clear

```swift
task.cancel()            // cancel this task on its pool (keeps it in the pool)
task.cancel(clear: true) // cancel and release it (and its dependencies) from the pool
task.clear()             // release task + dependencies from the pool
task.clear(force: true)  // release even if the result is still protected

await task.cancelAndWait() // async forms that wait for the pool to finish
await task.clearAndWait(force: true)
```

Clearing is protection-aware:

- A task that is currently **executing** is never released while it runs. If you
  clear it while it runs, it is released as soon as its run reaches a terminal
  state (unless the result is protected).
- A **completed** task stays in the pool while its result is protected — either
  because `isClearProtected` is set, or because `expiresAfter` has not elapsed yet.
  Its sink count still drops to `0`, so the cached result can be reused by later flows.
- **Failed**, canceled, or not-yet-run tasks are always released.
- Pass `force: true` to bypass the protection and release a retained task.

```swift
let config = TaskFlow(id: "config") { completion in
    /* ... */
    completion(nil)
}
config.expiresAfter = 60          // cached for 1 minute
config.isClearProtected = true    // never released on clear() once done
```

You can also cancel or clear tasks by their registered `id` without holding a
reference. These static methods default to the process-wide default pool:

```swift
TaskFlow.cancel(ids: ["A", "B"])                     // cancel tasks "A" and "B" on the default pool
TaskFlow.cancel(ids: ["A"], on: pool, clear: true)   // cancel and release them on `pool`
TaskFlow.clear(ids: ["A", "B"])                      // release tasks + dependencies on the default pool
TaskFlow.clear(ids: ["A"], on: pool, force: true)    // release even if still protected

TaskFlow.cancel(id: "A")                            // single-id convenience
TaskFlow.clear(id: "B", on: pool)                   // single-id convenience
```

Each method is asynchronous and can report completion via a trailing closure:

```swift
TaskFlow.cancel(ids: ["A"]) {
    // the batch operation has finished
}
```

Or await the outcome directly with the `…AndWait` variants:

```swift
await TaskFlow.cancelAndWait(ids: ["A"], on: pool, clear: true)
await TaskFlow.clearAndWait(id: "B")
```

Unknown ids are ignored. As with the instance API, `cancel` only affects tasks
that have not yet finished: a task that already completed (`.done`) is left
untouched.

### 8. Circular dependencies

Cycles are detected before execution starts and reported with the cycle trace:

```swift
do {
    try await pool.flow(a)   // a -> b -> a
} catch TaskFlowError.circularDependency(let nodes) {
    print(nodes)             // ["A", "B", "A"]
}
```

## How it works

- `TaskFlowPool` is an `actor` that owns the shared task registry and all state transitions. `TaskFlowPool.shared` is the process-wide default pool.
- `flow(_:)` validates the graph (`layers`), registers every node, then executes it layer by layer.
- `layers` assigns each node the depth of its longest dependency chain and uses a DFS visiting-set to detect cycles.
- Every flow registers one ownership share per reachable node and releases exactly one share per node when cleared, so a shared dependency is torn down only when its last owner is gone. A failed or timed-out flow releases only its own shares and never disturbs nodes that other flows still own.

## License

TaskFlow is released under the [MIT license](../LICENSE).
