# TaskFlow

一个轻量级的 Swift 原生任务编排库，把业务工作建模为**依赖图**。每个任务只会在其所有依赖完成后执行，共享依赖只运行一次，互相独立的任务会并行执行。

基于 Swift 并发（`actor`、结构化并发）和 Combine 构建。

## 特性

- **基于依赖的执行** —— 把工作表达成 DAG，由 TaskFlow 自动确定执行顺序。
- **分层并行** —— 同一层中所有就绪的任务并发执行。
- **共享依赖去重** —— 被多个任务共享的节点只运行一次，结果可复用。
- **循环依赖尽早检测** —— 检测到环时会在真正执行前抛出 `TaskFlowError.circularDependency`。
- **重试** —— 为失败的任务配置重试次数上限。
- **单任务执行超时** —— 中止卡住的任务并标记为失败。
- **整个流程超时** —— 限制整个流程的时长；超时后的节点会被取消，避免后续 flow 卡死。
- **结果过期** —— 缓存结果可设置 `expiresAfter`，过期后重新执行。
- **取消** —— 取消单个任务或整个可达依赖图。
- **清理** —— `clear()` 把任务及其依赖从池中释放；执行中的任务与仍然新鲜的缓存结果会被保留（除非传 `force: true`）。

## 系统要求

- Swift 6.0+
- iOS 15+ / macOS 13+

## 安装

### Swift Package Manager

在 `Package.swift` 中添加依赖：

```swift
dependencies: [
    .package(url: "https://github.com/chdaliu/task-flow", from: "0.1.0"),
]
```

或通过 Xcode：**File → Add Package Dependencies…**

### CocoaPods

在 `Podfile` 中添加：

```ruby
pod 'TaskFlow'
```

然后运行 `pod install`。

包源码位于 [`TaskFlow/`](TaskFlow/) 目录（同级的 iOS / macOS 示例工程在仓库根目录下）。

## 使用示例

### 1. 简单任务

```swift
import TaskFlow

let greet = TaskFlow(id: "greet") { completion in
    print("Hello, world!")
    completion(nil)
}

greet.flow { error in
    // 成功时 error 为 nil
}
```

`flow(on:timeout:completion:)` 在某个 pool 上执行该任务及其依赖。`pool` 参数默认使用进程级的 `mainPool`，因此直接写 `greet.flow { }` 即可。你也可以自行创建 `TaskFlowPool` 来隔离不同的流程：

```swift
let pool = TaskFlowPool()
greet.flow(on: pool) { error in
    // 在 `pool` 上执行，而不是共享的 main pool
}
```

### 2. 任务依赖

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
    // 仅在两个请求都完成后执行
    completion(nil)
}
```

同一层中的任务并发执行，依赖总是先完成。

### 3. 异步 handler

```swift
let download = TaskFlow(id: "download") { completion in
    URLSession.shared.dataTask(with: url) { data, _, error in
        completion(error)
    }.resume()
}
```

### 4. 重试与超时

```swift
let fragile = TaskFlow(id: "fragile") { completion in
    completion(SomeError())
}
fragile.retryLimit = 3          // 最多重试 3 次
fragile.executionTimeout = 5    // 单次执行超过 5 秒视为失败
```

### 5. 整个流程超时

```swift
greet.flow(timeout: 10) { error in
    // 若整个流程超过 10 秒，error == .timedOut
}
```

### 6. 结果缓存与过期

```swift
let config = TaskFlow(id: "config") { completion in
    /* ... */
    completion(nil)
}
config.expiresAfter = 60 // 缓存 1 分钟，过期后重新执行
```

### 7. 取消与清理

```swift
task.cancel()            // 在当前池上取消该任务（保留在池中）
task.cancel(clear: true) // 取消并把它（及依赖）从池中释放
task.clear()             // 从池中释放任务及其依赖
task.clear(force: true)  // 即使结果仍受保护也强制释放
```

清理是“保护感知”的：

- **执行中**的任务不会被释放。
- **已完成**的任务在其结果受保护期间会留在池中——要么设置了 `isClearProtected`，要么 `expiresAfter` 尚未过期。此时 sink 计数仍会减到 `0`，缓存结果可被后续 flow 复用。
- **失败**、已取消或尚未开始的任务会直接释放。
- 传 `force: true` 可绕过保护，强制释放被保留的任务。

```swift
let config = TaskFlow(id: "config") { completion in
    /* ... */
    completion(nil)
}
config.expiresAfter = 60          // 缓存 1 分钟
config.isClearProtected = true    // 完成后 clear() 永不释放
```

### 8. 循环依赖

环会在执行开始前被检测到，并携带循环路径抛出：

```swift
do {
    try await pool.flow(a)   // a -> b -> a
} catch TaskFlowError.circularDependency(let nodes) {
    print(nodes)             // ["A", "B", "A"]
}
```

## 工作原理

- `TaskFlowPool` 是一个 `actor`，拥有共享任务注册表与全部状态流转。
- `flow(_:)` 先校验依赖图（`layers`），注册所有节点，再按层执行。
- `layers` 用 DFS 为每个节点计算最长依赖链深度，并用 visiting 集合检测环。
- 池按 `id` 去重任务，并用 `sinkCount` 记录引用数；共享依赖只有在最后一个 owner 清理后才会被真正移除。

## 许可证

TaskFlow 基于 [MIT 许可证](LICENSE) 开源。
