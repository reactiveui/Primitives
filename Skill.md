---
name: reactiveui-primitives
description: Use when working with ReactiveUI.Primitives NuGet packages in .NET projects, including ReactiveUI.Disposables, ReactiveUI.Primitives.Core, ReactiveUI.Primitives, ReactiveUI.Primitives.Reactive, ReactiveUI.Primitives.Async.Core, ReactiveUI.Primitives.Async, ReactiveUI.Primitives.Async.Reactive, ReactiveUI.Primitives.Extensions.Core, ReactiveUI.Primitives.Extensions, ReactiveUI.Primitives.Extensions.Reactive, ReactiveUI.Primitives.Wpf, ReactiveUI.Primitives.WinForms, ReactiveUI.Primitives.WinUI, ReactiveUI.Primitives.Blazor, or ReactiveUI.Primitives.Maui; choosing Core vs lean vs System.Reactive package variants; using IObservable, IObservableAsync, signals, sequencers, disposable helpers, UI dispatch adapters, R3/R3Async generated bridges, or migration guidance from System.Reactive/R3.
---

# ReactiveUI.Primitives

Use this skill when adding or using ReactiveUI.Primitives packages. Prefer NuGet package guidance over repository project references unless the user is explicitly editing this repository.

## Package Chooser

Default to the non-Core leaf packages for applications. Choose Core packages only for library authors or advanced composition work that needs the shared type layer without the full leaf surface.

| Package | Use when | Key APIs and notes |
| --- | --- | --- |
| `ReactiveUI.Disposables` | The project only needs disposable lifetime helpers. | `ReactiveUI.Primitives.Disposables`; `Scope`, `MultipleDisposable`, `DisposableBag`, `Pocket`, `Slot`, `AssignmentSlot`, `SingleDisposable`, `SingleReplaceableDisposable`, `MutableDisposable`, `SwapDisposable`, `OnceDisposable`, `BooleanDisposable`, `CancellationDisposable`, `EmptyDisposable`. |
| `ReactiveUI.Primitives.Core` | Building a low-level library that needs the shared Primitives type layer without the full leaf package. | Root namespace remains `ReactiveUI.Primitives`. Includes core signal/state contracts and shared types such as `Result`, `Optional<T>`, `ISignal<T>`, `Signal<T>`, `BehaviorSignal<T>`, `StateSignal<T>`, `ReadOnlyState<T>`, `ConnectableSignal<T>`, `CommandSignal`, concurrency contracts, and advanced witnesses. Depends on `ReactiveUI.Disposables`. |
| `ReactiveUI.Primitives` | Most BCL `IObservable<T>` usage. | Lean package using Primitives `RxVoid` and `ISequencer`. Includes core package, shared signal factories/operators, `CurrentThreadSequencer`, `ImmediateSequencer`, `SynchronizationContextSequencer`, `TaskPoolSequencer`, `ThreadPoolSequencer`, virtual time, `ReplaySignal<T>`, `ScheduledSignal<T>`, `PrioritySemaphoreSignal<T>`, `LinqExtensions`, `SignalExtensions`, and R3 bridge analyzer packaging. |
| `ReactiveUI.Primitives.Reactive` | The project is System.Reactive-first and wants Primitives operators compiled with `System.Reactive.Unit` and `System.Reactive.Concurrency.IScheduler`. | Uses `.Reactive` namespaces such as `ReactiveUI.Primitives.Reactive`, `.Reactive.Concurrency`, `.Reactive.Signals`, and `.Reactive.Core`. Adds `System.Reactive`. This is a package variant, not a source-generator bridge. |
| `ReactiveUI.Primitives.Async.Core` | Building a low-level async library around Primitives async contracts. | `ReactiveUI.Primitives.Async`; `IObservableAsync<T>`, `IObserverAsync<T>`, `SignalAsync<T>`, `SignalAsync`, async signal factories, async operators, async disposables, helpers, and async signal implementations. Depends on `ReactiveUI.Primitives.Core`. |
| `ReactiveUI.Primitives.Async` | The app needs async-native observable pipelines where observer calls can await or observe cancellation. | Lean async leaf using Primitives `RxVoid` and `ISequencer`. Adds `AsyncContext`, `ContextSwitchSignalAsync<T>`, `SignalAsyncReactiveExtensions`, `Yield`, `WitnessOn`, `ObserveOnSafe`, `ObserveOnIf`, and R3/R3Async bridge analyzer packaging. Depends on `ReactiveUI.Primitives` and `ReactiveUI.Primitives.Async.Core`. |
| `ReactiveUI.Primitives.Async.Reactive` | Async-native pipelines in a System.Reactive-first project. | System.Reactive-flavoured async leaf with `System.Reactive.Unit` and `IScheduler` conventions. Depends on `ReactiveUI.Primitives.Reactive` and `ReactiveUI.Primitives.Async.Core`. |
| `ReactiveUI.Primitives.Extensions.Core` | Building a library that needs shared extension implementations without choosing lean or System.Reactive scheduler/unit conventions. | Root namespace remains `ReactiveUI.Primitives.Extensions`. Includes type-agnostic extension support such as `CurrentValueSubject<T>`, `Continuation`, `ConcurrencyLimiter<T>`, `Heartbeat<T>`, `Stale<T>`, `IHeartbeat<T>`, `IStale<T>`, and shared operator implementation types. |
| `ReactiveUI.Primitives.Extensions` | The app needs convenience operators over lean BCL `IObservable<T>` pipelines. | `ReactiveUI.Primitives.Extensions`; `ReactiveExtensions`, `ObservableSubscriptionExtensions`, buffering, debounce/throttle, stale detection, retry/backoff, heartbeat, observe-on helpers, pairwise/partition, `ToHotTask`, `ToHotValueTask`, `SubscribeAsync`, `WaitUntil`, `AsSignal`. Depends on `ReactiveUI.Primitives` and `ReactiveUI.Primitives.Extensions.Core`. |
| `ReactiveUI.Primitives.Extensions.Reactive` | The app needs the Extensions surface in System.Reactive-first code. | Same extension family as `ReactiveUI.Primitives.Extensions`, recompiled under `.Reactive` namespaces and `System.Reactive` scheduler/unit conventions. Depends on `ReactiveUI.Primitives.Reactive` and `ReactiveUI.Primitives.Extensions.Core`. |
| `ReactiveUI.Primitives.Wpf` | WPF UI code needs dispatcher marshalling. | `ReactiveUI.Primitives.Concurrency.DispatcherSequencer`. Depends on `ReactiveUI.Primitives`. |
| `ReactiveUI.Primitives.WinForms` | Windows Forms UI code needs control-thread marshalling. | `ReactiveUI.Primitives.Concurrency.ControlSequencer`. Depends on `ReactiveUI.Primitives`. |
| `ReactiveUI.Primitives.WinUI` | WinUI code needs `DispatcherQueue` marshalling. | `ReactiveUI.Primitives.Concurrency.DispatcherQueueSequencer` and `DispatcherQueueSequencerExtensions.ToSequencer()`. Depends on `ReactiveUI.Primitives` and `Microsoft.WindowsAppSDK`. |
| `ReactiveUI.Primitives.Blazor` | Blazor components need render-thread sequencing and component-bound subscriptions. | `ReactiveUI.Primitives.Blazor.Components.ReactiveComponentBase`, `Observe`, `Track`, `InvalidateAsync`, `ReactiveUI.Primitives.Blazor.Concurrency.BlazorRendererSequencer`. Depends on `ReactiveUI.Primitives` and `Microsoft.AspNetCore.Components`. |
| `ReactiveUI.Primitives.Maui` | .NET MAUI code needs dispatcher marshalling. | `ReactiveUI.Primitives.Concurrency.MauiDispatcherSequencer` and `MauiDispatcherSequencerExtensions.ToSequencer()`. Depends on `ReactiveUI.Primitives` and `Microsoft.Maui.Core`. |

Install examples:

```bash
dotnet add package ReactiveUI.Primitives
dotnet add package ReactiveUI.Primitives.Async
dotnet add package ReactiveUI.Primitives.Extensions
dotnet add package ReactiveUI.Primitives.Wpf
```

Use `.Core` packages deliberately:

```bash
dotnet add package ReactiveUI.Primitives.Core
dotnet add package ReactiveUI.Primitives.Async.Core
dotnet add package ReactiveUI.Primitives.Extensions.Core
```

Use `.Reactive` packages when the project already uses System.Reactive idioms:

```bash
dotnet add package ReactiveUI.Primitives.Reactive
dotnet add package ReactiveUI.Primitives.Async.Reactive
dotnet add package ReactiveUI.Primitives.Extensions.Reactive
```

Do not add `ReactiveUI.Primitives.R3Bridge.Generator` as a package. It is a non-packable Roslyn component whose output is packed as an analyzer by `ReactiveUI.Primitives` and `ReactiveUI.Primitives.Async`.

## Selection Rules

- Prefer `ReactiveUI.Primitives` for new app code that uses BCL `IObservable<T>`.
- Prefer `ReactiveUI.Primitives.Async` when subscription, notification, or completion work must be asynchronous.
- Prefer `ReactiveUI.Primitives.Extensions` for migrated helper operators from ReactiveUI.Extensions-style code.
- Prefer UI packages only in the matching UI framework.
- Prefer `.Reactive` variants only when public APIs should expose `System.Reactive.Unit`, `IScheduler`, or `.Reactive` namespaces.
- Prefer `.Core` variants only when composing packages or minimizing a library dependency layer. Most apps should reference the leaf package.
- Avoid mixing lean and `.Reactive` variants in the same pipeline without an explicit boundary; their namespaces and scheduler/unit conventions differ.
- Do not add `System.Reactive` just to use core Primitives. The `.Reactive` packages add it for System.Reactive-first projects.
- Do not expect a System.Reactive bridge generator. System.Reactive support is provided by the explicit `.Reactive` package variants.

## Common Imports

Lean synchronous packages:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
```

Async packages:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;
```

Extensions packages:

```csharp
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Operators;
```

System.Reactive-flavoured packages:

```csharp
using ReactiveUI.Primitives.Reactive;
using ReactiveUI.Primitives.Reactive.Concurrency;
using ReactiveUI.Primitives.Reactive.Signals;
using ReactiveUI.Primitives.Extensions.Reactive;
```

R3 generated bridges:

```csharp
using ReactiveUI.Primitives.R3Bridge;
```

## API Landmarks

Use these landmarks to choose APIs quickly; rely on IntelliSense/PublicAPI for exact overloads.

- Signals and factories: `Signal`, `Signal<T>`, `BehaviorSignal<T>`, `StateSignal<T>`, `ReplaySignal<T>`, `ScheduledSignal<T>`, `PrioritySemaphoreSignal<T>`, `AnonymousSignal<T>`, `ConnectableSignal<T>`.
- State and commands: `ReadOnlyState<T>`, `CommandSignal<TResult>`, `CommandExecution<TResult>`, `TaskSignal`, `TaskSignal<T>`.
- Sequencing: `ISequencer`, `CurrentThreadSequencer`, `ImmediateSequencer`, `SynchronizationContextSequencer`, `TaskPoolSequencer`, `ThreadPoolSequencer`, `VirtualTimeSequencer<TAbsolute, TRelative>`, `VirtualClock`.
- Operators: `Map`, `FlatMap`, `SelectMany`, `Where`, `Keep`, `KeepNotNull`, `Cast`, `Concat`, `Merge`, `Amb`, `Race`, `Switch`, `Retry`, `Recover`, `Rescue`, `Distinct`, `Take`, `Skip`, `Collect`, `Materialize`, `Dematerialize`, `ForkJoin`, `Pair`, `Latch`, `Synchronize`, `Timeout`, `Shift`, `Spark`, `Unspark`.
- Connectable helpers: `Replay`, `ReplayLive`, `Share`, `ShareLatest`, `AutoConnect`, `AutoShare`.
- Async core: `IObservableAsync<T>`, `IObserverAsync<T>`, `SignalAsync<T>`, `SignalAsync`, `WitnessAsync<T>`, `ConnectableSignalAsync<T>`, `ConcurrentWitnessCallsException`.
- Async factories/operators: `SignalAsync.Create`, `Return`, `Range`, `FromAsync`, `FromAsyncEnumerable`, `Every`, `Interval`, `Timer`, `Using`, `Blend`, `Chain`, `Map`, `Merge`, `CombineLatest`, `Switch`, `ForEachAsync`, `Collect*Async`, `WaitCompletionAsync`, `OnErrorResumeAsFailure`.
- Extension helpers: `AsSignal`, `BufferUntil`, `BufferUntilIdle`, `DebounceImmediate`, `DebounceUntil`, `DetectStale`, `Heartbeat`, `ObserveOnSafe`, `ObserveOnIf`, `Pairwise`, `Partition`, `ReplayLastOnSubscribe`, `RetryWithBackoff`, `RetryWithDelay`, `RetryForeverWithDelay`, `RunAll`, `Shuffle`, `SwitchIfEmpty`, `Throttle*`, `ToHotTask`, `ToHotValueTask`, `WaitUntil`.

## R3 And R3Async Bridges

R3 bridges are generated into the consuming assembly only when the consumer already references the required R3 symbols. The generated namespace is `ReactiveUI.Primitives.R3Bridge`.

When `R3.Observable<T>` is visible, `ReactiveUI.Primitives` emits:

- `AsPrimitivesSignal<T>(this R3.Observable<T>)`
- `AsR3Observable<T>(this System.IObservable<T>)`

When both `R3.Observable<T>` and `ReactiveUI.Primitives.Async.IObservableAsync<T>` are visible, `ReactiveUI.Primitives.Async` emits:

- `AsPrimitivesAsyncObservable<T>(this R3.Observable<T>)`
- `AsR3Observable<T>(this IObservableAsync<T>)`

When `R3Async.AsyncObservable<T>`, `R3Async.AsyncObserver<T>`, `R3Async.Result`, and `IObservableAsync<T>` are visible, the async bridge generator emits:

- `AsPrimitivesAsyncObservable<T>(this R3Async.AsyncObservable<T>)`
- `AsR3AsyncObservable<T>(this IObservableAsync<T>)`

Use bridge methods only at boundaries. Keep internal pipelines in one model after conversion.

## Framework And Platform Notes

- General libraries target `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`.
- `ReactiveUI.Primitives` also builds Android TFMs and Apple TFMs for platform sequencers in the base package.
- WPF and WinForms packages target Windows TFMs plus .NET Framework.
- WinUI targets `net*-windows10.0.19041.0`.
- MAUI targets `net9.0`, `net10.0`, and `net11.0`.
- Blazor targets the modern .NET TFMs.

## Repository Maintenance

When editing this repository:

- Use `src/ReactiveUI.Primitives.slnx` as the solution entrypoint.
- Build and test from `src`.
- Tests use Microsoft.Testing.Platform with TUnit; write TUnit tests and TUnit assertions only.
- Shipping public API baselines live under each package's `PublicAPI/<tfm>/PublicAPI.Shipped.txt` and `PublicAPI.Unshipped.txt`.
- `Skill.md` is the canonical skill file. `ReactiveUI.Primitives.csproj` packs it both at package root as `Skill.md` and at `.agents/skills/reactiveui-primitives/SKILL.md`.
- Keep package guidance synchronized with packable projects in the solution. Tests, benchmarks, and `ReactiveUI.Primitives.R3Bridge.Generator` are not NuGet packages to add directly.
