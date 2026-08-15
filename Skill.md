---
name: reactiveui-primitives
description: Use when working with ReactiveUI.Primitives NuGet packages in .NET projects, including ReactiveUI.Disposables, ReactiveUI.Primitives.Core, ReactiveUI.Primitives, ReactiveUI.Primitives.Reactive, ReactiveUI.Primitives.Async.Core, ReactiveUI.Primitives.Async, ReactiveUI.Primitives.Async.Reactive, ReactiveUI.Primitives.ObservableEvents, ReactiveUI.Primitives.R3Bridge.Generator, ReactiveUI.Primitives.Wpf, ReactiveUI.Primitives.Wpf.Reactive, ReactiveUI.Primitives.WinForms, ReactiveUI.Primitives.WinForms.Reactive, ReactiveUI.Primitives.WinUI, ReactiveUI.Primitives.WinUI.Reactive, ReactiveUI.Primitives.Blazor, ReactiveUI.Primitives.Blazor.Reactive, ReactiveUI.Primitives.Avalonia, ReactiveUI.Primitives.Avalonia.Reactive, ReactiveUI.Primitives.Maui, or ReactiveUI.Primitives.Maui.Reactive; choosing Core vs lean vs System.Reactive package variants; using IObservable, IObservableAsync, signals, extension helpers, sequencers, disposable helpers, UI dispatch adapters, generated observable events, R3/R3Async generated bridges, or migration guidance from System.Reactive/R3/R3Async repositories to Primitives or .Reactive package variants.
---

# ReactiveUI.Primitives

Use this skill when adding or using ReactiveUI.Primitives packages. Prefer NuGet package guidance over repository
project references unless the user is explicitly editing this repository.

## Package Chooser

Default to the non-Core leaf packages for applications. Choose Core packages only for library authors or advanced
composition work that needs the shared type layer without the full leaf surface.

| Package                                     | Use when                                                                                                                                               | Key APIs and notes                                                                                                                                                                                                                                                                                                                                                               |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `ReactiveUI.Disposables`                    | The project only needs disposable lifetime helpers.                                                                                                    | `ReactiveUI.Primitives.Disposables`; `Scope`, `MultipleDisposable`, `DisposableBag`, `Pocket`, `Slot`, `AssignmentSlot`, `SingleDisposable`, `SingleReplaceableDisposable`, `MutableDisposable`, `SwapDisposable`, `OnceDisposable`, `BooleanDisposable`, `CancellationDisposable`, `EmptyDisposable`.                                                                           |
| `ReactiveUI.Primitives.Core`                | Building a low-level library that needs the shared Primitives type layer without the full leaf package.                                                | Root namespace remains `ReactiveUI.Primitives`. Includes core signal/state contracts, concurrency contracts, advanced witnesses, and type-agnostic extension-helper support under `ReactiveUI.Primitives.Extensions`. Depends on `ReactiveUI.Disposables`.                                                                                                                        |
| `ReactiveUI.Primitives`                     | Most BCL `IObservable<T>` usage, including migrated `ReactiveUI.Extensions` helpers.                                                                   | Lean package using Primitives `RxVoid` and `ISequencer`. Includes signal factories/operators, sequencers, virtual time, and extension helpers under `ReactiveUI.Primitives.Extensions`. Depends on `ReactiveUI.Primitives.Core` and `ReactiveUI.Disposables`; no separate Extensions package is required.                                                                           |
| `ReactiveUI.Primitives.Reactive`            | The project is System.Reactive-first and wants Primitives operators and extension helpers compiled with `System.Reactive.Unit` and `IScheduler`.       | Uses `.Reactive` namespaces plus `ReactiveUI.Primitives.Extensions.Reactive`. Adds `System.Reactive` and depends on `ReactiveUI.Primitives.Core`. This is a package variant, not a source-generator bridge; no separate Extensions Reactive package is required.                                                                                                                     |
| `ReactiveUI.Primitives.Async.Core`          | Building a low-level async library around Primitives async contracts.                                                                                  | `ReactiveUI.Primitives.Async`; `IObservableAsync<T>`, `IObserverAsync<T>`, `SignalAsync<T>`, `SignalAsync`, async signal factories, async operators, async disposables, helpers, and async signal implementations. Depends on `ReactiveUI.Primitives.Core`.                                                                                                                      |
| `ReactiveUI.Primitives.Async`               | The app needs async-native observable pipelines where observer calls can await or observe cancellation.                                                | Lean async leaf using Primitives `RxVoid` and `ISequencer`. Adds `AsyncContext`, `ContextSwitchSignalAsync<T>`, `SignalAsyncReactiveExtensions`, `Yield`, `WitnessOn`, `ObserveOnSafe`, and `ObserveOnIf`. Depends on `ReactiveUI.Primitives` and `ReactiveUI.Primitives.Async.Core`.                                                                                            |
| `ReactiveUI.Primitives.Async.Reactive`      | Async-native pipelines in a System.Reactive-first project.                                                                                             | System.Reactive-flavoured async leaf with `System.Reactive.Unit` and `IScheduler` conventions. Depends on `ReactiveUI.Primitives.Reactive` and `ReactiveUI.Primitives.Async.Core`.                                                                                                                                                                                               |
| `ReactiveUI.Primitives.ObservableEvents`    | The project needs strongly typed `IObservable<T>` properties for .NET events.                                                                           | Standalone `netstandard2.0` analyzer package. Selects lean Primitives, `.Reactive`, or standalone System.Reactive APIs from consumer references and adds no runtime dependency of its own.                                                                                                                                                                                       |
| `ReactiveUI.Primitives.R3Bridge.Generator`  | The project needs generated adapters at R3 or R3Async boundaries.                                                                                       | Standalone `netstandard2.0` analyzer package. It emits bridges only when the consuming compilation already references the required R3/R3Async symbols; it is not embedded in or required by the runtime packages.                                                                                                                                                              |
| `ReactiveUI.Primitives.Wpf`                 | WPF UI code needs dispatcher marshalling.                                                                                                              | `ReactiveUI.Primitives.Concurrency.DispatcherSequencer`. Depends on `ReactiveUI.Primitives`.                                                                                                                                                                                                                                                                                     |
| `ReactiveUI.Primitives.Wpf.Reactive`        | WPF UI code is System.Reactive-first and needs dispatcher scheduling.                                                                                  | `ReactiveUI.Primitives.Reactive.Concurrency.DispatcherSequencer` implements System.Reactive scheduling conventions. Depends on `ReactiveUI.Primitives.Reactive`.                                                                                                                                                                                                                 |
| `ReactiveUI.Primitives.WinForms`            | Windows Forms UI code needs control-thread marshalling.                                                                                                | `ReactiveUI.Primitives.Concurrency.ControlSequencer`. Depends on `ReactiveUI.Primitives`.                                                                                                                                                                                                                                                                                        |
| `ReactiveUI.Primitives.WinForms.Reactive`   | Windows Forms UI code is System.Reactive-first and needs control-thread scheduling.                                                                    | `ReactiveUI.Primitives.Reactive.Concurrency.ControlSequencer`. Depends on `ReactiveUI.Primitives.Reactive`.                                                                                                                                                                                                                                                                      |
| `ReactiveUI.Primitives.WinUI`               | WinUI code needs `DispatcherQueue` marshalling.                                                                                                        | `ReactiveUI.Primitives.Concurrency.DispatcherQueueSequencer` and `DispatcherQueueSequencerExtensions.ToSequencer()`. Depends on `ReactiveUI.Primitives` and `Microsoft.WindowsAppSDK`.                                                                                                                                                                                           |
| `ReactiveUI.Primitives.WinUI.Reactive`      | WinUI code is System.Reactive-first and needs `DispatcherQueue` scheduling.                                                                            | `ReactiveUI.Primitives.Reactive.Concurrency.DispatcherQueueSequencer` and `DispatcherQueueSequencerExtensions.ToSequencer()`. Depends on `ReactiveUI.Primitives.Reactive` and `Microsoft.WindowsAppSDK`.                                                                                                                                                                         |
| `ReactiveUI.Primitives.Blazor`              | Blazor components need render-thread sequencing and component-bound subscriptions.                                                                     | `ReactiveUI.Primitives.Blazor.Components.ReactiveComponentBase`, `Observe`, `Track`, `InvalidateAsync`, `ReactiveUI.Primitives.Blazor.Concurrency.BlazorRendererSequencer`. Depends on `ReactiveUI.Primitives` and `Microsoft.AspNetCore.Components`.                                                                                                                            |
| `ReactiveUI.Primitives.Blazor.Reactive`     | Blazor components are System.Reactive-first and need render-thread scheduling.                                                                         | `ReactiveUI.Primitives.Blazor.Reactive.Components.ReactiveComponentBase` and `ReactiveUI.Primitives.Blazor.Reactive.Concurrency.BlazorRendererSequencer`. Depends on `ReactiveUI.Primitives.Reactive` and `Microsoft.AspNetCore.Components`.                                                                                                                                     |
| `ReactiveUI.Primitives.Avalonia`            | Avalonia UI code needs dispatcher marshalling.                                                                                                         | `ReactiveUI.Primitives.Concurrency.AvaloniaScheduler`; `Instance` uses `Dispatcher.UIThread` at background priority, or construct with a `Dispatcher` and optional `DispatcherPriority`. Depends on `ReactiveUI.Primitives` and `Avalonia`.                                                                                                                                          |
| `ReactiveUI.Primitives.Avalonia.Reactive`   | Avalonia UI code is System.Reactive-first and needs dispatcher scheduling.                                                                             | `ReactiveUI.Primitives.Reactive.Concurrency.AvaloniaScheduler`; `Instance` uses `Dispatcher.UIThread` at background priority, or construct with a `Dispatcher` and optional `DispatcherPriority`. Depends on `ReactiveUI.Primitives.Reactive` and `Avalonia`.                                                                                                                      |
| `ReactiveUI.Primitives.Maui`                | .NET MAUI code needs dispatcher marshalling.                                                                                                           | `ReactiveUI.Primitives.Concurrency.MauiDispatcherSequencer` and `MauiDispatcherSequencerExtensions.ToSequencer()`. Depends on `ReactiveUI.Primitives` and `Microsoft.Maui.Core`.                                                                                                                                                                                                 |
| `ReactiveUI.Primitives.Maui.Reactive`       | .NET MAUI code is System.Reactive-first and needs dispatcher scheduling.                                                                               | `ReactiveUI.Primitives.Reactive.Concurrency.MauiDispatcherSequencer` and `MauiDispatcherSequencerExtensions.ToSequencer()`. Depends on `ReactiveUI.Primitives.Reactive` and `Microsoft.Maui.Core`.                                                                                                                                                                               |

Install examples:

```bash
dotnet add package ReactiveUI.Primitives
dotnet add package ReactiveUI.Primitives.Async
dotnet add package ReactiveUI.Primitives.Wpf
dotnet add package ReactiveUI.Primitives.Avalonia
```

Use `.Core` packages deliberately:

```bash
dotnet add package ReactiveUI.Primitives.Core
dotnet add package ReactiveUI.Primitives.Async.Core
```

Use `.Reactive` packages when the project already uses System.Reactive idioms:

```bash
dotnet add package ReactiveUI.Primitives.Reactive
dotnet add package ReactiveUI.Primitives.Async.Reactive
dotnet add package ReactiveUI.Primitives.Wpf.Reactive
dotnet add package ReactiveUI.Primitives.Avalonia.Reactive
```

Add `ReactiveUI.Primitives.R3Bridge.Generator` only when the project needs generated R3 or R3Async bridge methods. It is
a standalone analyzer package and is not embedded in the runtime packages.

```bash
dotnet add package ReactiveUI.Primitives.R3Bridge.Generator
```

Add `ReactiveUI.Primitives.ObservableEvents` when public .NET events should be exposed as observables. It uses the
lean, `.Reactive`, or standalone System.Reactive provider already referenced by the consumer.

```bash
dotnet add package ReactiveUI.Primitives.ObservableEvents
```

## Selection Rules

- Prefer `ReactiveUI.Primitives` for new app code that uses BCL `IObservable<T>`.
- Prefer `ReactiveUI.Primitives.Async` when subscription, notification, or completion work must be asynchronous.
- Prefer `ReactiveUI.Primitives` for migrated helper operators from ReactiveUI.Extensions-style code; their existing
  `ReactiveUI.Primitives.Extensions` namespace is retained.
- Prefer UI packages only in the matching UI framework; use the lean UI package for `ISequencer` and the `.Reactive` UI
  package for `IScheduler`.
- For Avalonia UI dispatch, choose `ReactiveUI.Primitives.Avalonia`; choose
  `ReactiveUI.Primitives.Avalonia.Reactive` only when the project is System.Reactive-first.
- Prefer `.Reactive` variants when public APIs should expose `System.Reactive.Unit`, `IScheduler`, `.Reactive`
  namespaces, or existing Rx source should compile with minimal code changes.
- Prefer `.Core` variants only when composing packages or minimizing a library dependency layer. Most apps should
  reference the leaf package.
- Avoid mixing lean and `.Reactive` variants in the same pipeline without an explicit boundary; their namespaces and
  scheduler/unit conventions differ.
- Do not add `System.Reactive` just to use core Primitives. The `.Reactive` packages add it for System.Reactive-first
  projects.
- Do not expect a System.Reactive bridge generator. System.Reactive support is provided by the explicit `.Reactive`
  package variants.

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

Extension namespaces provided by the base packages:

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

Reactive UI packages expose lean sequencers under `ReactiveUI.Primitives.Concurrency` and System.Reactive schedulers
under `ReactiveUI.Primitives.Reactive.Concurrency`. Blazor also exposes component helpers under
`ReactiveUI.Primitives.Blazor.Reactive.Components`. Avalonia exposes `AvaloniaScheduler` in both concurrency namespaces;
its ready work is coalesced into one dispatcher drain and delayed work uses dispatcher-bound timers.

R3 generated bridges:

```csharp
using ReactiveUI.Primitives.R3Bridge;
```

## API Landmarks

Use these landmarks to choose APIs quickly; rely on IntelliSense/PublicAPI for exact overloads.

- Signals and factories: `Signal`, `Signal<T>`, `BehaviorSignal<T>`, `StateSignal<T>`, `ReplaySignal<T>`,
  `ScheduledSignal<T>`, `PrioritySemaphoreSignal<T>`, `AnonymousSignal<T>`, `ConnectableSignal<T>`.
- State and commands: `ReadOnlyState<T>`, `CommandSignal<TResult>`, `CommandExecution<TResult>`, `TaskSignal`,
  `TaskSignal<T>`.
- Sequencing: `ISequencer`, `CurrentThreadSequencer`, `ImmediateSequencer`, `SynchronizationContextSequencer`,
  `TaskPoolSequencer`, `ThreadPoolSequencer`, `VirtualTimeSequencer<TAbsolute, TRelative>`, `VirtualClock`.
- Operators: `Map`, `FlatMap`, `Bind`, `SelectMany`, `Where`, `Keep`, `KeepNotNull`, `Cast`, `Concat`, `Merge`, `Amb`,
  `Race`, `Switch`, `Retry`, `Recover`, `Rescue`, `Distinct`, `Take`, `Skip`, `Collect`, `Materialize`, `Dematerialize`,
  `ForkJoin`, `Pair`, `Latch`, `SyncLatest`, `CombineLatest`, `Synchronize`, `Timeout`, `Shift`, `Spark`, `Unspark`.
  `SyncLatest` and Rx-name `CombineLatest` support up to 16 total sources; Rx-name `SelectMany` observable overloads
  preserve concurrent merge semantics.
- Connectable helpers: `Replay`, `ReplayLive`, `Share`, `ShareLatest`, `AutoConnect`, `AutoShare`.
- Async core: `IObservableAsync<T>`, `IObserverAsync<T>`, `SignalAsync<T>`, `SignalAsync`, `WitnessAsync<T>`,
  `ConnectableSignalAsync<T>`, `ConcurrentWitnessCallsException`.
- Async factories/operators: `SignalAsync.Create`, `Return`, `Range`, `FromAsync`, `FromAsyncEnumerable`, `Every`,
  `Interval`, `Timer`, `Using`, `Blend`, `Chain`, `Map`, `Merge`, `CombineLatest`, `Switch`, `ForEachAsync`,
  `Collect*Async`, `WaitCompletionAsync`, `OnErrorResumeAsFailure`.
- Extension helpers: `AsSignal`, `BufferUntil`, `BufferUntilIdle`, `DebounceImmediate`, `DebounceUntil`, `DetectStale`,
  `Filter`, `Heartbeat`, `ObserveOnSafe`, `ObserveOnIf`, `Pairwise`, `Partition`, `ReplayLastOnSubscribe`,
  `RetryWithBackoff`, `RetryWithDelay`, `RetryForeverWithDelay`, `RunAll`, `Shuffle`, `SwitchIfEmpty`, `Throttle*`,
  `ToHotTask`, `ToHotValueTask`, `WaitUntil`. `Filter(string)` uses a 30-second regex timeout; `Filter(Regex)` preserves
  caller-supplied options and timeout.

## Rx Migration Process

Use this process when moving an Rx-based repository to ReactiveUI.Primitives. Decide first whether the immediate target
is a lean Primitives project, a System.Reactive-compatible `.Reactive` project, or both.

### Existing `xyz` Project To Lean Primitives

Use this path when the existing project should stop exposing System.Reactive `Unit`, `IScheduler`, and package
dependencies.

1. Inventory package references and public APIs. Find `System.Reactive`, `System.Reactive.Linq`,
   `System.Reactive.Subjects`, `System.Reactive.Disposables`, `System.Reactive.Concurrency`, `ReactiveUI.Extensions`,
   `Unit`, `IScheduler`, `Subject<T>`, `BehaviorSubject<T>`, `ReplaySubject<T>`, and `TestScheduler`.
2. Add the smallest lean package set:

```bash
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Async
```

3. Add only the matching lean UI package when UI dispatch is used:

```bash
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Wpf
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.WinForms
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.WinUI
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Blazor
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Avalonia
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Maui
```

4. Convert types at boundaries: `Unit` -> `RxVoid`, `IScheduler` -> `ISequencer`, `Subject<T>` -> `Signal<T>`,
   `BehaviorSubject<T>` -> `StateSignal<T>`, `ReplaySubject<T>` -> `ReplaySignal<T>`, `AsyncSubject<T>` ->
   `FinalSignal<T>`, `CompositeDisposable` -> `MultipleDisposable` or `Pocket`, `SerialDisposable` ->
   `SingleReplaceableDisposable` or `Slot`.
5. Convert factories: `Observable.Return` -> `Signal.Emit`, `Empty` -> `Signal.None`, `Never` -> `Signal.Silent`,
   `Throw` -> `Signal.Fail`, `Range` -> `Signal.Sequence`, `Defer` -> `Signal.Lazy`, `Timer` -> `Signal.After`,
   `Interval` -> `Signal.Pulse` or `Signal.Every`, `Create` -> `Signal.Create` or `Signal.CreateSafe`.
6. Keep code compiling with Rx-name compatibility aliases where useful: `Select`, `Where`, `Aggregate`, `Scan`, `Merge`,
   `Concat`, `SelectMany`, `CombineLatest`, `WithLatestFrom`, `Throttle`, and related names exist on the Primitives
   surface.
7. Prefer Primitives names in new or hot-path code: `Map`, `Keep`, `Reduce`, `Fold`, `Blend`, `Chain`, `FlatMap`/`Bind`,
   `SyncLatest`, `Latch`, `Calm`/`Stabilize`, `Probe`, `Shift`, `Expire`.
8. Replace schedulers with `Sequencer.Immediate`, `Sequencer.CurrentThread`, `ThreadPoolSequencer.Instance`,
   `TaskPoolSequencer.Instance`, `SynchronizationContextSequencer`, UI sequencers, or `VirtualClock` for tests.
9. Remove System.Reactive and ReactiveUI.Extensions package references only after imports and public APIs no longer
   require them. Keep bridge conversions at package edges.

### New `xyz.Reactive` Project Consuming `.Reactive` Packages

Use this path when an Rx-based source base should keep System.Reactive-facing APIs for existing consumers while sharing
implementation with a lean `xyz` package.

1. Create `xyz.Reactive` as a sibling project to `xyz`. Link or share implementation files instead of copying logic.
2. In shared implementation code, use neutral identifiers `RxVoid` and `ISequencer`. The lean project binds them to
   Primitives types; the reactive project aliases them to `System.Reactive.Unit` and
   `System.Reactive.Concurrency.IScheduler`.
3. Gate namespaces when package namespaces differ:

```csharp
#if REACTIVE_SHIM
namespace xyz.Reactive;
#else
namespace xyz;
#endif
```

4. Add the `.Reactive` package set that matches the source surface:

```bash
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Async.Reactive
```

5. Add only the matching `.Reactive` UI package when the project exposes UI scheduling:

```bash
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Wpf.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.WinForms.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.WinUI.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Blazor.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Avalonia.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Maui.Reactive
```

6. If the repository does not centralize this already, configure `xyz.Reactive`:

```xml
<PropertyGroup>
  <DefineConstants>$(DefineConstants);REACTIVE_SHIM</DefineConstants>
</PropertyGroup>
<ItemGroup>
  <Using Include="System.Reactive.Unit" Alias="RxVoid" />
  <Using Include="System.Reactive.Concurrency.IScheduler" Alias="ISequencer" />
</ItemGroup>
```

7. Preserve Rx source compatibility first. Keep names such as `Select`, `Where`, `SelectMany`, `CombineLatest`, `Merge`,
   `Concat`, `Throttle`, and `WithLatestFrom` when that avoids unnecessary code churn. The `.Reactive` packages provide
   those names over the Primitives implementation.
8. Build `xyz` and `xyz.Reactive` side by side. The lean `xyz` package should avoid System.Reactive runtime
   dependencies; `xyz.Reactive` intentionally references System.Reactive and exposes `Unit`/`IScheduler`.
9. Update tests to run both package variants. Prefer TUnit/Microsoft Testing Platform in this repository and use only
   TUnit assertions.

## R3 And R3Async Bridges

R3 bridges are generated into the consuming assembly only when the consumer references
`ReactiveUI.Primitives.R3Bridge.Generator` and already references the required R3 symbols. The generated namespace is
`ReactiveUI.Primitives.R3Bridge`.

When `R3.Observable<T>` is visible, the generator emits:

- `AsPrimitivesSignal<T>(this R3.Observable<T>)`
- `AsR3Observable<T>(this System.IObservable<T>)`

When both `R3.Observable<T>` and `ReactiveUI.Primitives.Async.IObservableAsync<T>` are visible, the generator emits:

- `AsPrimitivesAsyncObservable<T>(this R3.Observable<T>)`
- `AsR3Observable<T>(this IObservableAsync<T>)`

When `R3Async.AsyncObservable<T>`, `R3Async.AsyncObserver<T>`, `R3Async.Result`, and `IObservableAsync<T>` are visible,
the async bridge generator emits:

- `AsPrimitivesAsyncObservable<T>(this R3Async.AsyncObservable<T>)`
- `AsR3AsyncObservable<T>(this IObservableAsync<T>)`

Use bridge methods only at boundaries. Keep internal pipelines in one model after conversion.

## Observable Events

Reference `ReactiveUI.Primitives.ObservableEvents`, import `ReactiveUI.Primitives.ObservableEvents`, and call
`source.Events()` once to activate generation for the source type. Generated properties use `RxVoid` for lean
parameterless events and `System.Reactive.Unit` for `.Reactive` or standalone System.Reactive consumers. Use
`[assembly: GenerateStaticEventObservables(typeof(EventHost))]` for static events; generated properties appear on
`RxEvents` in the host namespace and use length-prefixed host/event identifiers to prevent naming collisions.

## Framework And Platform Notes

- `ReactiveUI.Disposables`, `ReactiveUI.Primitives.Core`, `ReactiveUI.Primitives.Async.Core`,
  `ReactiveUI.Primitives.Async`, and `ReactiveUI.Primitives.Async.Reactive` target `net8.0`, `net9.0`, `net10.0`,
  `net11.0`, `net462`, `net472`, `net48`, and `net481`.
- `ReactiveUI.Primitives.ObservableEvents` and `ReactiveUI.Primitives.R3Bridge.Generator` target `netstandard2.0`.
- `ReactiveUI.Primitives` and `ReactiveUI.Primitives.Reactive` target the same eight general-library TFMs plus
  `net10.0-android` and `net11.0-android`; on Windows or macOS they also target net10/net11 iOS, tvOS, macOS, and Mac
  Catalyst for platform sequencers.
- WPF and WinForms packages, including `.Reactive` variants, target `net8.0-windows` through `net11.0-windows` plus
  `net462`, `net472`, `net48`, and `net481`.
- WinUI packages, including `.Reactive` variants, target `net8.0-windows10.0.19041.0` through
  `net11.0-windows10.0.19041.0`.
- Avalonia packages, including `.Reactive` variants, target `net8.0`, `net9.0`, `net10.0`, and `net11.0` and reference
  `Avalonia`.
- MAUI packages, including `.Reactive` variants, target `net10.0` and `net11.0`.
- Blazor packages, including `.Reactive` variants, target `net8.0`, `net9.0`, `net10.0`, and `net11.0`.

## Repository Maintenance

When editing this repository:

- Use `src/ReactiveUI.Primitives.slnx` as the solution entrypoint.
- Build and test from `src`.
- Tests use Microsoft.Testing.Platform with TUnit; write TUnit tests and TUnit assertions only.
- Shipping public API baselines live under each package's `PublicAPI/<tfm>/PublicAPI.Shipped.txt` and
  `PublicAPI.Unshipped.txt`.
- `Skill.md` is the canonical skill file. `ReactiveUI.Primitives.csproj` packs it both at package root as `Skill.md` and
  at `.agents/skills/reactiveui-primitives/SKILL.md`; keep this filename singular and casing intact.
- Keep package guidance synchronized with packable projects in the solution. Tests and benchmarks are not NuGet packages
  to add directly.
