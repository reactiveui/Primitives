---
name: reactiveui-primitives
description: Use when working with ReactiveUI.Primitives NuGet packages, migrating from System.Reactive or R3, choosing Signal/Sequencer/Async/Extensions APIs, or using the generated bridge adapters without adding runtime Rx or R3 dependencies.
---

# ReactiveUI.Primitives

Use this skill when a .NET project consumes ReactiveUI.Primitives from NuGet and needs reactive streams, state, scheduling, async observables, migrated ReactiveUI.Extensions helpers, UI sequencer adapters, or migration guidance from System.Reactive or R3.

Assume package consumption from NuGet. Do not assume repository source paths, local project references, or repository-only workflows.

## Package Setup

Install the packages that match the target application surface:

```bash
dotnet add package ReactiveUI.Primitives
dotnet add package ReactiveUI.Primitives.Async
dotnet add package ReactiveUI.Primitives.Extensions
```

Add UI adapter packages only when the application needs that UI thread integration:

```bash
dotnet add package ReactiveUI.Primitives.Wpf
dotnet add package ReactiveUI.Primitives.WinForms
dotnet add package ReactiveUI.Primitives.WinUI
dotnet add package ReactiveUI.Primitives.Blazor
dotnet add package ReactiveUI.Primitives.Maui
```

Common imports:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Signals;
```

For async APIs:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Signals;
```

## Design Rules For Agents

- Prefer ReactiveUI.Primitives names when writing new code: `Signal`, `Map`, `Keep`, `Spark`, `ISequencer`.
- Do not introduce a System.Reactive or R3 runtime dependency unless the user's project already has that dependency or explicitly needs a bridge boundary.
- Use bridge source-generator methods only at interop boundaries. Convert at the edge, then keep internal code in Primitives types and naming.
- Use `ISequencer` for scheduling and UI marshalling. Do not design new code around `IScheduler`.
- Use `IDisposable` and `IAsyncDisposable` lifetimes explicitly. Store subscriptions in `MultipleDisposable`, `Pocket`, `Slot`, or async disposable containers as appropriate.
- Keep async pipelines in `IObservableAsync<T>` when the observer work is asynchronous or cancellation-aware.

## Core Package

`ReactiveUI.Primitives` is the base package. It uses BCL `IObservable<T>` and `IObserver<T>` contracts and has no runtime System.Reactive or R3 dependency.

Important public types:

- `Signal<T>`: hot subject-like source and sink. Use instead of `Subject<T>`.
- `BehaviorSignal<T>`: signal with current value replayed to new subscribers.
- `StateSignal<T>`: mutable current state signal with a `Value` setter and state projection helpers.
- `ReadOnlyState<T>` and `ProjectedReadOnlyState<TSource,TResult>`: read-only state projections.
- `ReplaySignal<T>` and `HistorySignal<T>`: replay buffered values by count and optional time window.
- `ConnectableSignal<T>`: explicit connectable source for share, replay, auto-connect, and ref-count flows.
- `TaskSignal` and `TaskSignal<T>`: task-backed signal sources with cancellation-aware execution.
- `CommandSignal<TResult>` and `CommandExecution<TResult>`: command execution, `CanRun`, `IsRunning`, results, faults, and execution lifecycle.
- `RxVoid`: no-value event marker, equivalent to Rx `Unit`.
- `Spark<T>` and `SparkKind`: materialized next/error/completed notifications.
- `EventPattern<TEventArgs>`, `Moment<T>`, and `TimeInterval<T>`: event and time wrappers.

Signal factory entry point:

```csharp
using ReactiveUI.Primitives.Signals;

IObservable<int> values = Signal.Sequence(1, 3);
IObservable<string> one = Signal.Emit("ready");
IObservable<string> none = Signal.None<string>();
IObservable<string> failed = Signal.Fail<string>(new InvalidOperationException("boom"));
IObservable<long> tick = Signal.Every(TimeSpan.FromSeconds(1));
```

Core factories:

- `Signal.Create`, `CreateSafe`, `CreateWithState`, `Lazy`
- `Emit`, `EmitRxVoid`, `None`, `Silent`, `Fail`
- `Sequence`, `Loop`, `Unfold`, `Iterate`
- `FromEnumerable`, `FromAsyncEnumerable`, `FromTask`, `FromAsync`, `FromEventPattern`
- `Start`, `Use`, `After`, `Every`, `Pulse`
- `Chain`, `Blend`, `Race`, `Pair`, `SyncLatest`, `PairLatest`, `ForkJoin`

Example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var input = new Signal<int>();

using var subscription = input
    .Keep(value => value > 0)
    .Map(value => value * 2)
    .Subscribe(value => Console.WriteLine(value));

input.OnNext(21);
input.OnCompleted();
```

## Operator Vocabulary

ReactiveUI.Primitives keeps common LINQ-compatible names where useful and adds distinct names to avoid confusion with System.Reactive and R3.

Prefer these Primitives names in new code:

| System.Reactive or R3 concept | ReactiveUI.Primitives name |
| --- | --- |
| `Subject<T>` | `Signal<T>` |
| `BehaviorSubject<T>` | `BehaviorSignal<T>` or `StateSignal<T>` |
| `ReplaySubject<T>` | `ReplaySignal<T>` or `HistorySignal<T>` |
| `Observable.Return` | `Signal.Emit` |
| `Observable.Empty` | `Signal.None` |
| `Observable.Never` | `Signal.Silent` |
| `Observable.Throw` | `Signal.Fail` |
| `Observable.Range` | `Signal.Sequence` |
| `Observable.Timer` | `Signal.After` |
| `Observable.Interval` | `Signal.Every` or `Signal.Pulse` |
| `Select` | `Map` |
| `Where` | `Keep` |
| `OfType` | `KeepType` |
| `Cast` | `CastTo` |
| `Where(x is not null)` | `KeepNotNull` |
| `Do` | `Tap` or `TapWith` |
| `Scan` | `Fold` |
| `Aggregate` | `Reduce` |
| `SelectMany` | `FlatMap` or `Bind` |
| `Concat` | `Chain` |
| `Merge` | `Blend` |
| `Amb` | `Race` |
| `Zip` | `Pair` |
| `CombineLatest` | `SyncLatest` or `PairLatest` |
| `WithLatestFrom` | `Latch` |
| `Switch` | `SwitchTo` |
| `Retry` | `Reattempt` |
| `Catch` | `Recover`, `Rescue`, or `Resume` |
| `Delay` | `Shift` |
| `Timeout` | `Expire` |
| `StartWith` | `Lead` |
| `Materialize` | `Spark` |
| `Dematerialize` | `Unspark` |
| `Unit` | `RxVoid` |
| `Notification<T>` | `Spark<T>` |
| `IScheduler` | `ISequencer` |
| `CompositeDisposable` | `MultipleDisposable`, `Pocket`, or the `CompositeDisposable` alias |
| `SerialDisposable` | `SingleReplaceableDisposable` or `Slot` |
| `SingleAssignmentDisposable` | `SingleDisposable` or `AssignmentSlot` |

Standard names such as `Take`, `Skip`, `Distinct`, `DistinctBy`, `DefaultIfEmpty`, `Buffer`, `Timestamp`, `TimeInterval`, `SubscribeOn`, `ObserveOn`, terminal collection methods, and async terminal methods may also be available. Prefer the Primitives name when both exist and the target code is intended to avoid Rx/R3 vocabulary.

## Sequencers

`ISequencer` is the scheduling abstraction. Use it instead of Rx `IScheduler`.

Common sequencers:

- `Sequencer.Immediate`
- `Sequencer.CurrentThread`
- `Sequencer.Default`
- `ImmediateSequencer`
- `CurrentThreadSequencer`
- `TaskPoolSequencer`
- `ThreadPoolSequencer`
- `SynchronizationContextSequencer`
- `VirtualClock`, `TestClock`, and `VirtualTimeSequencer<TAbsolute,TRelative>` for deterministic virtual time

Example:

```csharp
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

ISequencer ui = SynchronizationContextSequencer.Current;

using var subscription = Signal
    .Every(TimeSpan.FromSeconds(1), Sequencer.Default)
    .ObserveOn(ui)
    .Subscribe(_ => RefreshView());
```

UI adapter packages provide UI-specific sequencers:

- WPF: `DispatcherSequencer`
- WinForms: `ControlSequencer`
- WinUI: `DispatcherQueueSequencer` and `ToSequencer`
- Blazor: `BlazorRendererSequencer` and `ReactiveComponentBase`
- MAUI: `MauiDispatcherSequencer` and `ToSequencer`

## Disposables

Use `ReactiveUI.Primitives.Disposables` for subscription and resource lifetimes:

- `Disposable.Empty` and `Disposable.Create(Action)`
- `BooleanDisposable`
- `CancellationDisposable`
- `MultipleDisposable` and `CompositeDisposable`
- `Pocket`
- `SingleDisposable` and `AssignmentSlot`
- `SingleReplaceableDisposable` and `Slot`
- `MutableDisposable`, `SwapDisposable`, and `DisposableBag`
- `Handle`, `Handle<T>`, `Handle<T1,T2>`, `Handle<T1,T2,T3>`

Example:

```csharp
using ReactiveUI.Primitives.Disposables;

var subscriptions = new MultipleDisposable();

source.Subscribe(value => Console.WriteLine(value))
    .DisposeWith(subscriptions);

subscriptions.Dispose();
```

## Async Package

`ReactiveUI.Primitives.Async` adds asynchronous observable contracts:

- `IObservableAsync<T>`
- `IObserverAsync<T>`
- `WitnessAsync<T>`
- `SignalAsync<T>`
- `ISignalAsync<T>`
- `ConnectableSignalAsync<T>`
- `GroupedAsyncSignal<TKey,TValue>`
- `Result`
- `Optional<T>`
- `AsyncContext`

Async observer methods return `ValueTask` and accept cancellation where relevant:

- `OnNextAsync(T value, CancellationToken cancellationToken)`
- `OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)`
- `OnCompletedAsync(Result result)`
- `DisposeAsync()`

Use `ReactiveUI.Primitives.Async.SignalAsync` for async observable factories and operators:

```csharp
using ReactiveUI.Primitives.Async;

IObservableAsync<int> source = SignalAsync.Sequence(1, 3);

List<int> values = await source
    .Keep(value => value > 1)
    .Map(value => value * 10)
    .CollectListAsync();
```

Use `ReactiveUI.Primitives.Async.Signals.Signal` for mutable async signals:

```csharp
using AsyncSignal = ReactiveUI.Primitives.Async.Signals.Signal;

await using var signal = AsyncSignal.Create<int>();

await signal.SubscribeAsync(async (value, cancellationToken) =>
{
    await StoreAsync(value, cancellationToken);
});

await signal.OnNextAsync(42, CancellationToken.None);
await signal.OnCompletedAsync(Result.Success);
```

Async creation options:

- `SignalCreationOptions`
- `BehaviorSignalCreationOptions`
- `ReplayLatestSignalCreationOptions`
- `PublishingOption.Serial`
- `PublishingOption.Concurrent`

Async mutable signal factories:

- `Signal.Create<T>()`
- `Signal.Create<T>(SignalCreationOptions?)`
- `Signal.CreateBehavior<T>(T startValue)`
- `Signal.CreateBehavior<T>(T startValue, BehaviorSignalCreationOptions?)`
- `Signal.CreateReplayLatest<T>()`
- `Signal.CreateReplayLatest<T>(ReplayLatestSignalCreationOptions?)`

Async operator vocabulary follows the core Primitives names where possible:

- `Create`, `CreateAsBackgroundJob`, `Emit`, `Return`, `None`, `Empty`, `Never`, `Fail`, `Throw`, `Sequence`, `Range`, `FromEnumerable`, `FromAsyncEnumerable`, `FromAsync`, `Defer`, `After`, `Every`, `Pulse`, `Timer`, `Interval`, `Start`, `Use`, `Using`
- `Map`, `MapWith`
- `Keep`, `KeepWith`, `KeepNotNull`, `KeepType`
- `CastTo`
- `Tap`
- `Fold`, `ReduceAsync`
- `FlatMap`, `Bind`
- `Unique`, `UniqueBy`
- `Chain`, `Blend`, `SwitchTo`, `Concat`, `Merge`, `Switch`
- `Pair`, `SyncLatest`, `PairLatest`, `Zip`, `CombineLatest`
- `Reattempt`, `Recover`, `Rescue`, `Resume`, `Catch`, `OnErrorResumeAsFailure`
- `Shift`, `Expire`, `Lead`, `Delay`, `Timeout`, `Throttle`, `ObserveOn`, `Yield`
- `CollectListAsync`, `CollectArrayAsync`, `ToListAsync`, `ToDictionaryAsync`, `ToAsyncEnumerable`, `SubscribeAsync`, `WaitCompletionAsync`

Async also exposes parity operators such as `AggregateAsync`, `AnyAsync`, `AllAsync`, `CountAsync`, `LongCountAsync`, `ContainsAsync`, `FirstAsync`, `FirstOrDefaultAsync`, `LastAsync`, `LastOrDefaultAsync`, `SingleAsync`, `SingleOrDefaultAsync`, `ForEachAsync`, `WaitCompletionAsync`, `ToAsyncEnumerable`, `ToDictionaryAsync`, `GroupBy`, `Merge`, `Concat`, `Zip`, `Switch`, `Retry`, `Throttle`, `Timeout`, `Delay`, `ObserveOn`, and `SubscribeAsync`.

Async disposable helpers:

- `DisposableAsync.Empty`
- `DisposableAsync.Create(...)`
- `MultipleDisposableAsync`
- `SingleAssignmentDisposableAsync`
- `SingleReplaceableDisposableAsync`
- `DisposableAsyncSlot`

## Extensions Package

`ReactiveUI.Primitives.Extensions` contains migrated convenience operators that reference ReactiveUI.Primitives and avoid production System.Reactive/R3 dependencies.

High-value extension areas:

- Null and boolean helpers: `WhereIsNotNull`, `SkipWhileNull`, `WhereTrue`, `WhereFalse`, `Not`
- Signal helpers: `AsSignal`, `ToReadOnlyBehavior`, `ReplayLastOnSubscribe`
- Error helpers: `CatchIgnore`, `CatchAndReturn`, `CatchReturn`, `CatchReturnUnit`, `LogErrors`
- Retry helpers: `OnErrorRetry`, `RetryWithBackoff`, `RetryWithDelay`, `RetryForeverWithDelay`, `RetryWithFixedDelay`
- Timing helpers: `SyncTimer`, `Schedule`, `SampleLatest`, `ThrottleFirst`, `ThrottleUntilTrue`, `ThrottleOnScheduler`, `ThrottleDistinct`, `DebounceImmediate`, `DebounceUntil`
- Buffering and state helpers: `BufferUntil`, `BufferUntilIdle`, `BufferUntilInactive`, `Conflate`, `Heartbeat`, `DetectStale`, `LatestOrDefault`
- Async interop helpers: `SubscribeAsync`, `SelectAsync`, `SelectAsyncSequential`, `SelectLatestAsync`, `SelectAsyncConcurrent`, `ToHotTask`, `ToHotValueTask`, `WaitUntil`, `DropIfBusy`
- Collection and projection helpers: `ForEach`, `FromArray`, `Using`, `While`, `ScanWithInitial`, `Filter`, `Shuffle`, `Pairwise`, `Partition`, `WhereSelect`, `SelectConstant`, `TrySelect`, `SelectManyThen`, `RunAll`, `FirstMatchFromCandidates`
- Subscription helpers: `SubscribeGetValue`, `SubscribeGetError`, `WaitForValue`, `WaitForCompletion`, `WaitForError`

Example:

```csharp
using ReactiveUI.Primitives.Extensions;

IObservable<string> nonEmpty = names
    .WhereIsNotNull()
    .Keep(name => name.Length > 0)
    .ReplayLastOnSubscribe();
```

## Source Generator Bridges

The base package packs source generators as analyzers. The async package also packs them so async bridge methods can be generated.

The generators do not add runtime System.Reactive or R3 dependencies to ReactiveUI.Primitives. They emit bridge code only when the consuming project already references the relevant external package.

System.Reactive bridge:

- Generated namespace: `ReactiveUI.Primitives.SystemReactiveBridge`
- Available when `System.Reactive` symbols are present.
- `AsPrimitivesSignal<T>(this System.IObservable<T>)`
- `AsSystemObservable<T>(this System.IObservable<T>)`
- `AsSequencer(this System.Reactive.Concurrency.IScheduler)`
- `AsSystemScheduler(this ReactiveUI.Primitives.Concurrency.ISequencer)`
- If `ReactiveUI.Primitives.Async` is referenced: `ToObservableAsync<T>(this IObservable<T>)`
- If `ReactiveUI.Primitives.Async` is referenced: `ToObservable<T>(this IObservableAsync<T>)`

System.Reactive.Async bridge:

- Generated namespace: `ReactiveUI.Primitives.SystemReactiveBridge`
- Available when `System.Reactive.Async` provides `System.IAsyncObservable<T>` and `System.IAsyncObserver<T>`.
- Requires `ReactiveUI.Primitives.Async`.
- `AsPrimitivesAsyncObservable<T>(this System.IAsyncObservable<T>)`
- `AsSystemReactiveAsyncObservable<T>(this IObservableAsync<T>)`
- `AsPrimitivesAsyncObserver<T>(this System.IAsyncObserver<T>)`
- `AsSystemReactiveAsyncObserver<T>(this IObserverAsync<T>)`

R3 bridge:

- Generated namespace: `ReactiveUI.Primitives.R3Bridge`
- Available when `R3.Observable<T>` symbols are present.
- `AsPrimitivesSignal<T>(this R3.Observable<T>)`
- `AsR3Observable<T>(this System.IObservable<T>)`
- If `ReactiveUI.Primitives.Async` is referenced: `AsPrimitivesAsyncObservable<T>(this R3.Observable<T>)`
- If `ReactiveUI.Primitives.Async` is referenced: `AsR3Observable<T>(this IObservableAsync<T>)`

R3Async bridge:

- Generated namespace: `ReactiveUI.Primitives.R3Bridge`
- Available when `R3Async.AsyncObservable<T>`, `R3Async.AsyncObserver<T>`, and `R3Async.Result` symbols are present.
- Requires `ReactiveUI.Primitives.Async`.
- `AsPrimitivesAsyncObservable<T>(this R3Async.AsyncObservable<T>)`
- `AsR3AsyncObservable<T>(this IObservableAsync<T>)`

Bridge example for System.Reactive:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;

IObservable<int> primitives = rxObservable.AsPrimitivesSignal();

using var subscription = primitives
    .Map(value => value + 1)
    .Subscribe(Console.WriteLine);
```

Bridge example for System.Reactive.Async:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.SystemReactiveBridge;

IObservableAsync<int> primitives = systemAsyncObservable.AsPrimitivesAsyncObservable();
System.IAsyncObservable<int> systemAsync = primitives.AsSystemReactiveAsyncObservable();
```

Bridge example for R3:

```csharp
using ReactiveUI.Primitives.R3Bridge;

IObservable<int> primitives = r3Observable.AsPrimitivesSignal();
R3.Observable<int> r3 = primitives.AsR3Observable();
```

Generated bridge classes are internal to the consuming assembly. The extension methods are still usable by code in that assembly after importing the generated namespace.

## System.Reactive Migration

Use this migration path:

1. Add `ReactiveUI.Primitives`.
2. Add `ReactiveUI.Primitives.Async`, `ReactiveUI.Primitives.Extensions`, or UI adapter packages only where needed.
3. Keep System.Reactive package references temporarily only in assemblies that still consume Rx APIs or need bridge adapters.
4. Convert hot sources first: `Subject<T>` to `Signal<T>`, `BehaviorSubject<T>` to `StateSignal<T>` or `BehaviorSignal<T>`, and `ReplaySubject<T>` to `ReplaySignal<T>` or `HistorySignal<T>`.
5. Replace operators with Primitives vocabulary at the same time as touching code.
6. Replace `IScheduler` dependencies with `ISequencer` dependencies. Use generated scheduler bridge adapters only at external Rx boundaries.
7. Replace Rx disposables with Primitives disposables.
8. Remove System.Reactive package references from assemblies once no Rx symbols remain.

System.Reactive factory mapping:

```csharp
// Before
var values = System.Reactive.Linq.Observable.Range(1, 3);

// After
var values = ReactiveUI.Primitives.Signals.Signal.Sequence(1, 3);
```

System.Reactive subject mapping:

```csharp
// Before
var subject = new System.Reactive.Subjects.Subject<string>();

// After
var signal = new ReactiveUI.Primitives.Signals.Signal<string>();
```

State mapping:

```csharp
// Before
var current = new System.Reactive.Subjects.BehaviorSubject<int>(0);

// After
var state = new ReactiveUI.Primitives.Signals.StateSignal<int>(0);
state.Value = 1;
```

Command migration:

```csharp
using ReactiveUI.Primitives.Signals;

var canRun = new StateSignal<bool>(true);
var command = new CommandSignal<string>(
    async cancellationToken =>
    {
        await SaveAsync(cancellationToken);
        return "saved";
    },
    canRun);

using var results = command.Results.Subscribe(Console.WriteLine);
await command.ExecuteAsync(CancellationToken.None);
```

## System.Reactive.Async Migration

Use this migration path for projects that reference `System.Reactive.Async`:

1. Add `ReactiveUI.Primitives.Async`.
2. Keep `System.Reactive.Async` only in assemblies that still expose or consume `System.IAsyncObservable<T>` or `System.IAsyncObserver<T>`.
3. Convert external sources with `AsPrimitivesAsyncObservable()` at the boundary.
4. Convert external observers with `AsPrimitivesAsyncObserver()` when a Primitives pipeline must notify a System.Reactive.Async observer.
5. Keep new pipelines in `IObservableAsync<T>` with `SignalAsync` factories and async operators.
6. Convert back only at public boundaries with `AsSystemReactiveAsyncObservable()` or `AsSystemReactiveAsyncObserver()`.

Mapping:

| System.Reactive.Async | ReactiveUI.Primitives.Async |
| --- | --- |
| `System.IAsyncObservable<T>` | `IObservableAsync<T>` |
| `System.IAsyncObserver<T>` | `IObserverAsync<T>` |
| `SubscribeAsync(observer)` | `SubscribeAsync(observer, cancellationToken)` |
| `OnNextAsync(value)` | `OnNextAsync(value, cancellationToken)` |
| `OnErrorAsync(error)` | `OnCompletedAsync(Result.Failure(error))` at terminal boundaries |
| `OnCompletedAsync()` | `OnCompletedAsync(Result.Success)` |
| custom `AsyncObservable<T>` | `SignalAsync.Create<T>(...)` or `SignalAsync<T>` |

Example:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.SystemReactiveBridge;

IObservableAsync<int> native = systemAsyncSource.AsPrimitivesAsyncObservable();

await using var subscription = await native
    .Keep(value => value > 0)
    .SubscribeAsync(value => Console.WriteLine(value));
```

## R3 Migration

Use this migration path:

1. Add `ReactiveUI.Primitives`.
2. Keep R3 references temporarily only in assemblies that still expose or consume R3 APIs.
3. Convert R3 hot sources to `Signal<T>` or async signals depending on whether observer work is synchronous or asynchronous.
4. Convert `R3.Observable<T>` to BCL `IObservable<T>` with `AsPrimitivesSignal()` at the boundary when the generated bridge is available.
5. Convert Primitives streams back to R3 only at public boundaries that must still expose R3.
6. Prefer `ISequencer` and Primitives time operators for new scheduling code.
7. Remove R3 references from assemblies once no R3 symbols remain.

R3 bridge example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.R3Bridge;

IObservable<int> primitives = r3Source.AsPrimitivesSignal();

using var subscription = primitives
    .Keep(value => value > 0)
    .Map(value => value * 2)
    .Subscribe(Console.WriteLine);
```

Async R3 bridge example:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge;

IObservableAsync<int> asyncSource = r3Source.AsPrimitivesAsyncObservable();
List<int> values = await asyncSource.CollectListAsync();
```

R3Async migration:

1. Add `ReactiveUI.Primitives.Async`.
2. Convert `R3Async.AsyncObservable<T>` to `IObservableAsync<T>` with `AsPrimitivesAsyncObservable()` at the boundary.
3. Convert `IObservableAsync<T>` back to `R3Async.AsyncObservable<T>` only where an existing public API still requires R3Async.
4. Map `R3Async.Result` to `ReactiveUI.Primitives.Result`; the generated bridge handles this automatically.
5. Use `WitnessAsync<T>` for custom Primitives observers that need async disposal and cancellation-aware callbacks.

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge;

IObservableAsync<int> native = r3AsyncSource.AsPrimitivesAsyncObservable();
R3Async.AsyncObservable<int> external = native.AsR3AsyncObservable();
```

## Choosing Sync Or Async

Use `IObservable<T>` and the base package when:

- Values are pushed synchronously.
- Observer work is fast and does not need `await`.
- Existing consumers already use BCL `IObservable<T>`.

Use `IObservableAsync<T>` and the async package when:

- Observers perform asynchronous work.
- Backpressure-like awaiting between observer calls matters.
- Cancellation needs to flow through subscriptions and `OnNextAsync`.
- Completion needs the async `Result` shape, especially when bridging from R3.

## Common Mistakes To Avoid

- Do not rename `Signal<T>` back to `Subject<T>` in Primitives code.
- Do not introduce `System.Reactive.Linq.Observable` just for basic factories; use `ReactiveUI.Primitives.Signals.Signal`.
- Do not introduce `System.Reactive.Async` just for async factories; use `ReactiveUI.Primitives.Async.SignalAsync`.
- Do not expose `IScheduler` in new APIs; expose `ISequencer`.
- Do not mix `SignalAsync<T>` and `Signal<T>` in the same pipeline without an explicit bridge or conversion reason.
- Do not keep bridge conversions in the middle of a pipeline. Convert once at the boundary.
- Do not use UI sequencers from the base package; install the matching UI adapter package.
