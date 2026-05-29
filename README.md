# ReactiveUI.Primitives

ReactiveUI.Primitives is a compact, high-performance reactive library for .NET applications that want Rx-style composition without a runtime dependency on System.Reactive or R3. It keeps the BCL `IObservable<T>` / `IObserver<T>` contracts where they are useful, adds Primitives names for common concepts, and focuses on predictable AOT-friendly code paths with low allocation overhead.

## Goals and design posture

ReactiveUI.Primitives is designed to:

- Provide Rx-style stream creation, subscription, state, scheduling, and composition over `IObservable<T>`.
- Use a distinct vocabulary where it improves clarity: `Signal<T>` instead of `Subject<T>`, `Map` instead of only `Select`, `Keep` instead of only `Where`, `Spark` instead of notification materialization.
- Stay AOT-friendly: no runtime reflection, dynamic code generation, expression compilation, or hidden dependency on System.Reactive/R3 in the production package.
- Minimize allocations in hot paths, including direct single-action subscribers for `Signal<T>` and reusable immutable singleton signals for common return/empty/never cases.
- Support broad production use across modern .NET and .NET Framework base TFMs, with separate integration projects for Windows UI and platform-focused scenarios.
- Allow migration from System.Reactive/R3 through source-generator bridges when the consuming project already references those libraries.

## Table of contents

1. [Install](#install)
2. [Target frameworks and dependencies](#target-frameworks-and-dependencies)
3. [Core model](#core-model)
4. [Creation factories](#creation-factories)
5. [Operators](#operators)
6. [Stateful signals and subject-like types](#stateful-signals-and-subject-like-types)
7. [Sequencers](#sequencers)
8. [Threading, disposal, and error semantics](#threading-disposal-and-error-semantics)
9. [Source-generator bridge behavior](#source-generator-bridge-behavior)
10. [Migration guides](#systemreactive-to-reactiveuiprimitives-migration-guide)
11. [Benchmarks and performance posture](#benchmarks-and-performance-posture)
12. [Repository layout](#repository-layout)
13. [Validation commands](#validation-commands)

## Install

When the package is available on your configured NuGet feed:

```bash
dotnet add package ReactiveUI.Primitives
```

Optional UI/platform integration packages are split out so the base package stays free of UI framework references:

```bash
dotnet add package ReactiveUI.Primitives.Wpf
dotnet add package ReactiveUI.Primitives.WinForms
dotnet add package ReactiveUI.Primitives.Blazor
dotnet add package ReactiveUI.Primitives.Maui
```

Then import the namespaces you need:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
```

The package metadata is configured to include this README in the NuGet package via `PackageReadmeFile=README.md`. The base package also packs both bridge source-generator assemblies under `analyzers/dotnet/cs`:

- `ReactiveUI.Primitives.SystemReactiveBridge.Generator.dll`
- `ReactiveUI.Primitives.R3Bridge.Generator.dll`

Those generators are analyzers. They do not add runtime System.Reactive or R3 dependencies to ReactiveUI.Primitives. They emit bridge code only when the consuming compilation already references the relevant external library symbols.

## Target frameworks and dependencies

The base production `ReactiveUI.Primitives` library uses `$(LibraryTargetFrameworks)` from `src/Directory.Build.props` and currently targets:

- `net8.0`
- `net9.0`
- `net10.0`
- `net462`
- `net472`
- `net481`

Windows UI and platform-integration projects in this repository use their own TFM properties (for example `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, or MAUI/platform-focused TFMs where applicable). Those platform TFMs are not target frameworks of the base `ReactiveUI.Primitives` package.

The optional package TFMs are:

- `ReactiveUI.Primitives.Wpf`: `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net462`, `net472`, `net481`
- `ReactiveUI.Primitives.WinForms`: `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net462`, `net472`, `net481`
- `ReactiveUI.Primitives.Blazor`: `net8.0`, `net9.0`, `net10.0`
- `ReactiveUI.Primitives.Maui`: `net9.0`, `net10.0`

Runtime package dependencies are intentionally small. The base production package does not depend on System.Reactive or R3. The only runtime package reference declared directly by `src/ReactiveUI.Primitives/ReactiveUI.Primitives.csproj` is `System.ValueTuple` for `net462`; the bridge source generators are packed as analyzers in the base package rather than shipped as separate NuGet packages. `ReactiveUI.Primitives.Blazor` references `Microsoft.AspNetCore.Components`, and `ReactiveUI.Primitives.Maui` references `Microsoft.Maui.Core`. The remaining shared package references are analyzer, SourceLink, versioning, ILLink, reference-assembly, or build-time support packages such as Blazor.Common.Analyzers, Microsoft.SourceLink.GitHub, MinVer, Roslynator.Analyzers, SonarAnalyzer.CSharp, stylecop.analyzers, Microsoft.NET.ILLink.Tasks, and Microsoft.NETFramework.ReferenceAssemblies. Benchmark projects may reference System.Reactive and R3 as comparison baselines, but those references are not production dependencies.

## Core model

### `Signal<T>`

`Signal<T>` is the basic subject-like primitive. It implements `ISignal<T>`, which combines `IObserver<T>`, `IObservable<T>`, and `IsDisposed`.

Use it when code needs to push values into a stream and let observers subscribe:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var signal = new Signal<int>();

using IDisposable subscription = signal.Subscribe(
    value => Console.WriteLine($"next: {value}"),
    error => Console.WriteLine($"error: {error.Message}"),
    () => Console.WriteLine("completed"));

signal.OnNext(1);
signal.OnNext(2);
signal.OnCompleted();
```

Important behavior:

- `OnNext(T)` sends a value to active subscribers.
- `OnError(Exception)` terminates the signal with an error.
- `OnCompleted()` terminates the signal successfully.
- `Subscribe(...)` returns `IDisposable`; disposing the subscription unsubscribes.
- `HasObservers` and `IsDisposed` expose basic lifecycle state.
- The `Subscribe(Action<T>)` extension uses an optimized direct-action path for `Signal<T>` when possible.

### Observers and witnesses

ReactiveUI.Primitives keeps the standard `IObserver<T>` shape and provides helper observer implementations internally under the `Core` namespace.

Common user-facing subscription overloads live in `SubscribeMixins`:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var signal = new Signal<string>();

using var nextOnly = signal.Subscribe(value => Console.WriteLine(value));
using var full = signal.Subscribe(
    value => Console.WriteLine(value),
    error => Console.Error.WriteLine(error),
    () => Console.WriteLine("done"));
```

The library uses the term witness for lightweight observer wrappers. You normally use delegates or `IObserver<T>` directly rather than constructing witness types by hand.

### Disposables, handles, and slots

Subscriptions and scheduled work return `IDisposable`. ReactiveUI.Primitives includes lightweight disposable primitives in `ReactiveUI.Primitives.Disposables`:

| Type | Use |
|---|---|
| `Disposable.Create(Action)` | Create an `IDisposable` from a cleanup action. |
| `Disposable.Empty` | No-op disposable. |
| `BooleanDisposable` | Track simple disposed state. |
| `CancellationDisposable` | Tie disposal to a `CancellationTokenSource`. |
| `MultipleDisposable` | Composite-disposable equivalent; add/remove multiple disposables. |
| `CompositeDisposable` | System.Reactive-compatible alias over `MultipleDisposable`. |
| `Pocket` | Named `MultipleDisposable` specialization. |
| `SingleDisposable` / `AssignmentSlot` | Single-assignment disposable container. |
| `SingleReplaceableDisposable` / `Slot` | Replaceable disposable container. |
| `Handle`, `Handle<T>`, `Handle<T1,T2>`, `Handle<T1,T2,T3>` | Lightweight handle wrappers for resource lifetimes. |

Example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

var subscriptions = new MultipleDisposable();
var signal = new Signal<int>();

signal.Subscribe(value => Console.WriteLine(value)).DisposeWith(subscriptions);
signal.Subscribe(value => Console.WriteLine(value * 10)).DisposeWith(subscriptions);

signal.OnNext(3);
subscriptions.Dispose();
```

## Creation factories

Creation APIs live on `ReactiveUI.Primitives.Signals.Signal`.

| Factory | Purpose |
|---|---|
| `Signal.Create<T>(Func<IObserver<T>, IDisposable>)` | Build a custom observable. |
| `Signal.CreateSafe<T>(Func<IObserver<T>, IDisposable>)` | Build a custom observable with safety wrapping. |
| `Signal.CreateWithState<T,TState>(...)` | Build a custom observable while passing state explicitly. |
| `Signal.Lazy<T>(Func<IObservable<T>>)` | Create the source per subscription. |
| `Signal.Emit<T>(T)` | Emit one value and complete. Specialized fast paths exist for `bool`, `int`, and `RxVoid`. |
| `Signal.None<T>()` | Complete without values. |
| `Signal.Silent<T>()` / `Signal.Silent<T>(T witness)` | Never emit and never complete. |
| `Signal.Fail<T>(Exception)` | Terminate with an error. |
| `Signal.Sequence(int start, int count)` | Emit an integer range and complete. |
| `Signal.Loop<T>(T value)` / `Signal.Loop<T>(T value, int count)` | Repeat indefinitely or a fixed number of times. |
| `Signal.Unfold<TState,TResult>(...)` / `Signal.Iterate<TState,TResult>(...)` | Generate a finite sequence from state. |
| `Signal.Use<TResource,T>(...)` | Tie a resource lifetime to a subscription. |
| `Signal.FromEventPattern(...)` | Convert .NET events to `EventPattern<TEventArgs>` values. |
| `Signal.FromEnumerable<T>(IEnumerable<T>)` | Convert an enumerable. |
| `Signal.FromEnumerable<T>(IEnumerable<T>, CancellationToken)` | Convert an enumerable and stop synchronous enumeration when cancelled. |
| `Signal.FromAsyncEnumerable<T>(IAsyncEnumerable<T>, CancellationToken)` | Convert an async enumerable on modern TFMs. |
| `Signal.FromTask<T>(Task<T>)` | Convert a task to a signal. |
| `Signal.FromAsync<T>(...)` | Invoke a task factory per subscription. |
| `Signal.After(TimeSpan, ISequencer?)` | Emit one `long` tick after a delay. |
| `Signal.Every(TimeSpan, ISequencer?)` | Emit increasing `long` ticks repeatedly. |
| `Signal.Pulse(...)` | Alias of `Every`. |
| `Signal.After(...)` | One-shot and periodic timer overloads. |
| `Signal.Chain(...)`, `Signal.Blend(...)`, `Signal.Race(...)` | Compose multiple sources. |
| `Signal.Pair(...)`, `Signal.SyncLatest(...)`, `Signal.PairLatest(...)`, `Signal.ForkJoin(...)` | Pairwise combination helpers. |

Example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<int> values = Signal.Sequence(1, 5);

using var subscription = values.Subscribe(
    value => Console.WriteLine(value),
    error => Console.Error.WriteLine(error),
    () => Console.WriteLine("range completed"));
```

Custom source example:

```csharp
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

IObservable<string> source = Signal.CreateSafe<string>(observer =>
{
    observer.OnNext("ready");
    observer.OnCompleted();
    return Disposable.Empty;
});
```

## Operators

Operators are extension methods over `IObservable<T>`. ReactiveUI.Primitives uses a distinct vocabulary for operators that would otherwise collide with System.Reactive or R3.

### Transformation and filtering

| System.Reactive-style concept | ReactiveUI.Primitives API |
|---|---|
| `Select` | `Map` | Prefer `Map` for the distinct Primitives style. |
| stateful `Select` without closure | `MapWith` |
| `Where` | `Keep` |
| stateful `Where` without closure | `KeepWith` |
| non-null filtering | `KeepNotNull` |
| `OfType` / `Cast` | `KeepType<TResult>` / `CastTo<TResult>` |
| side effects | `Tap`, `TapWith` |
| `Scan` | `Fold` |
| `Aggregate` | `Reduce` |
| `Distinct` | `Distinct` |
| `DistinctUntilChanged` | `Unique` |
| key-based distinct | `DistinctBy`, `UniqueBy` |
| `Take` / `Skip` | `Take`, `Skip` |
| `TakeWhile` / `SkipWhile` | `TakeWhile`, `SkipWhile` |
| `IgnoreElements` | `IgnoreValues` |
| `DefaultIfEmpty` | `DefaultIfEmpty` |

Example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<string> labels = Signal.Sequence(1, 10)
    .Keep(value => value % 2 == 0)
    .Map(value => $"even:{value}")
    .Tap(label => Console.WriteLine($"observed {label}"));

using var subscription = labels.Subscribe(Console.WriteLine);
```

### Composition

| Concept | API |
|---|---|
| sequential concatenation | `Chain` |
| concurrent merge | `Blend` |
| first source wins | `Race` |
| latest inner source wins | `SwitchTo` |
| pairwise zip | `Pair` |
| latest-value combination | `SyncLatest` |
| combine left emission with latest right value | `Latch` |
| latest-fusion alias | `PairLatest`, `FuseLatest` |
| last values after both complete | `ForkJoin` |
| retry | `Reattempt` |
| catch/rescue | `Recover`, `Rescue`, `Resume`, `Signal.Recover` |
| final action | `Signal.OnCleanup` |

Blend example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<int> low = Signal.Sequence(1, 3);
IObservable<int> high = Signal.Sequence(100, 3);

using var merged = Signal.Blend(low, high)
    .Subscribe(value => Console.WriteLine(value));
```

SyncLatest example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var width = new StateSignal<int>(640);
var height = new StateSignal<int>(480);

using var area = Signal.SyncLatest(width, height, (w, h) => w * h)
    .Subscribe(value => Console.WriteLine($"area={value}"));

width.Value = 800;
height.Value = 600;
```

### Time, buffering, and async helpers

| Concept | API |
|---|---|
| delayed subscription | `DelayStart` |
| delayed values | `Shift` |
| quiet-period sampling | `Calm` / `Stabilize` |
| periodic sampling | `Probe` |
| timeout | `Expire` |
| schedule subscription | `SubscribeOn` |
| timestamp values | `Timestamp` |
| measure intervals | `TimeInterval` |
| fixed-size buffers | `Buffer(count)`, `Buffer(count, skip)` |
| collect to list/array signal | `CollectList`, `CollectArray`, `ToList`, `ToArray` |
| collect asynchronously | `CollectListAsync`, `CollectArrayAsync`, `ToListAsync`, `ToArrayAsync` |
| first/last value task | `FirstAsync`, `FirstOrDefaultAsync`, `LastAsync`, `LastOrDefaultAsync` |

After example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

using var subscription = Signal.After(
        dueTime: TimeSpan.FromMilliseconds(250),
        period: TimeSpan.FromSeconds(1),
        scheduler: ThreadPoolSequencer.Instance)
    .Take(3)
    .Subscribe(
        tick => Console.WriteLine($"tick {tick}"),
        error => Console.Error.WriteLine(error),
        () => Console.WriteLine("timer completed"));
```

### Spark materialization

`Spark<T>` represents value/error/completion notifications. Use `Spark` to convert stream events into values and `Unspark` to turn them back into observer notifications.

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

IObservable<Spark<int>> sparks = Signal.Sequence(1, 3).Spark();
IObservable<int> values = sparks.Unspark();
```

## Stateful signals and subject-like types

ReactiveUI.Primitives uses explicit names instead of cloning every System.Reactive subject type name.

| System.Reactive type | ReactiveUI.Primitives equivalent | Notes |
|---|---|---|
| `Subject<T>` | `Signal<T>` | Push values, errors, and completion to subscribers. |
| `BehaviorSubject<T>` | `StateSignal<T>` | Stores the latest value, exposes a mutable `Value`, and emits changes through `Changed`. |
| `ReplaySubject<T>` | `HistorySignal<T>` | Replays buffered values by size and/or time window. |
| `AsyncSubject<T>` | `FinalSignal<T>` | Awaitable subject-like signal; also implements `IAwaitSignal<T>`. |
| `ReactiveProperty<T>` / state holder | `StateSignal<T>` plus `ReadOnlyState<T>` | Mutable state and read-only projected state. |

State example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var temperature = new StateSignal<double>(21.5);
ReadOnlyState<string> status = temperature.ToReadOnlyState(value =>
    value >= 25.0 ? "warm" : "normal");

using var stateSubscription = status.Changed.Subscribe(Console.WriteLine);

temperature.Value = 26.2;
temperature.Refresh();
```

History example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var history = new HistorySignal<string>(bufferSize: 2);
history.OnNext("A");
history.OnNext("B");
history.OnNext("C");

using var subscription = history.Subscribe(Console.WriteLine); // replays B, C
```

Error and completion example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<int> failed = Signal.Fail<int>(new InvalidOperationException("not available"));

using var subscription = failed.Subscribe(
    value => Console.WriteLine(value),
    error => Console.WriteLine($"failed: {error.Message}"),
    () => Console.WriteLine("completed"));
```

## Sequencers

Sequencers live in `ReactiveUI.Primitives.Concurrency` and implement `ISequencer`. The core `ReactiveUI.Primitives` package does not reference WPF, Windows Forms, Blazor, or MAUI; UI-thread sequencers are provided by optional integration packages.

| Sequencer | Purpose |
|---|---|
| `Sequencer.Immediate` / `ImmediateSequencer.Instance` | Execute work immediately. |
| `Sequencer.CurrentThread` / `CurrentThreadSequencer.Instance` | Queue recursive/current-thread work deterministically. |
| `ThreadPoolSequencer.Instance` | Schedule work through the thread pool. |
| `TaskPoolSequencer.Instance` | Schedule work through tasks. |
| `SynchronizationContextSequencer` | Schedule through a `SynchronizationContext`. |
| `DispatcherSequencer` | Schedule onto a WPF dispatcher from `ReactiveUI.Primitives.Wpf`. |
| `ControlSequencer` | Schedule onto a Windows Forms control from `ReactiveUI.Primitives.WinForms`. |
| `BlazorRendererSequencer` | Schedule component work through Blazor's renderer from `ReactiveUI.Primitives.Blazor`. |
| `MauiDispatcherSequencer` | Schedule onto an MAUI dispatcher from `ReactiveUI.Primitives.Maui`. |
| `VirtualClock` / `TestClock` | Virtual-time scheduling for deterministic tests. |

Scheduling APIs include absolute, relative, recursive, and action-based overloads:

```csharp
using ReactiveUI.Primitives.Concurrency;

IDisposable scheduled = ThreadPoolSequencer.Instance.Schedule(
    TimeSpan.FromMilliseconds(100),
    () => Console.WriteLine("scheduled work"));

scheduled.Dispose();
```

Use virtual clocks for deterministic time-sensitive tests rather than sleeping a real thread.

## Threading, disposal, and error semantics

ReactiveUI.Primitives follows the BCL observer contract and keeps ownership explicit:

- `OnNext` is delivered synchronously on the thread that invokes it unless an operator or sequencer explicitly schedules work elsewhere.
- Time-based factories and operators use `ISequencer` overloads where deterministic or UI-thread dispatch matters. Use `TestClock`/`VirtualClock` for tests; avoid sleeping real threads.
- A subscription is an `IDisposable`. Disposing a subscription removes that observer and prevents later notifications to that subscription. Disposing a composite (`MultipleDisposable`, `Pocket`, `Slot`, etc.) cascades to contained disposables according to the container contract.
- Terminal notifications are single-assignment: `OnCompleted` and `OnError` end a signal, and later values are ignored by terminated sources.
- `OnError(Exception)` requires a non-null exception and propagates the terminal error to current subscribers. Operators such as `Recover`, `Rescue`, `Resume`, `Reattempt`, and `Signal.Recover` are the explicit recovery points.
- Observer callback exceptions are guarded by the operator/source that owns the callback. Prefer `CreateSafe` for custom sources unless you are deliberately implementing lower-level observer semantics.
- The production package has no runtime dependency on System.Reactive or R3; bridge generators only emit boundary adapters when a consuming project already references those packages.

## Source-generator bridge behavior

The base package includes two bridge generators as analyzers:

- System.Reactive bridge generator.
- R3 bridge generator.

The generators always emit small internal marker attributes. They emit bridge extension methods only when the consumer project already references the relevant external library:

- System.Reactive bridge checks for `System.Reactive.Linq.Observable`.
- R3 bridge checks for `R3.Observable<T>`.

Generated bridge namespaces:

- `ReactiveUI.Primitives.SystemReactiveBridge`
- `ReactiveUI.Primitives.R3Bridge`

Generated System.Reactive bridge methods:

- `AsPrimitivesSignal<T>(this System.IObservable<T> source)`
- `AsSystemObservable<T>(this System.IObservable<T> source)`

Generated R3 bridge methods:

- `AsPrimitivesSignal<T>(this R3.Observable<T> source)`
- `AsR3Observable<T>(this System.IObservable<T> source)`

System.Reactive bridge example, when the consuming project already references System.Reactive:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive.Linq;

IObservable<int> rxSource = Observable.Range(1, 3);
IObservable<int> PrimitivesSource = rxSource.AsPrimitivesSignal();

using var subscription = PrimitivesSource
    .Map(value => value * 10)
    .Subscribe(Console.WriteLine);

IObservable<int> systemObservable = Signal.Sequence(1, 3).AsSystemObservable();
```

R3 bridge example, when the consuming project already references R3:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.R3Bridge;
using ReactiveUI.Primitives.Signals;

// R3.Observable<int> r3Source = ...;
// IObservable<int> PrimitivesSource = r3Source.AsPrimitivesSignal();
// R3.Observable<int> r3Again = Signal.Sequence(1, 3).AsR3Observable();
```

The R3 snippet is intentionally shown as a migration shape because it requires the consuming application to reference R3. ReactiveUI.Primitives itself remains free of an R3 runtime dependency.

## System.Reactive to ReactiveUI.Primitives migration guide

ReactiveUI.Primitives is not a byte-for-byte clone of System.Reactive. It keeps the standard `IObservable<T>` contracts but favors a smaller runtime, explicit state types, and Primitives naming. Migrate one vertical slice at a time: factories first, then subject/state types, then operators and schedulers.

### Factory mapping

| System.Reactive | ReactiveUI.Primitives | Notes |
|---|---|---|
| `Observable.Return(value)` | `Signal.Emit(value)` | Emits one value and completes. |
| `Observable.Empty<T>()` | `Signal.None<T>()` | Completes immediately. |
| `Observable.Never<T>()` | `Signal.Silent<T>()` or `Signal.Silent<T>(witness)` | Non-terminating signal; witness overload helps type inference. |
| `Observable.Throw<T>(ex)` | `Signal.Fail<T>(ex)` | Emits terminal error. |
| `Observable.Range(start, count)` | `Signal.Sequence(start, count)` | Optional scheduler overload exists. |
| `Observable.Repeat(value)` | `Signal.Loop(value)` | Indefinite repeat. |
| `Observable.Repeat(value, count)` | `Signal.Loop(value, count)` | Fixed repeat. |
| `Observable.Defer(factory)` | `Signal.Lazy(factory)` | Create source per subscription. |
| `Observable.FromAsync(...)` | `Signal.FromAsync(...)` | Invoke a task factory per subscription. |
| `Observable.Create<T>(...)` | `Signal.Create<T>(...)` or `Signal.CreateSafe<T>(...)` | Prefer `CreateSafe` for general custom sources. |
| `Observable.Using(...)` | `Signal.Use(...)` | Resource scoped to subscription. |
| `Observable.Timer(dueTime)` | `Signal.After(dueTime)` | Emits `long` tick `0`. |
| `Observable.Timer(dueTime, period)` | `Signal.After(dueTime, period)` | Periodic `long` ticks. |
| `Observable.Interval(period)` | `Signal.Pulse(period)` or `Signal.Every(period)` | Repeating ticks. |
| `ToObservable()` from enumerable | `Signal.FromEnumerable(values)`, `values.ToSignal()`, or `values.ToObservable()` | Cancellation-token overloads are available. |
| task conversion | `Signal.FromTask(task)` | Function-based task signals also exist. |

### Subject/state mapping

| System.Reactive | ReactiveUI.Primitives | Migration detail |
|---|---|---|
| `new Subject<T>()` | `new Signal<T>()` | Use `OnNext`, `OnError`, `OnCompleted`, and `Subscribe`. |
| `new BehaviorSubject<T>(initial)` | `new StateSignal<T>(initial)` | Keeps `Value` getter/setter and emits changes through `Changed`. |
| mutable reactive property | `new StateSignal<T>(initial)` | Set `Value` to emit. Use `Changed` for observable state stream. |
| `new ReplaySubject<T>()` | `new HistorySignal<T>()` | Unbounded replay. |
| `new ReplaySubject<T>(bufferSize)` | `new HistorySignal<T>(bufferSize)` | Size-limited replay. |
| `new ReplaySubject<T>(window)` | `new HistorySignal<T>(window)` | Time-window replay. |
| `new AsyncSubject<T>()` | `new FinalSignal<T>()` | Awaitable final-value signal shape. |

### Operator mapping

| System.Reactive | ReactiveUI.Primitives | Notes |
|---|---|---|
| `Select` | `Map` | Prefer `Map` for distinct Primitives style. |
| `Where` | `Keep` | Predicate filtering. |
| `SelectMany` | `FlatMap` or `Bind` | `Bind` is a Primitives alias for flat mapping. |
| `Aggregate` | `Reduce` | Emits final accumulated value on completion. |
| `Scan` | `Fold` | Emits every accumulated value. |
| `Do` | `Tap` | Side effect while preserving values. |
| `Take` / `Skip` | `Take` / `Skip` | Count-based overloads. |
| `TakeWhile` / `SkipWhile` | `TakeWhile` / `SkipWhile` | Predicate-based. |
| `Distinct` | `Distinct` | Full seen-set distinct. |
| `DistinctUntilChanged` | `Unique` | Adjacent dedupe. |
| `OfType` / `Cast` | `KeepType` / `CastTo` | Object-source projections. |
| `Materialize` | `Spark` | Converts notifications into `Spark<T>`. |
| `Dematerialize` | `Unspark` | Converts `Spark<T>` values back into notifications. |
| `Merge` | `Blend` or `Signal.Blend` | Works over source-of-sources and params factories. |
| `Concat` | `Chain` or `Signal.Chain` | Sequential composition. |
| `Amb` | `Race` | First source to produce a value or terminal signal wins. |
| `Switch` | `SwitchTo` | Latest inner observable wins. |
| `Zip` | `Pair` or `Signal.Pair` | Pair values by index. |
| `CombineLatest` | `SyncLatest` or `Signal.SyncLatest` | Latest values after both sources have emitted. |
| `WithLatestFrom` | `Latch` | Left emission paired with latest right value. |
| `ForkJoin` | `ForkJoin` | Last values after completion. |
| `Throttle` | `Calm` / `Stabilize` | Quiet-period emission. |
| `Sample` | `Probe` | Periodic latest-value sampling. |
| `Delay` | `Shift` | Delay emitted values. |
| `DelaySubscription` | `DelayStart` | Delay source subscription. |
| `Timeout` | `Expire` | Error on missing value before due time. |
| `Buffer(count)` | `Buffer(count)` | Fixed-size buffers. |
| `SubscribeOn` | `SubscribeOn` | Schedule source subscription. |
| `ToList` / `ToArray` | `ToList` / `ToArray` or `CollectList` / `CollectArray` | Signal results. |
| `FirstAsync` / `LastAsync` | `FirstAsync` / `LastAsync` | Task result. |
| `CountAsync` / `AnyAsync` | `CountAsync` / `AnyAsync` | Task-shaped terminal helpers, including cancellation overloads. |

### Disposable mapping

| System.Reactive | ReactiveUI.Primitives |
|---|---|
| `Disposable.Create` | `Disposable.Create` |
| `Disposable.Empty` | `Disposable.Empty` |
| `BooleanDisposable` | `BooleanDisposable` |
| `CancellationDisposable` | `CancellationDisposable` |
| `CompositeDisposable` | `MultipleDisposable` or `Pocket` |
| `SerialDisposable` | `SingleReplaceableDisposable` or `Slot` |
| `SingleAssignmentDisposable` | `SingleDisposable` or `AssignmentSlot` |
| `IDisposable.Dispose()` | unchanged |

### Sequencer mapping

| System.Reactive scheduler concept | ReactiveUI.Primitives scheduler |
|---|---|
| `ImmediateSequencer.Instance` | `Sequencer.Immediate` or `ImmediateSequencer.Instance` |
| `CurrentThreadSequencer.Instance` | `Sequencer.CurrentThread` or `CurrentThreadSequencer.Instance` |
| `ThreadPoolSequencer.Instance` | `ThreadPoolSequencer.Instance` |
| task-pool scheduling | `TaskPoolSequencer.Instance` |
| synchronization-context scheduling | `SynchronizationContextSequencer` |
| WPF dispatcher scheduling | `DispatcherSequencer` from `ReactiveUI.Primitives.Wpf` |
| Windows Forms control scheduling | `ControlSequencer` from `ReactiveUI.Primitives.WinForms` |
| Blazor renderer scheduling | `BlazorRendererSequencer` from `ReactiveUI.Primitives.Blazor` |
| MAUI dispatcher scheduling | `MauiDispatcherSequencer` from `ReactiveUI.Primitives.Maui` |
| `TestScheduler` / virtual time | `VirtualClock` or `TestClock` |

### Testing migration

System.Reactive test code commonly uses `TestScheduler` and marble helpers. ReactiveUI.Primitives currently exposes virtual-time primitives rather than cloning the full Rx testing API. Prefer repository-native tests that:

- Use `TestClock` / `VirtualClock` for deterministic scheduling.
- Assert values collected through `Subscribe` delegates.
- Dispose subscriptions explicitly.
- Use `CollectArrayAsync`, `CollectListAsync`, or `FirstAsync` when a task-shaped assertion is clearer.

## R3 migration notes

R3 uses its own `Observable<T>` type and observer model. ReactiveUI.Primitives stays on the BCL `IObservable<T>` shape for runtime interoperability.

| R3 concept | ReactiveUI.Primitives equivalent |
|---|---|
| `R3.Observable<T>` | BCL `IObservable<T>` from ReactiveUI.Primitives factories/operators. |
| R3 subject | `Signal<T>` / `StateSignal<T>` / `HistorySignal<T>` depending on state/replay needs. |
| R3 `Select` / `Where` | `Map` / `Keep`. |
| R3 time operators | `Signal.After`, `Signal.Pulse`, `Calm`, `Probe`, `Shift`, scheduler overloads. |
| R3 bridge | Generated `AsPrimitivesSignal` / `AsR3Observable` when R3 is referenced by the consumer. |

Use the generated bridge only at boundaries. Prefer native ReactiveUI.Primitives operators inside new code.

## Benchmarks and performance posture

Benchmarks live in `src/benchmarks/ReactiveUI.Primitives.Benchmarks`. The benchmark project may reference System.Reactive and R3 to compare throughput and allocation behavior; the production package must not.

The latest complete BenchmarkDotNet run finished on 2026-05-29 at 00:04:23 Europe/London with .NET SDK/runtime 10.0.8 on Windows 11. It executed 201 benchmarks with no skipped suites in 00:21:18:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Latest artifact paths:

- `BenchmarkDotNet.Artifacts/BenchmarkRun-20260528-234302.log`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-29-00-04-23-report-github.md`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-29-00-04-23-report.html`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-29-00-04-23-report.csv`

Smoke validation for deterministic benchmark behavior passed for 67 benchmark groups with:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --smoke
```

The latest available direct test/coverage validation is 96.02% line coverage and 91.46% branch coverage from `src/tests/ReactiveUI.Primitives.Tests/TestResults/coverage-kanban-t_f3f3db71-20260528-final/coverage.cobertura.xml`. The original final-review target was 100% line and branch coverage; the remaining gap is a signed-off release exception rather than a passing 100% coverage result.

The table below is generated from the joined BenchmarkDotNet CSV and uses `Mean / Allocated` for each cell.

| Scenario | ReactiveUI.Primitives | System.Reactive | R3 |
|---|---:|---:|---:|
| Emit subscribe | 0.0057 ns / 0 B | 46.9118 ns / 120 B | 29.9493 ns / 80 B |
| None subscribe | 2.7336 ns / 40 B | 42.2124 ns / 96 B | 26.3975 ns / 56 B |
| Sequence subscribe | 46.1797 ns / 96 B | 2,422.0697 ns / 2472 B | 69.1228 ns / 80 B |
| Loop subscribe | 6.7699 ns / 0 B | 2,408.7297 ns / 2408 B | 69.0206 ns / 80 B |
| Fail subscribe | 54.1509 ns / 120 B | 102.8304 ns / 240 B | 87.5483 ns / 200 B |
| FromEnumerable subscribe | 51.2591 ns / 40 B | 2,275.3497 ns / 2504 B | 72.6697 ns / 88 B |
| Completed task bridge | 10.0635 ns / 88 B | 740.5938 ns / 793 B | 33.1432 ns / 88 B |
| Create subscribe | 48.6427 ns / 248 B | 43.5512 ns / 168 B | 53.1627 ns / 128 B |
| CreateSafe subscribe | 48.7596 ns / 248 B | 43.4466 ns / 168 B | 56.0799 ns / 128 B |
| Lazy subscribe | 71.4885 ns / 240 B | 1,357.2984 ns / 1512 B | 107.0656 ns / 152 B |
| Start subscribe | 52.2960 ns / 376 B | 791.8958 ns / 751 B | 53.1907 ns / 160 B |
| Unfold subscribe | 167.6973 ns / 680 B | 2,138.4558 ns / 2768 B | 85.8050 ns / 128 B |
| Use subscribe | 67.8147 ns / 432 B | 77.0960 ns / 168 B | 54.1505 ns / 128 B |
| FromAsyncEnumerable subscribe | 1,043.0716 ns / 600 B | 1,753.0905 ns / 2447 B | 1,234.0619 ns / 1023 B |
| Silent subscribe/dispose | 0.2325 ns / 0 B | 5.1319 ns / 40 B | 16.5168 ns / 56 B |
| Map + Keep over range | 123.8707 ns / 208 B | 2,428.8466 ns / 2616 B | 273.2223 ns / 272 B |
| Reduce + Any + Count | 179.4248 ns / 824 B | 5,336.3782 ns / 6352 B | 521.8730 ns / 1280 B |
| Prepend + Append + DefaultIfEmpty | 34.4296 ns / 168 B | 863.5180 ns / 1282 B | 131.1020 ns / 288 B |
| DefaultIfEmpty(empty) | 6.2632 ns / 64 B | 63.0172 ns / 144 B | 61.9132 ns / 136 B |
| FlatMap over ranges | 992.7952 ns / 712 B | 3,479.3261 ns / 3872 B | 1,017.6085 ns / 1040 B |
| Pair over ranges | 43.0110 ns / 232 B | 3,072.1161 ns / 2976 B | 664.5803 ns / 656 B |
| Chain ranges | 68.8759 ns / 256 B | 2,697.8110 ns / 2856 B | 259.8491 ns / 360 B |
| Blend ranges | 64.7049 ns / 256 B | 3,553.1522 ns / 3953 B | 651.8224 ns / 352 B |
| Race ranges | 39.1994 ns / 192 B | 1,468.6431 ns / 1760 B | 263.5987 ns / 360 B |
| SwitchTo ranges | 826.2645 ns / 1376 B | 2,078.3363 ns / 2336 B | 709.2868 ns / 392 B |
| SyncLatest ranges | 106.3367 ns / 504 B | 2,940.6474 ns / 2824 B | 627.6091 ns / 344 B |
| Latch ranges | 100.8010 ns / 504 B | 3,277.1620 ns / 2824 B | 339.8355 ns / 248 B |
| ForkJoin ranges | 77.9202 ns / 480 B | 3,322.5338 ns / 3136 B | 888.1171 ns / 504 B |
| Shift range | 135.2277 ns / 472 B | 5,406.6544 ns / 39584 B | 1,841.9540 ns / 2200 B |
| DelayStart range | 128.0190 ns / 472 B | 2,101.8070 ns / 26456 B | 286.8118 ns / 552 B |
| Calm burst | 573.9559 ns / 1200 B | 2,237.3107 ns / 36480 B | 1,511.0511 ns / 1512 B |
| Probe latest | 200.6871 ns / 584 B | 1,899.5578 ns / 26264 B | 339.7303 ns / 664 B |
| Timestamp range | 37.0613 ns / 144 B | 1,614.2007 ns / 1512 B | 331.7774 ns / 152 B |
| TimeInterval range | 27.9223 ns / 152 B | 1,693.1128 ns / 1616 B | 421.0940 ns / 160 B |
| Expire idle | 238.2700 ns / 648 B | 1,284.2471 ns / 29776 B | 403.0234 ns / 784 B |
| ObserveOn immediate | 23.3238 ns / 96 B | 15,659.1095 ns / 11307 B | 900.4654 ns / 432 B |
| History subscribe | 343.3323 ns / 320 B | 713.3733 ns / 696 B | 407.9522 ns / 688 B |
| StateSignal 32 values | 556.9331 ns / 176 B | 592.0564 ns / 200 B | 630.1859 ns / 192 B |
| StateSignal 1024 values | 15,802.3885 ns / 176 B | 15,678.0589 ns / 200 B | 16,769.1864 ns / 192 B |
| Signal emit, 32 values | 73.1036 ns / 136 B | 90.1915 ns / 136 B | 113.2880 ns / 160 B |
| Signal emit, 1024 values | 1,660.0302 ns / 136 B | 1,872.2655 ns / 136 B | 1,945.5030 ns / 160 B |
| Signal subscribe/dispose, 8 observers | 242.0847 ns / 592 B | 283.6230 ns / 1288 B | 447.5004 ns / 840 B |
| Signal subscribe/dispose, 64 observers | 3,168.8057 ns / 3800 B | 3,773.8536 ns / 38472 B | 3,517.6727 ns / 6216 B |
| ShareLive connect | 125.9119 ns / 384 B | 2,517.3733 ns / 2696 B | 382.2815 ns / 368 B |
| Share live subscribe | 217.9249 ns / 848 B | 2,680.1589 ns / 2880 B | 477.0529 ns / 488 B |
| Replay live late subscribe | 620.7328 ns / 568 B | 3,491.4185 ns / 3408 B | 806.9012 ns / 1360 B |
| AutoShare subscribe | 251.3467 ns / 848 B | 2,805.1629 ns / 2880 B | 444.0351 ns / 488 B |
| AutoConnect subscribe | 169.8557 ns / 728 B | 2,546.2257 ns / 2736 B | 369.0084 ns / 368 B |
| StateSignal updates | 584.5012 ns / 176 B | 568.2254 ns / 200 B | 615.4063 ns / 192 B |
| ReadOnlyState projection | 136.4027 ns / 248 B | 86.8555 ns / 328 B | 173.5577 ns / 312 B |
| TaskSignal subscribe | 2,737.5769 ns / 3878 B | 707.6171 ns / 886 B | 39.8419 ns / 160 B |
| Command execute | 127.2999 ns / 600 B | 677.8387 ns / 1089 B | 98.3740 ns / 296 B |
| Command result subscribe | 144.8945 ns / 672 B | 36.4350 ns / 136 B | 61.9539 ns / 160 B |
| CollectList range | 112.6690 ns / 688 B | 2,586.5353 ns / 3488 B | 148.3569 ns / 632 B |
| CollectArray range | 82.7140 ns / 656 B | 2,557.7999 ns / 3640 B | 157.9993 ns / 784 B |
| CollectArrayAsync range | 32.7851 ns / 384 B | 2,661.9972 ns / 3984 B | 159.1573 ns / 784 B |
| FirstAsync range | 5.6597 ns / 56 B | 2,361.3532 ns / 2792 B | 70.1823 ns / 208 B |
| ToTask range | 14.5996 ns / 192 B | 2,372.8129 ns / 2824 B | 88.6755 ns / 208 B |
| Count(predicate) range | 18.9800 ns / 96 B | 2,386.6117 ns / 2520 B | 96.0697 ns / 200 B |
| LongCount(predicate) range | 18.7174 ns / 104 B | 2,357.4243 ns / 2536 B | 99.2778 ns / 272 B |
| All range | 17.0749 ns / 96 B | 2,371.7189 ns / 2520 B | 81.2336 ns / 192 B |
| Contains range | 9.4992 ns / 96 B | 2,446.4642 ns / 2528 B | 89.9842 ns / 200 B |
| All + Contains range | 27.2560 ns / 192 B | 5,141.4378 ns / 5048 B | 209.5712 ns / 392 B |
| Pocket dispose | 59.5054 ns / 408 B | 80.7846 ns / 512 B | 77.7838 ns / 480 B |
| CurrentThread schedule | 13.0715 ns / 88 B | 15.7599 ns / 88 B | 28.5282 ns / 56 B |
| Safe witness | 21.8713 ns / 168 B | 12.3352 ns / 136 B | 18.2902 ns / 56 B |
| Completed Spark | 0.0000 ns / 0 B | 0.0162 ns / 0 B | 0.0000 ns / 0 B |

The five rows selected from the improvement review as the main time/scheduler optimization gate were `Shift range`, `DelayStart range`, `Calm burst`, `Probe latest`, and `Expire idle`. In the complete run all five beat both System.Reactive and R3 on mean time and allocation. The same optimization pass also brought `Timestamp range`, `TimeInterval range`, and `ObserveOn immediate` below both alternatives on mean time and allocation.

Interpretation notes:

- ReactiveUI.Primitives remains substantially faster and lower allocation in most factory, range, terminal collection, composition, sharing, time/scheduler, and subject subscription scenarios.
- The public API/operator matrix above is backed by deterministic smoke coverage in `Program.RunSmokeBenchmarksAsync`: every row has matching ReactiveUI.Primitives, System.Reactive, and R3 calls where alternatives exist, and the smoke run validates each benchmark path returns the same observable result before BenchmarkDotNet measures throughput and allocation unless the row is one of the documented scheduling-total exceptions below.
- The only intentional smoke output differences are `SwitchTo ranges`, `SyncLatest ranges`, and `Latch ranges`: System.Reactive produces different synchronous range totals for those coordinator operators, while ReactiveUI.Primitives and R3 agree on the emitted totals. `--smoke` permits only those System.Reactive differences and still fails if ReactiveUI.Primitives diverges from R3 or if any other benchmark group loses parity.
- Candidate scenarios where ReactiveUI.Primitives is not strictly both faster and lower-allocation than both alternatives are tracked explicitly below. Rows marked as exceptions are retained because the extra cost buys ReactiveUI.Primitives semantics (safe terminal/disposal behavior, `IObservable<T>`/`IObserver<T>` compatibility, deterministic `ISequencer` scheduling, or live-signal lifecycle ownership) while preserving the project rule that System.Reactive/R3 are benchmark-only dependencies.
- Near-zero singleton measurements (`Emit`, `Silent`, and `Spark` paths) may trigger BenchmarkDotNet `ZeroMeasurement` warnings; those warnings mean the method duration is indistinguishable from the empty-method overhead, not that the benchmark failed.

Candidate/performance exception matrix:

| Scenario | Observed gap | Decision and trade-off |
|---|---|---|
| `Sequence subscribe` | allocation > R3 (96 B vs 80 B). | Accepted exception: ReactiveUI.Primitives keeps BCL observable compatibility, safe disposal, and explicit scheduler/resource ownership; the remaining narrow allocation/time gap is documented. |
| `Completed task bridge` | allocation ties R3 (88 B vs 88 B). | Accepted exception: the bridge already has the lowest mean and ties R3 allocation while preserving task-observer completion semantics. |
| `Create subscribe` | time >= System.Reactive (48.6427 ns vs 43.5512 ns); allocation > System.Reactive (248 B vs 168 B); allocation > R3 (248 B vs 128 B). | Accepted exception: callback-based factories preserve the BCL `IObserver<T>` shape plus terminal/disposal guards; the adapter cost avoids production dependencies on either comparison library. |
| `CreateSafe subscribe` | time >= System.Reactive (48.7596 ns vs 43.4466 ns); allocation > System.Reactive (248 B vs 168 B); allocation > R3 (248 B vs 128 B). | Accepted exception: callback-based factories preserve the BCL `IObserver<T>` shape plus terminal/disposal guards; the adapter cost avoids production dependencies on either comparison library. |
| `Lazy subscribe` | allocation > R3 (240 B vs 152 B). | Accepted exception: ReactiveUI.Primitives keeps BCL observable compatibility, safe disposal, and explicit scheduler/resource ownership; the remaining narrow allocation/time gap is documented. |
| `Start subscribe` | allocation > R3 (376 B vs 160 B). | Accepted exception: ReactiveUI.Primitives keeps BCL observable compatibility, safe disposal, and explicit scheduler/resource ownership; the remaining narrow allocation/time gap is documented. |
| `Unfold subscribe` | time >= R3 (167.6973 ns vs 85.8050 ns); allocation > R3 (680 B vs 128 B). | Accepted exception: ReactiveUI.Primitives keeps BCL observable compatibility, safe disposal, and explicit scheduler/resource ownership; the remaining narrow allocation/time gap is documented. |
| `Use subscribe` | allocation > System.Reactive (432 B vs 168 B); time >= R3 (67.8147 ns vs 54.1505 ns); allocation > R3 (432 B vs 128 B). | Accepted exception: ReactiveUI.Primitives keeps BCL observable compatibility, safe disposal, and explicit scheduler/resource ownership; the remaining narrow allocation/time gap is documented. |
| `SwitchTo ranges` | time >= R3 (826.2645 ns vs 709.2868 ns); allocation > R3 (1376 B vs 392 B). | Accepted exception: coordinator operators keep general `IObservable<T>` subscription ownership and terminal arbitration; R3 has cheaper specialized observable paths in these microcases. |
| `SyncLatest ranges` | allocation > R3 (504 B vs 344 B). | Accepted exception: coordinator operators keep general `IObservable<T>` subscription ownership and terminal arbitration; R3 has cheaper specialized observable paths in these microcases. |
| `Latch ranges` | allocation > R3 (504 B vs 248 B). | Accepted exception: coordinator operators keep general `IObservable<T>` subscription ownership and terminal arbitration; R3 has cheaper specialized observable paths in these microcases. |
| `StateSignal 1024 values` | time >= System.Reactive (15,802.3885 ns vs 15,678.0589 ns). | Accepted exception: state primitives preserve current-value/state-signal semantics; the row records the small remaining gap against narrower comparison paths. |
| `Signal emit, 32 values` | allocation ties System.Reactive (136 B vs 136 B). | Accepted exception: emit loops now lead on throughput and tie System.Reactive allocation; strict lower allocation is not possible for this BCL-compatible shape in the measured path. |
| `Signal emit, 1024 values` | allocation ties System.Reactive (136 B vs 136 B). | Accepted exception: emit loops now lead on throughput and tie System.Reactive allocation; strict lower allocation is not possible for this BCL-compatible shape in the measured path. |
| `ShareLive connect` | allocation > R3 (384 B vs 368 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state; R3 remains lower allocation in this microcase. |
| `Share live subscribe` | allocation > R3 (848 B vs 488 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state; R3 remains lower allocation in this microcase. |
| `AutoShare subscribe` | allocation > R3 (848 B vs 488 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state; R3 remains lower allocation in this microcase. |
| `AutoConnect subscribe` | allocation > R3 (728 B vs 368 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state; R3 remains lower allocation in this microcase. |
| `StateSignal updates` | time >= System.Reactive (584.5012 ns vs 568.2254 ns). | Accepted exception: state primitives preserve current-value/state-signal semantics; the row records the small remaining gap against narrower comparison paths. |
| `ReadOnlyState projection` | time >= System.Reactive (136.4027 ns vs 86.8555 ns). | Accepted exception: state primitives preserve current-value/state-signal semantics; the row records the small remaining gap against narrower comparison paths. |
| `TaskSignal subscribe` | time >= System.Reactive (2,737.5769 ns vs 707.6171 ns); allocation > System.Reactive (3878 B vs 886 B); time >= R3 (2,737.5769 ns vs 39.8419 ns); allocation > R3 (3878 B vs 160 B). | Accepted exception: these primitives expose signal state, busy/result/error state, and async lifecycle coordination rather than only adapting a completed observable path. |
| `Command execute` | time >= R3 (127.2999 ns vs 98.3740 ns); allocation > R3 (600 B vs 296 B). | Accepted exception: these primitives expose signal state, busy/result/error state, and async lifecycle coordination rather than only adapting a completed observable path. |
| `Command result subscribe` | time >= System.Reactive (144.8945 ns vs 36.4350 ns); allocation > System.Reactive (672 B vs 136 B); time >= R3 (144.8945 ns vs 61.9539 ns); allocation > R3 (672 B vs 160 B). | Accepted exception: these primitives expose signal state, busy/result/error state, and async lifecycle coordination rather than only adapting a completed observable path. |
| `CollectList range` | allocation > R3 (688 B vs 632 B). | Accepted exception: the terminal collection helper returns through the BCL-compatible result path; throughput is ahead while R3 allocates slightly less. |
| `CurrentThread schedule` | allocation ties System.Reactive (88 B vs 88 B); allocation > R3 (88 B vs 56 B). | Accepted exception: current-thread scheduling uses the project sequencer queue contract; throughput is ahead of System.Reactive while R3 allocates less. |
| `Safe witness` | time >= System.Reactive (21.8713 ns vs 12.3352 ns); allocation > System.Reactive (168 B vs 136 B); time >= R3 (21.8713 ns vs 18.2902 ns); allocation > R3 (168 B vs 56 B). | Accepted exception: the wrapper enforces safe observer/terminal behavior; the row records that deliberate safety overhead. |
| `Completed Spark` | allocation ties System.Reactive (0 B vs 0 B); time >= R3 (0.0000 ns vs 0.0000 ns); allocation ties R3 (0 B vs 0 B). | Accepted exception: all implementations allocate zero and measure at empty-method scale, so a strict lower-allocation win is not meaningful. |

Performance constraints used by the project:

- Preserve observer and terminal notification semantics.
- Preserve safe unsubscription and disposal behavior.
- Avoid reflection and dynamic code generation in runtime hot paths.
- Prefer sealed helpers, direct fast paths, and predictable branch behavior.
- Keep allocations minimal in emit loops and single-subscriber cases.

## Repository layout

| Path | Purpose |
|---|---|
| `src/ReactiveUI.Primitives` | Production runtime library. |
| `src/ReactiveUI.Primitives.Wpf` | Optional WPF dispatcher integration library. |
| `src/ReactiveUI.Primitives.WinForms` | Optional Windows Forms control integration library. |
| `src/ReactiveUI.Primitives.Blazor` | Optional Blazor renderer integration library. |
| `src/ReactiveUI.Primitives.Maui` | Optional MAUI dispatcher integration library. |
| `src/ReactiveUI.Primitives.SystemReactiveBridge.Generator` | Source generator for System.Reactive bridge adapters. |
| `src/ReactiveUI.Primitives.R3Bridge.Generator` | Source generator for R3 bridge adapters. |
| `src/ReactiveUI.Primitives.Tests` | Test project using Microsoft Testing Platform/TUnit-style validation. |
| `src/benchmarks/ReactiveUI.Primitives.Benchmarks` | BenchmarkDotNet comparison harness. |

## Validation commands

### Latest local validation used for this README

```powershell
# Build solution
dotnet build src/ReactiveUI.Primitives.slnx --no-restore -c Release -v:minimal

# Test with the Microsoft Testing Platform runner from the src directory.
Push-Location src
dotnet test .\ReactiveUI.Primitives.slnx --no-build -c Release -v:minimal
Pop-Location

# Pack NuGet packages.
dotnet pack src\ReactiveUI.Primitives.slnx --no-build -c Release -v:minimal
```

Results: build passed with 0 warnings/0 errors; `dotnet test` passed 540/540 tests across net8.0, net9.0, and net10.0; `dotnet pack` produced `ReactiveUI.Primitives`, `ReactiveUI.Primitives.Wpf`, `ReactiveUI.Primitives.WinForms`, `ReactiveUI.Primitives.Blazor`, and `ReactiveUI.Primitives.Maui` packages. The latest coverage snapshot passed tests and reported 96.02% line (5285/5504) and 91.46% branch (1820/1990) coverage. That coverage remains below the original 100% line/branch final-review target and is documented as a signed-off release exception rather than represented as a 100% gate pass.

### Package verification

For NuGet package verification, inspect the generated `.nupkg` and confirm:

- `README.md` is present.
- The nuspec contains `<readme>README.md</readme>`.
- Bridge generator DLLs are present under `analyzers/dotnet/cs`.
- Production runtime dependencies do not include System.Reactive or R3.
- The core `ReactiveUI.Primitives` package does not reference WPF, Windows Forms, Blazor, or MAUI assemblies; those integrations ship from `ReactiveUI.Primitives.Wpf`, `ReactiveUI.Primitives.WinForms`, `ReactiveUI.Primitives.Blazor`, and `ReactiveUI.Primitives.Maui`.

## Practical migration checklist

1. Replace subject construction with `Signal<T>`, `StateSignal<T>`, or `HistorySignal<T>` depending on current behavior.
2. Replace factories: `Observable.Return/Empty/Throw/Timer/Interval` to `Signal.Emit/None/Fail/After/Pulse`.
3. Replace hot-path operators with Primitives names: `Select -> Map`, `Where -> Keep`, `SelectMany -> FlatMap`, `Do -> Tap`, `Scan -> Fold`, `Aggregate -> Reduce`, `Amb -> Race`.
4. Replace composite/serial disposables with `MultipleDisposable`/`Pocket` and `SingleReplaceableDisposable`/`Slot`.
5. Keep System.Reactive/R3 at application boundaries only when required; use generated bridge methods when those packages are already referenced.
6. Run build, tests, pack, and `git diff --check` before publishing or merging.

## License

ReactiveUI.Primitives is licensed under the MIT license. See `LICENSE` for details.
