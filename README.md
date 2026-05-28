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

Runtime package dependencies are intentionally small. The production package does not depend on System.Reactive or R3. The only runtime package reference declared directly by `src/ReactiveUI.Primitives/ReactiveUI.Primitives.csproj` is `System.ValueTuple` for `net462`; the remaining listed packages are analyzer, SourceLink, versioning, ILLink, reference-assembly, or build-time support packages such as Blazor.Common.Analyzers, Microsoft.SourceLink.GitHub, MinVer, Roslynator.Analyzers, SonarAnalyzer.CSharp, stylecop.analyzers, Microsoft.NET.ILLink.Tasks, and Microsoft.NETFramework.ReferenceAssemblies. Benchmark projects may reference System.Reactive and R3 as comparison baselines, but those references are not production dependencies.

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

Sequencers live in `ReactiveUI.Primitives.Concurrency` and implement `ISequencer`. The core `ReactiveUI.Primitives` package does not reference WPF or Windows Forms; UI-thread sequencers are provided by the optional `ReactiveUI.Primitives.Wpf` and `ReactiveUI.Primitives.WinForms` packages.

| Sequencer | Purpose |
|---|---|
| `Sequencer.Immediate` / `ImmediateSequencer.Instance` | Execute work immediately. |
| `Sequencer.CurrentThread` / `CurrentThreadSequencer.Instance` | Queue recursive/current-thread work deterministically. |
| `ThreadPoolSequencer.Instance` | Schedule work through the thread pool. |
| `TaskPoolSequencer.Instance` | Schedule work through tasks. |
| `SynchronizationContextSequencer` | Schedule through a `SynchronizationContext`. |
| `DispatcherSequencer` | Schedule onto a WPF dispatcher from `ReactiveUI.Primitives.Wpf`. |
| `ControlSequencer` | Schedule onto a Windows Forms control from `ReactiveUI.Primitives.WinForms`. |
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

Full BenchmarkDotNet runs were captured on 2026-05-28 with .NET SDK/runtime 10.0.8 on Windows 11. The latest complete run executed 201 benchmarks with no skipped suites in 00:21:51:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Latest artifact paths:

- `BenchmarkDotNet.Artifacts/BenchmarkRun-20260528-101607.log`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-28-10-38-00-report-github.md`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-28-10-38-00-report.html`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-28-10-38-00-report.csv`

Smoke validation for deterministic benchmark behavior passed for 67 benchmark groups with:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --smoke
```

Current direct test/coverage validation after the benchmark pass is 96.02% line coverage and 91.46% branch coverage from `src/tests/ReactiveUI.Primitives.Tests/TestResults/coverage-kanban-t_f3f3db71-20260528-final/coverage.cobertura.xml`. The original final-review target was 100% line and branch coverage; the remaining gap is a signed-off release exception rather than a passing 100% coverage result.

The table below is generated from the joined BenchmarkDotNet CSV and uses `Mean / Allocated` for each cell.

| Scenario | ReactiveUI.Primitives | System.Reactive | R3 |
|---|---:|---:|---:|
| Emit subscribe | 0.2186 ns / 0 B | 53.2379 ns / 120 B | 30.0486 ns / 80 B |
| None subscribe | 3.0541 ns / 40 B | 44.6828 ns / 96 B | 26.8593 ns / 56 B |
| Sequence subscribe | 49.8325 ns / 96 B | 2,459.2019 ns / 2,472 B | 74.6073 ns / 80 B |
| Loop subscribe | 7.0823 ns / 0 B | 2,324.1525 ns / 2,408 B | 75.9459 ns / 80 B |
| Fail subscribe | 55.2666 ns / 120 B | 107.4731 ns / 240 B | 88.8770 ns / 200 B |
| FromEnumerable subscribe | 50.5124 ns / 40 B | 2,369.0857 ns / 2,504 B | 78.1008 ns / 88 B |
| Completed task bridge | 9.1928 ns / 88 B | 822.4185 ns / 793 B | 38.3042 ns / 88 B |
| Create subscribe | 47.2199 ns / 248 B | 45.7875 ns / 168 B | 54.1915 ns / 128 B |
| CreateSafe subscribe | 47.3391 ns / 248 B | 45.6351 ns / 168 B | 54.6268 ns / 128 B |
| Lazy subscribe | 73.2374 ns / 240 B | 1,345.5338 ns / 1,512 B | 117.8853 ns / 152 B |
| Start subscribe | 60.3452 ns / 376 B | 816.3317 ns / 751 B | 56.4896 ns / 160 B |
| Unfold subscribe | 190.6104 ns / 736 B | 2,223.5963 ns / 2,768 B | 93.6088 ns / 128 B |
| Use subscribe | 72.3636 ns / 432 B | 86.6686 ns / 168 B | 55.2117 ns / 128 B |
| FromAsyncEnumerable subscribe | 1,881.6833 ns / 2,062 B | 1,532.8305 ns / 2,448 B | 1,060.5681 ns / 1,023 B |
| Silent subscribe/dispose | 0.2485 ns / 0 B | 6.0823 ns / 40 B | 16.9295 ns / 56 B |
| Map + Keep over range | 131.9449 ns / 208 B | 2,512.4803 ns / 2,616 B | 297.4630 ns / 272 B |
| Reduce + Any + Count | 191.4086 ns / 824 B | 5,298.7536 ns / 5,896 B | 635.6714 ns / 1,280 B |
| Prepend + Append + DefaultIfEmpty | 31.6353 ns / 168 B | 937.0711 ns / 1,257 B | 137.8890 ns / 288 B |
| DefaultIfEmpty(empty) | 5.5567 ns / 64 B | 66.3831 ns / 144 B | 60.2487 ns / 136 B |
| FlatMap over ranges | 906.9976 ns / 712 B | 3,489.8946 ns / 3,872 B | 973.1711 ns / 1,040 B |
| Pair over ranges | 45.5839 ns / 232 B | 3,139.5301 ns / 2,976 B | 693.6180 ns / 656 B |
| Chain ranges | 67.5521 ns / 256 B | 2,677.6141 ns / 2,856 B | 258.3191 ns / 360 B |
| Blend ranges | 70.9983 ns / 256 B | 3,770.6044 ns / 3,953 B | 661.3140 ns / 352 B |
| Race ranges | 39.1824 ns / 192 B | 1,442.9846 ns / 1,760 B | 254.1584 ns / 360 B |
| SwitchTo ranges | 755.5194 ns / 1,376 B | 2,089.1303 ns / 2,336 B | 688.3416 ns / 392 B |
| SyncLatest ranges | 98.9031 ns / 504 B | 3,028.5990 ns / 2,824 B | 611.6543 ns / 344 B |
| Latch ranges | 100.5133 ns / 504 B | 3,193.1360 ns / 2,824 B | 377.9174 ns / 248 B |
| ForkJoin ranges | 75.6980 ns / 480 B | 3,391.6289 ns / 3,136 B | 857.3522 ns / 504 B |
| Shift range | 3,201.9485 ns / 38,816 B | 5,196.2072 ns / 39,584 B | 1,821.4245 ns / 2,200 B |
| DelayStart range | 895.5792 ns / 25,520 B | 2,132.5495 ns / 26,456 B | 319.5858 ns / 552 B |
| Calm burst | 2,677.1477 ns / 38,384 B | 2,383.1439 ns / 36,480 B | 1,496.8547 ns / 1,512 B |
| Probe latest | 1,006.0773 ns / 26,072 B | 1,864.9357 ns / 26,264 B | 306.8848 ns / 664 B |
| Timestamp range | 399.6629 ns / 312 B | 1,618.9009 ns / 1,608 B | 337.9970 ns / 152 B |
| TimeInterval range | 488.7716 ns / 736 B | 1,651.9546 ns / 1,712 B | 426.0530 ns / 160 B |
| Expire idle | 978.2797 ns / 25,912 B | 1,165.9814 ns / 29,776 B | 385.2929 ns / 784 B |
| ObserveOn immediate | 24.7480 ns / 96 B | 16,606.7413 ns / 11,307 B | 887.0907 ns / 432 B |
| History subscribe | 330.6935 ns / 320 B | 670.7762 ns / 696 B | 405.9421 ns / 688 B |
| StateSignal 32 values | 536.3309 ns / 176 B | 548.6943 ns / 200 B | 587.0951 ns / 192 B |
| StateSignal 1024 values | 14,775.5376 ns / 176 B | 14,855.1132 ns / 200 B | 15,217.1773 ns / 192 B |
| Signal emit, 32 values | 69.9214 ns / 136 B | 98.0934 ns / 136 B | 127.8881 ns / 160 B |
| Signal emit, 1024 values | 1,698.9894 ns / 136 B | 1,716.7062 ns / 136 B | 2,323.2927 ns / 160 B |
| Signal subscribe/dispose, 8 observers | 235.0289 ns / 592 B | 313.7011 ns / 1,288 B | 440.8307 ns / 840 B |
| Signal subscribe/dispose, 64 observers | 2,776.5985 ns / 3,800 B | 3,866.7501 ns / 38,472 B | 3,381.8103 ns / 6,216 B |
| ShareLive connect | 124.6251 ns / 384 B | 2,647.5393 ns / 2,696 B | 428.5325 ns / 368 B |
| Share live subscribe | 254.7125 ns / 848 B | 3,050.7448 ns / 2,880 B | 616.1204 ns / 488 B |
| Replay live late subscribe | 662.7019 ns / 568 B | 3,683.1133 ns / 3,408 B | 923.3098 ns / 1,360 B |
| AutoShare subscribe | 232.0945 ns / 848 B | 2,832.0310 ns / 2,880 B | 595.3431 ns / 488 B |
| AutoConnect subscribe | 195.7574 ns / 728 B | 2,711.8025 ns / 2,736 B | 461.9903 ns / 368 B |
| StateSignal updates | 526.8121 ns / 176 B | 526.5334 ns / 200 B | 575.4000 ns / 192 B |
| ReadOnlyState projection | 125.7138 ns / 248 B | 89.6034 ns / 328 B | 166.5788 ns / 312 B |
| TaskSignal subscribe | 3,301.5409 ns / 3,937 B | 716.5774 ns / 886 B | 39.6398 ns / 160 B |
| Command execute | 124.0958 ns / 600 B | 673.8586 ns / 1,089 B | 104.3281 ns / 296 B |
| Command result subscribe | 148.7583 ns / 672 B | 37.9774 ns / 136 B | 62.7556 ns / 160 B |
| CollectList range | 125.1017 ns / 688 B | 2,691.0545 ns / 3,488 B | 193.8799 ns / 632 B |
| CollectArray range | 92.5524 ns / 656 B | 2,733.9446 ns / 3,640 B | 192.5716 ns / 784 B |
| CollectArrayAsync range | 34.7892 ns / 384 B | 2,752.4089 ns / 3,984 B | 188.2744 ns / 784 B |
| FirstAsync range | 6.5547 ns / 56 B | 2,450.1984 ns / 2,792 B | 77.3095 ns / 208 B |
| ToTask range | 14.1137 ns / 192 B | 2,446.6933 ns / 2,824 B | 96.6949 ns / 208 B |
| Count(predicate) range | 24.7896 ns / 96 B | 2,408.8926 ns / 2,520 B | 103.4699 ns / 200 B |
| LongCount(predicate) range | 24.6334 ns / 104 B | 2,423.2780 ns / 2,536 B | 106.1956 ns / 272 B |
| All range | 18.2455 ns / 96 B | 2,406.3606 ns / 2,520 B | 89.9046 ns / 192 B |
| Contains range | 10.0899 ns / 96 B | 2,453.9726 ns / 2,520 B | 90.3024 ns / 200 B |
| All + Contains range | 28.8726 ns / 192 B | 5,046.7377 ns / 5,040 B | 218.5446 ns / 392 B |
| Pocket dispose | 57.0307 ns / 408 B | 86.4195 ns / 512 B | 69.3605 ns / 480 B |
| CurrentThread schedule | 13.4939 ns / 88 B | 16.8119 ns / 88 B | 28.4136 ns / 56 B |
| Safe witness | 22.8934 ns / 168 B | 14.3072 ns / 136 B | 18.5158 ns / 56 B |
| Completed Spark | 0.0000 ns / 0 B | 0.0000 ns / 0 B | 0.0027 ns / 0 B |

Interpretation notes:

- ReactiveUI.Primitives remains substantially faster and lower allocation in most factory, range, terminal collection, composition, sharing, and subject subscription scenarios.
- The public API/operator matrix above is backed by deterministic smoke coverage in `Program.RunSmokeBenchmarksAsync`: every row has matching ReactiveUI.Primitives, System.Reactive, and R3 calls where alternatives exist, and the smoke run validates each benchmark path returns the same observable result before BenchmarkDotNet measures throughput and allocation unless the row is one of the documented scheduling-total exceptions below.
- The only intentional smoke output differences are `SwitchTo ranges`, `SyncLatest ranges`, and `Latch ranges`: System.Reactive produces different synchronous range totals for those coordinator operators, while ReactiveUI.Primitives and R3 agree on the emitted totals. `--smoke` permits only those System.Reactive differences and still fails if ReactiveUI.Primitives diverges from R3 or if any other benchmark group loses parity.
- Candidate scenarios where ReactiveUI.Primitives is not strictly both faster and lower-allocation than both alternatives are tracked explicitly below. Rows marked as exceptions are retained because the extra cost buys ReactiveUI.Primitives semantics (safe terminal/disposal behavior, `IObservable<T>`/`IObserver<T>` compatibility, deterministic `ISequencer` scheduling, or live-signal lifecycle ownership) while preserving the project rule that System.Reactive/R3 are benchmark-only dependencies.
- Near-zero singleton measurements (`Emit`, `Silent`, and `Spark` paths) may trigger BenchmarkDotNet `ZeroMeasurement` warnings; those warnings mean the method duration is indistinguishable from the empty-method overhead, not that the benchmark failed.

Candidate/performance exception matrix:

| Scenario | Observed gap | Decision and trade-off |
|---|---|---|
| `Sequence subscribe` | allocation > R3 (96 B vs 80 B). | Accepted exception: the range factory keeps the public `IObservable<int>`/current-thread subscription shape; the small R3 allocation delta is documented while throughput remains faster than both alternatives. |
| `Completed task bridge` | allocation ties R3 (88 B vs 88 B). | Accepted exception: the bridge already matches the lowest allocation and preserves task-observer completion semantics; strict-lower allocation is impossible for a tie. |
| `Create subscribe` | time >= System.Reactive (47.2199 ns vs 45.7875 ns); allocation > System.Reactive (248 B vs 168 B); allocation > R3 (248 B vs 128 B). | Accepted exception: `Signal.Create` exposes the BCL `IObserver<T>` callback shape and wraps subscription disposal for terminal safety; the adapter cost avoids adding a runtime System.Reactive/R3 dependency. |
| `CreateSafe subscribe` | time >= System.Reactive (47.3391 ns vs 45.6351 ns); allocation > System.Reactive (248 B vs 168 B); allocation > R3 (248 B vs 128 B). | Accepted exception: `CreateSafe` adds guarded terminal notification behavior over the raw factory callback; the safety contract is retained instead of optimizing away the wrapper. |
| `Lazy subscribe` | allocation > R3 (240 B vs 152 B). | Accepted exception: deferred construction allocates a factory subscription wrapper so disposal/errors route through the safe observer path; throughput remains ahead of both alternatives. |
| `Start subscribe` | time >= R3 (60.3452 ns vs 56.4896 ns); allocation > R3 (376 B vs 160 B). | Accepted exception: `Start` uses the project `ISequencer` abstraction and preserves immediate/current-thread scheduling parity; R3’s specialized scheduler path is not interchangeable. |
| `Unfold subscribe` | time >= R3 (190.6104 ns vs 93.6088 ns); allocation > R3 (736 B vs 128 B). | Accepted exception: the implementation keeps state-machine disposal and observer-safety guards for recursive generation; R3’s specialized observable shape is lower allocation. |
| `Use subscribe` | time >= R3 (72.3636 ns vs 55.2117 ns); allocation > System.Reactive (432 B vs 168 B); allocation > R3 (432 B vs 128 B). | Accepted exception: `Use` owns resource lifetime and source subscription together so resources are released across completion, error, and unsubscribe; the extra allocation is the lifecycle trade-off. |
| `FromAsyncEnumerable subscribe` | time >= System.Reactive (1,881.6833 ns vs 1,532.8305 ns); time >= R3 (1,881.6833 ns vs 1,060.5681 ns); allocation > R3 (2,062 B vs 1,023 B). | Accepted exception: the bridge preserves `IAsyncEnumerable<T>` cancellation/disposal and safe observer termination; the cost is isolated to async interop, not synchronous hot paths. |
| `SwitchTo ranges` | time >= R3 (755.5194 ns vs 688.3416 ns); allocation > R3 (1,376 B vs 392 B). | Accepted exception: `SwitchTo` maintains an inner subscription slot and terminal arbitration for general `IObservable<IObservable<T>>`; R3's range-specialized path is cheaper. |
| `SyncLatest ranges` | allocation > R3 (504 B vs 344 B). | Accepted exception: `SyncLatest` stores readiness/value state for both sides to preserve general `IObservable<T>` semantics; throughput remains well ahead of R3. |
| `Latch ranges` | allocation > R3 (504 B vs 248 B). | Accepted exception: the operator keeps left/right state and subscription ownership explicit for safe unsubscription; allocation premium buys generic semantics. |
| `Shift range` | time >= R3 (3,201.9485 ns vs 1,821.4245 ns); allocation > R3 (38,816 B vs 2,200 B). | Accepted exception: `Shift` uses deterministic `ISequencer` timer/scheduled-disposable handling for cancellation and ordering; R3's timer path is more allocation-efficient. |
| `DelayStart range` | time >= R3 (895.5792 ns vs 319.5858 ns); allocation > R3 (25,520 B vs 552 B). | Accepted exception: start-delay creates scheduled work through `ISequencer` for deterministic virtual/current-thread behavior; R3 wins allocation with a specialized path. |
| `Calm burst` | time >= System.Reactive (2,677.1477 ns vs 2,383.1439 ns); time >= R3 (2,677.1477 ns vs 1,496.8547 ns); allocation > System.Reactive (38,384 B vs 36,480 B); allocation > R3 (38,384 B vs 1,512 B). | Accepted exception: quiet-period scheduling keeps disposable timer slots and safe observer state per burst so cancellation races are explicit; this is documented as scheduler-heavy. |
| `Probe latest` | time >= R3 (1,006.0773 ns vs 306.8848 ns); allocation > R3 (26,072 B vs 664 B). | Accepted exception: `Probe` uses a coordinator and `ISequencer` timer loop to preserve latest-value/completion ordering; R3's scheduler specialization is lower allocation. |
| `Timestamp range` | time >= R3 (399.6629 ns vs 337.9970 ns); allocation > R3 (312 B vs 152 B). | Accepted exception: timestamps are produced through the project sequencer clock and `Moment<T>` path for deterministic scheduler injection; R3 is slightly cheaper natively. |
| `TimeInterval range` | time >= R3 (488.7716 ns vs 426.0530 ns); allocation > R3 (736 B vs 160 B). | Accepted exception: interval measurement tracks prior timestamp state through `ISequencer`; allocation buys deterministic clock injection and System.Reactive-compatible semantics. |
| `Expire idle` | time >= R3 (978.2797 ns vs 385.2929 ns); allocation > R3 (25,912 B vs 784 B). | Accepted exception: `Expire` owns a timer slot plus source subscription and routes timeout errors safely; the scheduler/disposal safety cost is retained. |
| `Signal emit, 32 values` | allocation ties System.Reactive (136 B vs 136 B). | Accepted exception: allocation ties System.Reactive while ReactiveUI.Primitives wins throughput and beats R3 allocation; strict-lower cannot be satisfied for a tie. |
| `Signal emit, 1024 values` | allocation ties System.Reactive (136 B vs 136 B). | Accepted exception: allocation is flat and tied with System.Reactive while throughput remains fastest; no extra optimization is justified for a strict tie. |
| `ShareLive connect` | allocation > R3 (384 B vs 368 B). | Accepted exception: live sharing owns the `ConnectableSignal<T>` connection/disposal boundary; the small R3 allocation delta buys explicit lifecycle ownership. |
| `Share live subscribe` | allocation > R3 (848 B vs 488 B). | Accepted exception: share/ref-count uses gate state to connect/disconnect the source safely across observers; R3’s lower allocation reflects a different specialized implementation. |
| `AutoShare subscribe` | allocation > R3 (848 B vs 488 B). | Accepted exception: auto-share keeps connection gate state and a disposable cleanup path for safe last-subscriber disconnect; throughput remains ahead while allocation is documented. |
| `AutoConnect subscribe` | allocation > R3 (728 B vs 368 B). | Accepted exception: auto-connect tracks subscriber thresholds and connection lifetime explicitly; allocation premium buys predictable connect ownership without R3 at runtime. |
| `StateSignal updates` | time >= System.Reactive (526.8121 ns vs 526.5334 ns). | Accepted exception: state updates retain current-value/state-signal semantics and lower allocation than both alternatives; the latest run is within noise of System.Reactive time while still faster than R3. |
| `ReadOnlyState projection` | time >= System.Reactive (125.7138 ns vs 89.6034 ns). | Accepted exception: projection preserves `ReadOnlyStateSignal<T>` current-value semantics; System.Reactive is faster in this microcase but allocates more and lacks the state-signal contract. |
| `TaskSignal subscribe` | time >= System.Reactive (3,301.5409 ns vs 716.5774 ns); time >= R3 (3,301.5409 ns vs 39.6398 ns); allocation > System.Reactive (3,937 B vs 886 B); allocation > R3 (3,937 B vs 160 B). | Accepted exception: `TaskSignal<T>` exposes signal state, completion/error observation, and task lifecycle semantics, not just a completed-task observable; use the completed-task bridge for the scalar bridge case. |
| `Command execute` | time >= R3 (124.0958 ns vs 104.3281 ns); allocation > R3 (600 B vs 296 B). | Accepted exception: command execution exposes busy/result/error state and async coordination; R3 is cheaper for the narrow command path, while ReactiveUI.Primitives keeps richer command semantics. |
| `Command result subscribe` | time >= System.Reactive (148.7583 ns vs 37.9774 ns); time >= R3 (148.7583 ns vs 62.7556 ns); allocation > System.Reactive (672 B vs 136 B); allocation > R3 (672 B vs 160 B). | Accepted exception: result subscription attaches to the command stateful signal pipeline rather than a bare subject; allocation/time buys command result replay/state semantics. |
| `CollectList range` | allocation > R3 (688 B vs 632 B). | Accepted exception: collection returns `IList<T>` through the general terminal observer path; the small R3 allocation delta is accepted because throughput remains faster. |
| `CurrentThread schedule` | allocation ties System.Reactive (88 B vs 88 B); allocation > R3 (88 B vs 56 B). | Accepted exception: current-thread scheduling uses the project sequencer queue contract; strict-lower allocation is not required for a tied/faster scheduler primitive. |
| `Safe witness` | time >= System.Reactive (22.8934 ns vs 14.3072 ns); time >= R3 (22.8934 ns vs 18.5158 ns); allocation > System.Reactive (168 B vs 136 B); allocation > R3 (168 B vs 56 B). | Accepted exception: `SafeWitness` intentionally wraps observer calls to enforce safe terminal/error behavior; the microbenchmark records this safety overhead. |
| `Completed Spark` | time >= System.Reactive (0.0000 ns vs 0.0000 ns); allocation ties System.Reactive (0 B vs 0 B); allocation ties R3 (0 B vs 0 B). | Accepted exception: all alternatives allocate zero and measure near zero, so strict-lower allocation cannot apply; this is a singleton sanity check. |

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
| `src/ReactiveUI.Primitives.SystemReactiveBridge.Generator` | Source generator for System.Reactive bridge adapters. |
| `src/ReactiveUI.Primitives.R3Bridge.Generator` | Source generator for R3 bridge adapters. |
| `src/ReactiveUI.Primitives.Tests` | Test project using Microsoft Testing Platform/TUnit-style validation. |
| `src/benchmarks/ReactiveUI.Primitives.Benchmarks` | BenchmarkDotNet comparison harness. |

## Validation commands

### Latest local validation used for this README

```powershell
# Build solution
dotnet build src/ReactiveUI.Primitives.slnx --no-restore -v:minimal

# Test with the Microsoft Testing Platform runner from the src directory.
Push-Location src
dotnet test .\ReactiveUI.Primitives.slnx --no-build -v:minimal
Pop-Location
```

Results: build passed with 0 warnings/0 errors; `dotnet test` passed 537/537 tests across net8.0, net9.0, and net10.0. The latest coverage snapshot passed tests and reported 96.02% line (5285/5504) and 91.46% branch (1820/1990) coverage. That coverage remains below the original 100% line/branch final-review target and is documented as a signed-off release exception rather than represented as a 100% gate pass.

### Package verification

For NuGet package verification, inspect the generated `.nupkg` and confirm:

- `README.md` is present.
- The nuspec contains `<readme>README.md</readme>`.
- Bridge generator DLLs are present under `analyzers/dotnet/cs`.
- Production runtime dependencies do not include System.Reactive or R3.
- The core `ReactiveUI.Primitives` package does not reference WPF or Windows Forms assemblies; those integrations ship from `ReactiveUI.Primitives.Wpf` and `ReactiveUI.Primitives.WinForms`.

## Practical migration checklist

1. Replace subject construction with `Signal<T>`, `StateSignal<T>`, or `HistorySignal<T>` depending on current behavior.
2. Replace factories: `Observable.Return/Empty/Throw/Timer/Interval` to `Signal.Emit/None/Fail/After/Pulse`.
3. Replace hot-path operators with Primitives names: `Select -> Map`, `Where -> Keep`, `SelectMany -> FlatMap`, `Do -> Tap`, `Scan -> Fold`, `Aggregate -> Reduce`, `Amb -> Race`.
4. Replace composite/serial disposables with `MultipleDisposable`/`Pocket` and `SingleReplaceableDisposable`/`Slot`.
5. Keep System.Reactive/R3 at application boundaries only when required; use generated bridge methods when those packages are already referenced.
6. Run build, tests, pack, and `git diff --check` before publishing or merging.

## License

ReactiveUI.Primitives is licensed under the MIT license. See `LICENSE` for details.
