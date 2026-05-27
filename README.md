# ReactiveUI.Primitives

ReactiveUI.Primitives is a compact, high-performance reactive library for .NET applications that want Rx-style composition without a runtime dependency on System.Reactive or R3. It keeps the BCL `IObservable<T>` / `IObserver<T>` contracts where they are useful, adds Primitives names for common concepts, and focuses on predictable AOT-friendly code paths with low allocation overhead.

## Goals and design posture

ReactiveUI.Primitives is designed to:

- Provide Rx-style stream creation, subscription, state, scheduling, and composition over `IObservable<T>`.
- Use a distinct vocabulary where it improves clarity: `Signal<T>` instead of `Subject<T>`, `Map` instead of only `Select`, `Keep` instead of only `Where`, `Spark` instead of notification materialization.
- Stay AOT-friendly: no runtime reflection, dynamic code generation, expression compilation, or hidden dependency on System.Reactive/R3 in the production package.
- Minimize allocations in hot paths, including direct single-action subscribers for `Signal<T>` and reusable immutable singleton signals for common return/empty/never cases.
- Support broad production target frameworks, including .NET Framework, Windows desktop, and modern mobile/desktop TFMs.
- Allow migration from System.Reactive/R3 through source-generator bridges when the consuming project already references those libraries.

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

The production library targets:

- `net462`
- `net472`
- `net481`
- `net9.0-windows10.0.19041.0`
- `net10.0-windows10.0.19041.0`
- `net9.0-ios`
- `net9.0-tvos`
- `net9.0-macos`
- `net9.0-maccatalyst`
- `net10.0-ios`
- `net10.0-tvos`
- `net10.0-macos`
- `net10.0-maccatalyst`
- `net9.0-android`
- `net10.0-android`

Runtime package dependencies are intentionally small. The production package does not depend on System.Reactive or R3. `System.ValueTuple` is used for `net462` only. Benchmark projects may reference System.Reactive and R3 as comparison baselines, but those references are not production dependencies.

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
| `Signal.Defer<T>(Func<IObservable<T>>)` | Create the source per subscription. |
| `Signal.Return<T>(T)` | Emit one value and complete. Specialized fast paths exist for `bool`, `int`, and `RxVoid`. |
| `Signal.Empty<T>()` | Complete without values. |
| `Signal.Never<T>()` / `Signal.Never<T>(T witness)` | Never emit and never complete. |
| `Signal.Throw<T>(Exception)` | Terminate with an error. |
| `Signal.Range(int start, int count)` | Emit an integer range and complete. |
| `Signal.Repeat<T>(T value)` / `Repeat<T>(T value, int count)` | Repeat indefinitely or a fixed number of times. |
| `Signal.Unfold<TState,TResult>(...)` / `Signal.Generate<TState,TResult>(...)` | Generate a finite sequence from state. |
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
| `Signal.Interval(...)` | Alias of `Every`. |
| `Signal.Timer(...)` | Alias/overload for one-shot and periodic timers. |
| `Signal.Concat(...)`, `Signal.Merge(...)`, `Signal.Race(...)` | Compose multiple sources. |
| `Signal.Zip(...)`, `Signal.CombineLatest(...)`, `Signal.ZipLatest(...)`, `Signal.ForkJoin(...)` | Pairwise combination helpers. |

Example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<int> values = Signal.Range(1, 5);

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

Operators are extension methods over `IObservable<T>`. ReactiveUI.Primitives intentionally includes both canonical LINQ/Rx names where useful and Primitives names where the library wants a distinct surface.

### Transformation and filtering

| System.Reactive-style concept | ReactiveUI.Primitives API |
|---|---|
| `Select` | `Map` | Prefer `Map` for the distinct Primitives style. |
| stateful `Select` without closure | `MapWith` |
| `Where` | `Keep`; `Where` delegates to `Keep`. |
| stateful `Where` without closure | `KeepWith` |
| non-null filtering | `KeepNotNull` |
| `OfType` / `Cast` | `OfType<TResult>` / `Cast<TResult>` |
| side effects | `Tap`, `TapWith` |
| `Scan` | `Scan` |
| `Aggregate` | `Fold` |
| `Distinct` | `Distinct` |
| `DistinctUntilChanged` | `DistinctUntilChanged` |
| key-based distinct | `DistinctBy`, `DistinctUntilChangedBy` |
| `Take` / `Skip` | `Take`, `Skip` |
| `TakeWhile` / `SkipWhile` | `TakeWhile`, `SkipWhile` |
| `IgnoreElements` | `IgnoreValues` |
| `DefaultIfEmpty` | `DefaultIfEmpty` |

Example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<string> labels = Signal.Range(1, 10)
    .Keep(value => value % 2 == 0)
    .Map(value => $"even:{value}")
    .Tap(label => Console.WriteLine($"observed {label}"));

using var subscription = labels.Subscribe(Console.WriteLine);
```

### Composition

| Concept | API |
|---|---|
| sequential concatenation | `Concat` |
| concurrent merge | `Merge` |
| first source wins | `Race` |
| latest inner source wins | `Switch` |
| pairwise zip | `Zip` |
| latest-value combination | `CombineLatest` |
| combine left emission with latest right value | `WithLatest` |
| latest-fusion alias | `ZipLatest`, `FuseLatest` |
| last values after both complete | `ForkJoin` |
| retry | `Retry` |
| catch/rescue | `Rescue`, `Resume`, `Signal.Catch` |
| final action | `Signal.Finally` |

Merge example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<int> low = Signal.Range(1, 3);
IObservable<int> high = Signal.Range(100, 3);

using var merged = Signal.Merge(low, high)
    .Subscribe(value => Console.WriteLine(value));
```

CombineLatest example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var width = new StateSignal<int>(640);
var height = new StateSignal<int>(480);

using var area = Signal.CombineLatest(width, height, (w, h) => w * h)
    .Subscribe(value => Console.WriteLine($"area={value}"));

width.Value = 800;
height.Value = 600;
```

### Time, buffering, and async helpers

| Concept | API |
|---|---|
| delayed subscription | `DelayStart` |
| delayed values | `Delay` |
| quiet-period sampling | `Throttle` |
| periodic sampling | `Sample` |
| timeout | `Timeout` |
| schedule subscription | `SubscribeOn` |
| timestamp values | `Timestamp` |
| measure intervals | `TimeInterval` |
| fixed-size buffers | `Buffer(count)`, `Buffer(count, skip)` |
| collect to list/array signal | `CollectList`, `CollectArray`, `ToList`, `ToArray` |
| collect asynchronously | `CollectListAsync`, `CollectArrayAsync`, `ToListAsync`, `ToArrayAsync` |
| first/last value task | `FirstAsync`, `FirstOrDefaultAsync`, `LastAsync`, `LastOrDefaultAsync` |

Timer example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

using var subscription = Signal.Timer(
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

`Spark<T>` represents value/error/completion notifications. Use `Sparkify` to convert stream events into values and `Unspark` to turn them back into observer notifications.

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

IObservable<Spark<int>> sparks = Signal.Range(1, 3).Sparkify();
IObservable<int> values = sparks.Unspark();
```

## Stateful signals and subject-like types

ReactiveUI.Primitives uses explicit names instead of cloning every System.Reactive subject type name.

| System.Reactive type | ReactiveUI.Primitives equivalent | Notes |
|---|---|---|
| `Subject<T>` | `Signal<T>` | Push values, errors, and completion to subscribers. |
| `BehaviorSubject<T>` | `BehaviorSignal<T>`, or `StateSignal<T>` | Stores the latest value and emits it to new subscribers. `StateSignal<T>` adds a mutable `Value` setter and `Changed`. |
| `ReplaySubject<T>` | `ReplaySignal<T>` | Replays buffered values by size and/or time window. |
| `AsyncSubject<T>` | `AsyncSignal<T>` | Awaitable subject-like signal; also implements `IAwaitSignal<T>`. |
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

Replay example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var replay = new ReplaySignal<string>(bufferSize: 2);
replay.OnNext("A");
replay.OnNext("B");
replay.OnNext("C");

using var subscription = replay.Subscribe(Console.WriteLine); // replays B, C
```

Error and completion example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

IObservable<int> failed = Signal.Throw<int>(new InvalidOperationException("not available"));

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

IObservable<int> systemObservable = Signal.Range(1, 3).AsSystemObservable();
```

R3 bridge example, when the consuming project already references R3:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.R3Bridge;
using ReactiveUI.Primitives.Signals;

// R3.Observable<int> r3Source = ...;
// IObservable<int> PrimitivesSource = r3Source.AsPrimitivesSignal();
// R3.Observable<int> r3Again = Signal.Range(1, 3).AsR3Observable();
```

The R3 snippet is intentionally shown as a migration shape because it requires the consuming application to reference R3. ReactiveUI.Primitives itself remains free of an R3 runtime dependency.

## System.Reactive to ReactiveUI.Primitives migration guide

ReactiveUI.Primitives is not a byte-for-byte clone of System.Reactive. It keeps the standard `IObservable<T>` contracts but favors a smaller runtime, explicit state types, and Primitives naming. Migrate one vertical slice at a time: factories first, then subject/state types, then operators and schedulers.

### Factory mapping

| System.Reactive | ReactiveUI.Primitives | Notes |
|---|---|---|
| `Observable.Return(value)` | `Signal.Return(value)` | Emits one value and completes. |
| `Observable.Empty<T>()` | `Signal.Empty<T>()` | Completes immediately. |
| `Observable.Never<T>()` | `Signal.Never<T>()` or `Signal.Never<T>(witness)` | Non-terminating signal; witness overload helps type inference. |
| `Observable.Throw<T>(ex)` | `Signal.Throw<T>(ex)` | Emits terminal error. |
| `Observable.Range(start, count)` | `Signal.Range(start, count)` | Optional scheduler overload exists. |
| `Observable.Repeat(value)` | `Signal.Repeat(value)` | Indefinite repeat. |
| `Observable.Repeat(value, count)` | `Signal.Repeat(value, count)` | Fixed repeat. |
| `Observable.Defer(factory)` | `Signal.Defer(factory)` | Create source per subscription. |
| `Observable.FromAsync(...)` | `Signal.FromAsync(...)` | Invoke a task factory per subscription. |
| `Observable.Create<T>(...)` | `Signal.Create<T>(...)` or `Signal.CreateSafe<T>(...)` | Prefer `CreateSafe` for general custom sources. |
| `Observable.Using(...)` | `Signal.Use(...)` | Resource scoped to subscription. |
| `Observable.Timer(dueTime)` | `Signal.Timer(dueTime)` or `Signal.After(dueTime)` | Emits `long` tick `0`. |
| `Observable.Timer(dueTime, period)` | `Signal.Timer(dueTime, period)` | Periodic `long` ticks. |
| `Observable.Interval(period)` | `Signal.Interval(period)` or `Signal.Every(period)` | Repeating ticks. |
| `ToObservable()` from enumerable | `Signal.FromEnumerable(values)`, `values.ToSignal()`, or `values.ToObservable()` | Cancellation-token overloads are available. |
| task conversion | `Signal.FromTask(task)` | Function-based task signals also exist. |

### Subject/state mapping

| System.Reactive | ReactiveUI.Primitives | Migration detail |
|---|---|---|
| `new Subject<T>()` | `new Signal<T>()` | Use `OnNext`, `OnError`, `OnCompleted`, and `Subscribe`. |
| `new BehaviorSubject<T>(initial)` | `new BehaviorSignal<T>(initial)` | Keeps `Value` getter and emits latest value to subscribers. |
| mutable reactive property | `new StateSignal<T>(initial)` | Set `Value` to emit. Use `Changed` for observable state stream. |
| `new ReplaySubject<T>()` | `new ReplaySignal<T>()` | Unbounded replay. |
| `new ReplaySubject<T>(bufferSize)` | `new ReplaySignal<T>(bufferSize)` | Size-limited replay. |
| `new ReplaySubject<T>(window)` | `new ReplaySignal<T>(window)` | Time-window replay. |
| `new AsyncSubject<T>()` | `new AsyncSignal<T>()` | Awaitable signal shape. |

### Operator mapping

| System.Reactive | ReactiveUI.Primitives | Notes |
|---|---|---|
| `Select` | `Map` | Prefer `Map` for distinct Primitives style. |
| `Where` | `Keep` or `Where` | `Where` delegates to `Keep`. |
| `SelectMany` | `SelectMany` or `Bind` | `Bind` is the Primitives alias. |
| `Aggregate` | `Fold` | Emits final accumulated value on completion. |
| `Scan` | `Scan` | Emits every accumulated value. |
| `Do` | `Tap` | Side effect while preserving values. |
| `Take` / `Skip` | `Take` / `Skip` | Count-based overloads. |
| `TakeWhile` / `SkipWhile` | `TakeWhile` / `SkipWhile` | Predicate-based. |
| `Distinct` | `Distinct` | Full seen-set distinct. |
| `DistinctUntilChanged` | `DistinctUntilChanged` | Adjacent dedupe. |
| `OfType` / `Cast` | `OfType` / `Cast` | Object-source projections. |
| `Materialize` | `Sparkify` | Converts notifications into `Spark<T>`. |
| `Dematerialize` | `Unspark` | Converts `Spark<T>` values back into notifications. |
| `Merge` | `Merge` or `Signal.Merge` | Works over source-of-sources and params factories. |
| `Concat` | `Concat` or `Signal.Concat` | Sequential composition. |
| `Amb` | `Race` | First source to produce a value or terminal signal wins. |
| `Switch` | `Switch` | Latest inner observable wins. |
| `Zip` | `Zip` or `Signal.Zip` | Pair values by index. |
| `CombineLatest` | `CombineLatest` or `Signal.CombineLatest` | Latest values after both sources have emitted. |
| `WithLatestFrom` | `WithLatest` | Left emission paired with latest right value. |
| `ForkJoin` | `ForkJoin` | Last values after completion. |
| `Throttle` | `Throttle` | Quiet-period emission. |
| `Sample` | `Sample` | Periodic latest-value sampling. |
| `Delay` | `Delay` | Delay emitted values. |
| `DelaySubscription` | `DelayStart` | Delay source subscription. |
| `Timeout` | `Timeout` | Error on missing value before due time. |
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
| R3 subject | `Signal<T>` / `StateSignal<T>` / `ReplaySignal<T>` depending on state/replay needs. |
| R3 `Select` / `Where` | `Map` / `Keep`. |
| R3 time operators | `Signal.Timer`, `Signal.Interval`, `Throttle`, `Sample`, `Delay`, scheduler overloads. |
| R3 bridge | Generated `AsPrimitivesSignal` / `AsR3Observable` when R3 is referenced by the consumer. |

Use the generated bridge only at boundaries. Prefer native ReactiveUI.Primitives operators inside new code.

## Benchmarks and performance posture

Benchmarks live in `src/benchmarks/ReactiveUI.Primitives.Benchmarks`. The benchmark project may reference System.Reactive and R3 to compare throughput and allocation behavior; the production package must not.

Full BenchmarkDotNet runs were captured on 2026-05-27 with .NET SDK 10.0.300 on Windows 11:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Short local jobs are useful for fast regression checks; rerun with a longer BenchmarkDotNet job before making release claims. Current local test coverage after this pass is 82.80% line coverage and 75.50% branch coverage from `coverage-local.cobertura.xml`; the 100% coverage target remains an active work item.

The table below is generated from the joined BenchmarkDotNet CSV and uses `Mean / Allocated` for each cell.

| Scenario | ReactiveUI.Primitives | System.Reactive | R3 |
|---|---:|---:|---:|
| Return subscribe | 0.2924 ns / 0 B | 57.3971 ns / 120 B | 36.4736 ns / 80 B |
| Empty subscribe | 2.6250 ns / 40 B | 49.4400 ns / 96 B | 31.7660 ns / 56 B |
| Range subscribe | 53.4814 ns / 96 B | 2,661.3499 ns / 2,472 B | 74.1203 ns / 80 B |
| Repeat subscribe | 7.4801 ns / 0 B | 2,561.7627 ns / 2,408 B | 72.9776 ns / 80 B |
| Throw subscribe | 65.5047 ns / 120 B | 121.8558 ns / 240 B | 101.9381 ns / 200 B |
| FromEnumerable subscribe | 53.8412 ns / 40 B | 2,463.3475 ns / 2,504 B | 79.5410 ns / 88 B |
| Completed task bridge | 14.7929 ns / 88 B | 1,069.5220 ns / 793 B | 42.9666 ns / 88 B |
| Create subscribe | 71.6670 ns / 248 B | 50.1209 ns / 168 B | 64.3239 ns / 128 B |
| CreateSafe subscribe | 58.6071 ns / 248 B | 50.7736 ns / 168 B | 64.0586 ns / 128 B |
| Defer subscribe | 80.2183 ns / 240 B | 1,494.6925 ns / 1,512 B | 120.5173 ns / 152 B |
| Start subscribe | 68.4794 ns / 376 B | 884.6336 ns / 751 B | 67.0051 ns / 160 B |
| Unfold subscribe | 205.0502 ns / 736 B | 2,413.8472 ns / 2,768 B | 106.4548 ns / 128 B |
| Use subscribe | 78.5460 ns / 432 B | 90.4731 ns / 168 B | 62.4567 ns / 128 B |
| FromAsyncEnumerable subscribe | 2,572.9176 ns / 2,065 B | 2,042.2704 ns / 2,445 B | 1,375.5498 ns / 1,023 B |
| Never subscribe/dispose | 0.2897 ns / 0 B | 5.9221 ns / 40 B | 21.0021 ns / 56 B |
| Map + Keep over range | 144.9399 ns / 208 B | 2,858.9877 ns / 2,616 B | 306.8607 ns / 272 B |
| Aggregate + Any + Count | 269.0177 ns / 992 B | 5,698.2445 ns / 5,896 B | 625.4828 ns / 1,280 B |
| StartWith + Append + DefaultIfEmpty | 56.9262 ns / 184 B | 1,055.3664 ns / 1,257 B | 157.0466 ns / 288 B |
| SelectMany over ranges | 1,054.3650 ns / 712 B | 3,870.5592 ns / 3,872 B | 1,152.8382 ns / 1,040 B |
| Zip over ranges | 45.4963 ns / 232 B | 3,656.1999 ns / 2,976 B | 747.8750 ns / 656 B |
| Concat ranges | 77.7447 ns / 256 B | 2,961.9928 ns / 2,856 B | 277.4494 ns / 360 B |
| Merge ranges | 81.1404 ns / 256 B | 4,211.2274 ns / 3,952 B | 722.6829 ns / 352 B |
| Race ranges | 42.3265 ns / 192 B | 1,643.5506 ns / 1,760 B | 295.8642 ns / 360 B |
| Switch ranges | 931.0946 ns / 1,376 B | 2,259.5812 ns / 2,336 B | 789.2157 ns / 392 B |
| CombineLatest ranges | 118.2750 ns / 504 B | 3,395.1528 ns / 2,824 B | 714.2596 ns / 344 B |
| WithLatest ranges | 112.4741 ns / 504 B | 3,630.0810 ns / 2,824 B | 409.9192 ns / 248 B |
| ForkJoin ranges | 82.2519 ns / 480 B | 3,726.2327 ns / 3,136 B | 1,016.8916 ns / 504 B |
| Delay range | 3,509.9035 ns / 38,816 B | 6,600.1541 ns / 39,584 B | 2,118.7826 ns / 2,200 B |
| DelayStart range | 1,029.2875 ns / 25,520 B | 2,440.2833 ns / 26,456 B | 349.4229 ns / 552 B |
| Throttle burst | 3,351.9829 ns / 38,384 B | 2,843.6348 ns / 36,480 B | 1,729.4249 ns / 1,512 B |
| Sample latest | 1,192.1561 ns / 26,072 B | 2,187.4022 ns / 26,264 B | 388.9601 ns / 664 B |
| Timestamp range | 452.1057 ns / 312 B | 1,870.3018 ns / 1,608 B | 373.1253 ns / 152 B |
| TimeInterval range | 542.5902 ns / 736 B | 1,853.5918 ns / 1,712 B | 479.2913 ns / 160 B |
| Timeout idle | 1,157.1993 ns / 25,912 B | 1,463.0736 ns / 29,776 B | 486.4943 ns / 784 B |
| ObserveOn immediate | 27.0202 ns / 96 B | 18,308.8684 ns / 11,309 B | 998.1707 ns / 432 B |
| Replay subscribe | 369.7292 ns / 320 B | 766.3089 ns / 696 B | 461.5158 ns / 688 B |
| BehaviorSignal 32 values | 613.2438 ns / 176 B | 618.4676 ns / 200 B | 669.5095 ns / 192 B |
| BehaviorSignal 1024 values | 17,167.4906 ns / 176 B | 16,983.8969 ns / 200 B | 16,967.7836 ns / 192 B |
| Signal emit, 32 values | 79.8202 ns / 136 B | 105.0434 ns / 136 B | 168.7808 ns / 160 B |
| Signal emit, 1024 values | 2,512.2955 ns / 136 B | 1,973.1489 ns / 136 B | 2,166.3308 ns / 160 B |
| Signal subscribe/dispose, 8 observers | 271.5037 ns / 592 B | 340.2902 ns / 1,288 B | 522.6329 ns / 840 B |
| Signal subscribe/dispose, 64 observers | 3,011.0357 ns / 3,800 B | 4,617.4255 ns / 38,472 B | 4,091.2084 ns / 6,216 B |
| Publish live connect | 173.3073 ns / 384 B | 3,236.7963 ns / 2,696 B | 561.8089 ns / 368 B |
| Share live subscribe | 276.6871 ns / 848 B | 3,461.6713 ns / 2,880 B | 548.8631 ns / 488 B |
| Replay live late subscribe | 712.4733 ns / 568 B | 4,472.9059 ns / 3,408 B | 1,132.5459 ns / 1,360 B |
| RefCount subscribe | 280.1471 ns / 848 B | 3,155.4108 ns / 2,880 B | 529.4785 ns / 488 B |
| AutoConnect subscribe | 245.5960 ns / 728 B | 3,474.4862 ns / 2,736 B | 466.0376 ns / 368 B |
| StateSignal updates | 609.7935 ns / 176 B | 609.6959 ns / 200 B | 662.1553 ns / 192 B |
| ReadOnlyState projection | 150.0491 ns / 248 B | 106.9365 ns / 328 B | 199.0058 ns / 312 B |
| TaskSignal subscribe | 6,126.1940 ns / 3,853 B | 874.2303 ns / 886 B | 47.2969 ns / 160 B |
| Command execute | 144.2864 ns / 600 B | 809.1453 ns / 1,089 B | 123.6877 ns / 296 B |
| Command result subscribe | 184.0972 ns / 672 B | 46.0881 ns / 136 B | 76.0079 ns / 160 B |
| CollectList range | 138.5013 ns / 688 B | 2,916.3119 ns / 3,488 B | 189.9245 ns / 632 B |
| CollectArray range | 114.7686 ns / 656 B | 4,302.7507 ns / 3,640 B | 231.4877 ns / 784 B |
| CollectArrayAsync range | 37.9519 ns / 384 B | 3,070.6184 ns / 3,984 B | 189.4382 ns / 784 B |
| FirstAsync range | 7.4257 ns / 56 B | 2,679.5795 ns / 2,792 B | 89.9454 ns / 208 B |
| ToTask range | 16.8438 ns / 192 B | 2,778.4730 ns / 2,824 B | 104.4256 ns / 208 B |
| Count(predicate) range | 64.3218 ns / 144 B | 2,804.5286 ns / 2,520 B | 111.1174 ns / 200 B |
| All + Contains range | 250.2919 ns / 1,024 B | 5,723.2875 ns / 5,816 B | 257.2693 ns / 392 B |
| Pocket dispose | 85.2006 ns / 408 B | 107.9840 ns / 512 B | 102.8140 ns / 480 B |
| CurrentThread schedule | 15.6813 ns / 88 B | 18.6513 ns / 88 B | 34.4560 ns / 56 B |
| Safe witness | 32.4502 ns / 168 B | 16.2257 ns / 136 B | 22.0434 ns / 56 B |
| Completed Spark | 0.0000 ns / 0 B | 0.0000 ns / 0 B | 0.2869 ns / 0 B |

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

For NuGet package verification, inspect the generated `.nupkg` and confirm:

- `README.md` is present.
- The nuspec contains `<readme>README.md</readme>`.
- Bridge generator DLLs are present under `analyzers/dotnet/cs`.
- Production runtime dependencies do not include System.Reactive or R3.
- The core `ReactiveUI.Primitives` package does not reference WPF or Windows Forms assemblies; those integrations ship from `ReactiveUI.Primitives.Wpf` and `ReactiveUI.Primitives.WinForms`.

## Practical migration checklist

1. Replace subject construction with `Signal<T>`, `StateSignal<T>`, or `ReplaySignal<T>` depending on current behavior.
2. Replace factories: `Observable.Return/Empty/Throw/Timer/Interval` to `Signal.Return/Empty/Throw/Timer/Interval`.
3. Replace hot-path operators with Primitives names: `Select -> Map`, `Where -> Keep`, `Do -> Tap`, `Aggregate -> Fold`, `Amb -> Race`.
4. Replace composite/serial disposables with `MultipleDisposable`/`Pocket` and `SingleReplaceableDisposable`/`Slot`.
5. Keep System.Reactive/R3 at application boundaries only when required; use generated bridge methods when those packages are already referenced.
6. Run build, tests, pack, and `git diff --check` before publishing or merging.

## License

ReactiveUI.Primitives is licensed under the MIT license. See `LICENSE` for details.
