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

`N/A` means the current benchmark harness does not contain a matching measured row for that library.

| Scenario | ReactiveUI.Primitives | System.Reactive | R3 |
|---|---:|---:|---:|
| Return subscribe | 0.2089 ns / 0 B | 45.7505 ns / 120 B | 28.1108 ns / 72 B |
| Empty subscribe | 2.6805 ns / 40 B | 40.1536 ns / 96 B | 25.8399 ns / 48 B |
| Range subscribe | 46.0466 ns / 96 B | 2,452.1549 ns / 2,472 B | 63.6342 ns / 72 B |
| Repeat subscribe | 6.7455 ns / 0 B | 2,275.7650 ns / 2,408 B | 64.8692 ns / 72 B |
| Throw subscribe | 54.9685 ns / 120 B | 106.8594 ns / 240 B | 85.8239 ns / 192 B |
| FromEnumerable subscribe | 49.3764 ns / 40 B | 2,171.2470 ns / 2,504 B | 70.3934 ns / 80 B |
| Completed task bridge | 8.7892 ns / 88 B | 769.1087 ns / 793 B | N/A |
| Create subscribe | 43.7376 ns / 248 B | N/A | N/A |
| CreateSafe subscribe | 44.1792 ns / 248 B | N/A | N/A |
| Defer subscribe | 66.0839 ns / 240 B | N/A | N/A |
| Start subscribe | 51.7062 ns / 376 B | N/A | N/A |
| Unfold subscribe | 167.5562 ns / 736 B | N/A | N/A |
| Use subscribe | 67.7116 ns / 432 B | N/A | N/A |
| FromAsyncEnumerable subscribe | 1,921.1208 ns / 2,052 B | N/A | N/A |
| Never subscribe/dispose | 0.2163 ns / 0 B | N/A | N/A |
| Map + Keep over range | 130.0149 ns / 208 B | 2,461.1312 ns / 2,616 B | 256.9470 ns / 264 B |
| Aggregate + Any + Count | 229.9964 ns / 992 B | 5,073.0502 ns / 5,896 B | 529.5892 ns / 1,280 B |
| StartWith + Append + DefaultIfEmpty | 45.2433 ns / 184 B | 869.3671 ns / 1,257 B | 128.0685 ns / 280 B |
| SelectMany over ranges | 939.2041 ns / 712 B | 3,357.9581 ns / 3,872 B | 965.1100 ns / 1,032 B |
| Zip over ranges | 38.8399 ns / 232 B | 2,903.8925 ns / 2,976 B | 658.2362 ns / 648 B |
| Concat ranges | 67.0788 ns / 256 B | N/A | N/A |
| Merge ranges | 66.9464 ns / 256 B | N/A | N/A |
| Race ranges | 37.7646 ns / 192 B | N/A | N/A |
| Switch ranges | 797.3144 ns / 1,376 B | N/A | N/A |
| CombineLatest ranges | 95.0119 ns / 504 B | N/A | N/A |
| WithLatest ranges | 104.8306 ns / 504 B | N/A | N/A |
| ForkJoin ranges | 67.7277 ns / 480 B | N/A | N/A |
| Delay range | 3,132.9781 ns / 38,816 B | N/A | N/A |
| DelayStart range | 874.3075 ns / 25,520 B | N/A | N/A |
| Throttle burst | 2,504.3725 ns / 38,384 B | N/A | N/A |
| Sample latest | 1,045.5780 ns / 26,072 B | N/A | N/A |
| Timestamp range | 394.0507 ns / 312 B | N/A | N/A |
| TimeInterval range | 478.5383 ns / 736 B | N/A | N/A |
| Timeout never | 976.3098 ns / 25,816 B | N/A | N/A |
| ObserveOn immediate | 21.8563 ns / 96 B | N/A | N/A |
| Replay subscribe | 324.2733 ns / 320 B | 665.7033 ns / 696 B | N/A |
| BehaviorSignal 32 values | 554.5077 ns / 176 B | 581.5846 ns / 200 B | 594.3121 ns / 184 B |
| BehaviorSignal 1024 values | 15,698.1415 ns / 176 B | 15,826.6246 ns / 200 B | 15,702.7802 ns / 184 B |
| Signal emit, 32 values | 65.8803 ns / 136 B | 90.0502 ns / 136 B | 116.1177 ns / 152 B |
| Signal emit, 1024 values | 1,650.9938 ns / 136 B | 1,676.8777 ns / 136 B | 1,984.4349 ns / 152 B |
| Signal subscribe/dispose, 8 observers | 240.6380 ns / 592 B | 284.9100 ns / 1,288 B | 450.5067 ns / 840 B |
| Signal subscribe/dispose, 64 observers | 2,599.1562 ns / 3,800 B | 3,600.1331 ns / 38,472 B | 3,401.7292 ns / 6,216 B |
| Publish live connect | 125.5809 ns / 384 B | N/A | N/A |
| Share live subscribe | 225.6140 ns / 848 B | N/A | N/A |
| Replay live late subscribe | 595.9243 ns / 568 B | N/A | N/A |
| RefCount subscribe | 222.4420 ns / 848 B | N/A | N/A |
| AutoConnect subscribe | 167.9847 ns / 728 B | N/A | N/A |
| StateSignal updates | 552.4185 ns / 176 B | N/A | N/A |
| ReadOnlyState projection | 123.5420 ns / 248 B | N/A | N/A |
| TaskSignal subscribe | 2,384.2121 ns / 3,875 B | N/A | N/A |
| Command execute | 114.0456 ns / 600 B | N/A | N/A |
| Command result subscribe | 137.9245 ns / 672 B | N/A | N/A |
| CollectList range | 115.9544 ns / 688 B | N/A | N/A |
| CollectArray range | 83.1385 ns / 656 B | N/A | N/A |
| CollectArrayAsync range | 33.8094 ns / 384 B | N/A | N/A |
| FirstAsync range | 5.9320 ns / 56 B | N/A | N/A |
| ToTask range | 13.9715 ns / 192 B | N/A | N/A |
| Count(predicate) range | 55.0441 ns / 144 B | N/A | N/A |
| All + Contains range | 204.1768 ns / 1,024 B | N/A | N/A |
| Pocket dispose | 60.2873 ns / 408 B | 93.1830 ns / 512 B | N/A |
| CurrentThread schedule | 12.4912 ns / 88 B | 14.7694 ns / 88 B | N/A |
| Safe witness | 21.7079 ns / 168 B | N/A | N/A |
| Completed Spark | 0.0006 ns / 0 B | N/A | N/A |

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
