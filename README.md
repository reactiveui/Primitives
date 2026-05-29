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
dotnet add package ReactiveUI.Primitives.WinUI
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
- `ReactiveUI.Primitives.WinUI`: `net8.0-windows10.0.19041.0`, `net9.0-windows10.0.19041.0`, `net10.0-windows10.0.19041.0`
- `ReactiveUI.Primitives.Blazor`: `net8.0`, `net9.0`, `net10.0`
- `ReactiveUI.Primitives.Maui`: `net9.0`, `net10.0`

Runtime package dependencies are intentionally small. The base production package does not depend on System.Reactive or R3. The only runtime package reference declared directly by `src/ReactiveUI.Primitives/ReactiveUI.Primitives.csproj` is `System.ValueTuple` for `net462`; the bridge source generators are packed as analyzers in the base package rather than shipped as separate NuGet packages. `ReactiveUI.Primitives.Blazor` references `Microsoft.AspNetCore.Components`, `ReactiveUI.Primitives.Maui` references `Microsoft.Maui.Core`, and `ReactiveUI.Primitives.WinUI` references `Microsoft.WindowsAppSDK`. The remaining shared package references are analyzer, SourceLink, versioning, ILLink, reference-assembly, or build-time support packages such as Blazor.Common.Analyzers, Microsoft.SourceLink.GitHub, MinVer, Roslynator.Analyzers, SonarAnalyzer.CSharp, stylecop.analyzers, Microsoft.NET.ILLink.Tasks, and Microsoft.NETFramework.ReferenceAssemblies. Benchmark projects may reference System.Reactive and R3 as comparison baselines, but those references are not production dependencies.

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

Sequencers live in `ReactiveUI.Primitives.Concurrency` and implement `ISequencer`. The core `ReactiveUI.Primitives` package does not reference WPF, Windows Forms, WinUI, Blazor, or MAUI; UI-thread sequencers are provided by optional integration packages.

| Sequencer | Purpose |
|---|---|
| `Sequencer.Immediate` / `ImmediateSequencer.Instance` | Execute work immediately. |
| `Sequencer.CurrentThread` / `CurrentThreadSequencer.Instance` | Queue recursive/current-thread work deterministically. |
| `ThreadPoolSequencer.Instance` | Schedule work through the thread pool. |
| `TaskPoolSequencer.Instance` | Schedule work through tasks. |
| `SynchronizationContextSequencer` | Schedule through a `SynchronizationContext`. |
| `DispatcherSequencer` | Schedule onto a WPF dispatcher from `ReactiveUI.Primitives.Wpf`. |
| `ControlSequencer` | Schedule onto a Windows Forms control from `ReactiveUI.Primitives.WinForms`. |
| `DispatcherQueueSequencer` | Schedule onto a WinUI dispatcher queue from `ReactiveUI.Primitives.WinUI`. |
| `BlazorRendererSequencer` | Schedule component work through Blazor's renderer from `ReactiveUI.Primitives.Blazor`. |
| `MauiDispatcherSequencer` | Schedule onto an MAUI dispatcher from `ReactiveUI.Primitives.Maui`. |
| `VirtualClock` / `TestClock` | Virtual-time scheduling for deterministic tests. |

WPF, Windows Forms, WinUI, Blazor, and MAUI sequencers derive from `DispatchSequencerBase`. That shared base batches ready work into a single posted dispatcher drain, preserves FIFO order, skips cancelled work lazily, and routes delayed UI work through the shared `ThreadPoolSequencer` timing queue before marshaling back to the UI thread. Platform packages only provide the final dispatcher-specific post primitive.

Scheduling APIs include absolute, relative, recursive, and action-based overloads:

```csharp
using ReactiveUI.Primitives.Concurrency;

IDisposable scheduled = ThreadPoolSequencer.Instance.Schedule(
    TimeSpan.FromMilliseconds(100),
    () => Console.WriteLine("scheduled work"));

scheduled.Dispose();
```

For hot convenience-call paths, prefer the stateful overload with a static callback to avoid closure capture:

```csharp
sequencer.Schedule(observer, static target => target.OnCompleted());
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

The generators always emit small internal marker attributes stamped with the generator contract version. They emit bridge extension methods only when the consumer project already references the relevant external library:

- System.Reactive bridge checks for `System.Reactive.Linq.Observable`.
- System.Reactive scheduler bridge checks for `System.Reactive.Concurrency.IScheduler`.
- R3 bridge checks for `R3.Observable<T>`.

Generated bridge namespaces:

- `ReactiveUI.Primitives.SystemReactiveBridge`
- `ReactiveUI.Primitives.R3Bridge`

Generated System.Reactive bridge methods:

- `AsPrimitivesSignal<T>(this System.IObservable<T> source)`
- `AsSystemObservable<T>(this System.IObservable<T> source)`
- `AsSequencer(this System.Reactive.Concurrency.IScheduler scheduler)`
- `AsSystemScheduler(this ReactiveUI.Primitives.Concurrency.ISequencer sequencer)`

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

The scheduler bridge is a compatibility boundary. Its generated adapters carry the recursive `IScheduler.Schedule` callback and `IDisposable` return shape so native `ISequencer`/`IWorkItem` paths stay on the lean core scheduler contract.

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
| WinUI dispatcher queue scheduling | `DispatcherQueueSequencer` from `ReactiveUI.Primitives.WinUI` |
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

The latest complete BenchmarkDotNet run finished on 2026-05-29 at 06:37:05 Europe/London with .NET SDK 10.0.300 and .NET runtime 10.0.8 on Windows 11. It executed 201 benchmarks with no skipped suites in 00:21:37:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Latest artifact paths:

- `BenchmarkDotNet.Artifacts/BenchmarkRun-20260529-061525.log`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-29-06-37-05-report-github.md`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-29-06-37-05-report.html`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-29-06-37-05-report.csv`

Smoke validation for deterministic benchmark behavior passed for 67 benchmark groups with:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --smoke
```

The latest direct test/coverage validation passed 181/181 net10.0 tests and reports 91.74% line coverage and 86.01% branch coverage from `.tmp/test-results-20260529-net10-readme/coverage.cobertura.xml`. The full solution test pass also passed 558/558 tests across net8.0, net9.0, and net10.0.

The table below is generated from the joined BenchmarkDotNet CSV and uses `Mean / Allocated` for each cell.

| Scenario | ReactiveUI.Primitives | System.Reactive | R3 |
|---|---:|---:|---:|
| Emit subscribe | 0.2230 ns / 0 B | 47.3735 ns / 120 B | 28.3341 ns / 80 B |
| None subscribe | 2.9952 ns / 40 B | 42.0283 ns / 96 B | 27.3123 ns / 56 B |
| Sequence subscribe | 46.4699 ns / 96 B | 2,389.8479 ns / 2472 B | 69.8169 ns / 80 B |
| Loop subscribe | 6.7140 ns / 0 B | 2,278.0993 ns / 2408 B | 67.8307 ns / 80 B |
| Fail subscribe | 55.9618 ns / 120 B | 104.6122 ns / 240 B | 83.5704 ns / 200 B |
| FromEnumerable subscribe | 49.9427 ns / 40 B | 2,242.8721 ns / 2504 B | 75.4156 ns / 88 B |
| Completed task bridge | 9.6957 ns / 88 B | 762.3672 ns / 793 B | 33.7027 ns / 88 B |
| Create subscribe | 33.3256 ns / 112 B | 44.6606 ns / 168 B | 52.2479 ns / 128 B |
| CreateSafe subscribe | 33.3246 ns / 112 B | 44.1128 ns / 168 B | 51.0502 ns / 128 B |
| Lazy subscribe | 66.2262 ns / 240 B | 1,287.3555 ns / 1512 B | 106.8332 ns / 152 B |
| Start subscribe | 39.6918 ns / 208 B | 769.3907 ns / 751 B | 55.2220 ns / 160 B |
| Unfold subscribe | 10.0288 ns / 0 B | 2,128.0362 ns / 2768 B | 91.3892 ns / 128 B |
| Use subscribe | 36.4206 ns / 144 B | 78.9546 ns / 168 B | 51.2890 ns / 128 B |
| FromAsyncEnumerable subscribe | 1,006.7269 ns / 600 B | 1,645.8238 ns / 2311 B | 1,136.1530 ns / 1023 B |
| Silent subscribe/dispose | 0.2315 ns / 0 B | 5.4195 ns / 40 B | 16.9768 ns / 56 B |
| Map + Keep over range | 124.3913 ns / 208 B | 2,472.8123 ns / 2616 B | 286.3641 ns / 272 B |
| Reduce + Any + Count | 174.3840 ns / 824 B | 5,174.9387 ns / 6216 B | 598.8228 ns / 1280 B |
| Prepend + Append + DefaultIfEmpty | 30.1072 ns / 168 B | 843.4910 ns / 1282 B | 127.8896 ns / 288 B |
| DefaultIfEmpty(empty) | 5.6240 ns / 64 B | 59.9335 ns / 144 B | 59.1663 ns / 136 B |
| FlatMap over ranges | 946.5697 ns / 712 B | 3,506.1713 ns / 3872 B | 1,003.7753 ns / 1040 B |
| Pair over ranges | 38.5910 ns / 232 B | 2,915.9901 ns / 2976 B | 652.8988 ns / 656 B |
| Chain ranges | 69.5590 ns / 256 B | 2,594.3044 ns / 2856 B | 232.0761 ns / 360 B |
| Blend ranges | 68.7321 ns / 256 B | 3,543.2549 ns / 3953 B | 642.6874 ns / 352 B |
| Race ranges | 33.5591 ns / 192 B | 1,404.2164 ns / 1760 B | 261.6344 ns / 360 B |
| SwitchTo ranges | 73.2220 ns / 336 B | 1,970.9442 ns / 2336 B | 704.1251 ns / 392 B |
| SyncLatest ranges | 79.4395 ns / 368 B | 2,944.0282 ns / 2824 B | 622.3868 ns / 344 B |
| Latch ranges | 81.0008 ns / 368 B | 3,225.7811 ns / 2824 B | 243.9050 ns / 248 B |
| ForkJoin ranges | 50.8807 ns / 344 B | 3,269.8635 ns / 3136 B | 907.5060 ns / 504 B |
| Shift range | 144.9936 ns / 528 B | 5,006.6565 ns / 39584 B | 1,842.7436 ns / 2200 B |
| DelayStart range | 134.7077 ns / 528 B | 2,128.8455 ns / 26456 B | 301.3397 ns / 552 B |
| Calm burst | 576.4417 ns / 1184 B | 2,324.6424 ns / 36480 B | 1,507.2980 ns / 1512 B |
| Probe latest | 209.6742 ns / 640 B | 1,811.5047 ns / 26264 B | 299.8862 ns / 664 B |
| Timestamp range | 34.7496 ns / 144 B | 1,578.6798 ns / 1512 B | 332.1624 ns / 152 B |
| TimeInterval range | 25.4590 ns / 152 B | 1,603.1043 ns / 1616 B | 419.3172 ns / 160 B |
| Expire idle | 231.4570 ns / 704 B | 1,137.3182 ns / 29776 B | 378.0569 ns / 784 B |
| ObserveOn immediate | 21.4403 ns / 96 B | 14,471.7122 ns / 11310 B | 885.3536 ns / 432 B |
| History subscribe | 322.3987 ns / 320 B | 670.2288 ns / 696 B | 387.4820 ns / 688 B |
| StateSignal 32 values | 237.1878 ns / 120 B | 565.4464 ns / 200 B | 594.9903 ns / 192 B |
| StateSignal 1024 values | 6,730.9924 ns / 120 B | 15,726.8158 ns / 200 B | 15,691.3635 ns / 192 B |
| Signal emit, 32 values | 65.5355 ns / 136 B | 89.1690 ns / 136 B | 111.8446 ns / 160 B |
| Signal emit, 1024 values | 1,637.7626 ns / 136 B | 1,673.6031 ns / 136 B | 1,986.4624 ns / 160 B |
| Signal subscribe/dispose, 8 observers | 228.5621 ns / 592 B | 283.1068 ns / 1288 B | 436.3383 ns / 840 B |
| Signal subscribe/dispose, 64 observers | 2,662.1290 ns / 3800 B | 3,546.0312 ns / 38472 B | 3,455.8392 ns / 6216 B |
| ShareLive connect | 130.7828 ns / 384 B | 2,528.9932 ns / 2696 B | 426.6950 ns / 368 B |
| Share live subscribe | 199.5178 ns / 712 B | 2,700.8301 ns / 2880 B | 471.9188 ns / 488 B |
| Replay live late subscribe | 608.7132 ns / 568 B | 3,480.7842 ns / 3408 B | 812.3979 ns / 1360 B |
| AutoShare subscribe | 202.3044 ns / 712 B | 2,697.0149 ns / 2880 B | 482.7859 ns / 488 B |
| AutoConnect subscribe | 155.5013 ns / 592 B | 2,502.8542 ns / 2736 B | 372.7091 ns / 368 B |
| StateSignal updates | 238.1559 ns / 120 B | 548.4785 ns / 200 B | 594.8021 ns / 192 B |
| ReadOnlyState projection | 52.5724 ns / 144 B | 84.4462 ns / 328 B | 161.3650 ns / 312 B |
| TaskSignal subscribe | 34.0054 ns / 240 B | 665.1771 ns / 886 B | 37.2785 ns / 160 B |
| Command execute | 32.5825 ns / 152 B | 645.0811 ns / 1089 B | 103.4041 ns / 296 B |
| Command result subscribe | 57.1177 ns / 224 B | 36.0275 ns / 136 B | 61.6326 ns / 160 B |
| CollectList range | 102.0529 ns / 552 B | 2,463.1842 ns / 3352 B | 149.5652 ns / 632 B |
| CollectArray range | 63.5489 ns / 520 B | 2,470.4433 ns / 3504 B | 158.9210 ns / 784 B |
| CollectArrayAsync range | 33.3098 ns / 384 B | 2,595.1312 ns / 3848 B | 155.9081 ns / 784 B |
| FirstAsync range | 5.8384 ns / 56 B | 2,353.8883 ns / 2792 B | 67.9760 ns / 208 B |
| ToTask range | 13.6859 ns / 192 B | 2,403.5159 ns / 2824 B | 88.2610 ns / 208 B |
| Count(predicate) range | 18.8970 ns / 96 B | 2,369.8963 ns / 2520 B | 92.8400 ns / 200 B |
| LongCount(predicate) range | 19.5172 ns / 104 B | 2,323.6443 ns / 2536 B | 98.4867 ns / 272 B |
| All range | 17.0983 ns / 96 B | 2,362.4686 ns / 2520 B | 80.4268 ns / 192 B |
| Contains range | 9.2651 ns / 96 B | 2,388.1732 ns / 2528 B | 86.6274 ns / 200 B |
| All + Contains range | 27.3462 ns / 192 B | 4,757.9435 ns / 5048 B | 210.3620 ns / 392 B |
| Pocket dispose | 60.8709 ns / 408 B | 91.0020 ns / 512 B | 68.8285 ns / 480 B |
| CurrentThread schedule | 6.6194 ns / 88 B | 16.0136 ns / 88 B | 27.2022 ns / 56 B |
| Safe witness | 16.9332 ns / 136 B | 11.8405 ns / 136 B | 16.9656 ns / 56 B |
| Completed Spark | 0.0056 ns / 0 B | 0.0000 ns / 0 B | 0.0271 ns / 0 B |

The five rows selected from the improvement review as the main time/scheduler optimization gate were `Shift range`, `DelayStart range`, `Calm burst`, `Probe latest`, and `Expire idle`. In the complete run all five beat both System.Reactive and R3 on mean time and allocation. The same optimization pass also brought `Timestamp range`, `TimeInterval range`, `ObserveOn immediate`, `SwitchTo ranges`, `StateSignal updates`, and `ReadOnlyState projection` below both alternatives on mean time and allocation.

Interpretation notes:

- ReactiveUI.Primitives leads on mean time in 64 of the 67 listed groups. The remaining material time gaps are `Command result subscribe` against a System.Reactive subject-only baseline and `Safe witness` against a less guarded delegate observer; `Completed Spark` measures at empty-method scale with zero allocation for every implementation.
- The public API/operator matrix above is backed by deterministic smoke coverage in `Program.RunSmokeBenchmarksAsync`: every row has matching ReactiveUI.Primitives, System.Reactive, and R3 calls where alternatives exist, and the smoke run validates each benchmark path returns the same observable result before BenchmarkDotNet measures throughput and allocation unless the row is one of the documented scheduling-total exceptions below.
- The only intentional smoke output differences are `SwitchTo ranges`, `SyncLatest ranges`, and `Latch ranges`: System.Reactive produces different synchronous range totals for those coordinator operators, while ReactiveUI.Primitives and R3 agree on the emitted totals. `--smoke` permits only those System.Reactive differences and still fails if ReactiveUI.Primitives diverges from R3 or if any other benchmark group loses parity.
- Candidate scenarios where ReactiveUI.Primitives is not strictly both faster and lower-allocation than both alternatives are tracked explicitly below. Most remaining rows are allocation-only gaps where ReactiveUI.Primitives is still faster on mean time; rows marked as exceptions are retained because the extra cost buys ReactiveUI.Primitives semantics (safe terminal/disposal behavior, `IObservable<T>`/`IObserver<T>` compatibility, deterministic `ISequencer` scheduling, or live-signal lifecycle ownership) while preserving the project rule that System.Reactive/R3 are benchmark-only dependencies.
- Near-zero singleton measurements (`Emit`, `Silent`, and `Spark` paths) may trigger BenchmarkDotNet `ZeroMeasurement` warnings; those warnings mean the method duration is indistinguishable from the empty-method overhead, not that the benchmark failed.

Candidate/performance exception matrix:

| Scenario | Observed gap | Decision and trade-off |
|---|---|---|
| `Sequence subscribe` | allocation >= R3 (96 B vs 80 B). | Accepted exception: the range path keeps BCL observable compatibility and explicit disposal ownership; mean time remains ahead of R3. |
| `Completed task bridge` | allocation ties R3 (88 B vs 88 B). | Accepted exception: the bridge has the lowest mean time and ties R3 allocation while preserving task-observer completion semantics. |
| `Lazy subscribe` | allocation >= R3 (240 B vs 152 B). | Accepted exception: lazy factory ownership keeps the BCL subscription and disposal contract; mean time remains ahead of R3. |
| `Start subscribe` | allocation >= R3 (208 B vs 160 B). | Accepted exception: start uses the Primitives scheduling/result surface; mean time remains ahead of R3. |
| `Use subscribe` | allocation >= R3 (144 B vs 128 B). | Accepted exception: resource ownership is explicit and BCL-compatible; mean time remains ahead of R3. |
| `SyncLatest ranges` | allocation >= R3 (368 B vs 344 B). | Accepted exception: coordinator operators keep general `IObservable<T>` subscription ownership and terminal arbitration while leading on mean time. |
| `Latch ranges` | allocation >= R3 (368 B vs 248 B). | Accepted exception: coordinator operators keep general `IObservable<T>` subscription ownership and terminal arbitration while leading on mean time. |
| `Signal emit, 32 values` | allocation ties System.Reactive (136 B vs 136 B). | Accepted exception: emit loops lead on throughput and tie System.Reactive allocation; strict lower allocation is not meaningful for this BCL-compatible shape. |
| `Signal emit, 1024 values` | allocation ties System.Reactive (136 B vs 136 B). | Accepted exception: emit loops lead on throughput and tie System.Reactive allocation; strict lower allocation is not meaningful for this BCL-compatible shape. |
| `ShareLive connect` | allocation >= R3 (384 B vs 368 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state while leading on mean time. |
| `Share live subscribe` | allocation >= R3 (712 B vs 488 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state while leading on mean time. |
| `AutoShare subscribe` | allocation >= R3 (712 B vs 488 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state while leading on mean time. |
| `AutoConnect subscribe` | allocation >= R3 (592 B vs 368 B). | Accepted exception: live sharing owns connection/ref-count lifecycle and safe disconnect state while leading on mean time. |
| `TaskSignal subscribe` | allocation >= R3 (240 B vs 160 B). | Accepted exception: `TaskSignal` preserves BCL signal completion semantics while leading on mean time. |
| `Command result subscribe` | time >= System.Reactive (57.1177 ns vs 36.0275 ns); allocation >= System.Reactive (224 B vs 136 B); allocation >= R3 (224 B vs 160 B). | Accepted exception: the System.Reactive row is subject-only synchronous publication, while `CommandSignal` includes command execution gating plus result publication and still beats R3 on mean time. |
| `CurrentThread schedule` | allocation ties System.Reactive (88 B vs 88 B); allocation >= R3 (88 B vs 56 B). | Accepted exception: current-thread scheduling uses the project sequencer queue contract; throughput leads both alternatives. |
| `Safe witness` | time >= System.Reactive (16.9332 ns vs 11.8405 ns); allocation ties System.Reactive (136 B vs 136 B); allocation >= R3 (136 B vs 56 B). | Accepted exception: the wrapper enforces safe observer/terminal behavior; the System.Reactive row is a less guarded delegate observer. |
| `Completed Spark` | time >= System.Reactive (0.0056 ns vs 0.0000 ns); allocation ties System.Reactive and R3 (0 B). | Accepted exception: all implementations allocate zero and measure at empty-method scale, so a strict lower-allocation win is not meaningful. |

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
| `src/ReactiveUI.Primitives.WinUI` | Optional WinUI dispatcher queue integration library. |
| `src/ReactiveUI.Primitives.Blazor` | Optional Blazor renderer integration library. |
| `src/ReactiveUI.Primitives.Maui` | Optional MAUI dispatcher integration library. |
| `src/ReactiveUI.Primitives.SystemReactiveBridge.Generator` | Source generator for System.Reactive bridge adapters. |
| `src/ReactiveUI.Primitives.R3Bridge.Generator` | Source generator for R3 bridge adapters. |
| `src/ReactiveUI.Primitives.Tests` | Test project using Microsoft Testing Platform/TUnit-style validation. |
| `src/benchmarks/ReactiveUI.Primitives.Benchmarks` | BenchmarkDotNet comparison harness. |

## Validation commands

### Latest local validation used for this README

```powershell
# Build solution.
dotnet build .\src\ReactiveUI.Primitives.slnx -c Release --no-restore -v:minimal -p:UseSharedCompilation=false -p:BuildInParallel=false -maxcpucount:1

# Net10 coverage run through the Microsoft Testing Platform/TUnit executable.
dotnet .\src\tests\ReactiveUI.Primitives.Tests\bin\Release\net10.0\ReactiveUI.Primitives.Tests.dll --results-directory .\.tmp\test-results-20260529-net10-readme --report-trx --report-trx-filename net10.trx --coverage --coverage-output coverage.cobertura.xml --coverage-output-format cobertura --no-progress --maximum-parallel-tests 1 --timeout 10m --output Normal --show-stdout Failed --show-stderr Failed --disable-logo

# All target-framework tests.
Push-Location .\src
dotnet test .\ReactiveUI.Primitives.slnx -c Release --no-build -v:minimal --results-directory ..\.tmp\test-results-20260529-solution-readme -- --report-trx --report-trx-filename solution.trx --no-progress --maximum-parallel-tests 1 --timeout 10m --disable-logo
Pop-Location

# Benchmark smoke and complete joined comparison run.
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --smoke
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Results: build passed with 0 warnings/0 errors; the net10.0 coverage run passed 181/181 tests; `dotnet test` passed 558/558 tests across net8.0, net9.0, and net10.0; benchmark smoke parity passed for 67 groups; the joined BenchmarkDotNet run executed 201 benchmarks. The latest coverage snapshot reported 91.74% line (10740/11707) and 86.01% branch (4280/4976) coverage.

### Package verification

For NuGet package verification, inspect the generated `.nupkg` and confirm:

- `README.md` is present.
- The nuspec contains `<readme>README.md</readme>`.
- Bridge generator DLLs are present under `analyzers/dotnet/cs`.
- Production runtime dependencies do not include System.Reactive or R3.
- The core `ReactiveUI.Primitives` package does not reference WPF, Windows Forms, WinUI, Blazor, or MAUI assemblies; those integrations ship from `ReactiveUI.Primitives.Wpf`, `ReactiveUI.Primitives.WinForms`, `ReactiveUI.Primitives.WinUI`, `ReactiveUI.Primitives.Blazor`, and `ReactiveUI.Primitives.Maui`.

## Practical migration checklist

1. Replace subject construction with `Signal<T>`, `StateSignal<T>`, or `HistorySignal<T>` depending on current behavior.
2. Replace factories: `Observable.Return/Empty/Throw/Timer/Interval` to `Signal.Emit/None/Fail/After/Pulse`.
3. Replace hot-path operators with Primitives names: `Select -> Map`, `Where -> Keep`, `SelectMany -> FlatMap`, `Do -> Tap`, `Scan -> Fold`, `Aggregate -> Reduce`, `Amb -> Race`.
4. Replace composite/serial disposables with `MultipleDisposable`/`Pocket` and `SingleReplaceableDisposable`/`Slot`.
5. Keep System.Reactive/R3 at application boundaries only when required; use generated bridge methods when those packages are already referenced.
6. Run build, tests, pack, and `git diff --check` before publishing or merging.

## License

ReactiveUI.Primitives is licensed under the MIT license. See `LICENSE` for details.
