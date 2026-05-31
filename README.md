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
2. [Agent Skills](#agent-skills)
3. [Target frameworks and dependencies](#target-frameworks-and-dependencies)
4. [Core model](#core-model)
5. [Creation factories](#creation-factories)
6. [Operators](#operators)
7. [ReactiveUI.Primitives.Async](#reactiveuiprimitivesasync)
8. [ReactiveUI.Primitives.Extensions](#reactiveuiprimitivesextensions)
9. [Stateful signals and subject-like types](#stateful-signals-and-subject-like-types)
10. [Sequencers](#sequencers)
11. [Threading, disposal, and error semantics](#threading-disposal-and-error-semantics)
12. [Source-generator bridge behavior](#source-generator-bridge-behavior)
13. [Migration guides](#systemreactive-to-reactiveuiprimitives-migration-guide)
14. [Benchmarks and performance posture](#benchmarks-and-performance-posture)
15. [Repository layout](#repository-layout)
16. [Validation commands](#validation-commands)

## Install

When the package is available on your configured NuGet feed:

```bash
dotnet add package ReactiveUI.Primitives
```

Optional Async, Extensions, and UI/platform integration packages are split out so the base package stays free of async-helper and UI framework references:

```bash
dotnet add package ReactiveUI.Primitives.Async
dotnet add package ReactiveUI.Primitives.Extensions
dotnet add package ReactiveUI.Primitives.Wpf
dotnet add package ReactiveUI.Primitives.WinForms
dotnet add package ReactiveUI.Primitives.WinUI
dotnet add package ReactiveUI.Primitives.Blazor
dotnet add package ReactiveUI.Primitives.Maui
```

Then import the namespaces you need:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Async.Signals;
using ReactiveUI.Primitives.Signals;
```

The package metadata is configured to include this README in the NuGet package via `PackageReadmeFile=README.md`. The base package also packs both bridge source-generator assemblies under `analyzers/dotnet/cs`:

- `ReactiveUI.Primitives.SystemReactiveBridge.Generator.dll`
- `ReactiveUI.Primitives.R3Bridge.Generator.dll`

Those generators are analyzers. They do not add runtime System.Reactive or R3 dependencies to ReactiveUI.Primitives. They emit bridge code only when the consuming compilation already references the relevant external library symbols.

## Agent Skills

The base `ReactiveUI.Primitives` NuGet package includes `Skills.md` at the package root. It is an agent-oriented guide for using ReactiveUI.Primitives, Async, Extensions, UI sequencers, bridge source generators, and migration from System.Reactive or R3 while assuming the libraries are consumed from NuGet packages.

After package restore, locate the file in the local NuGet package cache:

```powershell
$version = "<version>"
$skill = "$env:USERPROFILE\.nuget\packages\reactiveui.primitives\$version\Skills.md"
```

On macOS or Linux:

```bash
version="<version>"
skill="$HOME/.nuget/packages/reactiveui.primitives/$version/Skills.md"
```

Install the skill by copying the contents of `Skills.md` into the instruction location supported by the agent. Agents that expect a `SKILL.md` file should use a `reactiveui-primitives` directory and rename the copied file to `SKILL.md`.

| Agent | Recommended project-local install | Notes |
|---|---|---|
| [OpenAI Codex](https://developers.openai.com/codex/skills) | `.agents/skills/reactiveui-primitives/SKILL.md` | Codex also supports user-level skills under `$HOME/.agents/skills`. |
| [Claude Code](https://code.claude.com/docs/en/skills) | `.claude/skills/reactiveui-primitives/SKILL.md` | Claude Code also supports personal skills under `~/.claude/skills`. |
| [Cline](https://docs.cline.bot/customization/skills) | `.cline/skills/reactiveui-primitives/SKILL.md` | Cline skills must be enabled in Cline's feature settings. |
| [GitHub Copilot](https://docs.github.com/en/copilot/concepts/prompting/response-customization) | `.github/instructions/reactiveui-primitives.instructions.md` | For repository-wide behavior, summarize or link the skill from `.github/copilot-instructions.md`. |
| [Cursor](https://docs.cursor.com/en/context) | `.cursor/rules/reactiveui-primitives.mdc` | Cursor project rules are version-controlled under `.cursor/rules`; `AGENTS.md` is also supported. |
| [Windsurf](https://docs.windsurf.com/windsurf/cascade/memories) | `.windsurf/rules/reactiveui-primitives.md` | Windsurf also reads `AGENTS.md` through the same rules engine. |
| [Gemini CLI](https://google-gemini.github.io/gemini-cli/docs/cli/gemini-md.html) | `GEMINI.md` or an imported file referenced from `GEMINI.md` | Gemini CLI loads hierarchical context files and supports importing other markdown files with `@file.md`. |

## Target frameworks and dependencies

The base production `ReactiveUI.Primitives` library uses `$(LibraryTargetFrameworks)` from `src/Directory.Build.props` and currently targets:

- `net8.0`
- `net9.0`
- `net10.0`
- `net462`
- `net472`
- `net48`
- `net481`

Windows UI and platform-integration projects in this repository use their own TFM properties (for example `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, or MAUI/platform-focused TFMs where applicable). Those platform TFMs are not target frameworks of the base `ReactiveUI.Primitives` package.

The optional package TFMs are:

- `ReactiveUI.Primitives.Wpf`: `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net462`, `net472`, `net48`, `net481`
- `ReactiveUI.Primitives.WinForms`: `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net462`, `net472`, `net48`, `net481`
- `ReactiveUI.Primitives.WinUI`: `net8.0-windows10.0.19041.0`, `net9.0-windows10.0.19041.0`, `net10.0-windows10.0.19041.0`
- `ReactiveUI.Primitives.Blazor`: `net8.0`, `net9.0`, `net10.0`
- `ReactiveUI.Primitives.Maui`: `net9.0`, `net10.0`
- `ReactiveUI.Primitives.Async`: `net8.0`, `net9.0`, `net10.0`, `net462`, `net472`, `net48`, `net481`
- `ReactiveUI.Primitives.Extensions`: `net8.0`, `net9.0`, `net10.0`, `net462`, `net472`, `net48`, `net481`

Runtime package dependencies are intentionally small. The base production package does not depend on System.Reactive or R3. The only runtime package reference declared directly by `src/ReactiveUI.Primitives/ReactiveUI.Primitives.csproj` is `System.ValueTuple` for `net462`; the bridge source generators are packed as analyzers in the base package rather than shipped as separate NuGet packages. `ReactiveUI.Primitives.Async` and `ReactiveUI.Primitives.Extensions` reference `ReactiveUI.Primitives`; their additional package references are limited to .NET Framework compatibility/support packages such as `System.ValueTuple`, Polyfill, Microsoft.Bcl.TimeProvider, System.Threading.Channels, System.Runtime.CompilerServices.Unsafe, System.ComponentModel.Annotations, System.Buffers, System.Memory, and System.Collections.Immutable for `net4x` targets. `ReactiveUI.Primitives.Async` also packs the bridge source generators as analyzers so async bridge methods are generated for consumers that reference System.Reactive or R3. `ReactiveUI.Primitives.Extensions` has no production System.Reactive or R3 dependency. `ReactiveUI.Primitives.Blazor` references `Microsoft.AspNetCore.Components`, `ReactiveUI.Primitives.Maui` references `Microsoft.Maui.Core`, and `ReactiveUI.Primitives.WinUI` references `Microsoft.WindowsAppSDK`. The remaining shared package references are analyzer, SourceLink, versioning, ILLink, reference-assembly, or build-time support packages such as Blazor.Common.Analyzers, Microsoft.SourceLink.GitHub, MinVer, Roslynator.Analyzers, SonarAnalyzer.CSharp, stylecop.analyzers, Microsoft.NET.ILLink.Tasks, and Microsoft.NETFramework.ReferenceAssemblies. Benchmark projects may reference System.Reactive, R3, and ReactiveUI.Extensions as comparison baselines, but those references are not production dependencies.

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

## ReactiveUI.Primitives.Async

`ReactiveUI.Primitives.Async` is the async counterpart to the base `ReactiveUI.Primitives` surface. It keeps the Primitives vocabulary and adds `ValueTask`/`CancellationToken`-aware observer calls for producers and consumers that need asynchronous notification, asynchronous disposal, or async stream collection.

Core async contracts and data types:

| API | Purpose |
|---|---|
| `IObservableAsync<T>` | Async observable contract. `SubscribeAsync` receives an `IObserverAsync<T>` and returns an `IAsyncDisposable`. |
| `IObserverAsync<T>` | Async observer contract with `OnNextAsync`, `OnErrorResumeAsync`, `OnCompletedAsync`, and `DisposeAsync`. |
| `ObserverAsync<T>` | Base observer type for implementing async observers. |
| `ISignalAsync<T>` | Pushable async signal that combines `IObserverAsync<T>`, `IObservableAsync<T>`, and a `Values` observable. |
| `SignalAsync<T>` | Abstract base and static factory/operator host for async observables. |
| `ConnectableSignalAsync<T>` | Async connectable sequence returned by multicast/publish operators. |
| `Result` | Completion result that represents success or terminal failure. |
| `Optional<T>` | Allocation-free optional value used by replay/latest async signals. |
| `AsyncContext` | Dispatch abstraction over `SynchronizationContext`, `TaskScheduler`, or `ISequencer`. |
| `ConcurrentObserverCallsException` | Raised when a serial signal detects concurrent observer calls. |
| `UnhandledExceptionHandler` | Central handler for async fire-and-forget failures. |

Async signal factories live in two places. Use `ReactiveUI.Primitives.Async.Signals.Signal` when you need a mutable signal, and use `SignalAsync` when you need a sequence factory or operator:

| Factory group | APIs |
|---|---|
| Mutable signals | `Signal.Create<T>()`, `Signal.Create<T>(SignalCreationOptions)`, `Signal.CreateBehavior<T>(startValue)`, `Signal.CreateBehavior<T>(startValue, BehaviorSignalCreationOptions)`, `Signal.CreateReplayLatest<T>()`, `Signal.CreateReplayLatest<T>(ReplayLatestSignalCreationOptions)` |
| Signal options | `SignalCreationOptions`, `BehaviorSignalCreationOptions`, `ReplayLatestSignalCreationOptions`, `PublishingOption` |
| Stateless factories | `SignalAsync.Emit`, `EmitRxVoid`, `None`, `Fail`, `Return`, `Empty`, `Never`, `Throw` |
| Sequence factories | `Sequence`, `Range`, `FromEnumerable`, `FromAsyncEnumerable`, `ToAsyncSignal`, `Create`, `CreateAsBackgroundJob`, `Defer`, `FromAsync`, `Use`, `Using` |
| Time factories | `After`, `Every`, `Pulse`, `Timer`, `Interval` |
| Async disposables | `DisposableAsync.Empty`, `DisposableAsync.Create`, `DisposableAsyncSlot`, `SingleAssignmentDisposableAsync`, `SingleReplaceableDisposableAsync`, `MultipleDisposableAsync` |

Async operators follow the same naming style as the core package where that avoids collisions with System.Reactive/R3, while preserving familiar aliases for compatibility:

| Category | APIs |
|---|---|
| Projection/filtering | `Map`, `MapWith`, `Keep`, `KeepWith`, `KeepNotNull`, `KeepType`, `CastTo`, `Select`, `Where`, `OfType`, `Cast`, `Tap`, `Do`, `Fold`, `Scan`, `ReduceAsync`, `AggregateAsync`, `Distinct`, `Unique`, `DistinctBy`, `UniqueBy`, `DistinctUntilChanged`, `DistinctUntilChangedBy`, `SkipWhileNull`, `WhereIsNotNull`, `WhereTrue`, `WhereFalse`, `Not`, `GetMin`, `GetMax`, `ForEach` |
| Composition | `Bind`, `FlatMap`, `SelectMany`, `Chain`, `Concat`, `Blend`, `Merge`, `SwitchTo`, `Switch`, `Pair`, `Zip`, `SyncLatest`, `PairLatest`, `CombineLatest`, `CombineLatestValuesAreAllTrue`, `CombineLatestValuesAreAllFalse`, `GroupBy` |
| Error/retry/recovery | `Reattempt`, `Retry`, `Recover`, `Rescue`, `Resume`, `Catch`, `OnErrorResumeAsFailure` |
| Time/scheduling | `Shift`, `Delay`, `Expire`, `Timeout`, `Throttle`, `ObserveOn`, `Yield` |
| Lifetime/multicast | `Multicast`, `Publish`, `StatelessPublish`, `ReplayLatestPublish`, `StatelessReplayLatestPublish`, `RefCount`, `OnDispose`, `TakeUntil`, `TakeUntilOptions`, `CompletionSignalDelegate`, `Wrap` |
| Sequence boundaries | `Take`, `Skip`, `TakeWhile`, `SkipWhile`, `Lead`, `Prepend`, `StartWith` |
| Terminal helpers | `FirstAsync`, `FirstOrDefaultAsync`, `LastAsync`, `LastOrDefaultAsync`, `SingleAsync`, `SingleOrDefaultAsync`, `AnyAsync`, `AllAsync`, `ContainsAsync`, `CountAsync`, `LongCountAsync`, `ToListAsync`, `CollectListAsync`, `CollectArrayAsync`, `ToDictionaryAsync`, `ToAsyncEnumerable`, `WaitCompletionAsync`, `ForEachAsync`, `SubscribeAsync` |

Basic async sequence example:

```csharp
using ReactiveUI.Primitives.Async;

List<string> labels = await SignalAsync.Sequence(1, 12)
    .Keep(static value => value % 2 == 0)
    .Map(static value => $"even:{value}")
    .ToListAsync();
```

Mutable async signal example:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Signals;

ISignalAsync<int> requests = Signal.Create<int>();

await using IAsyncDisposable subscription = await requests.Values
    .Map(static value => value * 2)
    .SubscribeAsync(value => Console.WriteLine(value));

await requests.OnNextAsync(21, CancellationToken.None);
await requests.OnCompletedAsync(Result.Success);
```

Async context example:

```csharp
using ReactiveUI.Primitives.Async;

AsyncContext context = AsyncContext.From(TaskScheduler.Default);

await using IAsyncDisposable subscription = await SignalAsync.Sequence(1, 3)
    .ObserveOn(context)
    .SubscribeAsync(static value => Console.WriteLine(value));
```

`ReactiveUI.Primitives.Async` also packs the bridge source generators as analyzers. A consumer that references System.Reactive can use generated `ToObservableAsync<T>(this System.IObservable<T>)` and `ToObservable<T>(this IObservableAsync<T>)` adapters. A consumer that references R3 can use generated `AsPrimitivesAsyncObservable<T>(this R3.Observable<T>)` and `AsR3Observable<T>(this IObservableAsync<T>)` adapters.

## ReactiveUI.Primitives.Extensions

`ReactiveUI.Primitives.Extensions` migrates the non-async helper surface from `ReactiveUI.Extensions` onto `ReactiveUI.Primitives`. It is still based on the BCL `IObservable<T>` contract, but it uses `ISequencer` for scheduling and production references only `ReactiveUI.Primitives` plus framework compatibility packages. It does not reference System.Reactive or R3.

Core utility surface:

| API | Purpose |
|---|---|
| `Heartbeat<T>` / `IHeartbeat<T>` | Value plus heartbeat metadata from heartbeat operators. |
| `Stale<T>` / `IStale<T>` | Value plus stale/fresh state from stale-detection operators. |
| `Continuation` | Disposable continuation helper for bridging synchronous waits. |
| `Observables.Return<T>(value)` | Single-value observable factory. |
| `ObserverExtensions.FastForEach` | Pushes enumerable values into an observer with array/list fast paths. |
| `ObservableSubscriptionExtensions` | Synchronous test/utility helpers: `SubscribeGetValue`, `SubscribeAndComplete`, `SubscribeGetError`, `WaitForValue`, `WaitForCompletion`, `WaitForError`. |

Extension operators are grouped below by feature area:

| Category | APIs |
|---|---|
| Filtering/projection | `WhereIsNotNull`, `SkipWhileNull`, `Not`, `WhereTrue`, `WhereFalse`, `WhereSelect`, `SelectConstant`, `TrySelect`, `SelectManyThen`, `Pairwise`, `Partition`, `Filter`, `ForEach`, `Shuffle`, `LatestOrDefault`, `GetMin`, `GetMax`, `CombineLatestValuesAreAllTrue`, `CombineLatestValuesAreAllFalse` |
| Error/retry | `CatchIgnore`, `CatchAndReturn`, `CatchReturn`, `CatchReturnUnit`, `LogErrors`, `OnErrorRetry`, `RetryWithBackoff`, `RetryWithDelay`, `RetryForeverWithDelay`, `RetryWithFixedDelay` |
| Time/scheduling | `SyncTimer`, `ObserveOnIf`, `ScheduleSafe`, `Schedule`, `SampleLatest`, `DetectStale`, `Conflate`, `Heartbeat`, `ThrottleFirst`, `ThrottleUntilTrue`, `ThrottleOnScheduler`, `ThrottleDistinct`, `DebounceImmediate`, `DebounceUntil`, `WaitUntil` |
| Buffer/collection | `BufferUntil`, `BufferUntilIdle`, `BufferUntilInactive`, `FromArray`, `RunAll`, `FirstMatchFromCandidates` |
| Async/sync interaction | `SynchronizeSynchronous`, `SubscribeSynchronous`, `SynchronizeAsync`, `SubscribeAsync`, `SelectAsync`, `SelectAsyncSequential`, `SelectLatestAsync`, `SelectAsyncConcurrent`, `DropIfBusy`, `WithLimitedConcurrency` |
| State/property/lifetime | `AsSignal`, `ToReadOnlyBehavior`, `ReplayLastOnSubscribe`, `SwitchIfEmpty`, `TakeUntil`, `Start`, `Using`, `While`, `ScanWithInitial`, `ToHotTask`, `ToHotValueTask`, `ToPropertyObservable`, `OnNext(params)`, `DoOnSubscribe`, `DoOnDispose` |

Filtering and projection example:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Signals;

IObservable<string> labels = Signal.Sequence(1, 10)
    .WhereSelect(
        static value => value % 2 == 0,
        static value => $"even:{value}");

using IDisposable subscription = labels.Subscribe(Console.WriteLine);
```

Scheduling example:

```csharp
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;

ISequencer sequencer = ThreadPoolSequencer.Instance;

using IDisposable work = "ready"
    .Schedule(TimeSpan.FromMilliseconds(50), sequencer)
    .Subscribe(Console.WriteLine);
```

Async selector example over a BCL observable:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Signals;

IObservable<string> names = Signal.Sequence(1, 3)
    .SelectAsyncSequential(static async value =>
    {
        await Task.Yield();
        return $"item:{value}";
    });

using IDisposable subscription = names.Subscribe(Console.WriteLine);
```

The Extensions project is intended for applications that already use the helper operators from `ReactiveUI.Extensions` and want the same shapes without pulling System.Reactive or R3 into the production dependency graph.

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
- `ToObservableAsync<T>(this System.IObservable<T> source)` when `ReactiveUI.Primitives.Async` is referenced
- `ToObservable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when `ReactiveUI.Primitives.Async` is referenced

Generated R3 bridge methods:

- `AsPrimitivesSignal<T>(this R3.Observable<T> source)`
- `AsR3Observable<T>(this System.IObservable<T> source)`
- `AsPrimitivesAsyncObservable<T>(this R3.Observable<T> source)` when `ReactiveUI.Primitives.Async` is referenced
- `AsR3Observable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when `ReactiveUI.Primitives.Async` is referenced

System.Reactive bridge example, when the consuming project already references System.Reactive:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive.Linq;

IObservable<int> rxSource = Observable.Range(1, 3);
IObservable<int> PrimitivesSource = rxSource.AsPrimitivesSignal();

using var subscription = PrimitivesSource
    .Map(value => value * 10)
    .Subscribe(Console.WriteLine);

IObservable<int> systemObservable = Signal.Sequence(1, 3).AsSystemObservable();

IObservableAsync<int> asyncSource = rxSource.ToObservableAsync();
IObservable<int> rxAgain = asyncSource.ToObservable();
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
| R3 bridge | Generated `AsPrimitivesSignal` / `AsR3Observable`; async bridge methods add `AsPrimitivesAsyncObservable` / `AsR3Observable` when R3 and `ReactiveUI.Primitives.Async` are referenced by the consumer. |

Use the generated bridge only at boundaries. Prefer native ReactiveUI.Primitives operators inside new code.

## ReactiveUI.Extensions migration notes

`ReactiveUI.Primitives.Extensions` is the migration target for the non-async helpers that previously lived in `ReactiveUI.Extensions`. The package intentionally keeps the helper names where those names already describe the behavior and do not collide with the core Primitives vocabulary. Scheduling overloads use `ISequencer` instead of System.Reactive schedulers.

| ReactiveUI.Extensions usage | ReactiveUI.Primitives.Extensions usage |
|---|---|
| `WhereIsNotNull`, `SkipWhileNull`, `WhereTrue`, `WhereFalse`, `Not` | Same names over BCL `IObservable<T>`. |
| `WhereSelect`, `SelectConstant`, `TrySelect`, `SelectManyThen`, `Pairwise`, `Partition` | Same helper names; implemented with direct observers and fused operator shapes where useful. |
| `SyncTimer`, `ObserveOnIf`, `Schedule`, `ScheduleSafe`, throttle/debounce helpers | Same helper names; use `ISequencer` overloads for scheduling. |
| `CatchIgnore`, `CatchAndReturn`, `CatchReturn`, retry helpers | Same helper names; no System.Reactive dependency. |
| `SubscribeAsync`, `SelectAsync`, `SelectLatestAsync`, `DropIfBusy` | Same BCL observable helper names for Task/ValueTask interop. |
| `RunAll`, `BufferUntil`, `FirstMatchFromCandidates`, `ToHotTask`, `ToHotValueTask` | Same helper names; backed by ReactiveUI.Primitives runtime utilities. |

For async-native streams, prefer `ReactiveUI.Primitives.Async` and its `IObservableAsync<T>` operators. For existing BCL observable helpers, migrate to `ReactiveUI.Primitives.Extensions`.

## Benchmarks and performance posture

Benchmarks live in `src/benchmarks/ReactiveUI.Primitives.Benchmarks`. The benchmark project may reference System.Reactive, R3, and ReactiveUI.Extensions to compare throughput and allocation behavior; the production packages must not.

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

The latest direct test/coverage validation passed 2032/2032 net8.0 tests and reports 99.28% line coverage and 97.22% branch coverage for `ReactiveUI.Primitives.Extensions` from `src/tests/ReactiveUI.Primitives.Tests/TestResults/extensions-optimization/coverage.cobertura.xml`. Direct executable validation also passed 2032/2032 tests on net9.0 and 2032/2032 tests on net10.0.

Focused Async and Extensions BenchmarkDotNet runs were added on 2026-05-30 on Windows 11 with .NET SDK 10.0.300 and .NET runtime 10.0.8. They used `LaunchCount=1`, `WarmupCount=1`, and `IterationCount=3`.

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*ReactiveExtensionsComparisonBenchmarks*" --launchCount 1 --warmupCount 1 --iterationCount 3
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*AsyncExtensionsComparisonBenchmarks*" --launchCount 1 --warmupCount 1 --iterationCount 3
```

Extensions comparison against `ReactiveUI.Extensions` 4.0.0:

| Scenario | ReactiveUI.Primitives.Extensions | ReactiveUI.Extensions 4.0.0 |
|---|---:|---:|
| `WhereSelect` range | 75.87 ns / 136 B | 2,655.97 ns / 2512 B |
| `FromArray` subscribe | 58.42 ns / 40 B | 59.15 ns / 40 B |
| `Pairwise` range | 515.51 ns / 160 B | 3,016.54 ns / 2536 B |
| `BufferUntil` chars | 52.81 ns / 264 B | 53.01 ns / 264 B |
| `Not` + `WhereTrue` | 28.81 ns / 144 B | 30.42 ns / 144 B |

Async comparison against `ReactiveUI.Extensions.Async` 4.0.0:

| Scenario | ReactiveUI.Primitives.Async | ReactiveUI.Extensions.Async 4.0.0 |
|---|---:|---:|
| `Sequence` + `Map` + `Keep` + `ToListAsync` | 2,109.7 ns / 1600 B | 2,085.6 ns / 1600 B |
| Async `CountAsync` | 817.2 ns / 704 B | 808.0 ns / 704 B |
| Async signal/subject broadcast | 6,822.4 ns / 2320 B | 6,802.5 ns / 2320 B |

The focused Extensions results show the fused and direct-observer operators winning the high-value migrated scenarios while preserving allocations where both implementations use equivalent buffering. The Async results are allocation-equivalent to the baseline and within measurement noise on the selected parity paths.

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
| `src/ReactiveUI.Primitives.Async` | Async observable/signal library built on `IObservableAsync<T>` and `IObserverAsync<T>`. |
| `src/ReactiveUI.Primitives.Extensions` | Migrated non-async ReactiveUI.Extensions helper operators backed by ReactiveUI.Primitives. |
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
dotnet build .\src\ReactiveUI.Primitives.slnx -c Release -v minimal /nr:false -p:UseSharedCompilation=false -p:BuildInParallel=false -maxcpucount:1

# Net8 coverage run through the Microsoft Testing Platform/TUnit executable.
dotnet .\src\tests\ReactiveUI.Primitives.Tests\bin\Release\net8.0\ReactiveUI.Primitives.Tests.dll --coverage --coverage-output coverage.cobertura.xml --coverage-output-format cobertura --results-directory .\src\tests\ReactiveUI.Primitives.Tests\TestResults\extensions-optimization --no-ansi --no-progress --output Normal --timeout 10m

# Remaining test target frameworks through the generated TUnit executables.
dotnet .\src\tests\ReactiveUI.Primitives.Tests\bin\Release\net9.0\ReactiveUI.Primitives.Tests.dll --results-directory .\src\tests\ReactiveUI.Primitives.Tests\TestResults\extensions-optimization-net9 --no-ansi --no-progress --output Normal --timeout 10m
dotnet .\src\tests\ReactiveUI.Primitives.Tests\bin\Release\net10.0\ReactiveUI.Primitives.Tests.dll --results-directory .\src\tests\ReactiveUI.Primitives.Tests\TestResults\extensions-optimization-net10 --no-ansi --no-progress --output Normal --timeout 10m

# Benchmark smoke, focused Async/Extensions comparisons, and complete joined comparison run.
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --smoke
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*ReactiveExtensionsComparisonBenchmarks*" --launchCount 1 --warmupCount 1 --iterationCount 3
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*AsyncExtensionsComparisonBenchmarks*" --launchCount 1 --warmupCount 1 --iterationCount 3
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Results: build passed with 0 warnings/0 errors; the net8.0 coverage run passed 2032/2032 tests; direct TUnit executable validation passed 2032/2032 tests on net9.0 and 2032/2032 tests on net10.0. The latest `ReactiveUI.Primitives.Extensions` coverage snapshot reported 99.28% line (3171/3194) and 97.22% branch (875/900) coverage. Benchmark smoke parity passed for 67 groups; the joined BenchmarkDotNet run executed 201 benchmarks, and the focused Async/Extensions BenchmarkDotNet runs produced the comparison tables above.

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
