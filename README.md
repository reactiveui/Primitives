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
| [Cursor](https://docs.cursor.com/en/context) | `.cursor/rules/reactiveui-primitives.mdc` | Cursor project rules are version-controlled under `.cursor/rules`; `CLAUDE.md` is authoritative in this repo, and `AGENTS.md` can point to it for compatibility. |
| [Windsurf](https://docs.windsurf.com/windsurf/cascade/memories) | `.windsurf/rules/reactiveui-primitives.md` | Windsurf can consume repository guidance via markdown rules; `CLAUDE.md` is the canonical file in this repo. |
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

Operators are extension methods over `IObservable<T>`. ReactiveUI.Primitives has a distinct vocabulary (`Map`, `Keep`, `Fold`, `Blend`, `SwitchTo`, …) that avoids ambiguous-call collisions with System.Reactive or R3 — but the familiar System.Reactive / LINQ names are also available (see below), so you can write whichever reads best.

### System.Reactive / LINQ name layer

The everyday System.Reactive / LINQ names are first-class operators that build the **same sink** as their Primitives-named counterpart — identical behavior and allocation profile, not wrappers. Both name sets are fully supported and interchangeable; pick whichever reads best for your code.

| LINQ / System.Reactive name | Primitives name | | LINQ / System.Reactive name | Primitives name |
|---|---|---|---|---|
| `Select` | `Map` | | `Merge` | `Blend` |
| `SelectWith` | `MapWith` | | `Concat` | `Chain` |
| `Where` | `Keep` | | `Amb` | `Race` |
| `WhereWith` | `KeepWith` | | `Switch` | `SwitchTo` |
| `WhereNotNull` | `KeepNotNull` | | `Zip` | `Pair` |
| `Do` | `Tap` | | `CombineLatest` | `SyncLatest` |
| `DoWith` | `TapWith` | | `WithLatestFrom` | `Latch` |
| `Scan` | `Fold` | | `SelectMany` | `FlatMap` |
| `Aggregate` | `Reduce` | | `Delay` | `Shift` |
| `DistinctUntilChanged` | `Unique` | | `Timeout` | `Expire` |
| `DistinctUntilChangedBy` | `UniqueBy` | | `Sample` | `Probe` |
| `IgnoreElements` | `IgnoreValues` | | `Retry` | `Reattempt` |
| `Materialize` | `Spark` | | `Dematerialize` | `Unspark` |

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

// Reads exactly like System.Reactive — and builds the identical sinks as Map/Keep/Fold.
using var subscription = Signal.Sequence(1, 10)
    .Where(value => value % 2 == 0)
    .Select(value => value * value)
    .Scan(0, (total, value) => total + value)
    .Subscribe(Console.WriteLine);
```

> Caveat: because these names live in the `ReactiveUI.Primitives` namespace, a file that *also* imports `System.Reactive.Linq` will get ambiguous-call errors on shared names like `.Select`/`.Where`. Use the Primitives names (`Map`/`Keep`) in those mixed files, or migrate the file fully off System.Reactive.

### Transformation and filtering

| System.Reactive-style concept | ReactiveUI.Primitives API |
|---|---|
| `Select` | `Map` | Prefer `Map` for the distinct Primitives style. |
| stateful `Select` without closure | `MapWith` |
| `Where` | `Keep` |
| stateful `Where` without closure | `KeepWith` |
| non-null filtering | `KeepNotNull` |
| fused `Where` + `Select` | `Choose` | Chooser returns `(HasValue, Value)`; the explicit flag lets a non-nullable value type be skipped in one sink. |
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
| fused merge + adjacent distinct | `BlendUnique` |
| first source wins | `Race` |
| latest inner source wins | `SwitchTo` |
| filter-null + project + switch to latest inner | `SwitchSelect` |
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

Fused projection example (`Choose` and `SwitchSelect`):

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

// Choose folds Where + Select into one sink. The explicit HasValue flag lets a
// non-nullable value type be dropped without a nullable wrapper.
using var evens = Signal.Sequence(1, 6)
    .Choose(value => (value % 2 == 0, value * 10))
    .Subscribe(value => Console.WriteLine($"even*10={value}"));

// SwitchSelect folds WhereNotNull + Select + Switch: skips null keys, projects each
// to an inner source, and mirrors only the latest inner.
var key = new StateSignal<string?>(null);
using var latest = key
    .SwitchSelect(selectedKey => Signal.Sequence(selectedKey.Length, 3))
    .Subscribe(value => Console.WriteLine($"latest={value}"));

key.Value = "ab";
key.Value = "abcd";
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
| `Where` + `Select` | `Choose` | Single fused sink; chooser returns `(HasValue, Value)` so a non-nullable value type can be skipped. |
| `Merge` | `Blend` or `Signal.Blend` | Works over source-of-sources and params factories. |
| `Merge` + `DistinctUntilChanged` | `BlendUnique` | Single fused merge + adjacent dedupe over a params source set. |
| `Concat` | `Chain` or `Signal.Chain` | Sequential composition. |
| `Amb` | `Race` | First source to produce a value or terminal signal wins. |
| `Switch` | `SwitchTo` | Latest inner observable wins. |
| `Select` + `Switch` | `SwitchSelect` | Filters null source values, projects each to an inner observable, and mirrors only the latest. |
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

The latest complete BenchmarkDotNet run finished on 2026-05-31 at 15:10:19 Europe/London with .NET SDK 10.0.300 and .NET runtime 10.0.8 on Windows 11. It executed 610 benchmarks with no failed suites in 01:16:19:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Latest artifact paths:

- `BenchmarkDotNet.Artifacts/BenchmarkRun-20260531-135415.log`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-31-15-10-19-report-github.md`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-31-15-10-19-report.html`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-05-31-15-10-19-report.csv`

The joined run exports 610 raw BenchmarkDotNet rows: 235 ReactiveUI.Primitives or ReactiveUI.Primitives.Async cases, 155 System.Reactive cases, 130 R3 cases, and 90 ReactiveUI.Extensions cases. The previous 87-row README table covered only the `ReactiveExtensionsComparisonBenchmarks` synchronous extension slice; the remaining raw rows were present in the joined CSV but were not surfaced in the README table.

The table below groups `ReactiveUI.Primitives` and `ReactiveUI.Primitives.Async` into the `ReactiveUI.Primitives` column, aligns each primitive benchmark with any System.Reactive, R3, or ReactiveUI.Extensions alternative from the same benchmark scenario, and uses `NA` where no alternative exists. It contains 235 alphabetically ordered scenario rows. Cells use `Mean / Allocated`, and long `scenario` parameter values from BenchmarkDotNet are restored to their full names.

External-baseline posture from this run: `ReactiveUI.Primitives` is faster than System.Reactive in 150/155 measured comparisons, faster than R3 in 128/130 measured comparisons, and faster than `ReactiveUI.Extensions` 4.0.0 in 64/90 measured comparisons. Rows that are not faster remain listed for direct comparison.

| Scenario | ReactiveUI.Primitives | System.Reactive | R3 | ReactiveUI.Extensions |
|---|---:|---:|---:|---:|
| `After` | 155.3662 ns / 584 B | 1,001.4142 ns / 25056 B | 276.8260 ns / 552 B | NA |
| `AggregateAnyCount (Operator core GC profile)` | 201.0841 ns / 824 B | 6,602.1945 ns / 5856 B | 620.6079 ns / 1280 B | NA |
| `AggregateAnyCount (Operator map keep)` | 200.1816 ns / 824 B | 5,776.6805 ns / 5856 B | 611.5725 ns / 1280 B | NA |
| `All` | 19.4142 ns / 96 B | 2,639.9900 ns / 2520 B | 89.7295 ns / 192 B | NA |
| `AllContains` | 30.5337 ns / 192 B | 5,147.5512 ns / 5048 B | 231.7154 ns / 392 B | NA |
| `AllRange` | 18.5571 ns / 96 B | 2,554.2953 ns / 2520 B | 87.1035 ns / 192 B | NA |
| `AsSignal` | 39.1343 ns / 112 B | 2,497.5107 ns / 2536 B | 190.0920 ns / 160 B | 2,425.8592 ns / 2488 B |
| `AutoConnect` | 171.2824 ns / 400 B | 2,887.7501 ns / 2736 B | NA | NA |
| `AutoConnectSubscribe` | 140.2909 ns / 400 B | 2,842.0521 ns / 2736 B | NA | NA |
| `BehaviorEmit` | 15,767.1692 ns / 160 B | NA | NA | NA |
| `BufferRange` | 70.8486 ns / 304 B | 1,447.8858 ns / 1656 B | 118.2146 ns / 360 B | NA |
| `BufferUntil` | 43.7569 ns / 264 B | NA | NA | 43.1834 ns / 264 B |
| `BufferUntilIdle` | 1,872.8774 ns / 6504 B | NA | NA | 25,722.3674 ns / 21207 B |
| `BufferUntilInactive` | 1,922.3653 ns / 6504 B | NA | NA | 25,821.9421 ns / 21207 B |
| `CastTo` | 99.0222 ns / 200 B | 1,532.6529 ns / 1568 B | 167.3711 ns / 216 B | NA |
| `CatchAndReturn` | 19.9923 ns / 128 B | 173.5006 ns / 368 B | 116.0415 ns / 264 B | 59.8265 ns / 184 B |
| `CatchIgnore` | 18.1860 ns / 128 B | 168.1961 ns / 344 B | 113.0904 ns / 240 B | 59.7477 ns / 184 B |
| `CatchReturn` | 14.5591 ns / 128 B | 174.3701 ns / 368 B | 118.1259 ns / 264 B | 60.1389 ns / 184 B |
| `CatchReturnUnit` | 10.3368 ns / 88 B | NA | NA | 56.2182 ns / 144 B |
| `CollectArray (Terminal collection GC profile)` | 39.7344 ns / 360 B | 2,807.2484 ns / 3144 B | 181.5774 ns / 784 B | NA |
| `CollectArray (Terminal collection)` | 39.2840 ns / 360 B | 2,649.8519 ns / 3144 B | 196.2510 ns / 784 B | NA |
| `CollectArrayAsync` | 44.1010 ns / 384 B | 2,741.1916 ns / 3384 B | 193.5212 ns / 784 B | NA |
| `CollectList (Terminal collection GC profile)` | 77.6725 ns / 392 B | 2,731.9333 ns / 2992 B | 171.8614 ns / 632 B | NA |
| `CollectList (Terminal collection)` | 77.1326 ns / 392 B | 2,661.1326 ns / 2992 B | 180.3147 ns / 632 B | NA |
| `CollectListAsync` | 47.5884 ns / 352 B | 1,525.7612 ns / 2056 B | 125.6062 ns / 480 B | NA |
| `CombineLatest` | 46.0690 ns / 192 B | 3,279.3736 ns / 2824 B | 678.1482 ns / 344 B | NA |
| `CombineLatestRanges` | 39.7819 ns / 192 B | 3,319.2938 ns / 2824 B | 688.9252 ns / 344 B | NA |
| `CombineLatestValuesAreAllFalse` | 186.1148 ns / 848 B | 346.7049 ns / 648 B | NA | 218.2402 ns / 1176 B |
| `CombineLatestValuesAreAllTrue` | 188.9175 ns / 848 B | 332.3094 ns / 648 B | NA | 212.3348 ns / 1176 B |
| `CommandExecuteAsync` | 35.1944 ns / 152 B | 740.3134 ns / 1089 B | 106.6560 ns / 296 B | NA |
| `CommandResultSubscribeAsync` | 63.0803 ns / 224 B | 40.1994 ns / 136 B | 68.4171 ns / 160 B | NA |
| `CompletedSpark` | 0.0292 ns / 0 B | 0.0065 ns / 0 B | 0.0000 ns / 0 B | NA |
| `CompletedTaskBridge` | 12.1256 ns / 88 B | 882.4698 ns / 793 B | 43.4786 ns / 88 B | NA |
| `Concat` | 77.7316 ns / 256 B | 3,073.5477 ns / 2856 B | 266.3856 ns / 360 B | NA |
| `ConcatRanges` | 80.0323 ns / 256 B | 2,836.8918 ns / 2856 B | 264.9149 ns / 360 B | NA |
| `Conflate` | 3,988.0580 ns / 2304 B | NA | NA | 32,870.4854 ns / 16969 B |
| `Contains` | 12.8455 ns / 96 B | 2,785.3176 ns / 2528 B | 91.1712 ns / 200 B | NA |
| `ContainsRange` | 11.2374 ns / 96 B | 2,552.2694 ns / 2528 B | 92.9637 ns / 200 B | NA |
| `Continuation.Dispose` | 24.1712 ns / 192 B | NA | NA | 24.2349 ns / 192 B |
| `Continuation.Lock` | 1,015.9123 ns / 464 B | NA | NA | 1,158.6503 ns / 464 B |
| `Continuation.LockValueTask` | 1,160.4224 ns / 464 B | NA | NA | 1,155.7749 ns / 464 B |
| `CountPredicate (Terminal collection GC profile)` | 23.6151 ns / 96 B | 2,644.9360 ns / 2520 B | 97.9358 ns / 200 B | NA |
| `CountPredicate (Terminal collection)` | 19.8555 ns / 96 B | 2,541.2862 ns / 2520 B | 97.2371 ns / 200 B | NA |
| `CreateSafeSubscribe` | 39.8372 ns / 112 B | NA | NA | NA |
| `CreateSubscribe` | 40.0776 ns / 112 B | 52.0066 ns / 168 B | 59.6140 ns / 128 B | NA |
| `CreateWithState` | 60.2325 ns / 192 B | 89.0048 ns / 256 B | 131.1856 ns / 216 B | NA |
| `CurrentThreadSchedule` | 9.5961 ns / 88 B | 21.8966 ns / 88 B | 31.0668 ns / 56 B | NA |
| `DebounceImmediate` | 1,622.7895 ns / 4064 B | NA | NA | 27,081.5247 ns / 18065 B |
| `DebounceUntil` | 1,094.9352 ns / 776 B | NA | NA | 7,205.8484 ns / 6132 B |
| `DefaultIfEmptyEmpty` | 7.0131 ns / 64 B | 69.8937 ns / 144 B | 69.2199 ns / 136 B | NA |
| `DeferSubscribe` | 86.7555 ns / 240 B | 1,676.0302 ns / 1512 B | 123.6352 ns / 152 B | NA |
| `DelayRange` | 169.3880 ns / 536 B | 6,256.1473 ns / 39584 B | 2,034.4247 ns / 2200 B | NA |
| `DelayStartRange` | 174.1631 ns / 536 B | 2,673.2576 ns / 26456 B | 342.5660 ns / 552 B | NA |
| `DematerializeRange` | 72.7926 ns / 184 B | 1,533.0044 ns / 1528 B | 199.5775 ns / 208 B | NA |
| `DetectStale` | 192.7663 ns / 600 B | NA | NA | 835.0784 ns / 25128 B |
| `DisposableCollectionDispose` | 78.7620 ns / 424 B | 99.6767 ns / 512 B | 89.6395 ns / 480 B | NA |
| `DoOnDispose` | 71.1486 ns / 232 B | NA | NA | 70.9375 ns / 232 B |
| `DoOnSubscribe` | 70.2411 ns / 192 B | NA | NA | 69.6332 ns / 192 B |
| `DropIfBusy` | 349.7954 ns / 240 B | NA | NA | 358.5479 ns / 240 B |
| `Emit1024` | 1,728.8903 ns / 184 B | 1,746.7394 ns / 136 B | 2,057.8585 ns / 160 B | NA |
| `Empty` | 4.1186 ns / 40 B | 49.6034 ns / 96 B | 32.3730 ns / 56 B | NA |
| `EmptySubscribe` | 3.5444 ns / 40 B | 45.9526 ns / 96 B | 30.7072 ns / 56 B | NA |
| `Every` | 535.2985 ns / 1192 B | 3,066.3586 ns / 34001 B | 353.4427 ns / 552 B | NA |
| `FastForEach` | 48.9880 ns / 40 B | NA | NA | 48.4052 ns / 40 B |
| `Filter` | 124.5601 ns / 120 B | 736.4327 ns / 984 B | NA | 123.2022 ns / 120 B |
| `FirstAsync` | 8.7337 ns / 56 B | 2,605.0640 ns / 2792 B | 78.7451 ns / 208 B | NA |
| `FirstMatchFromCandidates` | 42.6804 ns / 216 B | NA | NA | 44.7428 ns / 216 B |
| `FirstOrDefaultAsync` | 6.9475 ns / 56 B | 1,388.2233 ns / 1768 B | 65.4337 ns / 208 B | NA |
| `FlatMap` | 732.1660 ns / 728 B | 3,839.0203 ns / 3872 B | 1,183.4716 ns / 1040 B | NA |
| `FlatMapRange` | 720.1574 ns / 728 B | 3,724.1633 ns / 3872 B | 1,191.5244 ns / 1040 B | NA |
| `Fold (Operator stateful filter GC profile)` | 1,829.4350 ns / 144 B | NA | NA | NA |
| `Fold (Operator stateful filter)` | 104.7071 ns / 144 B | 2,692.9914 ns / 2560 B | NA | NA |
| `ForEach` | 73.3841 ns / 160 B | 150.0873 ns / 200 B | NA | 75.0841 ns / 160 B |
| `ForkJoin` | 25.4948 ns / 192 B | 4,002.9279 ns / 3136 B | 985.3837 ns / 504 B | NA |
| `ForkJoinRanges` | 35.7868 ns / 192 B | 3,966.4459 ns / 3136 B | 973.5322 ns / 504 B | NA |
| `FromArray` | 61.9788 ns / 72 B | 2,519.9627 ns / 2504 B | 85.4346 ns / 88 B | 70.7992 ns / 72 B |
| `FromAsyncEnumerableSubscribeAsync` | 1,278.0260 ns / 600 B | 1,639.4955 ns / 1827 B | 1,442.5958 ns / 1023 B | NA |
| `FromEnumerable` | 53.3886 ns / 40 B | 2,668.0614 ns / 2504 B | 80.7768 ns / 88 B | NA |
| `FromEnumerableSubscribe` | 52.7429 ns / 40 B | 2,525.1923 ns / 2504 B | 79.2217 ns / 88 B | NA |
| `FromEventPattern` | 159.9328 ns / 624 B | 1,930.1362 ns / 2422 B | NA | NA |
| `GetMax` | 108.9092 ns / 408 B | 181.7760 ns / 328 B | NA | 227.3460 ns / 1152 B |
| `GetMin` | 115.1313 ns / 408 B | 179.6450 ns / 328 B | NA | 226.6057 ns / 1152 B |
| `Heartbeat` | 299.3094 ns / 800 B | NA | NA | 2,377.8947 ns / 26096 B |
| `HistorySubscribe` | 342.4926 ns / 352 B | 720.4683 ns / 696 B | 433.0458 ns / 688 B | NA |
| `IgnoreValuesRange` | 29.7856 ns / 128 B | 1,436.1516 ns / 1504 B | 82.7043 ns / 160 B | NA |
| `Iterate` | 11.6964 ns / 0 B | 2,376.7106 ns / 2768 B | NA | NA |
| `KeepNotNull` | 108.3266 ns / 192 B | 1,741.8934 ns / 1656 B | 231.0055 ns / 312 B | NA |
| `KeepType` | 109.4705 ns / 192 B | 1,520.8447 ns / 1568 B | 177.0656 ns / 216 B | NA |
| `KeepWith` | 63.4511 ns / 232 B | 1,474.3146 ns / 1608 B | 126.5203 ns / 280 B | NA |
| `LastOrDefaultAsync` | 13.9226 ns / 192 B | 1,432.8369 ns / 1872 B | 80.8937 ns / 208 B | NA |
| `LatestOrDefault` | 53.9602 ns / 136 B | NA | NA | 58.1188 ns / 136 B |
| `LogErrors` | 68.0775 ns / 224 B | NA | NA | 72.0710 ns / 224 B |
| `LongCountPredicate` | 25.2287 ns / 104 B | 2,608.8275 ns / 2536 B | 109.6102 ns / 272 B | NA |
| `MapKeep` | 134.0625 ns / 208 B | 2,946.0442 ns / 2616 B | 320.7422 ns / 272 B | NA |
| `MapWith` | 65.0591 ns / 232 B | 1,443.5638 ns / 1608 B | 136.9318 ns / 248 B | NA |
| `MaterializeRange` | 46.8644 ns / 120 B | 1,506.6878 ns / 1880 B | 100.4292 ns / 136 B | NA |
| `Merge` | 80.2232 ns / 256 B | 4,278.2346 ns / 3952 B | 793.7542 ns / 352 B | NA |
| `MergeRanges` | 78.0233 ns / 256 B | 3,920.3102 ns / 3952 B | 732.4915 ns / 352 B | NA |
| `MulticastConnect` | 159.3122 ns / 360 B | 2,789.8379 ns / 2696 B | 441.0353 ns / 368 B | NA |
| `NeverSubscribeDispose` | 0.2446 ns / 0 B | 6.2409 ns / 40 B | 19.4228 ns / 56 B | NA |
| `Not` | 27.8941 ns / 120 B | 834.5748 ns / 1040 B | 92.3225 ns / 152 B | 28.4602 ns / 120 B |
| `ObserveOnIf` | 64.2175 ns / 104 B | NA | NA | 66.5979 ns / 104 B |
| `ObserveOnImmediate` | 23.1313 ns / 96 B | 16,018.5333 ns / 11309 B | 905.3638 ns / 432 B | NA |
| `ObserveOnSafe` | 65.7747 ns / 104 B | NA | NA | 67.1360 ns / 104 B |
| `OnCleanup` | 127.6024 ns / 504 B | 1,466.0726 ns / 1528 B | 135.0002 ns / 216 B | NA |
| `OnErrorRetry` | 133.5035 ns / 424 B | NA | NA | 134.3144 ns / 424 B |
| `OnNext` | 50.6179 ns / 40 B | NA | NA | 51.3304 ns / 40 B |
| `Pairwise` | 511.8533 ns / 160 B | 2,840.1602 ns / 3072 B | NA | 536.1441 ns / 160 B |
| `Partition` | 277.7601 ns / 440 B | NA | NA | 265.2119 ns / 440 B |
| `Publish` | 149.1594 ns / 360 B | 2,843.8596 ns / 2696 B | 407.4740 ns / 368 B | NA |
| `PublishLiveConnect` | 147.7717 ns / 360 B | 2,765.0237 ns / 2696 B | 426.4617 ns / 368 B | NA |
| `Race` | 42.3790 ns / 192 B | 1,633.0064 ns / 1760 B | 311.2532 ns / 360 B | NA |
| `RaceRanges` | 40.0232 ns / 192 B | 1,578.4900 ns / 1760 B | 264.0182 ns / 360 B | NA |
| `Range` | 53.6816 ns / 96 B | 2,791.1538 ns / 2472 B | 84.1106 ns / 80 B | NA |
| `RangeMapKeep` | 155.3300 ns / 208 B | 2,674.9530 ns / 2616 B | 327.1681 ns / 272 B | NA |
| `RangeSubscribe` | 56.5469 ns / 96 B | 2,598.6621 ns / 2472 B | 75.2467 ns / 80 B | NA |
| `ReadOnlyStateProjection` | 103.6867 ns / 224 B | 90.8319 ns / 328 B | 174.2237 ns / 312 B | NA |
| `ReattemptRange` | 93.4540 ns / 432 B | 1,482.2007 ns / 1664 B | NA | NA |
| `Recover` | 89.5049 ns / 336 B | 1,454.6806 ns / 1560 B | 169.3545 ns / 264 B | NA |
| `Reduce (Operator stateful filter GC profile)` | 641.4023 ns / 144 B | NA | NA | NA |
| `Reduce (Operator stateful filter)` | 50.7020 ns / 144 B | 2,649.4832 ns / 2560 B | NA | NA |
| `RefCount` | 192.5290 ns / 480 B | NA | 712.2789 ns / 488 B | NA |
| `RefCountSubscribe` | 191.4625 ns / 480 B | NA | 562.0081 ns / 488 B | NA |
| `Repeat` | 8.0617 ns / 0 B | 2,591.9693 ns / 2408 B | 82.4938 ns / 80 B | NA |
| `RepeatSubscribe` | 8.0203 ns / 0 B | 2,519.3663 ns / 2408 B | 76.6248 ns / 80 B | NA |
| `Replay (Connectable GC profile)` | 689.8670 ns / 512 B | 3,902.0874 ns / 3408 B | 931.1544 ns / 1360 B | NA |
| `Replay (Subject GC profile)` | 371.6089 ns / 352 B | 740.9560 ns / 696 B | 435.6562 ns / 688 B | NA |
| `ReplayEmit` | 15,921.0642 ns / 352 B | NA | NA | NA |
| `ReplayLastOnSubscribe` | 63.6625 ns / 104 B | NA | NA | 63.7188 ns / 104 B |
| `ReplayLiveLateSubscribe` | 637.8266 ns / 512 B | 3,865.4959 ns / 3408 B | 1,029.3317 ns / 1360 B | NA |
| `Resume` | 96.2466 ns / 424 B | 1,677.1264 ns / 1720 B | NA | NA |
| `RetryForeverWithDelay` | 131.7468 ns / 352 B | NA | NA | 126.2633 ns / 352 B |
| `RetryWithBackoff` | 127.8894 ns / 336 B | NA | NA | 122.5359 ns / 336 B |
| `RetryWithDelay` | 112.6661 ns / 264 B | NA | NA | 114.9763 ns / 264 B |
| `RetryWithFixedDelay` | 132.6439 ns / 336 B | NA | NA | 124.9002 ns / 336 B |
| `Return (Factory GC profile)` | 0.7472 ns / 0 B | 54.9276 ns / 120 B | 34.2162 ns / 80 B | NA |
| `Return (Reactive extensions)` | 6.7822 ns / 64 B | 54.6806 ns / 120 B | 31.3894 ns / 56 B | 6.9682 ns / 64 B |
| `ReturnSubscribe` | 0.2191 ns / 0 B | 50.4895 ns / 120 B | 32.2581 ns / 80 B | NA |
| `RunAll` | 26.0295 ns / 136 B | NA | NA | 24.2351 ns / 136 B |
| `SafeWitness` | 21.4309 ns / 136 B | 15.1710 ns / 136 B | 26.0649 ns / 128 B | NA |
| `SampleLatest (Operator time scheduler)` | 257.2775 ns / 776 B | 2,892.3456 ns / 26264 B | 359.7949 ns / 664 B | NA |
| `SampleLatest (Reactive extensions)` | 1,048.3322 ns / 488 B | NA | NA | 1,135.7206 ns / 840 B |
| `ScanWithInitial` | 591.7863 ns / 200 B | 2,672.0292 ns / 2560 B | NA | 547.3744 ns / 200 B |
| `Schedule` | 38.7331 ns / 216 B | NA | NA | 863.5171 ns / 677 B |
| `ScheduleSafe` | 32.2703 ns / 144 B | NA | NA | 943.9625 ns / 597 B |
| `SelectAsync` | 1,252.7429 ns / 2104 B | 32,738.8855 ns / 32276 B | NA | 1,292.9070 ns / 2104 B |
| `SelectAsyncConcurrent` | 1,168.3446 ns / 2120 B | NA | NA | 1,191.4728 ns / 2120 B |
| `SelectAsyncSequential` | 1,339.6799 ns / 2104 B | NA | NA | 1,302.4360 ns / 2104 B |
| `SelectConstant` | 59.5848 ns / 136 B | 2,715.4348 ns / 2544 B | 198.1385 ns / 160 B | 58.8155 ns / 136 B |
| `SelectLatestAsync` | 1,719.1326 ns / 2032 B | NA | NA | 1,731.7319 ns / 2032 B |
| `SelectManyThen` | 34.6976 ns / 224 B | 423.6935 ns / 752 B | NA | 37.1627 ns / 224 B |
| `SequenceCountAsync` | 816.4266 ns / 704 B | NA | NA | 799.9606 ns / 704 B |
| `SequenceMapKeepToListAsync` | 2,009.7575 ns / 1600 B | NA | NA | 1,944.0266 ns / 1600 B |
| `Share` | 193.6808 ns / 480 B | 3,045.8913 ns / 2880 B | 504.3583 ns / 488 B | NA |
| `ShareLiveSubscribe` | 204.3741 ns / 480 B | 3,020.7905 ns / 2880 B | 534.5208 ns / 488 B | NA |
| `Shuffle` | 152.0990 ns / 96 B | NA | NA | 154.7871 ns / 96 B |
| `SignalBroadcastAsync` | 7,179.9438 ns / 2320 B | NA | NA | 6,977.3867 ns / 2320 B |
| `SignalEmit` | 1,749.7628 ns / 184 B | NA | NA | NA |
| `SignalFanOutChurn` | 39,454.1463 ns / 33064 B | NA | NA | NA |
| `SignalSubscribeDisposeChurn` | 38,197.5708 ns / 32920 B | NA | NA | NA |
| `Skip (Operator stateful filter GC profile)` | 1,806.9367 ns / 136 B | NA | NA | NA |
| `Skip (Operator stateful filter)` | 83.9017 ns / 136 B | 2,629.1569 ns / 2512 B | NA | NA |
| `SkipWhile (Operator stateful filter GC profile)` | 1,886.5043 ns / 144 B | NA | NA | NA |
| `SkipWhile (Operator stateful filter)` | 94.8720 ns / 144 B | 2,755.9981 ns / 2520 B | NA | NA |
| `SkipWhileNull` | 26.8510 ns / 112 B | 652.5813 ns / 944 B | NA | 26.3809 ns / 112 B |
| `Start` | 25.1025 ns / 96 B | NA | NA | 1,011.5645 ns / 535 B |
| `StartSubscribe` | 51.1140 ns / 208 B | 892.2476 ns / 751 B | 61.2631 ns / 160 B | NA |
| `StartWithAppend` | 36.6330 ns / 168 B | 1,225.1370 ns / 1283 B | 162.2376 ns / 288 B | NA |
| `StartWithAppendDefaultIfEmpty` | 36.9278 ns / 168 B | 1,061.6973 ns / 1283 B | 152.9743 ns / 288 B | NA |
| `State1024` | 15,745.6177 ns / 160 B | 17,003.8483 ns / 200 B | 17,026.8046 ns / 192 B | NA |
| `StateEmit` | 16,305.4321 ns / 160 B | NA | NA | NA |
| `StateSignal1024` | 15,226.0101 ns / 160 B | 16,860.4563 ns / 200 B | 16,525.8596 ns / 192 B | NA |
| `StateSignal32` | 555.0760 ns / 160 B | 619.2484 ns / 200 B | 629.6037 ns / 192 B | NA |
| `StateSignalUpdates` | 559.4035 ns / 160 B | 586.0605 ns / 200 B | 638.8654 ns / 192 B | NA |
| `SubjectEmit1024` | 1,741.3116 ns / 184 B | 1,767.3735 ns / 136 B | 1,988.2607 ns / 160 B | NA |
| `SubjectEmit32` | 91.5766 ns / 184 B | 97.2691 ns / 136 B | 123.9790 ns / 160 B | NA |
| `SubjectSubscribeDispose64` | 3,442.9656 ns / 3848 B | 4,184.0096 ns / 38472 B | 3,923.8215 ns / 6728 B | NA |
| `SubjectSubscribeDispose8` | 353.1304 ns / 640 B | 313.9104 ns / 1288 B | 485.8430 ns / 904 B | NA |
| `SubscribeAndComplete` | 0.5452 ns / 0 B | NA | NA | 0.2295 ns / 0 B |
| `SubscribeAsync` | 1,020.8799 ns / 544 B | NA | NA | 1,086.0720 ns / 544 B |
| `SubscribeDispose64` | 3,514.5795 ns / 3848 B | 4,330.8894 ns / 38472 B | 3,776.1935 ns / 6728 B | NA |
| `SubscribeGetError` | 6.9580 ns / 48 B | NA | NA | 53.4505 ns / 104 B |
| `SubscribeGetValue` | 17.8834 ns / 56 B | NA | NA | 18.5553 ns / 56 B |
| `SubscribeOnImmediate` | 92.8357 ns / 384 B | 2,088.8631 ns / 2257 B | 143.1142 ns / 200 B | NA |
| `SubscribeSynchronous` | 1,041.8758 ns / 544 B | NA | NA | 1,002.4885 ns / 544 B |
| `Switch` | 85.8795 ns / 312 B | 2,390.3422 ns / 2360 B | 799.1355 ns / 448 B | NA |
| `SwitchIfEmpty` | 69.8495 ns / 224 B | NA | NA | 111.8583 ns / 280 B |
| `SwitchRanges` | 86.2799 ns / 312 B | 2,403.2789 ns / 2360 B | 794.1218 ns / 448 B | NA |
| `SynchronizeAsync` | 819.5469 ns / 1280 B | NA | NA | 825.0399 ns / 1280 B |
| `SynchronizeSynchronous` | 832.0162 ns / 1280 B | NA | NA | 817.5703 ns / 1280 B |
| `SyncTimer` | 2,596.8934 ns / 1080 B | NA | NA | 15,106.0211 ns / 26240 B |
| `TakeRange` | 72.9409 ns / 200 B | 1,440.9925 ns / 1552 B | 103.1167 ns / 160 B | NA |
| `TakeUntil` | 512.1911 ns / 192 B | 2,662.6592 ns / 2520 B | NA | 516.0572 ns / 192 B |
| `TakeWhile (Operator stateful filter GC profile)` | 1,859.1993 ns / 144 B | NA | NA | NA |
| `TakeWhile (Operator stateful filter)` | 95.4222 ns / 144 B | 2,717.1335 ns / 2520 B | NA | NA |
| `TapRange` | 77.2577 ns / 224 B | 1,568.4737 ns / 1520 B | 135.1513 ns / 216 B | NA |
| `TapWith` | 78.4952 ns / 320 B | 1,476.9664 ns / 1608 B | 146.3279 ns / 304 B | NA |
| `TaskSignalSubscribe` | 37.9870 ns / 240 B | 779.9820 ns / 886 B | 39.5601 ns / 160 B | NA |
| `ThrottleBurst` | 612.2450 ns / 1176 B | 2,778.8308 ns / 36480 B | 1,667.2384 ns / 1512 B | NA |
| `ThrottleDistinct` | 1,849.1563 ns / 4232 B | NA | NA | 30,604.1331 ns / 18701 B |
| `ThrottleFirst` | 1,161.0701 ns / 224 B | NA | NA | 1,163.6336 ns / 224 B |
| `ThrottleOnScheduler` | 1,912.9753 ns / 2400 B | NA | NA | 34,581.3985 ns / 16354 B |
| `ThrottleUntilTrue` | 5,027.6983 ns / 1633 B | NA | NA | 6,217.3368 ns / 1378 B |
| `Throw` | 63.0707 ns / 120 B | 128.1170 ns / 240 B | 96.0727 ns / 200 B | NA |
| `ThrowSubscribe` | 64.2616 ns / 120 B | 118.8961 ns / 240 B | 103.2737 ns / 200 B | NA |
| `TimeIntervalRange` | 27.1561 ns / 120 B | 1,784.3594 ns / 1616 B | 454.4125 ns / 160 B | NA |
| `TimeoutIdle` | 293.2704 ns / 800 B | 1,294.2879 ns / 29776 B | 414.6542 ns / 784 B | NA |
| `TimestampRange` | 42.6172 ns / 144 B | 1,749.8641 ns / 1512 B | 360.3036 ns / 152 B | NA |
| `ToHotTask` | 36.3653 ns / 112 B | 101.5059 ns / 240 B | NA | 35.7259 ns / 112 B |
| `ToHotValueTask` | 27.0451 ns / 72 B | NA | NA | 27.8467 ns / 72 B |
| `ToPropertyObservable` | 27,071.4355 ns / 4941 B | NA | NA | 27,486.0006 ns / 4941 B |
| `ToReadOnlyBehavior` | 66.0075 ns / 192 B | NA | NA | 67.9050 ns / 192 B |
| `ToTask` | 13.3485 ns / 192 B | 2,572.0790 ns / 2824 B | 97.3992 ns / 208 B | NA |
| `TrySelect` | 101.8332 ns / 120 B | NA | NA | 110.8990 ns / 120 B |
| `UnfoldSubscribe` | 12.8181 ns / 0 B | 2,345.7369 ns / 2768 B | 100.1733 ns / 128 B | NA |
| `Unique (Operator stateful filter GC profile)` | 1,839.5060 ns / 144 B | NA | NA | NA |
| `Unique (Operator stateful filter)` | 109.1269 ns / 144 B | 2,863.5156 ns / 2568 B | NA | NA |
| `UniqueBy (Operator stateful filter GC profile)` | 2,029.5043 ns / 152 B | NA | NA | NA |
| `UniqueBy (Operator stateful filter)` | 109.3574 ns / 152 B | 2,735.0480 ns / 2568 B | NA | NA |
| `UseSubscribe` | 40.9663 ns / 144 B | 88.1730 ns / 168 B | 63.0011 ns / 128 B | NA |
| `Using` | 7.6564 ns / 56 B | NA | NA | 9.4516 ns / 56 B |
| `WaitForCompletion` | 25.1638 ns / 96 B | NA | NA | 24.6338 ns / 96 B |
| `WaitForError` | 26.0917 ns / 96 B | NA | NA | 73.9819 ns / 152 B |
| `WaitForValue` | 32.1534 ns / 104 B | NA | NA | 34.4935 ns / 104 B |
| `WaitUntil` | 534.5543 ns / 224 B | 891.0865 ns / 1080 B | NA | 540.1772 ns / 224 B |
| `WhereFalse` | 23.6723 ns / 120 B | 806.3629 ns / 1040 B | 91.3808 ns / 184 B | 24.8538 ns / 120 B |
| `WhereIsNotNull` | 23.7931 ns / 104 B | 622.7868 ns / 904 B | 92.9625 ns / 264 B | 23.3656 ns / 104 B |
| `WhereSelect` | 80.4235 ns / 152 B | 2,638.5579 ns / 2616 B | 193.5426 ns / 240 B | 77.0247 ns / 152 B |
| `WhereTrue` | 23.7493 ns / 120 B | 804.6906 ns / 1040 B | 86.8553 ns / 184 B | 26.0367 ns / 120 B |
| `While` | 127.7396 ns / 280 B | NA | NA | 127.4206 ns / 280 B |
| `WithLatest` | 40.7850 ns / 192 B | 3,451.6327 ns / 2824 B | 274.0358 ns / 248 B | NA |
| `WithLatestRanges` | 45.1321 ns / 192 B | 3,505.9546 ns / 2824 B | 345.9029 ns / 248 B | NA |
| `WithLimitedConcurrency` | 2,489.2455 ns / 5448 B | NA | NA | 2,604.7204 ns / 5448 B |
| `Zip (Operator core GC profile)` | 39.0931 ns / 192 B | 3,389.6512 ns / 2976 B | 765.7075 ns / 656 B | NA |
| `Zip (Operator zip)` | 37.0413 ns / 192 B | 3,060.4356 ns / 2976 B | 687.6359 ns / 656 B | NA |

BenchmarkDotNet emitted `ZeroMeasurement` warnings for several singleton or empty-method-scale paths, including `Return`, `CompletedSpark`, and `Never`-style subscriptions. Those warnings mean the measured duration is indistinguishable from empty method overhead; the benchmark run still completed and exported all 610 rows.

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

# ReactiveUI.Primitives.Extensions TUnit executable validation.
dotnet .\src\tests\ReactiveUI.Primitives.Extensions.Tests\bin\Release\net8.0\ReactiveUI.Primitives.Extensions.Tests.dll --results-directory .\src\tests\ReactiveUI.Primitives.Extensions.Tests\TestResults\benchmark-validation-net8 --no-ansi --no-progress --output Normal --timeout 10m
dotnet .\src\tests\ReactiveUI.Primitives.Extensions.Tests\bin\Release\net9.0\ReactiveUI.Primitives.Extensions.Tests.dll --results-directory .\src\tests\ReactiveUI.Primitives.Extensions.Tests\TestResults\benchmark-validation-net9 --no-ansi --no-progress --output Normal --timeout 10m
dotnet .\src\tests\ReactiveUI.Primitives.Extensions.Tests\bin\Release\net10.0\ReactiveUI.Primitives.Extensions.Tests.dll --results-directory .\src\tests\ReactiveUI.Primitives.Extensions.Tests\TestResults\benchmark-validation-net10 --no-ansi --no-progress --output Normal --timeout 10m

# Extension scenario smoke and complete joined comparison run.
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --extensions-smoke
dotnet run --project .\src\benchmarks\ReactiveUI.Primitives.Benchmarks\ReactiveUI.Primitives.Benchmarks.csproj --configuration Release --no-build -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Results: release solution build passed with 0 warnings/0 errors; `ReactiveUI.Primitives.Extensions.Tests` passed 681/681 tests on net8.0, net9.0, and net10.0; extension scenario smoke validated 212 scenario paths; reflection/API coverage confirmed 87/87 public `ReactiveUI.Primitives.Extensions` method families have Primitives benchmark scenarios; the joined BenchmarkDotNet run executed 610 benchmarks in 01:16:19. The Mtpunittest MCP read existing solution coverage reports after validation and reported 69.88% line coverage and 74.21% branch coverage; coverage was not regenerated during this benchmark refresh.
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
