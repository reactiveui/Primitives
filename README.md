[![Build](https://github.com/reactiveui/Primitives/actions/workflows/ci-build.yml/badge.svg)](https://github.com/reactiveui/Primitives/actions/workflows/ci-build.yml)
[![Code Coverage](https://codecov.io/gh/reactiveui/Primitives/branch/main/graph/badge.svg)](https://codecov.io/gh/reactiveui/Primitives)
[![#yourfirstpr](https://img.shields.io/badge/first--timers--only-friendly-blue.svg)](https://reactiveui.net/contribute)
[![](https://img.shields.io/badge/chat-slack-blue.svg)](https://reactiveui.net/slack)
<br>
<a href="https://www.nuget.org/packages/ReactiveUI.Primitives">
  <img src="https://img.shields.io/nuget/dt/ReactiveUI.Primitives.svg">
</a>
<br>
<a href="https://github.com/reactiveui/Primitives">
  <img width="160" heigth="160" src="https://github.com/reactiveui/styleguide/blob/master/logo_primitives/logo.png?raw=true">
</a>
<br>


# ReactiveUI.Primitives

ReactiveUI.Primitives is a compact, high-performance reactive library for .NET applications that want Rx-style composition without a runtime dependency on System.Reactive, R3, or R3Async. It keeps the BCL `IObservable<T>` / `IObserver<T>` contracts where they are useful, adds Primitives names for common concepts, and focuses on predictable AOT-friendly code paths with low allocation overhead.

## Goals and design posture

ReactiveUI.Primitives is designed to:

- Provide Rx-style stream creation, subscription, state, scheduling, and composition over `IObservable<T>`.
- Use a distinct vocabulary where it improves clarity: `Signal<T>` instead of `Subject<T>`, `Map` instead of only `Select`, `Keep` instead of only `Where`, `Spark` instead of notification materialization.
- Stay AOT-friendly: no runtime reflection, dynamic code generation, expression compilation, or hidden dependency on System.Reactive/R3/R3Async in the production package.
- Minimize allocations in hot paths, including direct single-action subscribers for `Signal<T>` and reusable immutable singleton signals for common return/empty/never cases.
- Support broad production use across modern .NET and .NET Framework base TFMs, with separate integration projects for Windows UI and platform-focused scenarios.
- Allow migration from System.Reactive/R3/R3Async through source-generator bridges when the consuming project already references those libraries.

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

Those generators are analyzers. They do not add runtime System.Reactive, R3, or R3Async dependencies to ReactiveUI.Primitives. They emit bridge code only when the consuming compilation already references the relevant external library symbols.

## Agent Skills

The base `ReactiveUI.Primitives` NuGet package includes `Skills.md` at the package root. It is an agent-oriented guide for using ReactiveUI.Primitives, Async, Extensions, UI sequencers, bridge source generators, and migration from System.Reactive, R3, or R3Async while assuming the libraries are consumed from NuGet packages.

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
- `net11.0`
- `net462`
- `net472`
- `net48`
- `net481`

Windows UI and platform-integration projects in this repository use their own TFM properties (for example `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net11.0-windows`, or MAUI/platform-focused TFMs where applicable). Those platform TFMs are not target frameworks of the base `ReactiveUI.Primitives` package.

The optional package TFMs are:

- `ReactiveUI.Primitives.Wpf`: `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net11.0-windows`, `net462`, `net472`, `net48`, `net481`
- `ReactiveUI.Primitives.WinForms`: `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net11.0-windows`, `net462`, `net472`, `net48`, `net481`
- `ReactiveUI.Primitives.WinUI`: `net8.0-windows10.0.19041.0`, `net9.0-windows10.0.19041.0`, `net10.0-windows10.0.19041.0`, `net11.0-windows10.0.19041.0`
- `ReactiveUI.Primitives.Blazor`: `net8.0`, `net9.0`, `net10.0`, `net11.0`
- `ReactiveUI.Primitives.Maui`: `net9.0`, `net10.0`, `net11.0`
- `ReactiveUI.Primitives.Async`: `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, `net481`
- `ReactiveUI.Primitives.Extensions`: `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, `net481`

Runtime package dependencies are intentionally small. The base production package does not depend on System.Reactive, System.Reactive.Async, R3, or R3Async. The only runtime package reference declared directly by `src/ReactiveUI.Primitives/ReactiveUI.Primitives.csproj` is `System.ValueTuple` for `net462`; the bridge source generators are packed as analyzers in the base package rather than shipped as separate NuGet packages. `ReactiveUI.Primitives.Async` and `ReactiveUI.Primitives.Extensions` reference `ReactiveUI.Primitives`; their additional package references are limited to .NET Framework compatibility/support packages such as `System.ValueTuple`, Polyfill, Microsoft.Bcl.TimeProvider, System.Threading.Channels, System.Runtime.CompilerServices.Unsafe, System.ComponentModel.Annotations, System.Buffers, System.Memory, and System.Collections.Immutable for `net4x` targets. `ReactiveUI.Primitives.Async` also packs the bridge source generators as analyzers so async bridge methods are generated for consumers that reference System.Reactive, System.Reactive.Async, R3, or R3Async. `ReactiveUI.Primitives.Extensions` has no production System.Reactive or R3 dependency. `ReactiveUI.Primitives.Blazor` references `Microsoft.AspNetCore.Components`, `ReactiveUI.Primitives.Maui` references `Microsoft.Maui.Core`, and `ReactiveUI.Primitives.WinUI` references `Microsoft.WindowsAppSDK`. The remaining shared package references are analyzer, SourceLink, versioning, ILLink, reference-assembly, or build-time support packages such as Blazor.Common.Analyzers, Microsoft.SourceLink.GitHub, MinVer, Roslynator.Analyzers, SonarAnalyzer.CSharp, stylecop.analyzers, Microsoft.NET.ILLink.Tasks, and Microsoft.NETFramework.ReferenceAssemblies. Benchmark projects may reference System.Reactive, System.Reactive.Async 6.0.0-alpha.18, R3, and ReactiveUI.Extensions as comparison baselines, but those references are not production dependencies.

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

`ReactiveUI.Primitives.Async` is the async counterpart to the base `ReactiveUI.Primitives` surface. It is designed with source-generator compatibility for [R3Async](https://github.com/fedeAlterio/R3Async) and System.Reactive.Async, so some APIs intentionally mirror those async observable surfaces while keeping the Primitives vocabulary. It adds `ValueTask`/`CancellationToken`-aware observer calls for producers and consumers that need asynchronous notification, asynchronous disposal, or async stream collection.

Core async contracts and data types:

| API | Purpose |
|---|---|
| `IObservableAsync<T>` | Async observable contract. `SubscribeAsync` receives an `IObserverAsync<T>` and returns an `IAsyncDisposable`. |
| `IObserverAsync<T>` | Async observer contract with `OnNextAsync`, `OnErrorResumeAsync`, `OnCompletedAsync`, and `DisposeAsync`. |
| `WitnessAsync<T>` | Base observer type for implementing async observers with disposal, cancellation linking, and concurrency checks. |
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

`ReactiveUI.Primitives.Async` also packs the bridge source generators as analyzers. A consumer that references System.Reactive can use generated `ToObservableAsync<T>(this System.IObservable<T>)` and `ToObservable<T>(this IObservableAsync<T>)` adapters for sync Rx boundaries. A consumer that references System.Reactive.Async can use generated `AsPrimitivesAsyncObservable<T>(this System.IAsyncObservable<T>)`, `AsSystemReactiveAsyncObservable<T>(this IObservableAsync<T>)`, `AsPrimitivesAsyncObserver<T>(this System.IAsyncObserver<T>)`, and `AsSystemReactiveAsyncObserver<T>(this IObserverAsync<T>)`. A consumer that references R3 can use generated `AsPrimitivesAsyncObservable<T>(this R3.Observable<T>)` and `AsR3Observable<T>(this IObservableAsync<T>)`; a consumer that references R3Async can use `AsPrimitivesAsyncObservable<T>(this R3Async.AsyncObservable<T>)` and `AsR3AsyncObservable<T>(this IObservableAsync<T>)`.

## ReactiveUI.Primitives.Extensions

`ReactiveUI.Primitives.Extensions` migrates the non-async helper surface from `ReactiveUI.Extensions` onto `ReactiveUI.Primitives`. It is still based on the BCL `IObservable<T>` contract, but it uses `ISequencer` for scheduling and production references only `ReactiveUI.Primitives` plus framework compatibility packages. It does not reference System.Reactive, R3 or R3Async.

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

The base package includes two bridge generator assemblies as analyzers:

- `ReactiveUI.Primitives.SystemReactiveBridge.Generator.dll`
- `ReactiveUI.Primitives.R3Bridge.Generator.dll`

Those assemblies currently contain four conditional generators:

- `SystemReactiveBridgeGenerator` for System.Reactive `IObservable<T>` and scheduler boundaries.
- `SystemReactiveAsyncBridgeGenerator` for System.Reactive.Async `System.IAsyncObservable<T>` and `System.IAsyncObserver<T>` boundaries.
- `R3BridgeGenerator` for R3 `Observable<T>` boundaries and R3-to-Primitives.Async adapters.
- `R3AsyncBridgeGenerator` for R3Async `AsyncObservable<T>` boundaries.

The generators always emit small internal marker attributes stamped with the generator contract version. They emit bridge extension methods only when the consumer project already references the relevant external library:

- System.Reactive bridge checks for `System.Reactive.Linq.Observable`.
- System.Reactive scheduler bridge checks for `System.Reactive.Concurrency.IScheduler`.
- System.Reactive.Async bridge checks for `System.IAsyncObservable<T>` and `System.IAsyncObserver<T>`.
- R3 bridge checks for `R3.Observable<T>`.
- R3Async bridge checks for `R3Async.AsyncObservable<T>`, `R3Async.AsyncObserver<T>`, and `R3Async.Result`.

Generated bridge namespaces:

- `ReactiveUI.Primitives.SystemReactiveBridge`
- `ReactiveUI.Primitives.R3Bridge`

Generated System.Reactive bridge methods:

- `AsPrimitivesSignal<T>(this System.IObservable<T> source)`
- `AsSystemObservable<T>(this System.IObservable<T> source)`
- `AsSequencer(this System.Reactive.Concurrency.IScheduler scheduler)`
- `AsSystemScheduler(this ReactiveUI.Primitives.Concurrency.ISequencer sequencer)`
- `ToObservableAsync<T>(this System.IObservable<T> source)` when System.Reactive and `ReactiveUI.Primitives.Async` are referenced
- `ToObservable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when System.Reactive and `ReactiveUI.Primitives.Async` are referenced

Generated System.Reactive.Async bridge methods:

- `AsPrimitivesAsyncObservable<T>(this System.IAsyncObservable<T> source)` when System.Reactive.Async and `ReactiveUI.Primitives.Async` are referenced
- `AsSystemReactiveAsyncObservable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when System.Reactive.Async and `ReactiveUI.Primitives.Async` are referenced
- `AsPrimitivesAsyncObserver<T>(this System.IAsyncObserver<T> observer)` when System.Reactive.Async and `ReactiveUI.Primitives.Async` are referenced
- `AsSystemReactiveAsyncObserver<T>(this ReactiveUI.Primitives.Async.IObserverAsync<T> observer)` when System.Reactive.Async and `ReactiveUI.Primitives.Async` are referenced

Generated R3 bridge methods:

- `AsPrimitivesSignal<T>(this R3.Observable<T> source)`
- `AsR3Observable<T>(this System.IObservable<T> source)`
- `AsPrimitivesAsyncObservable<T>(this R3.Observable<T> source)` when `ReactiveUI.Primitives.Async` is referenced
- `AsR3Observable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when `ReactiveUI.Primitives.Async` is referenced

Generated R3Async bridge methods:

- `AsPrimitivesAsyncObservable<T>(this R3Async.AsyncObservable<T> source)` when R3Async and `ReactiveUI.Primitives.Async` are referenced
- `AsR3AsyncObservable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when R3Async and `ReactiveUI.Primitives.Async` are referenced

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

System.Reactive.Async bridge example, when the consuming project references System.Reactive.Async 6.0.0-alpha.18 or another compatible version:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.SystemReactiveBridge;

// System.IAsyncObservable<int> rxAsync = ...
IObservableAsync<int> primitivesAsync = rxAsync.AsPrimitivesAsyncObservable();
System.IAsyncObservable<int> systemAsync = primitivesAsync.AsSystemReactiveAsyncObservable();

// System.IAsyncObserver<int> rxObserver = ...
IObserverAsync<int> primitivesObserver = rxObserver.AsPrimitivesAsyncObserver();
System.IAsyncObserver<int> systemObserver = primitivesObserver.AsSystemReactiveAsyncObserver();
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

## System.Reactive.Async and R3Async to ReactiveUI.Primitives.Async migration guide

`ReactiveUI.Primitives.Async` is the native async-observable package. Use it when observer work is asynchronous, subscription/disposal needs `ValueTask`, or cancellation must flow through each notification. It differs from System.Reactive.Async by carrying cancellation through `OnNextAsync` and `OnErrorResumeAsync`, and it differs from R3Async by using `ReactiveUI.Primitives.Result` for completion.

| System.Reactive.Async | ReactiveUI.Primitives.Async | Migration detail |
|---|---|---|
| `System.IAsyncObservable<T>` | `IObservableAsync<T>` | Use the generated `AsPrimitivesAsyncObservable()` bridge at boundaries, then keep native code in `IObservableAsync<T>`. |
| `System.IAsyncObserver<T>` | `IObserverAsync<T>` | Use `AsPrimitivesAsyncObserver()` at boundaries. Primitives adds cancellation-aware `OnNextAsync` and `OnErrorResumeAsync`. |
| `SubscribeAsync(IAsyncObserver<T>)` | `SubscribeAsync(IObserverAsync<T>, CancellationToken)` | Pass a real cancellation token when subscription startup or notification flow can be cancelled. |
| `OnErrorAsync(Exception)` | `OnCompletedAsync(Result.Failure(error))` at terminal boundaries | System.Reactive.Async has a terminal error method; Primitives represents terminal success/failure through `Result`. |
| `OnCompletedAsync()` | `OnCompletedAsync(Result.Success)` | Completion carries a result value. |
| custom `AsyncObservable<T>` | `SignalAsync.Create<T>(...)` or a `SignalAsync<T>` subclass | Prefer `SignalAsync.Create` unless a reusable type needs protected overrides. |

| R3Async | ReactiveUI.Primitives.Async | Migration detail |
|---|---|---|
| `R3Async.AsyncObservable<T>` | `IObservableAsync<T>` / `SignalAsync<T>` | Use generated `AsPrimitivesAsyncObservable()` at external boundaries. |
| `R3Async.AsyncObserver<T>` | `IObserverAsync<T>` / `WitnessAsync<T>` | Use `WitnessAsync<T>` for custom observers that need disposal, cancellation, and concurrency checks. |
| `R3Async.Result` | `ReactiveUI.Primitives.Result` | Both carry success/failure; bridge adapters convert between them. |
| `OnErrorResumeAsync` | `OnErrorResumeAsync` | Same error-resume concept; Primitives passes the active `CancellationToken`. |
| `OnCompletedAsync(R3Async.Result)` | `OnCompletedAsync(ReactiveUI.Primitives.Result)` | Completion remains result-based. |

System.Reactive.Async bridge example:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.SystemReactiveBridge;

IObservableAsync<int> native = systemAsyncSource.AsPrimitivesAsyncObservable();

await using IAsyncDisposable subscription = await native
    .Keep(static value => value > 0)
    .Map(static value => value * 2)
    .SubscribeAsync(static value => Console.WriteLine(value));

System.IAsyncObservable<int> external = native.AsSystemReactiveAsyncObservable();
```

R3Async bridge example:

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge;

IObservableAsync<int> native = r3AsyncSource.AsPrimitivesAsyncObservable();
R3Async.AsyncObservable<int> external = native.AsR3AsyncObservable();
```

Keep async bridge conversions at package or API edges. Inside the application or library, prefer `SignalAsync` factories, `IObservableAsync<T>` operators, and `IObserverAsync<T>` observers directly.

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

Benchmarks live in `src/benchmarks/ReactiveUI.Primitives.Benchmarks`. The benchmark project may reference System.Reactive, System.Reactive.Async 6.0.0-alpha.18, R3, and ReactiveUI.Extensions to compare throughput and allocation behavior; the production packages must not.

The latest complete BenchmarkDotNet run finished on 2026-06-08 at 19:39:12 Europe/London with .NET SDK 11.0.100-preview.4.26230.115 and .NET runtime 10.0.8 on Windows 11. It executed 617 benchmarks with no failed benchmark process in 01:16:58:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --framework net10.0 --configuration Release --no-restore -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Latest artifact paths:

- `BenchmarkDotNet.Artifacts/BenchmarkRun-20260608-182233.log`
- `BenchmarkDotNet.Artifacts/run-full-benchmarks-20260608-182212.outer.log`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-06-08-19-39-12-report-github.md`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-06-08-19-39-12-report.html`
- `BenchmarkDotNet.Artifacts/results/BenchmarkRun-joined-2026-06-08-19-39-12-report.csv`

The joined run exports 617 raw BenchmarkDotNet rows: 238 ReactiveUI.Primitives or ReactiveUI.Primitives.Async cases, 157 System.Reactive cases, 132 R3 cases, and 90 ReactiveUI.Extensions cases. The current table includes the async replay-latest subscription scenario and subject multicast fan-out scenarios that were not present in the previous 610-row run.

The table below groups `ReactiveUI.Primitives` and `ReactiveUI.Primitives.Async` into the `ReactiveUI.Primitives` column, aligns each primitive benchmark with any System.Reactive, R3, or ReactiveUI.Extensions alternative from the same benchmark scenario, and uses `NA` where no alternative exists. It contains 238 alphabetically ordered scenario rows. Cells use `Mean / Allocated`, and long `scenario` parameter values from BenchmarkDotNet are restored to their full names.

External-baseline posture from this run: ReactiveUI.Primitives is faster than System.Reactive in 151/157 measured comparisons, faster than R3 in 131/132 measured comparisons, and faster than ReactiveUI.Extensions 4.0.0 in 58/90 measured comparisons. Rows that are not faster remain listed for direct comparison.

| Scenario | ReactiveUI.Primitives | System.Reactive | R3 | ReactiveUI.Extensions |
|---|---:|---:|---:|---:|
| `After` | 161.4435 ns / 584 B | 934.2132 ns / 25056 B | 273.3047 ns / 552 B | NA |
| `AggregateAnyCount (Operator core GC profile)` | 197.3247 ns / 824 B | 5,651.5205 ns / 5856 B | 670.9280 ns / 1280 B | NA |
| `AggregateAnyCount (Operator map keep)` | 209.1845 ns / 824 B | 5,801.8443 ns / 5856 B | 612.6859 ns / 1280 B | NA |
| `All` | 19.2005 ns / 96 B | 2,664.6337 ns / 2520 B | 89.5099 ns / 192 B | NA |
| `AllContains` | 29.5381 ns / 192 B | 5,262.9316 ns / 5048 B | 213.4439 ns / 392 B | NA |
| `AllRange` | 20.3476 ns / 96 B | 2,605.7495 ns / 2520 B | 90.4437 ns / 192 B | NA |
| `AsSignal` | 41.0844 ns / 112 B | 2,688.2408 ns / 2536 B | 194.0944 ns / 160 B | 2,646.8338 ns / 2488 B |
| `AutoConnect` | 141.0078 ns / 408 B | 2,760.6903 ns / 2736 B | NA | NA |
| `AutoConnectSubscribe` | 143.5240 ns / 408 B | 2,837.9738 ns / 2736 B | NA | NA |
| `BehaviorEmit` | 15,580.1615 ns / 160 B | NA | NA | NA |
| `BufferRange` | 70.1760 ns / 304 B | 1,463.5930 ns / 1656 B | 118.6141 ns / 360 B | NA |
| `BufferUntil` | 48.1740 ns / 264 B | NA | NA | 45.4763 ns / 264 B |
| `BufferUntilIdle` | 2,070.5617 ns / 6504 B | NA | NA | 28,683.3995 ns / 21207 B |
| `BufferUntilInactive` | 2,101.6589 ns / 6504 B | NA | NA | 28,331.4789 ns / 21206 B |
| `CastTo` | 95.6295 ns / 200 B | 1,507.3750 ns / 1568 B | 168.6829 ns / 216 B | NA |
| `CatchAndReturn` | 20.4972 ns / 128 B | 195.6230 ns / 368 B | 129.3629 ns / 264 B | 68.4632 ns / 184 B |
| `CatchIgnore` | 19.7715 ns / 128 B | 177.7583 ns / 344 B | 123.2607 ns / 240 B | 64.5144 ns / 184 B |
| `CatchReturn` | 14.7025 ns / 128 B | 185.6928 ns / 368 B | 127.5877 ns / 264 B | 63.3613 ns / 184 B |
| `CatchReturnUnit` | 10.5709 ns / 88 B | NA | NA | 61.0721 ns / 144 B |
| `CollectArray (Terminal collection GC profile)` | 39.4273 ns / 360 B | 2,932.9110 ns / 3144 B | 184.3065 ns / 784 B | NA |
| `CollectArray (Terminal collection)` | 37.4360 ns / 360 B | 2,742.8235 ns / 3144 B | 180.8473 ns / 784 B | NA |
| `CollectArrayAsync` | 35.0986 ns / 384 B | 2,838.6592 ns / 3384 B | 169.3271 ns / 784 B | NA |
| `CollectList (Terminal collection GC profile)` | 76.1907 ns / 392 B | 2,682.1894 ns / 2992 B | 177.1869 ns / 632 B | NA |
| `CollectList (Terminal collection)` | 72.8729 ns / 392 B | 2,645.8995 ns / 2992 B | 167.3003 ns / 632 B | NA |
| `CollectListAsync` | 47.9789 ns / 352 B | 1,498.7641 ns / 2056 B | 124.4369 ns / 480 B | NA |
| `CombineLatest` | 41.1023 ns / 192 B | 3,327.7110 ns / 2824 B | 689.0555 ns / 344 B | NA |
| `CombineLatestRanges` | 41.6113 ns / 192 B | 3,203.0270 ns / 2824 B | 676.1603 ns / 344 B | NA |
| `CombineLatestValuesAreAllFalse` | 214.4874 ns / 936 B | 363.2088 ns / 648 B | NA | 232.9241 ns / 1176 B |
| `CombineLatestValuesAreAllTrue` | 209.2835 ns / 936 B | 373.2756 ns / 648 B | NA | 230.6682 ns / 1176 B |
| `CommandExecuteAsync` | 36.1038 ns / 152 B | 725.3990 ns / 1089 B | 115.4143 ns / 296 B | NA |
| `CommandResultSubscribeAsync` | 63.7978 ns / 224 B | 41.2514 ns / 136 B | 70.1836 ns / 160 B | NA |
| `CompletedSpark` | 0.0000 ns / 0 B | 0.0083 ns / 0 B | 0.0167 ns / 0 B | NA |
| `CompletedTaskBridge` | 10.4882 ns / 88 B | 867.1664 ns / 793 B | 45.4824 ns / 88 B | NA |
| `Concat` | 75.7136 ns / 256 B | 2,931.5501 ns / 2856 B | 260.7747 ns / 360 B | NA |
| `ConcatRanges` | 76.5450 ns / 256 B | 2,961.9462 ns / 2856 B | 255.5487 ns / 360 B | NA |
| `Conflate` | 4,146.7812 ns / 2312 B | NA | NA | 35,228.6641 ns / 16970 B |
| `Contains` | 10.9311 ns / 96 B | 2,733.3856 ns / 2528 B | 99.1364 ns / 200 B | NA |
| `ContainsRange` | 10.1873 ns / 96 B | 2,670.6758 ns / 2528 B | 94.0961 ns / 200 B | NA |
| `Continuation.Dispose` | 25.3797 ns / 192 B | NA | NA | 25.5549 ns / 192 B |
| `Continuation.Lock` | 1,260.4535 ns / 464 B | NA | NA | 1,190.6156 ns / 464 B |
| `Continuation.LockValueTask` | 1,175.4602 ns / 464 B | NA | NA | 1,208.8696 ns / 464 B |
| `CountPredicate (Terminal collection GC profile)` | 37.8831 ns / 96 B | 2,621.1035 ns / 2520 B | 98.5233 ns / 200 B | NA |
| `CountPredicate (Terminal collection)` | 20.0000 ns / 96 B | 2,647.6879 ns / 2520 B | 99.6155 ns / 200 B | NA |
| `CreateSafeSubscribe` | 38.6123 ns / 112 B | NA | NA | NA |
| `CreateSubscribe` | 38.9402 ns / 112 B | 49.9716 ns / 168 B | 67.1235 ns / 152 B | NA |
| `CreateWithState` | 61.2168 ns / 192 B | 87.3642 ns / 256 B | 120.9910 ns / 240 B | NA |
| `CurrentThreadSchedule` | 8.3874 ns / 88 B | 18.1439 ns / 88 B | 32.2492 ns / 56 B | NA |
| `DebounceImmediate` | 1,754.9197 ns / 4064 B | NA | NA | 30,137.8438 ns / 18054 B |
| `DebounceUntil` | 1,221.7361 ns / 776 B | NA | NA | 7,788.1093 ns / 6126 B |
| `DefaultIfEmptyEmpty` | 5.5498 ns / 64 B | 68.9009 ns / 144 B | 67.3876 ns / 136 B | NA |
| `DeferSubscribe` | 82.5640 ns / 240 B | 1,447.0622 ns / 1512 B | 122.1637 ns / 152 B | NA |
| `DelayRange` | 165.0811 ns / 536 B | 6,285.0703 ns / 39584 B | 2,091.7877 ns / 2200 B | NA |
| `DelayStartRange` | 164.0063 ns / 536 B | 2,503.0134 ns / 26456 B | 338.4165 ns / 552 B | NA |
| `DematerializeRange` | 71.9757 ns / 184 B | 1,473.7932 ns / 1528 B | 205.2736 ns / 208 B | NA |
| `DetectStale` | 208.3042 ns / 600 B | NA | NA | 938.7462 ns / 25128 B |
| `DisposableCollectionDispose` | 68.9755 ns / 424 B | 103.6309 ns / 512 B | 86.0407 ns / 480 B | NA |
| `DoOnDispose` | 76.8536 ns / 232 B | NA | NA | 80.8925 ns / 232 B |
| `DoOnSubscribe` | 76.4659 ns / 192 B | NA | NA | 77.0226 ns / 192 B |
| `DropIfBusy` | 387.0132 ns / 240 B | NA | NA | 378.0149 ns / 240 B |
| `Emit1024` | 1,581.4852 ns / 192 B | 1,750.5569 ns / 136 B | 2,029.8888 ns / 160 B | NA |
| `Empty` | 3.0465 ns / 40 B | 48.0018 ns / 96 B | 30.6985 ns / 56 B | NA |
| `EmptySubscribe` | 2.9831 ns / 40 B | 52.8440 ns / 96 B | 34.6125 ns / 56 B | NA |
| `Every` | 526.0310 ns / 1192 B | 2,858.5448 ns / 34001 B | 337.4532 ns / 552 B | NA |
| `FastForEach` | 52.5630 ns / 40 B | NA | NA | 52.4816 ns / 40 B |
| `Filter` | 128.6757 ns / 120 B | 787.8662 ns / 984 B | NA | 123.8859 ns / 120 B |
| `FirstAsync` | 5.9440 ns / 56 B | 2,582.4415 ns / 2792 B | 77.0095 ns / 208 B | NA |
| `FirstMatchFromCandidates` | 48.3142 ns / 216 B | NA | NA | 40.4904 ns / 216 B |
| `FirstOrDefaultAsync` | 6.0260 ns / 56 B | 1,410.5826 ns / 1768 B | 66.3999 ns / 208 B | NA |
| `FlatMap` | 737.5430 ns / 728 B | 3,836.4955 ns / 3872 B | 1,104.8553 ns / 1040 B | NA |
| `FlatMapRange` | 723.2042 ns / 728 B | 3,745.7840 ns / 3872 B | 1,090.7939 ns / 1040 B | NA |
| `Fold (Operator stateful filter GC profile)` | 1,963.0732 ns / 144 B | NA | NA | NA |
| `Fold (Operator stateful filter)` | 97.7211 ns / 144 B | 2,642.7624 ns / 2520 B | NA | NA |
| `ForEach` | 75.0722 ns / 160 B | 157.6810 ns / 200 B | NA | 78.4165 ns / 160 B |
| `ForkJoin` | 25.3112 ns / 192 B | 3,744.3144 ns / 3136 B | 1,155.6442 ns / 504 B | NA |
| `ForkJoinRanges` | 21.9770 ns / 192 B | 3,497.1976 ns / 3136 B | 968.2838 ns / 504 B | NA |
| `FromArray` | 61.7537 ns / 72 B | 2,471.6468 ns / 2504 B | 79.5506 ns / 88 B | 60.1525 ns / 72 B |
| `FromAsyncEnumerableSubscribeAsync` | 1,126.3758 ns / 600 B | 1,623.9187 ns / 1838 B | 1,272.2635 ns / 1023 B | NA |
| `FromEnumerable` | 53.6114 ns / 40 B | 2,548.2366 ns / 2504 B | 78.8076 ns / 88 B | NA |
| `FromEnumerableSubscribe` | 54.1935 ns / 40 B | 2,552.4211 ns / 2504 B | 78.3198 ns / 88 B | NA |
| `FromEventPattern` | 121.1161 ns / 624 B | 1,735.8404 ns / 2422 B | NA | NA |
| `GetMax` | 114.3242 ns / 408 B | 182.2547 ns / 328 B | NA | 216.9035 ns / 1152 B |
| `GetMin` | 112.0888 ns / 408 B | 183.0003 ns / 328 B | NA | 218.7370 ns / 1152 B |
| `Heartbeat` | 291.0539 ns / 800 B | NA | NA | 2,565.3634 ns / 26096 B |
| `HistorySubscribe` | 345.0974 ns / 352 B | 707.3779 ns / 696 B | 423.2712 ns / 688 B | NA |
| `IgnoreValuesRange` | 28.7628 ns / 128 B | 1,424.1602 ns / 1504 B | 78.7061 ns / 160 B | NA |
| `Iterate` | 11.8113 ns / 0 B | 2,363.1381 ns / 2768 B | NA | NA |
| `KeepNotNull` | 106.0897 ns / 192 B | 1,546.9787 ns / 1624 B | 231.1263 ns / 312 B | NA |
| `KeepType` | 103.0512 ns / 192 B | 1,515.3027 ns / 1568 B | 193.8181 ns / 216 B | NA |
| `KeepWith` | 52.2025 ns / 136 B | 1,468.1596 ns / 1608 B | 129.0034 ns / 280 B | NA |
| `LastOrDefaultAsync` | 12.7311 ns / 192 B | 1,421.3154 ns / 1872 B | 75.4668 ns / 208 B | NA |
| `LatestOrDefault` | 54.2813 ns / 136 B | NA | NA | 53.2314 ns / 136 B |
| `LogErrors` | 70.2575 ns / 224 B | NA | NA | 68.4577 ns / 224 B |
| `LongCountPredicate` | 20.3864 ns / 104 B | 2,559.9402 ns / 2536 B | 109.9617 ns / 272 B | NA |
| `MapKeep` | 133.9801 ns / 208 B | 2,770.7661 ns / 2584 B | 319.2388 ns / 272 B | NA |
| `MapWith` | 46.4196 ns / 136 B | 1,461.4366 ns / 1608 B | 137.8286 ns / 248 B | NA |
| `MaterializeRange` | 46.3840 ns / 120 B | 1,487.8726 ns / 1880 B | 100.6545 ns / 136 B | NA |
| `Merge` | 78.0824 ns / 256 B | 4,071.0686 ns / 3952 B | 718.8251 ns / 352 B | NA |
| `MergeRanges` | 76.2008 ns / 256 B | 4,001.1660 ns / 3952 B | 702.6162 ns / 352 B | NA |
| `MulticastConnect` | 149.9700 ns / 368 B | 2,745.4174 ns / 2696 B | 392.8761 ns / 368 B | NA |
| `NeverSubscribeDispose` | 0.0180 ns / 0 B | 5.1627 ns / 40 B | 19.4681 ns / 56 B | NA |
| `Not` | 27.1375 ns / 120 B | 857.5258 ns / 1040 B | 91.8254 ns / 152 B | 28.2047 ns / 120 B |
| `ObserveOnIf` | 67.2485 ns / 104 B | NA | NA | 65.0702 ns / 104 B |
| `ObserveOnImmediate` | 27.0528 ns / 96 B | 17,127.3834 ns / 11307 B | 993.3502 ns / 432 B | NA |
| `ObserveOnSafe` | 65.0889 ns / 104 B | NA | NA | 65.1163 ns / 104 B |
| `OnCleanup` | 139.4753 ns / 504 B | 1,500.6293 ns / 1528 B | 141.2798 ns / 216 B | NA |
| `OnErrorRetry` | 134.0780 ns / 424 B | NA | NA | 133.8272 ns / 424 B |
| `OnNext` | 51.8824 ns / 40 B | NA | NA | 51.7027 ns / 40 B |
| `Pairwise` | 512.4518 ns / 160 B | 3,585.0555 ns / 5120 B | NA | 520.0611 ns / 160 B |
| `Partition` | 275.7950 ns / 440 B | NA | NA | 263.9068 ns / 440 B |
| `Publish` | 150.2879 ns / 368 B | 2,773.0882 ns / 2696 B | 418.1471 ns / 368 B | NA |
| `PublishLiveConnect` | 153.5308 ns / 368 B | 3,153.5601 ns / 2696 B | 438.2998 ns / 368 B | NA |
| `Race` | 39.9629 ns / 192 B | 1,584.4902 ns / 1760 B | 303.8675 ns / 360 B | NA |
| `RaceRanges` | 41.2323 ns / 192 B | 1,567.0142 ns / 1760 B | 274.3877 ns / 360 B | NA |
| `Range` | 53.4398 ns / 96 B | 2,740.4466 ns / 2472 B | 94.6010 ns / 80 B | NA |
| `RangeMapKeep` | 152.2807 ns / 208 B | 2,722.9541 ns / 2584 B | 303.9360 ns / 272 B | NA |
| `RangeSubscribe` | 53.7344 ns / 96 B | 2,687.8141 ns / 2472 B | 75.1094 ns / 80 B | NA |
| `ReadOnlyStateProjection` | 103.6957 ns / 224 B | 96.4655 ns / 328 B | 177.4105 ns / 312 B | NA |
| `ReattemptRange` | 88.9289 ns / 432 B | 1,510.6942 ns / 1664 B | NA | NA |
| `Recover` | 97.4338 ns / 336 B | 1,504.3451 ns / 1560 B | 163.6832 ns / 264 B | NA |
| `Reduce (Operator stateful filter GC profile)` | 624.4331 ns / 144 B | NA | NA | NA |
| `Reduce (Operator stateful filter)` | 45.3575 ns / 144 B | 2,781.7262 ns / 2520 B | NA | NA |
| `RefCount` | 211.3754 ns / 488 B | NA | 570.7335 ns / 488 B | NA |
| `RefCountSubscribe` | 187.9160 ns / 488 B | NA | 597.1460 ns / 488 B | NA |
| `Repeat` | 8.8482 ns / 0 B | 2,586.2862 ns / 2408 B | 76.6422 ns / 80 B | NA |
| `RepeatSubscribe` | 7.3905 ns / 0 B | 2,537.4997 ns / 2408 B | 73.8453 ns / 80 B | NA |
| `Replay (Connectable GC profile)` | 639.9297 ns / 512 B | 3,954.6961 ns / 3408 B | 912.8537 ns / 1360 B | NA |
| `Replay (Subject GC profile)` | 352.6527 ns / 352 B | 725.4187 ns / 696 B | 426.9037 ns / 688 B | NA |
| `ReplayEmit` | 16,608.7830 ns / 352 B | NA | NA | NA |
| `ReplayLastOnSubscribe` | 64.3566 ns / 104 B | NA | NA | 64.9676 ns / 104 B |
| `ReplayLatestSubscribeDisposeAsync` | 2,729.7429 ns / 4736 B | NA | NA | NA |
| `ReplayLiveLateSubscribe` | 634.1315 ns / 512 B | 3,933.9536 ns / 3408 B | 946.8956 ns / 1360 B | NA |
| `Resume` | 90.3509 ns / 336 B | 1,607.4910 ns / 1720 B | NA | NA |
| `RetryForeverWithDelay` | 126.4210 ns / 352 B | NA | NA | 125.3468 ns / 352 B |
| `RetryWithBackoff` | 126.0700 ns / 336 B | NA | NA | 127.8376 ns / 336 B |
| `RetryWithDelay` | 113.8488 ns / 264 B | NA | NA | 111.5882 ns / 264 B |
| `RetryWithFixedDelay` | 127.4925 ns / 336 B | NA | NA | 135.5845 ns / 336 B |
| `Return (Factory GC profile)` | 0.6648 ns / 0 B | 54.5551 ns / 120 B | 34.2715 ns / 80 B | NA |
| `Return (Reactive extensions)` | 5.4016 ns / 64 B | 51.9597 ns / 120 B | 29.6606 ns / 56 B | 4.8531 ns / 64 B |
| `ReturnSubscribe` | 0.2305 ns / 0 B | 51.1068 ns / 120 B | 31.9637 ns / 80 B | NA |
| `RunAll` | 21.7965 ns / 136 B | NA | NA | 24.1500 ns / 136 B |
| `SafeWitness` | 17.4100 ns / 136 B | 15.8467 ns / 136 B | 24.8238 ns / 128 B | NA |
| `SampleLatest (Operator time scheduler)` | 260.1148 ns / 784 B | 2,328.9173 ns / 26264 B | 360.7133 ns / 664 B | NA |
| `SampleLatest (Reactive extensions)` | 1,005.8374 ns / 488 B | NA | NA | 1,054.2662 ns / 840 B |
| `ScanWithInitial` | 500.3109 ns / 200 B | 2,538.0716 ns / 2560 B | NA | 510.5500 ns / 200 B |
| `Schedule` | 32.4787 ns / 216 B | NA | NA | 765.7171 ns / 677 B |
| `ScheduleSafe` | 23.9532 ns / 144 B | NA | NA | 1,510.3594 ns / 597 B |
| `SelectAsync` | 1,277.8845 ns / 2104 B | 28,626.1719 ns / 32266 B | NA | 1,240.8623 ns / 2104 B |
| `SelectAsyncConcurrent` | 1,154.4372 ns / 2120 B | NA | NA | 1,180.0831 ns / 2120 B |
| `SelectAsyncSequential` | 1,190.9455 ns / 2104 B | NA | NA | 1,274.9363 ns / 2104 B |
| `SelectConstant` | 56.2966 ns / 136 B | 2,540.6148 ns / 2544 B | 184.1691 ns / 160 B | 54.6658 ns / 136 B |
| `SelectLatestAsync` | 1,675.4246 ns / 2032 B | NA | NA | 1,653.0930 ns / 2032 B |
| `SelectManyThen` | 31.9010 ns / 224 B | 354.3868 ns / 752 B | NA | 31.3855 ns / 224 B |
| `SequenceCountAsync` | 813.8323 ns / 704 B | NA | NA | 798.8903 ns / 704 B |
| `SequenceMapKeepToListAsync` | 2,001.8091 ns / 1600 B | NA | NA | 1,950.6413 ns / 1600 B |
| `Share` | 197.9768 ns / 488 B | 2,925.1001 ns / 2880 B | 560.0629 ns / 488 B | NA |
| `ShareLiveSubscribe` | 190.3772 ns / 488 B | 2,960.3324 ns / 2880 B | 544.7921 ns / 488 B | NA |
| `Shuffle` | 145.1861 ns / 96 B | NA | NA | 146.1696 ns / 96 B |
| `SignalBroadcastAsync` | 6,400.1401 ns / 2256 B | NA | NA | 6,840.4302 ns / 2320 B |
| `SignalEmit` | 1,598.2307 ns / 192 B | NA | NA | NA |
| `SignalFanOutChurn` | 40,048.2300 ns / 41256 B | NA | NA | NA |
| `SignalMulticast4` | 3,417.1940 ns / 600 B | 3,268.6291 ns / 728 B | 7,277.5419 ns / 608 B | NA |
| `SignalMulticast8` | 6,441.3053 ns / 1072 B | 6,053.3424 ns / 1656 B | 13,045.9407 ns / 1120 B | NA |
| `SignalSubscribeDisposeChurn` | 39,876.6439 ns / 41112 B | NA | NA | NA |
| `Skip (Operator stateful filter GC profile)` | 1,735.5486 ns / 136 B | NA | NA | NA |
| `Skip (Operator stateful filter)` | 86.5946 ns / 136 B | 2,658.2239 ns / 2512 B | NA | NA |
| `SkipWhile (Operator stateful filter GC profile)` | 1,800.1429 ns / 144 B | NA | NA | NA |
| `SkipWhile (Operator stateful filter)` | 94.1695 ns / 144 B | 2,700.6545 ns / 2520 B | NA | NA |
| `SkipWhileNull` | 22.9011 ns / 112 B | 644.6796 ns / 944 B | NA | 22.1427 ns / 112 B |
| `Start` | 23.0806 ns / 96 B | NA | NA | 936.0223 ns / 535 B |
| `StartSubscribe` | 47.5984 ns / 208 B | 860.5110 ns / 751 B | 66.4836 ns / 160 B | NA |
| `StartWithAppend` | 35.9490 ns / 168 B | 1,030.8613 ns / 1283 B | 157.9538 ns / 288 B | NA |
| `StartWithAppendDefaultIfEmpty` | 36.0423 ns / 168 B | 994.2108 ns / 1283 B | 151.1573 ns / 288 B | NA |
| `State1024` | 15,860.3556 ns / 160 B | 16,763.4572 ns / 200 B | 16,556.1635 ns / 192 B | NA |
| `StateEmit` | 15,824.8088 ns / 160 B | NA | NA | NA |
| `StateSignal1024` | 16,183.5083 ns / 160 B | 17,070.4020 ns / 200 B | 16,583.5164 ns / 192 B | NA |
| `StateSignal32` | 546.1293 ns / 160 B | 605.4201 ns / 200 B | 630.1873 ns / 192 B | NA |
| `StateSignalUpdates` | 557.0397 ns / 160 B | 583.7040 ns / 200 B | 622.1236 ns / 192 B | NA |
| `SubjectEmit1024` | 1,590.1508 ns / 192 B | 1,761.6808 ns / 136 B | 2,091.8153 ns / 160 B | NA |
| `SubjectEmit32` | 94.0824 ns / 192 B | 99.2329 ns / 136 B | 124.9866 ns / 160 B | NA |
| `SubjectSubscribeDispose64` | 3,557.3485 ns / 4360 B | 3,994.1933 ns / 38472 B | 3,825.6100 ns / 6728 B | NA |
| `SubjectSubscribeDispose8` | 352.1052 ns / 704 B | 318.3937 ns / 1288 B | 493.2540 ns / 904 B | NA |
| `SubscribeAndComplete` | 0.2067 ns / 0 B | NA | NA | 0.2286 ns / 0 B |
| `SubscribeAsync` | 967.6081 ns / 544 B | NA | NA | 996.4746 ns / 544 B |
| `SubscribeDispose64` | 3,743.0097 ns / 4360 B | 4,234.4889 ns / 38472 B | 3,772.4909 ns / 6728 B | NA |
| `SubscribeGetError` | 6.0310 ns / 48 B | NA | NA | 50.6131 ns / 104 B |
| `SubscribeGetValue` | 15.8250 ns / 56 B | NA | NA | 15.8805 ns / 56 B |
| `SubscribeOnImmediate` | 102.0478 ns / 416 B | 2,089.6776 ns / 2257 B | 134.4352 ns / 200 B | NA |
| `SubscribeSynchronous` | 1,030.9847 ns / 544 B | NA | NA | 999.5322 ns / 544 B |
| `Switch` | 84.3075 ns / 312 B | 2,342.9420 ns / 2360 B | 797.3904 ns / 448 B | NA |
| `SwitchIfEmpty` | 65.5825 ns / 224 B | NA | NA | 109.1341 ns / 280 B |
| `SwitchRanges` | 84.5506 ns / 312 B | 2,248.5388 ns / 2360 B | 765.5251 ns / 448 B | NA |
| `SynchronizeAsync` | 796.1634 ns / 1280 B | NA | NA | 818.4586 ns / 1280 B |
| `SynchronizeSynchronous` | 818.4827 ns / 1280 B | NA | NA | 793.0097 ns / 1280 B |
| `SyncTimer` | 2,513.3305 ns / 1080 B | NA | NA | 12,247.8994 ns / 26240 B |
| `TakeRange` | 65.3818 ns / 200 B | 1,487.1980 ns / 1552 B | 99.5176 ns / 160 B | NA |
| `TakeUntil` | 518.1361 ns / 192 B | 2,618.4101 ns / 2520 B | NA | 508.4543 ns / 192 B |
| `TakeWhile (Operator stateful filter GC profile)` | 1,668.5275 ns / 144 B | NA | NA | NA |
| `TakeWhile (Operator stateful filter)` | 100.4530 ns / 144 B | 2,649.6499 ns / 2520 B | NA | NA |
| `TapRange` | 62.0108 ns / 200 B | 1,460.5331 ns / 1520 B | 130.6363 ns / 216 B | NA |
| `TapWith` | 38.1821 ns / 136 B | 1,479.6060 ns / 1608 B | 147.0685 ns / 304 B | NA |
| `TaskSignalSubscribe` | 37.9094 ns / 240 B | 731.7046 ns / 886 B | 40.2932 ns / 160 B | NA |
| `ThrottleBurst` | 598.0057 ns / 1184 B | 2,818.8936 ns / 36480 B | 1,717.3992 ns / 1512 B | NA |
| `ThrottleDistinct` | 1,787.3767 ns / 4232 B | NA | NA | 28,694.7072 ns / 18678 B |
| `ThrottleFirst` | 1,119.1755 ns / 224 B | NA | NA | 1,143.5209 ns / 224 B |
| `ThrottleOnScheduler` | 1,835.5199 ns / 2400 B | NA | NA | 30,618.2012 ns / 16366 B |
| `ThrottleUntilTrue` | 4,465.7651 ns / 1633 B | NA | NA | 5,547.6550 ns / 1385 B |
| `Throw` | 63.0356 ns / 120 B | 129.4794 ns / 240 B | 98.1077 ns / 200 B | NA |
| `ThrowSubscribe` | 63.0707 ns / 120 B | 115.1738 ns / 240 B | 98.3755 ns / 200 B | NA |
| `TimeIntervalRange` | 26.6122 ns / 120 B | 2,009.7466 ns / 1616 B | 480.1429 ns / 160 B | NA |
| `TimeoutIdle` | 311.1149 ns / 808 B | 1,442.3126 ns / 29776 B | 441.6512 ns / 784 B | NA |
| `TimestampRange` | 40.2701 ns / 120 B | 1,796.7855 ns / 1512 B | 360.3219 ns / 152 B | NA |
| `ToHotTask` | 35.2807 ns / 112 B | 91.3744 ns / 240 B | NA | 33.2869 ns / 112 B |
| `ToHotValueTask` | 26.8257 ns / 72 B | NA | NA | 26.8298 ns / 72 B |
| `ToPropertyObservable` | 26,227.7832 ns / 4941 B | NA | NA | 26,438.4603 ns / 4941 B |
| `ToReadOnlyBehavior` | 58.7424 ns / 192 B | NA | NA | 58.4042 ns / 192 B |
| `ToTask` | 14.9083 ns / 192 B | 2,635.3250 ns / 2824 B | 95.4554 ns / 208 B | NA |
| `TrySelect` | 104.2941 ns / 120 B | NA | NA | 103.1948 ns / 120 B |
| `UnfoldSubscribe` | 10.4787 ns / 0 B | 2,327.2008 ns / 2768 B | 98.2107 ns / 152 B | NA |
| `Unique (Operator stateful filter GC profile)` | 1,900.0763 ns / 144 B | NA | NA | NA |
| `Unique (Operator stateful filter)` | 109.5600 ns / 144 B | 2,694.3320 ns / 2520 B | NA | NA |
| `UniqueBy (Operator stateful filter GC profile)` | 1,957.1433 ns / 152 B | NA | NA | NA |
| `UniqueBy (Operator stateful filter)` | 103.9614 ns / 152 B | 2,661.1942 ns / 2568 B | NA | NA |
| `UseSubscribe` | 43.0683 ns / 144 B | 88.4371 ns / 168 B | 76.1832 ns / 176 B | NA |
| `Using` | 6.0721 ns / 56 B | NA | NA | 6.2145 ns / 56 B |
| `WaitForCompletion` | 22.0032 ns / 96 B | NA | NA | 22.5720 ns / 96 B |
| `WaitForError` | 25.0017 ns / 96 B | NA | NA | 65.9992 ns / 152 B |
| `WaitForValue` | 30.4019 ns / 104 B | NA | NA | 30.6159 ns / 104 B |
| `WaitUntil` | 518.4325 ns / 224 B | 835.7113 ns / 1080 B | NA | 543.8398 ns / 224 B |
| `WhereFalse` | 21.3931 ns / 120 B | 758.8829 ns / 1040 B | 88.0082 ns / 184 B | 19.7443 ns / 120 B |
| `WhereIsNotNull` | 20.9544 ns / 104 B | 621.3007 ns / 904 B | 100.0026 ns / 264 B | 21.2683 ns / 104 B |
| `WhereSelect` | 80.1184 ns / 152 B | 2,631.6110 ns / 2616 B | 174.5875 ns / 240 B | 77.8284 ns / 152 B |
| `WhereTrue` | 20.9512 ns / 120 B | 766.8200 ns / 1040 B | 83.7375 ns / 184 B | 21.0377 ns / 120 B |
| `While` | 122.1943 ns / 280 B | NA | NA | 123.8769 ns / 280 B |
| `WithLatest` | 40.6575 ns / 192 B | 3,589.8584 ns / 2824 B | 396.8615 ns / 248 B | NA |
| `WithLatestRanges` | 40.9675 ns / 192 B | 3,514.5040 ns / 2824 B | 269.3590 ns / 248 B | NA |
| `WithLimitedConcurrency` | 2,505.1640 ns / 5448 B | NA | NA | 2,486.2790 ns / 5448 B |
| `Zip (Operator core GC profile)` | 43.2304 ns / 192 B | 3,821.7608 ns / 2976 B | 781.9091 ns / 656 B | NA |
| `Zip (Operator zip)` | 37.2355 ns / 192 B | 3,337.3608 ns / 2976 B | 753.2998 ns / 656 B | NA |

BenchmarkDotNet emitted `ZeroMeasurement` warnings for several singleton or empty-method-scale paths, including `Return`, `CompletedSpark`, `Never`-style subscriptions, and `SubscribeAndComplete`. Those warnings mean the measured duration is indistinguishable from empty method overhead; the benchmark run still completed and exported all 617 rows.

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
| `src/ReactiveUI.Primitives.SystemReactiveBridge.Generator` | Source generators for System.Reactive and System.Reactive.Async bridge adapters. |
| `src/ReactiveUI.Primitives.R3Bridge.Generator` | Source generator for R3 bridge adapters. |
| `src/ReactiveUI.Primitives.Tests` | Test project using Microsoft Testing Platform/TUnit-style validation. |
| `src/benchmarks/ReactiveUI.Primitives.Benchmarks` | BenchmarkDotNet comparison harness. |

## Practical migration checklist

1. Replace subject construction with `Signal<T>`, `StateSignal<T>`, or `HistorySignal<T>` depending on current behavior.
2. Replace factories: `Observable.Return/Empty/Throw/Timer/Interval` to `Signal.Emit/None/Fail/After/Pulse`.
3. Replace hot-path operators with Primitives names: `Select -> Map`, `Where -> Keep`, `SelectMany -> FlatMap`, `Do -> Tap`, `Scan -> Fold`, `Aggregate -> Reduce`, `Amb -> Race`.
4. Replace composite/serial disposables with `MultipleDisposable`/`Pocket` and `SingleReplaceableDisposable`/`Slot`.
5. Keep System.Reactive/R3 at application boundaries only when required; use generated bridge methods when those packages are already referenced.
6. Run build, tests, pack, and `git diff --check` before publishing or merging.

## License

ReactiveUI.Primitives is licensed under the MIT license. See `LICENSE` for details.
