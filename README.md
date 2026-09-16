[![NuGet Stats](https://img.shields.io/nuget/v/ReactiveUI.Primitives.svg)](https://www.nuget.org/packages/ReactiveUI.Primitives) [![Build](https://github.com/reactiveui/Primitives/actions/workflows/ci-build.yml/badge.svg)](https://github.com/reactiveui/Primitives/actions/workflows/ci-build.yml) [![Code Coverage](https://codecov.io/gh/reactiveui/Primitives/branch/main/graph/badge.svg)](https://codecov.io/gh/reactiveui/Primitives) [![#yourfirstpr](https://img.shields.io/badge/first--timers--only-friendly-blue.svg)](https://reactiveui.net/contribute)
<br>
<a href="https://www.nuget.org/packages/ReactiveUI.Primitives">
<img src="https://img.shields.io/nuget/dt/ReactiveUI.Primitives.svg">
</a>
<a href="https://reactiveui.net/slack">
<img src="https://img.shields.io/badge/chat-slack-blue.svg">
</a>

<img alt="ReactiveUI.Primitives" width="160" height="160" src="https://github.com/reactiveui/styleguide/blob/master/logo_primitives/logo.png?raw=true">

# ReactiveUI.Primitives

ReactiveUI.Primitives is a small, fast library for values that arrive over time. A button click, a timer tick and a
network reply are all values that arrive over time. You subscribe to a source, and the source pushes each value to you
as it happens. The library builds on `IObservable<T>` and `IObserver<T>`, two interfaces that .NET already ships, so you
keep the types your code already uses. It adds no runtime reflection and generates no code at run time, so it works
under ahead-of-time compilation.

## Contents

- [The problem it solves](#the-problem-it-solves)
- [Your first signal](#your-first-signal)
- [Core model](#core-model)
- [Install](#install)
- [Target frameworks](#target-frameworks)
- [Operators](#operators)
- [Extension helpers](#extension-helpers)
- [Async operators](#async-operators)
- [Subjects and stateful signals](#subjects-and-stateful-signals)
- [Scheduling work](#scheduling-work)
- [Disposing subscriptions](#disposing-subscriptions)
- [Threading, errors and disposal rules](#threading-errors-and-disposal-rules)
- [Why not System.Reactive or R3?](#why-not-systemreactive-or-r3)
- [Observable event source generation](#observable-event-source-generation)
- [Source-generator bridge behavior](#source-generator-bridge-behavior)
- [Moving from System.Reactive](#moving-from-systemreactive)
- [Moving from R3](#moving-from-r3)
- [Moving from R3Async](#moving-from-r3async)
- [Moving from ReactiveUI.Extensions](#moving-from-reactiveuiextensions)
- [Benchmarks](#benchmarks)
- [Repository layout](#repository-layout)
- [For advanced users](#for-advanced-users)
- [Contribute](#contribute)
- [Code of Conduct](#code-of-conduct)
- [License](#license)

## The problem it solves

You have a search box. You want to run a search when the user stops typing. You do not want to search on every
keystroke, and you do not want to search a single letter.

C# gives you an event for the typing. The rest you write yourself:

```csharp
// Search when the user stops typing for 300ms.
private System.Timers.Timer? _timer;

private void OnTextChanged(object? sender, EventArgs e)
{
    var text = box.Text;

    if (text.Length <= 2)
    {
        return;
    }

    // Cancel the search we were about to run, start the wait again,
    // and capture the text so the timer sees the value the user typed last.
    _timer?.Stop();
    _timer?.Dispose();
    _timer = new System.Timers.Timer(300) { AutoReset = false };
    _timer.Elapsed += (_, _) => RunSearch(text);
    _timer.Start();
}

// Then detach the handler and dispose the timer when the view goes away.
```

With this library, you write the same rules as a chain:

```csharp
using var search = Signal.FromEventPattern(
        handler => box.TextChanged += handler,
        handler => box.TextChanged -= handler)
    .Map(_ => box.Text)
    .Keep(text => text.Length > 2)
    .Throttle(TimeSpan.FromMilliseconds(300))
    .Subscribe(text => RunSearch(text));
```

Each line does one job. `FromEventPattern` turns the event into a source. `Map` turns each value into another value.
`Keep` drops the values you do not want. `Throttle` waits for a quiet period after the most recent value. `Subscribe`
runs your code on what is left.

`Subscribe` hands back an `IDisposable`. Disposing it detaches the event handler and cancels the pending wait. That is
the whole cleanup.

> [!NOTE]
> `Throttle` times the wait on the thread pool. It does not move the value to the UI thread. Pass a sequencer to
> `Throttle`, or add `ObserveOn`, when the subscriber needs the UI thread.

## Your first signal

**1. Install the package.**

```bash
dotnet add package ReactiveUI.Primitives
```

**2. Create a signal.** A `Signal<T>` is a source you can push values into. It is also a source you can subscribe to.

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var signal = new Signal<int>();
```

**3. Subscribe.** You give `Subscribe` up to three callbacks. The first takes each value. The second takes an error.
The third runs when the signal finishes.

```csharp
using IDisposable subscription = signal.Subscribe(
    value => Console.WriteLine($"next: {value}"),
    error => Console.WriteLine($"error: {error.Message}"),
    () => Console.WriteLine("completed"));
```

**4. Push values.** `OnNext` sends a value to every current subscriber. Each call reaches your first callback right
away.

```csharp
signal.OnNext(1);   // prints next: 1
signal.OnNext(2);   // prints next: 2
```

**5. Finish the signal.** `OnCompleted` ends it successfully. `OnError` ends it with an exception. A signal that ends
sends nothing more.

```csharp
signal.OnCompleted();   // prints completed
```

**6. Dispose the subscription.** Disposing unsubscribes you. The `using` on step 3 does this at the end of the block.
A subscription you never dispose keeps your callbacks alive.

```csharp
subscription.Dispose();
```

Collect several subscriptions in a `MultipleDisposable` and dispose them together:

```csharp
using ReactiveUI.Primitives.Disposables;

var subscriptions = new MultipleDisposable();

signal.Subscribe(value => Console.WriteLine(value)).DisposeWith(subscriptions);
signal.Subscribe(value => Console.WriteLine(value * 10)).DisposeWith(subscriptions);

subscriptions.Dispose();
```

## Core model

**`IObservable<T>` is a source.** You call `Subscribe` on it. It pushes values to you.

**`IObserver<T>` is a subscriber.** It has three methods. The source calls `OnNext` for each value, `OnError` once if
something fails, and `OnCompleted` once when there is nothing more. `OnError` and `OnCompleted` both end the source. The
source calls one of them, once. It sends no values after that.

**A `Signal<T>` is both.** It implements `ISignal<T>`, which combines `IObserver<T>`, `IObservable<T>` and `IsDisposed`.
So you push values into one end and subscribe at the other. `HasObservers` tells you whether anyone is subscribed.
`IsDisposed` tells you whether the signal is disposed.

**A witness is a lightweight observer wrapper.** The library builds one for you when you pass callbacks to `Subscribe`.
You rarely write one by hand. Pass delegates or an `IObserver<T>` instead.

**Subscribing hands you an `IDisposable`.** Dispose it to unsubscribe. Scheduled work hands you one too. Dispose that to
cancel the work. `DisposeWith` adds a disposable to a `MultipleDisposable` so you can dispose a group in one call.

> [!NOTE]
> Another package can pull in System.Reactive, which declares its own `Subscribe` extension methods for
> `IObservable<T>`. Both sets are then in scope and the call is ambiguous. Call `SubscribePrimitives` to pick this
> library's implementation. It has the same callback overloads and the same behaviour as `Subscribe`.

## Install

Every package ships on [NuGet.org](https://www.nuget.org/packages?q=ReactiveUI.Primitives) at the same version. Start
with the base package:

```bash
dotnet add package ReactiveUI.Primitives
```

Then import the namespaces you need:

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
```

Each package covers one integration point. Take the ones you need.

| Package | NuGet | Use when |
|---------|-------|----------|
| [ReactiveUI.Disposables][Disp] | [![DispB]][Disp] | You want only the disposable types, such as `Disposable`, `MultipleDisposable`, `Slot` or `Pocket`. |
| [ReactiveUI.Primitives.Core][Core] | [![CoreB]][Core] | The shared core behind the lean and System.Reactive packages. You usually get it as a dependency. |
| [ReactiveUI.Primitives][Prim] | [![PrimB]][Prim] | The default package: signals, operators, sequencers and the extension helpers. |
| [ReactiveUI.Primitives.Reactive][Rx] | [![RxB]][Rx] | You want the same API compiled against System.Reactive `Unit` and `IScheduler`. |
| [ReactiveUI.Primitives.Async.Core][AsyncCore] | [![AsyncCoreB]][AsyncCore] | The shared core behind the async packages. |
| [ReactiveUI.Primitives.Async][Async] | [![AsyncB]][Async] | You want native `IObservableAsync<T>` and `IObserverAsync<T>` signals. |
| [ReactiveUI.Primitives.Async.Reactive][AsyncRx] | [![AsyncRxB]][AsyncRx] | You want the async API compiled against System.Reactive `Unit` and `IScheduler`. |
| [ReactiveUI.Primitives.ObservableEvents][Events] | [![EventsB]][Events] | You want .NET events exposed as `IObservable<T>` properties. This package is an analyzer. |
| [ReactiveUI.Primitives.R3Bridge.Generator][R3Bridge] | [![R3BridgeB]][R3Bridge] | You want generated R3 and R3Async bridge adapters. This package is an analyzer. |
| [ReactiveUI.Primitives.Wpf][Wpf] | [![WpfB]][Wpf] | You want the WPF dispatcher as a sequencer. |
| [ReactiveUI.Primitives.Wpf.Reactive][WpfRx] | [![WpfRxB]][WpfRx] | You want the WPF dispatcher as a System.Reactive scheduler. |
| [ReactiveUI.Primitives.WinForms][WinForms] | [![WinFormsB]][WinForms] | You want a Windows Forms control as a sequencer. |
| [ReactiveUI.Primitives.WinForms.Reactive][WinFormsRx] | [![WinFormsRxB]][WinFormsRx] | You want a Windows Forms control as a System.Reactive scheduler. |
| [ReactiveUI.Primitives.WinUI][WinUI] | [![WinUIB]][WinUI] | You want the WinUI dispatcher queue as a sequencer. |
| [ReactiveUI.Primitives.WinUI.Reactive][WinUIRx] | [![WinUIRxB]][WinUIRx] | You want the WinUI dispatcher queue as a System.Reactive scheduler. |
| [ReactiveUI.Primitives.Blazor][Blazor] | [![BlazorB]][Blazor] | You want the Blazor renderer as a sequencer. |
| [ReactiveUI.Primitives.Blazor.Reactive][BlazorRx] | [![BlazorRxB]][BlazorRx] | You want the Blazor renderer as a System.Reactive scheduler. |
| [ReactiveUI.Primitives.Avalonia][Avalonia] | [![AvaloniaB]][Avalonia] | You want the Avalonia UI thread as a sequencer. |
| [ReactiveUI.Primitives.Avalonia.Reactive][AvaloniaRx] | [![AvaloniaRxB]][AvaloniaRx] | You want the Avalonia UI thread as a System.Reactive scheduler. |
| [ReactiveUI.Primitives.Maui][Maui] | [![MauiB]][Maui] | You want the MAUI dispatcher as a sequencer. |
| [ReactiveUI.Primitives.Maui.Reactive][MauiRx] | [![MauiRxB]][MauiRx] | You want the MAUI dispatcher as a System.Reactive scheduler. |

A sequencer decides when and on which thread work runs. `ISequencer` is this library's own scheduling contract.

The R3 and R3Async bridge lives in its own analyzer package:

```bash
dotnet add package ReactiveUI.Primitives.R3Bridge.Generator
```

That generator adds no runtime dependency. It writes bridge code only when your project already references the R3 or
R3Async types.

[Disp]: https://www.nuget.org/packages/ReactiveUI.Disposables/

[DispB]: https://img.shields.io/nuget/v/ReactiveUI.Disposables.svg

[Core]: https://www.nuget.org/packages/ReactiveUI.Primitives.Core/

[CoreB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Core.svg

[Prim]: https://www.nuget.org/packages/ReactiveUI.Primitives/

[PrimB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.svg

[Rx]: https://www.nuget.org/packages/ReactiveUI.Primitives.Reactive/

[RxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Reactive.svg

[AsyncCore]: https://www.nuget.org/packages/ReactiveUI.Primitives.Async.Core/

[AsyncCoreB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Async.Core.svg

[Async]: https://www.nuget.org/packages/ReactiveUI.Primitives.Async/

[AsyncB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Async.svg

[AsyncRx]: https://www.nuget.org/packages/ReactiveUI.Primitives.Async.Reactive/

[AsyncRxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Async.Reactive.svg

[Events]: https://www.nuget.org/packages/ReactiveUI.Primitives.ObservableEvents/

[EventsB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.ObservableEvents.svg

[R3Bridge]: https://www.nuget.org/packages/ReactiveUI.Primitives.R3Bridge.Generator/

[R3BridgeB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.R3Bridge.Generator.svg

[Wpf]: https://www.nuget.org/packages/ReactiveUI.Primitives.Wpf/

[WpfB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Wpf.svg

[WpfRx]: https://www.nuget.org/packages/ReactiveUI.Primitives.Wpf.Reactive/

[WpfRxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Wpf.Reactive.svg

[WinForms]: https://www.nuget.org/packages/ReactiveUI.Primitives.WinForms/

[WinFormsB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.WinForms.svg

[WinFormsRx]: https://www.nuget.org/packages/ReactiveUI.Primitives.WinForms.Reactive/

[WinFormsRxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.WinForms.Reactive.svg

[WinUI]: https://www.nuget.org/packages/ReactiveUI.Primitives.WinUI/

[WinUIB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.WinUI.svg

[WinUIRx]: https://www.nuget.org/packages/ReactiveUI.Primitives.WinUI.Reactive/

[WinUIRxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.WinUI.Reactive.svg

[Blazor]: https://www.nuget.org/packages/ReactiveUI.Primitives.Blazor/

[BlazorB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Blazor.svg

[BlazorRx]: https://www.nuget.org/packages/ReactiveUI.Primitives.Blazor.Reactive/

[BlazorRxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Blazor.Reactive.svg

[Avalonia]: https://www.nuget.org/packages/ReactiveUI.Primitives.Avalonia/

[AvaloniaB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Avalonia.svg

[AvaloniaRx]: https://www.nuget.org/packages/ReactiveUI.Primitives.Avalonia.Reactive/

[AvaloniaRxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Avalonia.Reactive.svg

[Maui]: https://www.nuget.org/packages/ReactiveUI.Primitives.Maui/

[MauiB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Maui.svg

[MauiRx]: https://www.nuget.org/packages/ReactiveUI.Primitives.Maui.Reactive/

[MauiRxB]: https://img.shields.io/nuget/v/ReactiveUI.Primitives.Maui.Reactive.svg

## Target frameworks

A target framework (TFM) is the .NET version and platform a build targets, such as `net8.0`. Most packages share one
list: `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48` and `net481`. The repository calls that list
`$(LibraryTargetFrameworks)` and sets it in `src/Directory.Build.props`.

| Package | Target frameworks |
|---------|-------------------|
| `ReactiveUI.Disposables`, `ReactiveUI.Primitives.Core`, `ReactiveUI.Primitives.Async.Core`, `ReactiveUI.Primitives.Async`, `ReactiveUI.Primitives.Async.Reactive` | The shared list above. |
| `ReactiveUI.Primitives`, `ReactiveUI.Primitives.Reactive` | The shared list, plus `net10.0-android` and `net11.0-android`, plus the Apple TFMs `net10.0-ios`, `net11.0-ios`, `net10.0-tvos`, `net11.0-tvos`, `net10.0-macos`, `net11.0-macos`, `net10.0-maccatalyst` and `net11.0-maccatalyst`. The mobile and Apple TFMs build on Windows or macOS. |
| `ReactiveUI.Primitives.Wpf`, `ReactiveUI.Primitives.Wpf.Reactive`, `ReactiveUI.Primitives.WinForms`, `ReactiveUI.Primitives.WinForms.Reactive` | `net8.0-windows`, `net9.0-windows`, `net10.0-windows`, `net11.0-windows`, `net462`, `net472`, `net48`, `net481`. |
| `ReactiveUI.Primitives.WinUI`, `ReactiveUI.Primitives.WinUI.Reactive` | `net8.0-windows10.0.19041.0`, `net9.0-windows10.0.19041.0`, `net10.0-windows10.0.19041.0`, `net11.0-windows10.0.19041.0`. |
| `ReactiveUI.Primitives.Blazor`, `ReactiveUI.Primitives.Blazor.Reactive`, `ReactiveUI.Primitives.Avalonia`, `ReactiveUI.Primitives.Avalonia.Reactive` | `net8.0`, `net9.0`, `net10.0`, `net11.0`. |
| `ReactiveUI.Primitives.Maui`, `ReactiveUI.Primitives.Maui.Reactive` | `net10.0`, `net11.0`. |
| `ReactiveUI.Primitives.ObservableEvents`, `ReactiveUI.Primitives.R3Bridge.Generator` | `netstandard2.0`. |

The dependency list is short by design. `ReactiveUI.Primitives` references `ReactiveUI.Disposables` and
`ReactiveUI.Primitives.Core`. `ReactiveUI.Disposables` references `System.ValueTuple`, and only for `net462`. The
`.Reactive` packages reference `System.Reactive`. The .NET Framework TFMs pull in
support packages such as `System.ValueTuple`, `Microsoft.Bcl.TimeProvider`, `System.Threading.Channels`,
`System.Runtime.CompilerServices.Unsafe`, `System.ComponentModel.Annotations`, `System.Buffers`, `System.Memory` and
`System.Collections.Immutable`.

The platform packages reference their platform. Blazor references `Microsoft.AspNetCore.Components`. Avalonia references
`Avalonia`. MAUI references `Microsoft.Maui.Core` and the Microsoft.Extensions packages it needs. WinUI references
`Microsoft.WindowsAppSDK`.

## Operators

A signal is a stream of values you can subscribe to. A signal hands you values one at a time. It ends in one of two
ways: it completes, or it fails with an error. When you subscribe you get back an `IDisposable`. Dispose it to stop
listening.

A sequencer decides which thread runs your code. You pass one to an operator when you care where the work lands.

A hub is an object that takes values in and hands them to every subscriber.

Most operators have a short name and a second name. Both names run the same code, so pick the one you like and use it
everywhere. The last column of each table gives you the matching name from the libraries you may know.

`RxVoid` is this library's stand-in for "a value that carries no information".

### Creation factories

These build a signal from something else: a number range, a collection, a task, an event, or your own code. Unless a
row says otherwise, the method is `static` on `Signal`.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Signal.Emit(value)` | Emits one value, then completes. | `Observable.Return` |
| `Signal.Emit(value, sequencer)` | Emits one value on the sequencer you name. | `Return(value, scheduler)` |
| `Signal.Emit(RxVoid)`, `Signal.EmitRxVoid()` | Returns a shared signal that emits the unit value. | `Return(Unit.Default)` |
| `Signal.Emit(bool)` | Returns one of two cached signals, one for true and one for false. | `Return(bool)` |
| `Signal.Emit(int)` | Emits one int, then completes. | `Return(int)` |
| `Signal.Return(value)`, `Signal.Return(value, sequencer)` | Another name for `Emit`. | `Observable.Return` |
| `Signal.None<T>()` | Completes at once and emits nothing. | `Observable.Empty` |
| `Signal.None<T>(sequencer)` | Completes on the sequencer and emits nothing. | `Empty(scheduler)` |
| `Signal.None<T>(witness)`, `Signal.None<T>(sequencer, witness)` | Completes with no values and reads the element type from an unused argument. | `Empty<T>()` |
| `Signal.Empty<T>()`, `Signal.Empty<T>(sequencer)` | Another name for `None`. | `Observable.Empty` |
| `Signal.Silent<T>()`, `Signal.Silent<T>(witness)` | Emits nothing and never ends. | `Observable.Never` |
| `Signal.Never<T>()` | Another name for `Silent`. | `Observable.Never` |
| `Signal.Fail<T>(error)` | Fails with your error as soon as someone subscribes. | `Observable.Throw` |
| `Signal.Fail<T>(error, sequencer)` | Fails on the sequencer you name. | `Throw(e, scheduler)` |
| `Signal.Fail<T>(error, witness)`, `Signal.Fail<T>(error, sequencer, witness)` | Fails and reads the element type from an unused argument. | `Throw<T>(e)` |
| `Signal.Throw<T>(error)`, `Signal.Throw<T>(error, sequencer)` | Another name for `Fail`. | `Observable.Throw` |
| `Signal.Loop(value)` | Emits one value over and over and never ends. | `Observable.Repeat(value)` |
| `Signal.Loop(value, count)` | Emits one value `count` times, then completes. | `Repeat(value, count)` |
| `Signal.Repeat(value)`, `Signal.Repeat(value, repeatCount)` | Another name for `Loop`. | `Observable.Repeat` |
| `Signal.Sequence(start, count)` | Emits `count` whole numbers in a row, then completes. | `Observable.Range` |
| `Signal.Sequence(start, count, sequencer)` | Emits the same numbers on the sequencer you name. | `Range(..., scheduler)` |
| `Signal.Range(start, count)`, `Signal.Range(start, count, sequencer)` | Another name for `Sequence`. | `Observable.Range` |
| `Signal.Unfold(initialState, condition, iterate, resultSelector)` | Steps a state value forward and emits a projection of each step while the condition holds. | `Observable.Generate` |
| `Signal.Iterate(initialState, condition, iterator, resultSelector)` | Another name for `Unfold`. | `Generate` |
| `Signal.Generate(initialState, condition, iterate, resultSelector)` | A third name for `Unfold`. | `Observable.Generate` |
| `Signal.Create<T>(subscribe)` | Builds a signal from your own subscribe function and keeps the subscription alive when a subscriber's `OnNext` throws. | `Observable.Create` |
| `Signal.Create<T>(subscribe, isRequiredSubscribeOnCurrentThread)` | Builds the same signal and states that subscription must go through the current-thread sequencer. | `Observable.Create` |
| `Signal.Create<T>(async subscribe)`, `Signal.Create<T>(async subscribe with token)` | Builds a signal from an async subscribe function. | `Observable.Create` (async) |
| `Signal.CreateSafe<T>(subscribe)`, `Signal.CreateSafe<T>(subscribe, isRequiredSubscribeOnCurrentThread)` | Builds a signal that releases the subscription when a subscriber's `OnNext` throws. | `Observable.Create` |
| `Signal.CreateWithState<T, TState>(state, subscribe)` | Builds a signal and threads a state object through, so your callback can be `static` and allocate nothing. | - |
| `Signal.CreateWithState<T, TState>(state, subscribe, isRequiredSubscribeOnCurrentThread)` | Does the same with the current-thread flag. | - |
| `Signal.Lazy<T>(observableFactory)` | Calls your factory once per subscriber, so each one gets a fresh source. | `Observable.Defer` |
| `Signal.Defer<T>(observableFactory)` | Another name for `Lazy`. | `Observable.Defer` |
| `Signal.Defer<T>(async observableFactory)`, `Signal.Defer<T>(async observableFactory with token)` | Builds the source asynchronously for each subscriber. | `Observable.Defer` (async) |
| `Signal.If<T>(condition, thenSource)` | Picks your source or an empty one for each subscriber. | `Observable.If` |
| `Signal.If<T>(condition, thenSource, elseSource)` | Picks one of two sources for each subscriber. | `Observable.If` |
| `Signal.Case<TKey, T>(selector, sources)` | Picks a source out of a dictionary by key for each subscriber, and uses an empty one when the key is missing. | `Observable.Case` |
| `Signal.Case<TKey, T>(selector, sources, defaultSource)` | Picks a source by key and falls back to the one you name. | `Observable.Case` |
| `Signal.Use<TResource, T>(resourceFactory, signalFactory)` | Ties a resource to the subscription and disposes it when the subscription ends. | `Observable.Using` |
| `Signal.Using<TResource, T>(resourceFactory, observableFactory)` | Another name for `Use`. | `Observable.Using` |
| `Signal.FromEnumerable<T>(values)` | Emits each item of a collection in order, then completes. | `ToObservable()` |
| `Signal.FromEnumerable<T>(values, cancellationToken)` | Emits the same items and stops when the token fires. | `ToObservable()` |
| `Signal.FromAsyncEnumerable<T>(values)`, `Signal.FromAsyncEnumerable<T>(values, cancellationToken)` | Emits each item of an async stream. | `ToObservable()` |
| `Signal.FromTask<T>(task)` | Emits the task's result, then completes. | `task.ToObservable()` |
| `Signal.FromAsync<T>(taskFactory)` | Starts a task for each subscriber and emits its result. | `Observable.FromAsync` |
| `Signal.FromAsync<T>(taskFactory with token)` | Starts a task whose token cancels when you dispose the subscription. | `Observable.FromAsync` |
| `Signal.FromAsync<T>(taskFactory with token, cancellationToken)` | Starts a task and watches a token you supply. | `Observable.FromAsync` |
| `Signal.FromTask(execution)` | Returns an `ITaskSignal<RxVoid>` and hands your work a cancellation source that disposal cancels. | - |
| `Signal.FromTask(execution, sequencer)`, `Signal.FromTask(execution, sequencer, cts)` | Does the same on a sequencer, or with a cancellation source you own. | - |
| `Signal.FromTask<TResult>(actionAsync)` and its sequencer and `cts` overloads | Does the same for work that returns a value. | - |
| `Signal.FromEvent<TEventArgs>(addHandler, removeHandler)` | Turns an add and remove callback pair into a signal of the event's argument. | `Observable.FromEvent` |
| `Signal.FromEvent<TEventHandler, TEventArgs>(conversion, addHandler, removeHandler)` | Does the same when the event uses its own delegate type. | `Observable.FromEvent` |
| `Signal.FromEvent<TEventHandler, TEventArgs>(conversion, addHandler, removeHandler, sequencer)` | Attaches and detaches the handler on a sequencer. | `Observable.FromEvent` |
| `Signal.FromEventPattern(addHandler, removeHandler)`, and the sequencer overload | Turns a plain `EventHandler` event into a signal of `EventPattern<EventArgs>`. | `Observable.FromEventPattern` |
| `Signal.FromEventPattern<TEventArgs>(addHandler, removeHandler)`, and the sequencer overload | Does the same for an `EventHandler<TEventArgs>` event. | `FromEventPattern` |
| `Signal.FromEventPattern<TEventHandler, TEventArgs>(addHandler, removeHandler)`, and the sequencer overload | Does the same for an event with its own delegate type. | `FromEventPattern` |
| `Signal.FromEventPattern<TEventHandler, TEventArgs>(conversion, addHandler, removeHandler)`, and the sequencer overload | Does the same and takes an explicit conversion. | `FromEventPattern` |
| `Signal.FromEventPattern<TEventHandler, TSender, TEventArgs>(conversion, addHandler, removeHandler)`, and the sequencer overload | Does the same and keeps the sender's type. | `FromEventPattern` |
| `Signal.Start(action)`, `Signal.Start(action, sequencer)` | Runs an action on a sequencer and emits the unit value when it finishes. | `Observable.Start` |
| `Signal.Start<T>(function)`, `Signal.Start<T>(function, sequencer)` | Runs a function on a sequencer and emits its result. | `Observable.Start` |
| `Signal.After(dueTime)`, `Signal.After(dueTime, sequencer)` | Emits `0L` once after the delay, then completes. | `Observable.Timer` |
| `Signal.After(DateTimeOffset)`, `Signal.After(DateTimeOffset, sequencer)` | Emits `0L` once at the time you name. | `Timer` |
| `Signal.After(dueTime, period)`, `Signal.After(dueTime, period, sequencer)` | Emits a rising count after the delay, then once per period. | `Timer` |
| `Signal.Timer(...)`, all six overloads | Another name for `After`. | `Observable.Timer` |
| `Signal.Every(period)`, `Signal.Every(period, sequencer)` | Emits a rising count once per period, forever. | `Observable.Interval` |
| `Signal.Interval(period)`, `Signal.Interval(period, sequencer)` | Another name for `Every`. | `Observable.Interval` |
| `Signal.Pulse(period)`, `Signal.Pulse(period, sequencer)` | A third name for `Every`. | `Observable.Interval` |
| `Signal.Blend<T>(params sources)` | Subscribes to every source at once and passes values along as they arrive. | `Observable.Merge` |
| `Signal.Merge<T>(params sources)`, `Signal.Merge<T>(IEnumerable sources)` | Another name for `Blend`. | `Observable.Merge` |
| `Signal.Chain<T>(params sources)` | Runs the sources one after another, each starting when the one before it completes. | `Observable.Concat` |
| `Signal.Concat<T>(params sources)`, `Signal.Concat<T>(IEnumerable sources)` | Another name for `Chain`. | `Observable.Concat` |
| `Signal.Race<T>(params sources)` | Subscribes to every source and keeps the first one to react. | `Observable.Amb` |
| `Signal.Switch<T>(sources)` | Follows only the newest inner signal. | `Observable.Switch` |
| `Signal.Pair<TLeft, TRight, TResult>(left, right, selector)` | Joins values by position: first with first, second with second. | `Observable.Zip` |
| `Signal.SyncLatest<TLeft, TRight, TResult>(left, right, selector)` | Combines the latest value from each side whenever either one fires. | `Observable.CombineLatest` |
| `Signal.PairLatest<TLeft, TRight, TResult>(left, right, selector)` | Another name for the static `SyncLatest`. | `CombineLatest` |
| `Signal.ForkJoin<TLeft, TRight, TResult>(left, right, selector)` | Waits for both sides to complete, then emits one result built from their last values. | `Observable.ForkJoin` |
| `Signal.OnErrorResumeNext<T>(first, second)`, and the params and collection overloads | Runs the sources in order and ignores an error from any of them. | `Observable.OnErrorResumeNext` |
| `Signal.Recover<TSource>(params sources)` | Tries each source in turn and moves to the next one only when a source fails. | `Observable.Catch(params)` |
| `Signal.Scheduled<T>(sequencer)` | Creates a hub that delivers its values on a sequencer. | `Subject` + `ObserveOn` |
| `Signal.Scheduled<T>(sequencer, defaultObserver)` | Creates the same hub and names an observer to receive values when nobody else subscribes. | - |
| `Signal.Serialized<T>()`, `Signal.Serialized<T>(signal)` | Creates a hub that takes calls from any thread and delivers them one at a time. | `Subject.Synchronize` |
| `Signal.Delayable<T>(isDelayed, flushDistinct)` | Creates a hub that holds values back while it is delayed and emits one batch without duplicates when you call `Flush()`. | - |
| `TaskSignal.Create<TResult>(observableFactory)`, and its sequencer and `cts` overloads | Creates a task-backed signal whose source is built from the signal itself. | - |
| `IEnumerable<T>.ToSignal()`, `IEnumerable<T>.ToSignal(cancellationToken)` | Turns a collection into a signal that emits each item, then completes. | `ToObservable()` |
| `IEnumerable<T>.ToObservable()`, and the sequencer and token overloads | Another name for `ToSignal`. | `ToObservable()` |
| `Task<T>.ToSignal()` | Turns a task into a signal that emits its result. | `ToObservable()` |
| `Task<T>.ToObservable()` | Another name for `Task<T>.ToSignal`. | `ToObservable()` |
| `Observables.Return<T>(value)` | Emits one value inside the `Subscribe` call itself. | `Observable.Return(value, ImmediateScheduler)` |

`Emit` gives you a signal with one value in it.

```csharp
Signal.Emit(42).Subscribe(value => Console.WriteLine(value));
// prints: 42
```

`None` completes without giving you a value.

```csharp
Signal.None<string>().Subscribe(
    value => Console.WriteLine(value),
    () => Console.WriteLine("done"));
// prints: done
```

`Fail` hands every subscriber an error.

```csharp
Signal.Fail<int>(new InvalidOperationException("no rows"))
    .Subscribe(
        value => Console.WriteLine(value),
        error => Console.WriteLine(error.Message));
// prints: no rows
```

`Sequence` counts up from a starting number.

```csharp
Signal.Sequence(5, 3).Subscribe(number => Console.Write(number + " "));
// prints: 5 6 7
```

`ToSignal` turns any collection into a signal.

```csharp
string[] names = ["ana", "bo", "cy"];
names.ToSignal().Subscribe(name => Console.Write(name + " "));
// prints: ana bo cy
```

`Create` lets you push values yourself. Return a disposable that cleans up when the subscriber leaves.

```csharp
var countdown = Signal.Create<int>(observer =>
{
    observer.OnNext(3);
    observer.OnNext(2);
    observer.OnNext(1);
    observer.OnCompleted();
    return EmptyDisposable.Instance;
});

countdown.Subscribe(number => Console.Write(number + " "));
// prints: 3 2 1
```

`Lazy` builds the source at subscribe time, so each subscriber gets its own.

```csharp
var stamp = Signal.Lazy(() => Signal.Emit(DateTime.UtcNow));

stamp.Subscribe(time => Console.WriteLine(time));
stamp.Subscribe(time => Console.WriteLine(time));
// prints: two different times, one per subscriber
```

`FromAsync` starts a task for each subscriber.

```csharp
var page = Signal.FromAsync(async token =>
{
    using var client = new HttpClient();
    return await client.GetStringAsync("https://example.com", token);
});

page.Subscribe(body => Console.WriteLine(body.Length));
// prints: the number of characters in the page
```

`FromEventPattern` turns a .NET event into a signal. Each value carries the sender and the event arguments.

```csharp
// chat.MessageReceived is an event EventHandler<MessageEventArgs>
Signal.FromEventPattern<MessageEventArgs>(
        handler => chat.MessageReceived += handler,
        handler => chat.MessageReceived -= handler)
    .Subscribe(pattern => Console.WriteLine(pattern.EventArgs.Text));
// prints: the text of each message as it arrives
```

`After` fires once, later.

```csharp
Signal.After(TimeSpan.FromSeconds(2))
    .Subscribe(tick => Console.WriteLine("two seconds later"));
// prints: two seconds later
```

`Every` fires over and over on a timer.

```csharp
Signal.Every(TimeSpan.FromSeconds(1))
    .Take(3)
    .Subscribe(count => Console.Write(count + " "));
// prints: 0 1 2
```

### Transformation

These change each value into something else.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Map(selector)` | Runs your function on each value and emits the result. | `Select` |
| `MapIndexed(selector)` | Runs your function on each value and its position, counting from zero. | `Select` (indexed) |
| `MapWith(state, selector)` | Runs your function on each value and threads a state object through, so your lambda can be `static`. | - |
| `Select(selector)` | Another name for `Map`. | `Select` |
| `Select(selector with index)` | Another name for `MapIndexed`. | `Select` |
| `SelectWith(state, selector)` | Another name for `MapWith`. | - |
| `Spark()` | Turns values, errors and completion into `Spark<T>` records you can read as plain data. | `Materialize` |
| `Materialize()` | Another name for `Spark`. | `Materialize` |
| `Unspark()` | Turns `Spark<T>` records back into real values, errors and completion. | `Dematerialize` |
| `Dematerialize()` | Another name for `Unspark`. | `Dematerialize` |
| `CastTo<TResult>()` | Casts each value to the type you name and fails when one does not fit. | `Cast` |
| `Cast<TResult>()` | Another name for `CastTo`. | `Cast` |
| `Bind(selector)` | Turns each value into an inner signal and passes along everything those signals emit. | `SelectMany` |
| `FlatMap(selector)` | Another name for `Bind`. | `SelectMany` |
| `FlatMap(collectionSelector, resultSelector)` | Flattens the inner signals and pairs each inner value with the value it came from. | `SelectMany` |
| `FlatMapValues(selector)` | Turns each value into a plain collection and emits every item. | `SelectMany` (enumerable) |
| `SelectMany(selector)` | Another name for `FlatMap`. | `SelectMany` |
| `SelectMany(other)` | Replaces each value with the same inner signal and merges the results. | `SelectMany` |
| `SelectMany(enumerable selector)` | Another name for `FlatMapValues`. | `SelectMany` |
| `SelectMany(collectionSelector, resultSelector)` | Another name for the two-selector `FlatMap`. | `SelectMany` |
| `SwitchMap(selector)` | Turns each value into an inner signal and follows only the newest one. | `Select(...).Switch()` |
| `SwitchSelect(selector)` | Works like `SwitchMap` and skips null source values instead of switching away. | `WhereNotNull().Select(...).Switch()` |
| `Choose(chooser)` | Maps and filters in one pass, and emits `Value` only when `HasValue` is true. | `Select(...).Where(...)` |
| `SwitchTo()` | Follows only the newest inner signal. | `Switch` |
| `Switch()` | Another name for `SwitchTo`. | `Switch` |
| `Timestamp()`, `Timestamp(sequencer)` | Attaches the clock's current time to each value as a `Moment<T>`. | `Timestamp` |
| `TimeInterval()`, `TimeInterval(sequencer)` | Attaches the gap since the value before it. | `TimeInterval` |

`Map` changes every value.

```csharp
Signal.Sequence(1, 4)
    .Map(number => number * 10)
    .Subscribe(value => Console.Write(value + " "));
// prints: 10 20 30 40
```

`MapIndexed` gives you the position too.

```csharp
new[] { "red", "green", "blue" }.ToSignal()
    .MapIndexed((colour, index) => $"{index}:{colour}")
    .Subscribe(line => Console.Write(line + " "));
// prints: 0:red 1:green 2:blue
```

`FlatMap` turns one value into many.

```csharp
new[] { 1, 2 }.ToSignal()
    .FlatMap(number => new[] { number, number * 100 }.ToSignal())
    .Subscribe(value => Console.Write(value + " "));
// prints: 1 100 2 200
```

`SwitchMap` drops the old inner signal as soon as a new value arrives. Use it for searches and lookups.

```csharp
searchTerms
    .SwitchMap(term => SearchAsync(term).ToSignal())
    .Subscribe(results => Console.WriteLine(results.Count));
// prints: results for the newest term only; older searches are dropped
```

`SwitchTo` does the same when you hold a signal of signals.

```csharp
var pages = new[] { Signal.Sequence(1, 2), Signal.Sequence(10, 2) }.ToSignal();

pages.SwitchTo().Subscribe(value => Console.Write(value + " "));
// prints: 1 2 10 11
```

### Filtering

These decide which values get through.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Keep(predicate)` | Passes along only the values your test accepts. | `Where` |
| `KeepWith(state, predicate)` | Works like `Keep` and threads a state object through, so your test can be `static`. | - |
| `Where(predicate)` | Another name for `Keep`. | `Where` |
| `WhereWith(state, predicate)` | Another name for `KeepWith`. | - |
| `KeepNotNull()` | Drops nulls and hands back a stream that cannot be null. | `Where(x => x != null)` |
| `WhereNotNull()` | Another name for `KeepNotNull`. | `WhereNotNull` |
| `KeepType<TResult>()` | Keeps only the values of the type you name. | `OfType` |
| `OfType<TResult>()` | Another name for `KeepType`. | `OfType` |
| `Take(count)` | Emits at most `count` values, then completes. | `Take` |
| `Skip(count)` | Drops the first `count` values. | `Skip` |
| `TakeWhile(predicate)` | Emits values while your test holds, then completes. | `TakeWhile` |
| `SkipWhile(predicate)` | Drops the leading values while your test holds, then passes the rest along. | `SkipWhile` |
| `TakeUntil(other)` | Passes values along until `other` emits anything; `other` completing does not stop it. | `TakeUntil` |
| `TakeUntil(cancellationToken)` | Passes values along until the token cancels, then completes. | `TakeUntil` |
| `Distinct()`, `Distinct(comparer)` | Drops a value you have seen anywhere earlier in the stream. | `Distinct` |
| `DistinctBy(keySelector)`, `DistinctBy(keySelector, comparer)` | Keeps only the first value for each key. | `DistinctBy` |
| `Unique()`, `Unique(comparer)` | Drops a value only when it matches the one right before it. | `DistinctUntilChanged` |
| `UniqueBy(keySelector)`, `UniqueBy(keySelector, comparer)` | Drops a value when its key matches the key right before it. | `DistinctUntilChangedBy` |
| `DistinctUntilChanged()`, `DistinctUntilChanged(comparer)` | Another name for `Unique`. | `DistinctUntilChanged` |
| `DistinctUntilChangedBy(keySelector)`, `DistinctUntilChangedBy(keySelector, comparer)` | Another name for `UniqueBy`. | `DistinctUntilChangedBy` |
| `IgnoreValues()` | Drops every value and passes along only completion or an error. | `IgnoreElements` |
| `IgnoreElements()` | Another name for `IgnoreValues`. | `IgnoreElements` |
| `DefaultIfEmpty()` | Emits `default` when the source completes without a value. | `DefaultIfEmpty` |
| `DefaultIfEmpty(defaultValue)` | Emits your fallback when the source completes without a value. | `DefaultIfEmpty` |

`Keep` passes along the values your test accepts.

```csharp
Signal.Sequence(1, 6)
    .Keep(number => number % 2 == 0)
    .Subscribe(value => Console.Write(value + " "));
// prints: 2 4 6
```

`KeepNotNull` removes nulls and fixes the type for you.

```csharp
new string?[] { "a", null, "b" }.ToSignal()
    .KeepNotNull()
    .Subscribe(text => Console.Write(text + " "));
// prints: a b
```

`Take` stops after a set number of values.

```csharp
Signal.Sequence(1, 100)
    .Take(3)
    .Subscribe(value => Console.Write(value + " "));
// prints: 1 2 3
```

`TakeUntil` stops on a token or another signal.

```csharp
using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(4));

Signal.Every(TimeSpan.FromSeconds(1))
    .TakeUntil(cancel.Token)
    .Subscribe(count => Console.Write(count + " "));
// prints: 0 1 2 then completes
```

`Unique` drops repeats that sit next to each other.

```csharp
new[] { 1, 1, 2, 2, 1 }.ToSignal()
    .Unique()
    .Subscribe(value => Console.Write(value + " "));
// prints: 1 2 1
```

### Combination

These put two or more signals together.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Lead(value)` | Emits one value before the source's own values. | `StartWith` / `Prepend` |
| `Prepend(value)` | Another name for `Lead`. | `Prepend` |
| `Prepend(params values)`, `Prepend(IEnumerable values)` | Emits several values before the source's own values. | `Prepend` |
| `StartWith(params values)`, `StartWith(IEnumerable values)` | Another name for the multi-value `Prepend`. | `StartWith` |
| `Append(value)` | Emits one extra value after the source completes. | `Append` |
| `Chain(second)` | Runs `second` after this signal completes. | `Concat` |
| `Concat(second)` | Another name for `Chain`. | `Concat` |
| `Chain()` on a signal of signals | Runs the inner signals one at a time, in the order they arrive. | `Concat` |
| `Concat()` on a signal of signals | Another name for the row above. | `Concat` |
| `Chain()` on a signal of tasks | Awaits each task in order and emits its result. | `Concat` |
| `Concat()` on a signal of tasks | Another name for the row above. | `Concat` |
| `Blend()` on a signal of signals | Subscribes to every inner signal at once and passes values along as they arrive. | `Merge` |
| `Merge()` on a signal of signals | Another name for `Blend`. | `Merge` |
| `Merge(second)` | Runs this signal and one other at the same time. | `Merge` |
| `Blend()` on a collection of signals | Runs a whole collection of signals at the same time. | `Merge` |
| `Blend(maxConcurrent)` on a collection of signals | Runs the same collection with a cap on how many run at once. | `Merge(maxConcurrent)` |
| `Merge()`, `Merge(maxConcurrent)` on a collection of signals | Another name for the two rows above. | `Merge` |
| `LinqExtensions.BlendUnique(params sources)`, `LinqExtensions.BlendUnique(sources, comparer)` | Merges the sources and drops a value that matches the one passed along before it. | `Merge().DistinctUntilChanged()` |
| `Race()` on a signal of signals | Keeps the inner signal that reacts first and drops the rest. | `Amb` |
| `Amb()` on a signal of signals | Another name for `Race`. | `Amb` |
| `Pair(right, selector)` | Joins values by position: first with first, second with second. | `Zip` |
| `Zip(right, selector)` | Another name for `Pair`. | `Zip` |
| `SyncLatest(right, selector)` | Combines the latest value from each side whenever either one fires, once both have produced a value. | `CombineLatest` |
| `PairLatest(right, selector)` | Another name for the two-source `SyncLatest`. | `CombineLatest` |
| `FuseLatest(right, selector)` | A third name for the two-source `SyncLatest`. | `CombineLatest` |
| `CombineLatest(right, selector)` | A fourth name for the two-source `SyncLatest`. | `CombineLatest` |
| `SyncLatest(...)` for 3 to 16 sources, 14 overloads | Combines the latest value from 3 to 16 signals through your selector. | `CombineLatest` |
| `CombineLatest(...)` for 3 to 16 sources, 14 overloads | Another name for the row above. | `CombineLatest` |
| `CombineLatest(source2)` through `CombineLatest(source2, ..., source16)`, 15 overloads | Combines the latest values and hands you a named tuple, so you write no selector. | `CombineLatest` (tuple) |
| `CombineLatest()` on a collection of signals | Combines a collection of same-typed signals into one list per notification. | `CombineLatest` |
| `CombineLatest(resultSelector)` on a collection of signals | Does the same and runs your function on the list. | `CombineLatest` |
| `LinqExtensions.CombineLatest(new[] { a, b })` | Array form of the collection combine. Pass an array: listing the sources one by one binds to the tuple overload and gives you a tuple, not a list. | `CombineLatest` |
| `Latch(right, selector)` | Emits once per left value and attaches whatever the right side produced last. | `WithLatestFrom` |
| `WithLatestFrom(right, selector)` | Another name for `Latch`. | `WithLatestFrom` |
| `ForkJoin(right, selector)` | Waits for both sides to complete, then emits one result from their final values. | `ForkJoin` |

`Lead` puts a starting value in front.

```csharp
Signal.Sequence(2, 2)
    .Lead(0)
    .Subscribe(value => Console.Write(value + " "));
// prints: 0 2 3
```

`Append` adds a value at the end.

```csharp
Signal.Sequence(1, 2)
    .Append(99)
    .Subscribe(value => Console.Write(value + " "));
// prints: 1 2 99
```

`Chain` runs the second signal after the first one finishes.

```csharp
Signal.Sequence(1, 2)
    .Chain(Signal.Sequence(10, 2))
    .Subscribe(value => Console.Write(value + " "));
// prints: 1 2 10 11
```

`Blend` runs signals at the same time and passes values along as they arrive.

```csharp
var fast = Signal.Every(TimeSpan.FromMilliseconds(100)).Map(_ => "fast");
var slow = Signal.Every(TimeSpan.FromMilliseconds(250)).Map(_ => "slow");

Signal.Blend(fast, slow).Take(4).Subscribe(label => Console.Write(label + " "));
// prints: fast fast slow fast
```

`Pair` lines values up by position.

```csharp
new[] { "a", "b" }.ToSignal()
    .Pair(Signal.Sequence(1, 2), (letter, number) => letter + number)
    .Subscribe(text => Console.Write(text + " "));
// prints: a1 b2
```

`SyncLatest` fires whenever either side changes, using the newest value from both.

```csharp
var names = new[] { "ana" }.ToSignal();
var ages = new[] { 30, 31 }.ToSignal();

names.SyncLatest(ages, (who, years) => $"{who} is {years}")
    .Subscribe(line => Console.WriteLine(line));
// prints: ana is 30
//         ana is 31
```

`Latch` fires only on the left side and reads the right side's newest value.

```csharp
var clicks = Signal.Every(TimeSpan.FromSeconds(1));
var temperatures = Signal.Every(TimeSpan.FromMilliseconds(200)).Map(tick => 20 + tick);

clicks.Latch(temperatures, (_, reading) => reading)
    .Take(2)
    .Subscribe(reading => Console.WriteLine(reading));
// prints: the newest temperature at each click
```

### Time

These change when values arrive.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Shift(dueTime)`, `Shift(dueTime, sequencer)` | Holds every notification back by a fixed delay. | `Delay` |
| `Delay(dueTime)`, `Delay(dueTime, sequencer)` | Another name for `Shift`. | `Delay` |
| `Delay(DateTimeOffset)`, `Delay(DateTimeOffset, sequencer)` | Holds notifications until the time you name. | `Delay` |
| `DelayStart(dueTime)`, `DelayStart(dueTime, sequencer)` | Waits before it subscribes to the source at all. | `DelaySubscription` |
| `DelaySubscription(dueTime)`, `DelaySubscription(dueTime, sequencer)` | Another name for `DelayStart`. | `DelaySubscription` |
| `DelaySubscription(DateTimeOffset)`, `DelaySubscription(DateTimeOffset, sequencer)` | Subscribes at the time you name. | `DelaySubscription` |
| `Calm(dueTime)`, `Calm(dueTime, sequencer)` | Emits a value once nothing newer has arrived for the quiet period. | `Throttle` |
| `Stabilize(dueTime)`, `Stabilize(dueTime, sequencer)` | Another name for `Calm`. | `Throttle` |
| `Throttle(dueTime)`, `Throttle(dueTime, sequencer)` | A third name for `Calm`. | `Throttle` |
| `EmitIfQuiet(dueTime)`, `EmitIfQuiet(dueTime, sequencer)` | Works like `Calm` and hands back the source unchanged when `dueTime` is zero or less. | `Throttle` |
| `Probe(period)`, `Probe(period, sequencer)` | Emits the latest value once the period has passed since the value that started the timer. A quiet source sends nothing, a steady source drifts away from a fixed schedule, and a value still waiting when the source completes is dropped. Use `Calm` when you need that last value. | `Sample` |
| `Sample(interval)`, `Sample(interval, sequencer)` | Another name for `Probe`. | `Sample` |
| `Buffer(timeSpan)`, `Buffer(timeSpan, sequencer)` | Gathers values into one batch per time window. | `Buffer` |
| `Buffer(count)` | Gathers values into batches of a fixed size that do not overlap. | `Buffer` |
| `Buffer(count, skip)` | Opens a fixed-size batch every `skip` values, so batches can overlap. | `Buffer` |
| `Collect(timeSpan)`, `Collect(timeSpan, sequencer)` | Another name for the time-window `Buffer`. | `Buffer(TimeSpan)` |
| `Expire(dueTime)`, `Expire(dueTime, sequencer)` | Fails with `TimeoutException` when no value arrives within the time you give. Each value restarts the clock, so a busy source never fails. | `Timeout` |
| `Signal.Expire(source, dueTime)`, `Signal.Expire(source, dueTime, sequencer)` | Static form of `Expire`. | `Timeout` |
| `Timeout(dueTime)` and its sequencer overload | Another name for `Expire`. Each value restarts the clock. | `Timeout` |
| `Timeout(DateTimeOffset)` and its sequencer overload | Fails with `TimeoutException` if the sequence has not finished by that moment, whatever values arrive first. | `Timeout` |
| `Signal.Timeout(source, dueTime)`, `Signal.Timeout(source, dueTime, sequencer)` | Static form of `Timeout`. | `Timeout` |

`Shift` moves everything later by a fixed amount.

```csharp
Signal.Emit("late")
    .Shift(TimeSpan.FromSeconds(1))
    .Subscribe(text => Console.WriteLine(text));
// prints: late, one second after you subscribe
```

`Calm` waits for quiet. Use it on a text box so you search once the typing stops.

```csharp
keystrokes
    .Calm(TimeSpan.FromMilliseconds(300))
    .Subscribe(term => Console.WriteLine(term));
// prints: the search term once the user stops typing for 300 milliseconds
```

`Probe` takes the newest value on a schedule and ignores the rest.

```csharp
Signal.Every(TimeSpan.FromMilliseconds(100))
    .Probe(TimeSpan.FromMilliseconds(500))
    .Take(2)
    .Subscribe(count => Console.Write(count + " "));
// prints: 4 9
```

`Buffer` gathers values into batches.

```csharp
Signal.Sequence(1, 7)
    .Buffer(3)
    .Subscribe(batch => Console.Write($"[{string.Join(",", batch)}] "));
// prints: [1,2,3] [4,5,6] [7]
```

`Expire` fails when the source takes too long.

```csharp
slowRequest
    .Expire(TimeSpan.FromSeconds(5))
    .Subscribe(
        result => Console.WriteLine(result),
        error => Console.WriteLine(error.GetType().Name));
// prints: TimeoutException when the request takes more than 5 seconds
```

### Error handling

These decide what happens when a signal fails.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Recover(handler)` | Switches to the replacement signal your handler builds when an error arrives. | `Catch` |
| `Rescue(handler)` | Another name for `Recover`. | `Catch` |
| `Recover<TException>(handler)` | Handles only the one exception type you name. | `Catch<TException>` |
| `Catch<TException>(handler)` | Another name for the typed `Recover`. | `Catch` |
| `Recover()` on a collection of signals | Tries each source in turn until one finishes without an error. | `Catch(params)` |
| `Resume(fallback)` | Carries on with your fallback signal after an error. | `OnErrorResumeNext` |
| `OnErrorResumeNext(second)` | Carries on with `second` when this signal completes or fails. Unlike `Resume`, it also moves on after a clean completion. | `OnErrorResumeNext` |
| `Reattempt(retryCount)` | Subscribes again after an error, up to the number of extra tries you allow, then passes the last error along. The count is extra tries, so `Reattempt(2)` subscribes three times. | `Retry` |
| `Retry(retryCount)` | Another name for `Reattempt`. | `Retry` |
| `Repeat()` | Subscribes again each time the source completes, forever. | `Repeat` |
| `Repeat(repeatCount)` | Runs the source the number of times you name in total, starting each run when the one before it completes. `Repeat(3)` runs it three times, not four. | `Repeat` |
| `Finally(finallyAction)` | Runs your cleanup action once when the subscription ends, whatever ends it. | `Finally` |
| `OnCleanup(finallyAction)` | Another name for `Finally`. | `Finally` |
| `Exception.Throw()` | Throws the exception and keeps its stack trace on older frameworks. | - |
| `Exception?.Rethrow()` | Throws the exception when there is one, and does nothing when it is null. | - |

`Recover` swaps in another signal when something fails.

```csharp
Signal.Fail<string>(new HttpRequestException("offline"))
    .Recover(error => Signal.Emit("cached value"))
    .Subscribe(text => Console.WriteLine(text));
// prints: cached value
```

`Resume` carries straight on with a fallback.

```csharp
Signal.Fail<int>(new InvalidOperationException())
    .Resume(Signal.Sequence(1, 2))
    .Subscribe(value => Console.Write(value + " "));
// prints: 1 2
```

`Reattempt` tries the whole source again.

```csharp
var attempts = 0;

Signal.Lazy(() => ++attempts < 3
        ? Signal.Fail<string>(new TimeoutException())
        : Signal.Emit("ok"))
    .Reattempt(3)
    .Subscribe(text => Console.WriteLine($"{text} after {attempts} tries"));
// prints: ok after 3 tries
```

`Finally` runs your cleanup however the subscription ends.

```csharp
Signal.Sequence(1, 2)
    .Finally(() => Console.WriteLine("cleaned up"))
    .Subscribe(value => Console.Write(value + " "));
// prints: 1 2 cleaned up
```

### Aggregation and terminal operations

These boil a whole signal down to one answer. The first group hands you another signal. The second group hands you a
task or a plain value, so you leave signals behind.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Fold(seed, accumulator)` | Emits the running total after every value. | `Scan` |
| `Scan(seed, accumulator)` | Another name for `Fold`. | `Scan` |
| `Reduce(seed, accumulator)` | Emits one final total when the source completes. | `Aggregate` |
| `Aggregate(seed, accumulator)` | Another name for `Reduce`. | `Aggregate` |
| `Count()`, `Count(predicate)` | Emits how many values arrived, or how many matched. | `Count` |
| `LongCount()`, `LongCount(predicate)` | Does the same as a 64-bit number. | `LongCount` |
| `Any()`, `Any(predicate)` | Emits whether at least one value arrived, or matched. | `Any` |
| `All(predicate)` | Emits whether every value matched. | `All` |
| `Contains(value)`, `Contains(value, comparer)` | Emits whether that value ever arrived. | `Contains` |
| `IsEmpty()` | Emits whether the source completed without a value. | `IsEmpty` |
| `CollectList()` | Gathers every value and emits one list when the source completes. | `ToList` |
| `CollectArray()` | Does the same and emits an array. | `ToArray` |
| `ToList()` | Another name for `CollectList`. | `ToList` |
| `ToArray()` | Another name for `CollectArray`. | `ToArray` |
| `FirstAsync()`, `FirstAsync(cancellationToken)` | Returns a task for the first value, and fails when the source is empty. | `FirstAsync().ToTask()` |
| `FirstOrDefaultAsync()` and its token, default-value and combined overloads | Returns a task for the first value, or your fallback when the source is empty. | `FirstOrDefaultAsync` |
| `LastAsync()`, `LastAsync(cancellationToken)` | Returns a task for the final value. | `LastAsync().ToTask()` |
| `LastOrDefaultAsync()` and its token, default-value and combined overloads | Returns a task for the final value, or your fallback when the source is empty. | `LastOrDefaultAsync` |
| `ToTask()`, `ToTask(cancellationToken)` | Returns a task that gives you the last value once the source completes. | `ToTask()` |
| `Signal.ToTask(source)`, `Signal.ToTask(source, cancellationToken)` | Static form of `ToTask`. | `ToTask` |
| `CountAsync()` and its predicate and token overloads | Returns a task for the value count. | `Count().ToTask()` |
| `AnyAsync()` and its predicate and token overloads | Returns a task for whether anything arrived or matched. | `Any().ToTask()` |
| `CollectArrayAsync()` | Returns a task for every value as an array. | `ToArray().ToTask()` |
| `CollectListAsync()` | Returns a task for every value as a list. | `ToList().ToTask()` |
| `ToArrayAsync()` | Another name for `CollectArrayAsync`. | - |
| `ToListAsync()` | Another name for `CollectListAsync`. | - |
| `ToEnumerable()` | Blocks the calling thread until the source completes, then hands back the values. | `ToEnumerable` |
| `GetAwaiter()`, `GetAwaiter(cancellationToken)` | Lets you `await` the signal for its last value. | `GetAwaiter` |
| `Signal.RunAsync(source)`, `Signal.RunAsync(source, cancellationToken)` | Subscribes at once and returns an awaiter for the final value. | `RunAsync` |
| `HandleCancellation(token)`, `HandleCancellation(action, token)` | Awaits the final value and returns `default` instead of failing when the token cancels. | - |
| `FirstAsTaskHelper.FirstAsTask(source)` | Returns a task for the first value. | `FirstAsync().ToTask()` |
| `FirstAsValueTaskHelper<T>.FirstAsValueTask(source)` | Does the same as a `ValueTask<T>` that allocates less. | - |

`Fold` shows the running total as it grows.

```csharp
Signal.Sequence(1, 4)
    .Fold(0, (total, number) => total + number)
    .Subscribe(total => Console.Write(total + " "));
// prints: 1 3 6 10
```

`Reduce` gives you the total once, at the end.

```csharp
Signal.Sequence(1, 4)
    .Reduce(0, (total, number) => total + number)
    .Subscribe(total => Console.WriteLine(total));
// prints: 10
```

`Count` tells you how many values matched.

```csharp
new[] { "a", "bb", "ccc" }.ToSignal()
    .Count(text => text.Length > 1)
    .Subscribe(count => Console.WriteLine(count));
// prints: 2
```

`CollectList` gathers everything into one list.

```csharp
Signal.Sequence(1, 3)
    .CollectList()
    .Subscribe(list => Console.WriteLine(string.Join(",", list)));
// prints: 1,2,3
```

`FirstAsync` gives you a task for the first value.

```csharp
var first = await Signal.Sequence(7, 3).FirstAsync();
Console.WriteLine(first);
// prints: 7
```

You can `await` a signal directly for its last value.

```csharp
var last = await Signal.Sequence(1, 5);
Console.WriteLine(last);
// prints: 5
```

### Utility

These cover subscribing, choosing threads, watching values go past, cleaning up, and sharing one subscription between
several subscribers.

A connectable signal waits for a `Connect()` call before it subscribes to its source. That lets several subscribers
share one subscription instead of starting the work over each time.

| Operator | What it does | LINQ / System.Reactive name |
|---|---|---|
| `Subscribe()` | Starts the signal and ignores its values. | `Subscribe()` |
| `Subscribe(onNext)` | Runs your callback for each value, and rethrows a terminal error to the producer. | `Subscribe` |
| `Subscribe(onNext, onCompleted)` | Adds a callback for completion. | `Subscribe` |
| `Subscribe(onNext, onError)` | Adds a callback for errors. | `Subscribe` |
| `Subscribe(onNext, onError, onCompleted)` | Takes all three callbacks. | `Subscribe` |
| `SubscribePrimitives()` and its 4 overloads | Gives you the same five methods under a name that cannot clash with another library. | `Subscribe` |
| `SubscribeSafe(observer)` | Subscribes and keeps your observer's exceptions away from the producer. | `SubscribeSafe` |
| `SubscribeSafePrimitives(observer)` | Gives you the same method under a name that cannot clash. | `SubscribeSafe` |
| `SubscribeSafe(onNext, onError)`, `SubscribeSafe(onNext, onError, onCompleted)`, `SubscribeSafe(onError)`, `SubscribeSafe(onError, onCompleted)` | Callback forms of `SubscribeSafe`. | `SubscribeSafe` |
| `LinqExtensions.SubscribeSafe(source, ...)`, 14 static overloads | Lets a nullable source pick one overload without ambiguity; the `params` array is a marker and is never read. | `SubscribeSafe` |
| `IObserver<T>.FastForEach(source)` | Pushes a whole collection into an observer and indexes arrays and lists directly. | - |
| `ObserveOn(sequencer)` | Delivers notifications to subscribers on the sequencer you name. | `ObserveOn` |
| `WitnessOn(sequencer)` | Another name for `ObserveOn`. | `ObserveOn` |
| `SubscribeOn(sequencer)` | Runs the subscription itself on the sequencer you name. | `SubscribeOn` |
| `Synchronize()`, `Synchronize(object gate)` | Delivers notifications one at a time behind a lock, which you can share with other signals. | `Synchronize` |
| `Synchronize(Lock gate)` | The same, taking a `System.Threading.Lock`. Available on net9.0 and later only. | `Synchronize` |
| `Serialize()` | Delivers notifications one at a time and holds no lock while your code runs, so a late arrival queues instead of blocking. | `Synchronize` (deadlock-safe) |
| `Tap(onNext)` | Runs your action for each value and passes the value through unchanged. | `Do` |
| `Tap(onNext, onError, onCompleted)` | Does the same and hooks errors and completion too. | `Do` |
| `TapWith(state, onNext)` | Works like `Tap` and threads a state object through, so your lambda can be `static`. | `Do` |
| `Do(onNext)` and its 3 overloads | Another name for `Tap`. | `Do` |
| `DoWith(state, onNext)` | Another name for `TapWith`. | `Do` |
| `AsObservable()` | Wraps the signal in a read-only view, so a caller cannot cast it back and push values in. | `AsObservable` |
| `ToSignal()` on a signal | Hands back the same signal after a null check. | `AsObservable` |
| `IDisposable.DisposeWith()` | Wraps a disposable so the wrapper disposes it exactly once. | - |
| `IDisposable.DisposeWith(action)` | Does the same and runs your action just before disposal. | - |
| `T.DisposeWith(MultipleDisposable)` | Hands the disposable to a group and returns it, so you chain it onto the line that creates it. | `DisposeWith` |
| `ShareLive()` | Wraps the source in a connectable signal that starts when you call `Connect()`. | `Publish` |
| `Publish()` | Another name for `ShareLive`. | `Publish` |
| `Share()` | A third name for `ShareLive`. | `Publish` |
| `Publish(selector)` | Shares the source for the length of one expression, so it is subscribed once. | `Publish(selector)` |
| `ReplayLive()` | Wraps the source in a connectable signal that replays every past value to a late subscriber. | `Replay` |
| `ReplayLive(bufferSize)` | Replays the last few values, up to the size you name. | `Replay(bufferSize)` |
| `ReplayLive(bufferSize, window)` | Replays values limited by both count and age. | `Replay(n, window)` |
| `Replay()`, `Replay(bufferSize)`, `Replay(bufferSize, window)` | Another name for the three `ReplayLive` overloads. | `Replay` |
| `Multicast(hub)` | Pushes the source through a hub you supply. | `Multicast` |
| `AutoShare()` | Connects on the first subscriber and disconnects when the last one leaves. | `RefCount` |
| `RefCount()` | Another name for `AutoShare`. | `RefCount` |
| `AutoConnect()` | Connects on the first subscriber and never disconnects. | `AutoConnect` |
| `AutoConnect(subscriberCount)` | Waits for that many subscribers, then connects. | `AutoConnect(n)` |
| `AutoConnect(subscriberCount, onConnect)` | Does the same and hands you the connection to dispose. | `AutoConnect(n, onConnect)` |
| `ShareLatest()` | Shares one live subscription for as long as anyone listens, and drops it when the last one leaves. It does not replay: a late subscriber sees nothing until the next value, despite the name. | `Publish().RefCount()` |
| `ToReadOnlyState(initialValue, selector)` | Turns a signal into an object with a current `Value` and a `Changed` signal. `Changed` sends the current value when you subscribe, then once per source value, including when your selector returns the same value again. | `ToProperty`, which notifies only on a real change |

`Subscribe` starts the signal. Dispose the result to stop listening.

```csharp
using var subscription = Signal.Sequence(1, 3).Subscribe(
    value => Console.Write(value + " "),
    error => Console.WriteLine(error.Message),
    () => Console.WriteLine("done"));
// prints: 1 2 3 done
```

`Tap` watches values go past without changing them. It is handy for logging.

```csharp
Signal.Sequence(1, 3)
    .Tap(value => Console.Write($"saw {value} "))
    .Keep(value => value > 1)
    .Subscribe(value => Console.Write($"kept {value} "));
// prints: saw 1 saw 2 kept 2 saw 3 kept 3
```

`ObserveOn` moves delivery onto the thread you want.

```csharp
downloads
    .ObserveOn(uiSequencer)
    .Subscribe(file => label.Text = file.Name);
// the label is set on the UI thread
```

`DisposeWith` collects subscriptions so you can stop them together.

```csharp
var subscriptions = new MultipleDisposable();

Signal.Every(TimeSpan.FromSeconds(1))
    .Subscribe(tick => Console.WriteLine(tick))
    .DisposeWith(subscriptions);

subscriptions.Dispose();
// stops every subscription in the group at once
```

`ShareLatest` gives two subscribers one shared source instead of two.

```csharp
var shared = Signal.Every(TimeSpan.FromSeconds(1)).ShareLatest();

shared.Subscribe(tick => Console.WriteLine("a " + tick));
shared.Subscribe(tick => Console.WriteLine("b " + tick));
// both subscribers see the same ticks from one timer
```

`ToReadOnlyState` gives you a value you can read at any moment, plus a signal of changes.

```csharp
using var name = people.ToReadOnlyState("unknown", person => person.Name);

Console.WriteLine(name.Value);
name.Changed.Subscribe(value => Console.WriteLine(value));
// prints: unknown, then each new name as it changes
```

## Extension helpers

`ReactiveUI.Primitives` ships a large set of extension helpers on top of the core operators. They live on a static class
called `ReactiveExtensions`, they sit in the namespace `ReactiveUI.Primitives.Extensions`, and they ship inside the main
`ReactiveUI.Primitives` package, so you get them with no extra reference.

A few words come up in every table. A **source** is a sequence of values you subscribe to. A **sequencer** decides which
thread runs your code; you pass one in when you care where the work lands. A **subscription** is the `IDisposable` you
get back when you subscribe; dispose it to stop listening. `RxVoid` is the stand-in for a value that carries no
information.

To use these helpers, add:

```csharp
using ReactiveUI.Primitives;            // Subscribe(onNext)
using ReactiveUI.Primitives.Extensions; // the helpers below
using ReactiveUI.Primitives.Signals;    // Signal<T>
```

### Creation

These turn something that is not a sequence into one.

| Member | What it does |
|---|---|
| `Observables.Return(value)` | Emits one value and completes, both inside the `Subscribe` call. |
| `ReactiveExtensions.Start(function, sequencer)` | Runs the function once per subscriber, emits its result, then completes. |
| `action.Start(sequencer)` | Runs the action once per subscriber, emits `RxVoid`, then completes. |
| `condition.While(action)` | Runs the action over and over on the calling thread while the condition returns true. |
| `condition.While(action, sequencer)` | Runs each round of the loop on the sequencer instead of the calling thread. |
| `enumerable.FromArray()` | Emits each element of the collection in order. |
| `enumerable.FromArray(sequencer)` | Emits each element of the collection on the sequencer. |
| `resource.Using(action)` | Runs the action against the resource, emits `RxVoid`, completes, then disposes the resource. |
| `resource.Using(action, sequencer)` | Same as above, with the action run on the sequencer. |
| `resource.Using(function)` | Runs the function against the resource, emits its result, completes, then disposes the resource. |
| `resource.Using(function, sequencer)` | Same as above, with the function run on the sequencer. |
| `tasks.WithLimitedConcurrency(maxConcurrency)` | Runs the tasks with at most `maxConcurrency` in flight and emits results as they finish. |
| `owner.ToPropertyObservable(propertyExpression)` | Emits the property value on subscribe, then again each time that property raises a change. |
| `ReactiveExtensions.ToReadOnlyBehavior(initialValue)` | Returns a read side and a write side that share one latest value. |

### Transformation

These change each value into a different value.

| Member | What it does |
|---|---|
| `source.AsSignal()` | Replaces every value with `RxVoid`, so only the timing survives. |
| `source.SelectConstant(constant)` | Replaces every value with the constant you pass in. |
| `source.TrySelect(selector)` | Runs the selector on each value and forwards only the results that are not null. |
| `source.WhereSelect(predicate, selector)` | Keeps the values the predicate accepts and runs the selector on those, in one step. |
| `source.SelectManyThen(first, second)` | Feeds each value through two sequence-returning selectors in a row. |
| `source.ScanWithInitial(initial, accumulator)` | Emits the initial value, then the running total after each source value. |
| `source.Pairwise()` | Emits each next-door pair as `(Previous, Current)`, so the first value alone emits nothing. |
| `source.ForEach()` | Flattens a source of collections into one value at a time. |
| `source.ForEach(sequencer)` | Flattens a source of collections and delivers the values on the sequencer. |
| `source.Shuffle()` | Reorders each array in place at random and forwards that same array. |
| `source.Not()` | Flips each boolean value. |
| `source.SelectAsync(asyncSelector)` | Runs the async selector for each value and emits the results in source order. |
| `source.SelectAsyncSequential(selector)` | Same as `SelectAsync`, under a name that states the ordering. |
| `source.SelectLatestAsync(selector)` | Runs the async selector for each value and emits only the newest result. |
| `source.SelectAsyncConcurrent(selector, maxConcurrency)` | Runs the async selector with a cap on parallel calls and emits results as they finish. |
| `source.BufferUntil(startsWith, endsWith)` | Gathers characters between the two marker characters into a string, markers included. |
| `source.ReplayLastOnSubscribe(initialValue)` | Emits the initial value to each new subscriber before the source values. |
| `source.LatestOrDefault(defaultValue)` | Emits the default on subscribe, then each value that differs from the one before it. |

### Filtering

These drop values you do not want.

| Member | What it does |
|---|---|
| `source.WhereIsNotNull()` | Drops null values and passes everything else through. |
| `source.WhereTrue()` | Keeps only the true values. |
| `source.WhereFalse()` | Keeps only the false values. |
| `source.SkipWhileNull()` | Drops nulls until the first non-null value, then forwards everything, nulls included. |
| `source.TakeUntil(predicate)` | Forwards values up to and including the first match, then completes. |
| `source.WaitUntil(predicate)` | Emits only the first matching value, then completes. |
| `source.Filter(regexPattern)` | Keeps the strings that match the pattern, using a 30 second match timeout. |
| `source.Filter(regex)` | Keeps the strings that match the regular expression you built yourself. |
| `source.Partition(predicate)` | Splits the source into a true sequence and a false sequence that share one subscription. |
| `source.DropIfBusy(asyncAction)` | Runs the async action for a value and drops any value that arrives while it is running. |

### Combination

These mix several sources into one.

| Member | What it does |
|---|---|
| `sources.CombineLatestValuesAreAllTrue()` | Emits true while the newest value of every source is true. |
| `sources.CombineLatestValuesAreAllFalse()` | Emits true while the newest value of every source is false. |
| `source.GetMax(sources)` | Emits the largest of the newest values, once every source has emitted. |
| `source.GetMin(sources)` | Emits the smallest of the newest values, once every source has emitted. |
| `source.SwitchIfEmpty(fallback)` | Switches to the fallback when the source completes without emitting anything. |
| `source.SampleLatest(trigger)` | Emits the newest source value each time the trigger fires. |
| `sources.RunAll()` | Runs the sources one after another, then emits `RxVoid` and completes. |
| `candidates.FirstMatchFromCandidates(project, transform, predicate, fallback)` | Tries each candidate in order and emits the first match, or the fallback when none match. |

### Time

These change when a value reaches you.

| Member | What it does |
|---|---|
| `source.ThrottleFirst(window)` | Emits the first value in each window and drops the rest of that window. |
| `source.ThrottleFirst(window, sequencer)` | Same as above, with the sequencer supplying the clock. |
| `source.ThrottleOnScheduler(timeSpan, sequencer)` | Emits the newest value once the gap since the last one reaches `timeSpan`. |
| `source.ThrottleDistinct(throttle)` | Emits the newest value after the throttle window, and skips it when it repeats the last one emitted. |
| `source.ThrottleDistinct(throttle, sequencer)` | Same as above, with the sequencer supplying the clock. |
| `source.ThrottleUntilTrue(throttle, predicate)` | Emits a matching value right away and delays every other value by the throttle. |
| `source.DebounceImmediate(dueTime)` | Emits the first value right away, then the newest value after each quiet stretch. |
| `source.DebounceImmediate(dueTime, sequencer)` | Same as above, with the sequencer supplying the clock. |
| `source.DebounceUntil(debounce, condition)` | Emits a matching value right away and delays every other value by the debounce time. |
| `source.DebounceUntil(debounce, condition, sequencer)` | Same as above, with the sequencer supplying the clock. |
| `source.Conflate(minimumUpdatePeriod, sequencer)` | Keeps emissions at least that far apart and holds back only the newest waiting value. |
| `source.BufferUntilIdle(idleTime)` | Collects values into a list and emits the list once the source goes quiet. |
| `source.BufferUntilIdle(idleTime, sequencer)` | Same as above, with the sequencer supplying the clock. |
| `source.BufferUntilInactive(inactivityPeriod)` | Another name for `BufferUntilIdle`. |
| `source.BufferUntilInactive(inactivityPeriod, sequencer)` | Another name for `BufferUntilIdle` with a sequencer. |
| `source.DetectStale(stalenessPeriod, sequencer)` | Wraps each value as an update and emits a stale marker for each quiet stretch. |
| `source.Heartbeat(heartbeatPeriod, sequencer)` | Wraps each value as an update and emits a heartbeat every period the source stays quiet. |
| `timeSpan.SyncTimer()` | Returns a ticking clock that every caller using the same period shares. |
| `timeSpan.SyncTimer(sequencer)` | Returns a shared ticking clock for that period and sequencer. |

### Scheduling

These choose which thread your code runs on, and when.

| Member | What it does |
|---|---|
| `source.ObserveOnSafe(sequencer)` | Moves delivery onto the sequencer, or leaves the source untouched when you pass null. |
| `source.ObserveOnIf(condition, sequencer)` | Moves delivery onto the sequencer when the boolean is true. |
| `source.ObserveOnIf(condition, trueSequencer, falseSequencer)` | Picks one of two sequencers from the boolean. |
| `source.ObserveOnIf(conditionSource, trueSequencer, falseSequencer)` | Picks one of two sequencers from the newest value of a boolean source. |
| `source.ObserveOnIf(conditionSource, sequencer)` | Uses the sequencer while the boolean source says true, and runs inline otherwise. |
| `source.Schedule(dueTime, sequencer)` | Emits each value on the sequencer after the delay, and forwards no end signal. |
| `source.Schedule(absoluteDueTime, sequencer)` | Emits each value on the sequencer at that clock time, and forwards no end signal. |
| `source.Schedule(dueTime, sequencer, action)` | Runs the action on each value after the delay, then emits it. |
| `source.Schedule(absoluteDueTime, sequencer, action)` | Runs the action on each value at that clock time, then emits it. |
| `source.Schedule(sequencer, function)` | Runs the function on each value on the sequencer and emits the result. |
| `source.Schedule(dueTime, sequencer, function)` | Runs the function on each value after the delay and emits the result. |
| `value.Schedule(dueTime, sequencer)` | Emits the single value on the sequencer after the delay, and never completes. |
| `value.Schedule(absoluteDueTime, sequencer)` | Emits the single value on the sequencer at that clock time, and never completes. |
| `value.Schedule(dueTime, sequencer, action)` | Runs the action on the value after the delay, then emits it. |
| `value.Schedule(absoluteDueTime, sequencer, action)` | Runs the action on the value at that clock time, then emits it. |
| `value.Schedule(sequencer, function)` | Runs the function on the value on the sequencer and emits the result. |
| `value.Schedule(dueTime, sequencer, function)` | Runs the function on the value after the delay and emits the result. |
| `sequencer.ScheduleSafe(action)` | Runs the action on the sequencer, or inline when the sequencer is null. |
| `sequencer.ScheduleSafe(dueTime, action)` | Runs the action after the delay, using a plain timer when the sequencer is null. |

### Error handling

These decide what happens when a source fails.

| Member | What it does |
|---|---|
| `source.CatchIgnore()` | Swallows any error and completes instead. |
| `source.CatchIgnore(errorAction)` | Hands a matching error to your action and completes; other errors pass through. |
| `source.CatchReturn(fallback)` | Replaces any error with the fallback value, then completes. |
| `source.CatchAndReturn(fallback)` | Another name for `CatchReturn`. |
| `source.CatchAndReturn(fallbackFactory)` | Builds the fallback value from a matching error, then completes. |
| `source.CatchReturnUnit()` | Replaces any error with a single `RxVoid`, then completes. |
| `source.OnErrorRetry()` | Subscribes to the source again after every error, forever. |
| `source.OnErrorRetry(onError)` | Runs your handler for a matching error, then subscribes again, forever. |
| `source.OnErrorRetry(onError, delay)` | Runs your handler, waits the delay, then subscribes again, forever. |
| `source.OnErrorRetry(onError, retryCount)` | Runs your handler and subscribes again, up to that many times. |
| `source.OnErrorRetry(onError, retryCount, delay)` | Runs your handler, waits the delay, and subscribes again, up to that many times. |
| `source.OnErrorRetry(onError, retryCount, delay, delaySequencer)` | Same as above, with the sequencer timing the delay. |
| `source.RetryWithBackoff(maxRetries, initialDelay)` | Subscribes again after each error and doubles the wait each time. |
| `source.RetryWithBackoff(maxRetries, initialDelay, backoffFactor, maxDelay, sequencer)` | Same, with your own growth factor, an upper limit on the wait, and a sequencer. |
| `source.RetryWithDelay(retryCount, delaySelector)` | Subscribes again after each error, asking your function for each wait. |
| `source.RetryForeverWithDelay(delay)` | Subscribes again after each error, always waiting the same delay, forever. |
| `source.RetryWithFixedDelay(retryCount, delay)` | Subscribes again after each error with a fixed wait, up to that many times. |

### Side effects and diagnostics

These run your code beside the sequence without changing the values.

| Member | What it does |
|---|---|
| `source.LogErrors(logger)` | Hands the error to your logger, then still passes it on to your subscriber. |
| `source.DoOnSubscribe(action)` | Runs the action each time someone subscribes, before the source is subscribed. |
| `source.DoOnDispose(disposeAction)` | Runs the action once when the subscription is disposed. |
| `observer.OnNext(events)` | Pushes several values to an observer in one call. |
| `observer.FastForEach(collection)` | Pushes every element of a collection to an observer, indexing lists instead of enumerating them. |

### Subscription

These start listening, and some of them block until a result arrives.

| Member | What it does |
|---|---|
| `source.SubscribeAsync(onNext)` | Queues values and runs your async handler on one value at a time. |
| `source.SubscribeAsync(onNext, onCompleted)` | Same, and runs your completion handler once the queue drains. |
| `source.SubscribeAsync(onNext, onError)` | Same, and runs your error handler on a source error or a handler failure. |
| `source.SubscribeAsync(onNext, onError, onCompleted)` | Same, with both an error handler and a completion handler. |
| `source.SubscribeSynchronous(onNext)` | Another name for `SubscribeAsync(onNext)`. |
| `source.SubscribeSynchronous(onNext, onError)` | Another name for `SubscribeAsync(onNext, onError)`. |
| `source.SubscribeSynchronous(onNext, onCompleted)` | Another name for `SubscribeAsync(onNext, onCompleted)`. |
| `source.SubscribeSynchronous(onNext, onError, onCompleted)` | Another name for the three-handler `SubscribeAsync`. |
| `source.SynchronizeAsync()` | Pairs each value with a handle, and the producer waits for that handle to be disposed. |
| `source.SynchronizeSynchronous()` | Another name for `SynchronizeAsync`. |
| `source.ToHotTask()` | Subscribes at once and gives you a `Task` for the first value. |
| `source.ToHotValueTask()` | Subscribes at once and gives you a `ValueTask` for the first value, which you may read only once. |
| `source.SubscribeAndComplete()` | Subscribes, throws the values away, and disposes the subscription. |
| `source.SubscribeGetValue()` | Returns the last value the source emitted during the `Subscribe` call itself. |
| `source.SubscribeGetError()` | Returns the error the source emitted during the `Subscribe` call itself, or null. |
| `source.WaitForValue()` | Blocks up to 30 seconds and returns the last value before the source ended. |
| `source.WaitForValue(timeout)` | Same, with your own timeout. |
| `source.WaitForValue(sequencer)` | Same, with the subscribe call dispatched through the sequencer. |
| `source.WaitForValue(sequencer, timeout)` | Same, with both a sequencer and your own timeout. |
| `source.WaitForCompletion()` | Blocks up to 30 seconds for the source to end, and rethrows any error it carried. |
| `source.WaitForCompletion(timeout)` | Same, with your own timeout. |
| `source.WaitForCompletion(sequencer)` | Same, with the subscribe call dispatched through the sequencer. |
| `source.WaitForCompletion(sequencer, timeout)` | Same, with both a sequencer and your own timeout. |
| `source.WaitForError()` | Blocks up to 30 seconds and returns the error without throwing it. |
| `source.WaitForError(timeout)` | Same, with your own timeout. |
| `source.WaitForError(sequencer)` | Same, with the subscribe call dispatched through the sequencer. |
| `source.WaitForError(sequencer, timeout)` | Same, with both a sequencer and your own timeout. |

Every `WaitFor` helper throws a `TimeoutException` when the source does not end in time.

### Types you can name

You call the helpers above most of the time. These public types back them, and you can build or accept them yourself.

| Type | What it is |
|---|---|
| `CurrentValueSubject<T>` | Takes values in, keeps the latest one, replays it to each new subscriber, and exposes it as `Value`. |
| `ConcurrencyLimiter<T>` | The sequence behind `WithLimitedConcurrency`, which drains tasks with a cap on parallel work. |
| `Continuation` | A one-at-a-time handoff that pairs an item with a release handle and waits for that handle. |
| `Heartbeat<T>` and `IHeartbeat<T>` | The value `Heartbeat` emits, carrying either a tick or an update. |
| `Stale<T>` and `IStale<T>` | The value `DetectStale` emits, carrying either a stale marker or an update. |
| `SingleValueSignal<T>` | A sequence that emits one value and completes, both inside `Subscribe`. |
| `ScanWithInitialObservable<TSource, TAccumulate>` | The sequence behind `ScanWithInitial`. |
| `FirstAsTaskHelper` and `FirstAsValueTaskHelper<T>` | The helpers behind `ToHotTask` and `ToHotValueTask`. |
| `ObserverArrayHelpers` | Pushes a value to an array of observers, and removes one observer from such an array. |
| `TimerSinkState<T>` | The timer slot and ordered delivery queue that the timing operators share. |
| `IDrainTarget` and `DrainNotificationKind` | The callback and the notification tag used to deliver queued values. |
| `ReactiveUI.Primitives.Extensions.Operators.*` | One public class per helper, such as `PairwiseObservable<T>`, which you can build directly instead of calling the helper. |

Reading `Stale<T>.Update` while `IsStale` is true throws. Check `IsStale` first.

### Worked examples

#### Pairwise

```csharp
Signal<int> source = new();
source.Pairwise().Subscribe(pair => Console.WriteLine(pair));

source.OnNext(1);
source.OnNext(2);
source.OnNext(3);
// prints: (1, 2)
// prints: (2, 3)
```

#### WhereIsNotNull

```csharp
Signal<string?> source = new();
source.WhereIsNotNull().Subscribe(name => Console.WriteLine(name));

source.OnNext("ada");
source.OnNext(null);
source.OnNext("grace");
// prints: ada
// prints: grace
```

#### Partition

```csharp
Signal<int> source = new();
var (even, odd) = source.Partition(value => value % 2 == 0);

even.Subscribe(value => Console.WriteLine($"even {value}"));
odd.Subscribe(value => Console.WriteLine($"odd {value}"));

source.OnNext(1);
source.OnNext(2);
// prints: odd 1
// prints: even 2
```

#### ScanWithInitial

```csharp
Signal<int> source = new();
source.ScanWithInitial(0, (total, value) => total + value)
      .Subscribe(total => Console.WriteLine(total));

source.OnNext(5);
source.OnNext(3);
// prints: 0
// prints: 5
// prints: 8
```

#### ThrottleFirst

```csharp
Signal<string> clicks = new();
clicks.ThrottleFirst(TimeSpan.FromSeconds(1))
      .Subscribe(label => Console.WriteLine(label));

clicks.OnNext("first");   // arrives at 0.0s
clicks.OnNext("second");  // arrives at 0.2s
clicks.OnNext("third");   // arrives at 1.5s
// prints: first
// prints: third
```

#### SwitchIfEmpty

```csharp
Signal<string> source = new();
source.SwitchIfEmpty(Observables.Return("no results"))
      .Subscribe(text => Console.WriteLine(text));

source.OnCompleted();
// prints: no results
```

#### SampleLatest

```csharp
Signal<int> prices = new();
Signal<object> tick = new();
prices.SampleLatest(tick).Subscribe(price => Console.WriteLine(price));

prices.OnNext(10);
prices.OnNext(11);
tick.OnNext(new object());
// prints: 11
```

#### CatchAndReturn

```csharp
Signal<int> source = new();
source.CatchAndReturn(-1).Subscribe(value => Console.WriteLine(value));

source.OnNext(7);
source.OnError(new InvalidOperationException("broken"));
// prints: 7
// prints: -1
```

#### CatchIgnore

```csharp
Signal<int> source = new();
source.CatchIgnore<InvalidOperationException>(error => Console.WriteLine($"logged {error.Message}"))
      .Subscribe(value => Console.WriteLine(value), () => Console.WriteLine("done"));

source.OnNext(7);
source.OnError(new InvalidOperationException("broken"));
// prints: 7
// prints: logged broken
// prints: done
```

#### RetryWithBackoff

```csharp
// Scope lives in ReactiveUI.Primitives.Disposables
var attempts = 0;
IObservable<string> flaky = new AnonymousSignal<string>(observer =>
{
    attempts++;
    observer.OnError(new InvalidOperationException("offline"));
    return Scope.Empty;
});
flaky.RetryWithBackoff(3, TimeSpan.FromMilliseconds(100))
     .Subscribe(_ => { }, error => Console.WriteLine($"gave up after {attempts} tries"));
// prints: gave up after 4 tries, waiting 100ms, 200ms and 400ms in between
```

#### LogErrors

```csharp
Signal<int> source = new();
source.LogErrors(error => Console.WriteLine($"log: {error.Message}"))
      .Subscribe(value => Console.WriteLine(value), error => Console.WriteLine("subscriber saw it too"));

source.OnError(new InvalidOperationException("broken"));
// prints: log: broken
// prints: subscriber saw it too
```

#### DropIfBusy

```csharp
Signal<int> jobs = new();
jobs.DropIfBusy(async value =>
    {
        await Task.Delay(500);
    })
    .Subscribe(value => Console.WriteLine($"handled {value}"));

jobs.OnNext(1);   // starts work
jobs.OnNext(2);   // dropped, work is still running
// prints: handled 1
```

#### SubscribeAsync

```csharp
Signal<int> source = new();
using var subscription = source.SubscribeAsync(async value =>
{
    await Task.Delay(10);
    Console.WriteLine($"saved {value}");
});

source.OnNext(1);
source.OnNext(2);
// prints: saved 1
// prints: saved 2
```

#### SelectLatestAsync

```csharp
Signal<string> queries = new();
queries.SelectLatestAsync(async text =>
       {
           await Task.Delay(100);
           return $"results for {text}";
       })
       .Subscribe(text => Console.WriteLine(text));

queries.OnNext("ca");
queries.OnNext("cat");   // replaces the pending "ca" lookup
// prints: results for cat
```

#### BufferUntilIdle

```csharp
Signal<char> keys = new();
keys.BufferUntilIdle(TimeSpan.FromMilliseconds(300))
    .Subscribe(batch => Console.WriteLine(string.Concat(batch)));

keys.OnNext('h');
keys.OnNext('i');
// after 300ms of quiet
// prints: hi
```

#### WaitForValue

```csharp
IObservable<int> answer = Observables.Return(42);

var value = answer.WaitForValue(TimeSpan.FromSeconds(1));
Console.WriteLine(value);
// prints: 42
```

## Async operators

`IObservableAsync<T>` is a stream of values you subscribe to with `await`. Every step returns a `ValueTask`, so the stream waits for your handler to finish before it sends the next value. Reach for it instead of `IObservable<T>` when your handlers do real async work, such as a database read or an HTTP call, and you want the producer to slow down for you rather than pile up callbacks.

Two static classes carry most of this surface. `SignalAsync` creates streams. `SignalAsyncExtensions` adds every operator and terminal you call on a stream, so you write them as normal extension methods.

### Creation factories

These build a stream from scratch. You call them on `SignalAsync`, not on an existing stream.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `SignalAsync.Emit(value)` | Emits one value, then completes. | - |
| `SignalAsync.Return(value)` | Another name for `Emit`. | - |
| `SignalAsync.Empty<T>()` | Completes at once without emitting anything. | - |
| `SignalAsync.None<T>()` | Another name for `Empty`. | - |
| `SignalAsync.Never<T>()` | Never emits and never completes. | - |
| `SignalAsync.Throw<T>(error)` | Ends the stream at once with the exception you pass. | - |
| `SignalAsync.Fail<T>(error)` | Another name for `Throw`. | - |
| `SignalAsync.Range(start, count)` | Emits a run of consecutive ints. | - |
| `SignalAsync.Sequence(start, count)` | Another name for `Range`. | - |
| `SignalAsync.FromEnumerable(values)` | Emits each item of a collection, once per subscriber. | - |
| `SignalAsync.FromAsyncEnumerable(values)` | Emits each item of an async sequence. | - |
| `SignalAsync.FromAsync(factory)` | Runs an async function once per subscriber and emits its one result. | - |
| `SignalAsync.Create(subscribeAsync)` | Builds a stream from subscribe logic you write by hand. | - |
| `SignalAsync.CreateAsBackgroundJob(job)` | Runs your async job per subscriber and pushes values into the observer. | - |
| `SignalAsync.Defer(factory)` | Builds a fresh stream for each subscriber at the moment it subscribes. | both |
| `SignalAsync.Timer(dueTime)` | Emits `0` after a delay, and can then tick every period. | - |
| `SignalAsync.After(dueTime)` | Another name for `Timer`. | - |
| `SignalAsync.Every(period)` | Ticks forever, starting one period from now. | - |
| `SignalAsync.Pulse(period)` | Another name for `Every`. | - |
| `SignalAsync.Interval(period)` | Emits a counter that starts at 1 and rises on every tick. | - |
| `SignalAsync.Blend(sources)` | Merges several streams into one. | - |
| `SignalAsync.Chain(sources)` | Runs streams in order, each starting when the one before it ends. | - |
| `SignalAsync.Start(function)` | Runs a plain function and emits what it returns. | - |
| `SignalAsync.Use(resourceFactory, signalFactory)` | Makes a resource per subscriber and disposes it when the stream ends. | - |
| `SignalAsync.Using(resourceFactory, signalFactory)` | Another name for `Use`. | - |
| `ToAsyncSignal()` | Turns a collection, an async sequence or a `Task<T>` into a stream. | - |

Signals are streams you push into yourself. You create them on `Signal`.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `Signal.Create<T>()` | Makes a live signal that holds no value. | - |
| `Signal.CreateBehavior(startValue)` | Makes a signal that holds a current value and hands it to each new subscriber. | - |
| `Signal.CreateReplayLatest<T>()` | Makes a signal that replays only the newest value to late subscribers. | - |

The `ReactiveUI.Primitives.Async` package adds factories for `RxVoid`, a value that means "something happened" and carries no data.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `SignalAsync.EmitRxVoid()` | Emits one `RxVoid.Default`, then completes. | - |
| `action.Start()` | Runs the action and emits `RxVoid.Default` when it finishes. | - |
| `asyncFunction.FromAsync()` | Runs the async operation per subscriber and reports that it finished. | - |
| `task.ToAsyncSignal()` | Turns a `Task` into a stream that ends when the task ends. | - |

```csharp
var names = SignalAsync.FromEnumerable(new[] { "ann", "bob" });
await names.ForEachAsync(n => Console.WriteLine(n));
// ann
// bob
```

```csharp
var ticks = SignalAsync.Interval(TimeSpan.FromSeconds(1));
var firstThree = await ticks.Take(3).ToListAsync();
// firstThree holds 1, 2, 3
```

### Transformation

These change each value, or turn one value into a whole stream.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `Map(selector)` | Runs each value through a function. | both |
| `MapWith(state, selector)` | Maps each value, passing your state in so the function needs no closure. | - |
| `Select(selector)` | Another name for `Map`. | both |
| `Cast<TResult>()` | Casts every value, failing the stream on a bad cast. | - |
| `CastTo<TResult>()` | Casts an untyped stream to `TResult`. | - |
| `OfType<TResult>()` | Keeps only values of that type and drops the rest. | - |
| `KeepType<TResult>()` | Another name for `OfType`, for an untyped stream. | - |
| `FlatMap(selector)` | Turns each value into a stream and merges them all at once. | both |
| `SelectMany(selector)` | Another name for `FlatMap`, with an overload that recombines the outer and inner value. | both |
| `Bind(selector)` | Another name for `FlatMap`. | - |
| `Scan(seed, accumulator)` | Adds each value to a running total and emits that total. | both |
| `Fold(seed, accumulator)` | Another name for `Scan`. | both |
| `ScanWithInitial(initial, accumulator)` | Runs a total like `Scan`, and emits the seed as soon as you subscribe. | both |
| `Pairwise()` | Emits each value paired with the one before it. | - |
| `GroupBy(keySelector)` | Splits the stream into one sub-stream per key. | - |
| `ForEach()` | Flattens each emitted collection into single values, in order. | - |
| `Not()` | Flips every `bool` value. | - |
| `ToAsyncEnumerable(channelFactory)` | Turns the stream into a sequence you can `await foreach` over. | - |
| `AsSignal()` | Drops the values and emits a bare `RxVoid` pulse instead. | - |

```csharp
var ids = SignalAsync.FromEnumerable(new[] { 1, 2 });
var names = ids.Map(async (id, ct) => await LoadNameAsync(id, ct));
await names.ForEachAsync(n => Console.WriteLine(n));
// Ann
// Bob
```

```csharp
var users = SignalAsync.FromEnumerable(new[] { 1, 2 });
var orders = users.FlatMap(id => SignalAsync.FromEnumerable(OrdersFor(id)));
var all = await orders.ToListAsync();
// all holds every order from user 1 and user 2
```

```csharp
var amounts = SignalAsync.FromEnumerable(new[] { 10, 5, 1 });
var running = await amounts.Scan(0, (total, next) => total + next).ToListAsync();
// running holds 10, 15, 16
```

### Filtering

These drop values you do not want, or stop the stream early.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `Keep(predicate)` | Keeps only the values that match. | both |
| `KeepWith(state, predicate)` | Filters values, passing your state in so the predicate needs no closure. | - |
| `Where(predicate)` | Another name for `Keep`. | both |
| `KeepNotNull()` | Drops nulls and makes the value type non-nullable. | - |
| `WhereIsNotNull()` | Another name for `KeepNotNull`. | - |
| `SkipWhileNull()` | Ignores nulls at the start, then forwards everything after that. | - |
| `WhereTrue()` | Keeps only `true` values. | - |
| `WhereFalse()` | Keeps only `false` values. | - |
| `Distinct()` | Emits each value only the first time it ever appears. | - |
| `DistinctBy(keySelector)` | Drops a value when its key has appeared before. | - |
| `Unique()` | Another name for `Distinct`. | - |
| `UniqueBy(keySelector)` | Another name for `DistinctBy`. | - |
| `DistinctUntilChanged()` | Drops a value when it equals the one right before it. | - |
| `DistinctUntilChangedBy(keySelector)` | Drops a value when its key equals the key right before it. | - |
| `Take(count)` | Emits at most that many values, then completes. | - |
| `Skip(count)` | Ignores that many values at the start. | - |
| `TakeWhile(predicate)` | Emits while the predicate holds, then completes. | both |
| `SkipWhile(predicate)` | Drops values until the predicate first fails, then forwards the rest. | both |
| `WaitUntil(predicate)` | Emits the first matching value, then completes and shuts the source down. | - |
| `Partition(predicate)` | Splits one stream into a matching branch and a non-matching branch. | - |
| `DropIfBusy(asyncAction)` | Runs an async action one at a time and throws away values that arrive while it runs. | - |
| `LatestOrDefault(defaultValue)` | Emits your default straight away, then only values that differ from the last one. | - |
| `TakeUntil(other)` | Stops the stream when another stream, a task, a token or a predicate says so. | both |

```csharp
var evens = await SignalAsync.Range(1, 5).Keep(n => n % 2 == 0).ToListAsync();
// evens holds 2, 4
```

```csharp
var status = SignalAsync.FromEnumerable(new[] { "on", "on", "off", "on" });
var changes = await status.DistinctUntilChanged().ToListAsync();
// changes holds on, off, on
```

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(2500));
var ticks = SignalAsync.Interval(TimeSpan.FromSeconds(1));
var seen = await ticks.TakeUntil(cts.Token).ToListAsync();
// seen holds 1, 2
```

### Combination

These join two or more streams. Every selector here is a plain function, so there are no async forms.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `SyncLatest(other, selector)` | Emits a fresh combined value whenever any source emits, once all of them have emitted once. | - |
| `CombineLatest(other, selector)` | Another name for `SyncLatest`. | - |
| `PairLatest(other, selector)` | `SyncLatest` for exactly two sources. | - |
| `CombineLatestValuesAreAllTrue()` | Emits whether every source's newest value is `true`. | - |
| `CombineLatestValuesAreAllFalse()` | Emits whether every source's newest value is `false`. | - |
| `Zip(second)` | Pairs values by position, waiting for the slower source. | - |
| `Pair(second, resultSelector)` | Another name for `Zip`. | - |
| `Blend(other)` | Interleaves two streams into one as values arrive. | - |
| `Merge(other)` | Another name for `Blend`, with an overload that caps how many inner streams run at once. | - |
| `Chain(second)` | Runs the second stream only after the first one completes. | - |
| `Concat(second)` | Another name for `Chain`. | - |
| `SwitchTo()` | Follows only the newest inner stream and drops the one before it. | - |
| `Switch()` | Another name for `SwitchTo`. | - |
| `Lead(value)` | Emits one value ahead of the source's own values. | - |
| `Prepend(value)` | Another name for `Lead`. | - |
| `StartWith(values)` | Another name for `Lead`, taking one value, an array or a collection. | - |
| `GetMin(sources)` | Emits the smallest of the newest values across all the sources. | - |
| `GetMax(sources)` | Emits the largest of the newest values across all the sources. | - |

`SyncLatest` and `CombineLatest` take 2 to 16 streams. They also work on a collection of same-typed streams, where they hand you a snapshot list.

```csharp
var first = Signal.CreateBehavior("ann");
var last = Signal.CreateBehavior("lee");
var full = first.SyncLatest(last, (f, l) => f + " " + l);

await using var sub = await full.SubscribeAsync(name => Console.WriteLine(name));
await last.OnNextAsync("king", CancellationToken.None);
// ann lee
// ann king
```

```csharp
var clicks = SignalAsync.FromEnumerable(new[] { "click" });
var keys = SignalAsync.FromEnumerable(new[] { "key" });
var all = await clicks.Merge(keys).ToListAsync();
// all holds click, key
```

```csharp
var queries = Signal.Create<string>();
var results = queries.Map(q => SignalAsync.FromAsync(ct => SearchAsync(q, ct))).SwitchTo();

await using var sub = await results.SubscribeAsync(r => Console.WriteLine(r));
await queries.OnNextAsync("ca", CancellationToken.None);
await queries.OnNextAsync("cat", CancellationToken.None);
// only the result for "cat" prints, because the "ca" search is dropped
```

### Time

These move values around in time. `Delay`, `Throttle`, `ThrottleDistinct`, `DebounceUntil`, `Timeout`, `Timer` and `Interval` each take an optional `TimeProvider`, so a test can drive a fake clock. `Shift`, `Expire`, `After`, `Every` and `Pulse` always use the system clock.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `Delay(delayInterval)` | Holds each value back by a fixed amount, and passes errors and completion straight through. | - |
| `Shift(delayInterval)` | Another name for `Delay`. | - |
| `Throttle(dueTime)` | Emits a value once the stream has been quiet for that long. | - |
| `ThrottleDistinct(throttle)` | Waits for quiet like `Throttle`, and also drops repeats on both sides of the wait. | - |
| `DebounceUntil(debounce, condition)` | Delays values, except matching ones, which go straight through and cancel the pending value. | - |
| `Timeout(dueTime)` | Fails with `TimeoutException` when the gap between values grows too large. | - |
| `Expire(dueTime)` | Another name for `Timeout`. | - |

`Timeout` has an overload that switches to a fallback stream instead of failing.

```csharp
var keystrokes = Signal.Create<string>();
var settled = keystrokes.Throttle(TimeSpan.FromMilliseconds(300));

await using var sub = await settled.SubscribeAsync(text => Console.WriteLine(text));
await keystrokes.OnNextAsync("ca", CancellationToken.None);
await keystrokes.OnNextAsync("cat", CancellationToken.None);
// cat
```

```csharp
var slow = SignalAsync.Timer(TimeSpan.FromSeconds(10)).Map(_ => "done");
await slow.Timeout(TimeSpan.FromSeconds(2)).WaitCompletionAsync();
// throws TimeoutException after 2 seconds
```

### Error handling

A stream can fail in two ways. A terminal failure ends it. A resumable error is reported and the stream carries on.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `Catch(handler)` | Carries on with the stream your handler returns when the source fails. | - |
| `Rescue(handler)` | Another name for `Catch`. | - |
| `Recover(handler)` | Another name for `Catch`, where a throwing handler ends the stream with its own exception. | - |
| `Resume(fallback)` | Switches to a fixed fallback stream when the source fails. | - |
| `CatchAndIgnoreErrorResume(handler)` | Recovers from a failure and sends resumable errors to the global handler instead of downstream. | - |
| `CatchIgnore()` | Swallows a failure and completes normally, with an overload that matches one exception type. | - |
| `CatchAndReturn(fallback)` | Emits one fallback value and completes when the source fails. | - |
| `Retry()` | Subscribes again every time the source fails, forever or up to a count you set. | - |
| `Reattempt(retryCount)` | Another name for `Retry(retryCount)`. | - |
| `OnErrorResumeAsFailure()` | Turns a resumable error into a terminal failure. | - |
| `LogErrors(logger)` | Reports errors to your logger as they pass, and changes nothing. | - |

```csharp
var risky = SignalAsync.Throw<int>(new InvalidOperationException("boom"));
var safe = risky.Catch(ex => SignalAsync.Emit(-1));
var values = await safe.ToListAsync();
// values holds -1
```

```csharp
var load = SignalAsync.FromAsync(ct => LoadAsync(ct));
var value = await load.Retry(3).FirstAsync();
// LoadAsync runs again after each failure, up to three extra times
```

### Terminal operations

These end the pipeline and hand you a `ValueTask` result, not a new stream, so you `await` them. Every predicate here is a plain function. No terminal operation takes an awaitable predicate.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `SubscribeAsync(onNext)` | Starts the stream and returns the handle you dispose to stop it. | both |
| `FirstAsync()` | Gives you the first value, or fails when the stream ends empty. | - |
| `FirstOrDefaultAsync()` | Gives you the first value, or a default when nothing matches. | - |
| `LastAsync()` | Gives you the final value once the stream ends. | - |
| `LastOrDefaultAsync()` | Gives you the final value, or a default when nothing matches. | - |
| `SingleAsync()` | Expects exactly one value and fails otherwise. | - |
| `SingleOrDefaultAsync()` | Expects at most one value and gives you a default when there is none. | - |
| `CountAsync()` | Counts the values once the stream ends. | - |
| `LongCountAsync()` | Counts the values as a 64-bit number. | - |
| `AnyAsync()` | Tells you whether at least one value appears. | - |
| `AllAsync(predicate)` | Tells you whether every value matches. | - |
| `ContainsAsync(value)` | Tells you whether that value ever appears. | - |
| `AggregateAsync(seed, accumulator)` | Folds the whole stream into one value, and can project the result. | both |
| `ReduceAsync(seed, accumulator)` | Another name for `AggregateAsync`. | both |
| `ToListAsync()` | Collects every value into a list, in arrival order. | - |
| `CollectListAsync()` | Another name for `ToListAsync`. | - |
| `CollectArrayAsync()` | Collects every value into an array. | - |
| `ToDictionaryAsync(keySelector)` | Indexes the values by a key you pick. | - |
| `ForEachAsync(onNext)` | Runs your callback for every value and completes when the stream ends. | both |
| `WaitCompletionAsync()` | Ignores the values and completes when the stream ends, failing if it failed. | - |

`FirstAsync`, `LastAsync`, `SingleAsync`, `CountAsync` and their `OrDefault` partners all take an optional predicate and an optional `CancellationToken`.

```csharp
var ticks = SignalAsync.Interval(TimeSpan.FromSeconds(1));
await using var sub = await ticks.SubscribeAsync(t => Console.WriteLine(t));
// 1
// 2
// the stream stops when sub is disposed
```

```csharp
var values = await SignalAsync.Range(1, 3).ToListAsync();
// values holds 1, 2, 3
```

```csharp
var firstEven = await SignalAsync.Range(1, 10).FirstAsync(n => n % 2 == 0);
// firstEven is 2
```

### Utility

These add side effects, share one subscription between subscribers, or move work onto another thread.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `Do(onNext)` | Runs a side effect for each value, error and completion, and changes nothing. | both |
| `Tap(onNext)` | Another name for `Do`. | both |
| `DoOnSubscribe(action)` | Runs an action every time someone subscribes, before the source is wired up. | both |
| `OnDispose(disposeAction)` | Runs an action when the subscription is torn down. | both |
| `Multicast(signal)` | Shares one upstream subscription through a signal you supply. | - |
| `Publish()` | Shares one subscription with every subscriber, and can seed them with a starting value. | - |
| `ReplayLatestPublish()` | Shares one subscription and replays the newest value to late subscribers. | - |
| `StatelessPublish()` | Shares one subscription and keeps nothing between connections. | - |
| `StatelessReplayLatestPublish()` | Replays the newest value inside a connection and keeps nothing across connections. | - |
| `RefCount()` | Connects on the first subscriber and disconnects when the last one leaves. | - |
| `ReplayLastOnSubscribe(initialValue)` | Shares one subscription and replays the newest value, in a single call. | - |
| `Wrap()` | Wraps your observer so it receives one notification at a time, receives nothing after disposal, and cannot fault the producer. | - |
| `AsObserverAsync()` | Exposes a signal as a plain observer, so callers can only write to it. | - |
| `MapValues(mapper)` | Returns a signal whose read side runs through the pipeline you give it. | - |
| `TryGetValue(out value)` | Reads an `Optional<T>` with the try pattern. | - |
| `ToDisposableAsync()` | Adapts a plain `IDisposable` to the async disposable subscriptions use. | - |
| `UnhandledExceptionHandler.Register(handler)` | Installs the process-wide sink for exceptions no operator can deliver downstream. | - |
| `DisposableAsync.Create(action)` | Builds an async disposable, with a state overload that avoids a closure. | - |

A connectable stream sits still until you call `ConnectAsync` or add `RefCount`. That is how you stop a shared source from running once per subscriber.

These move callbacks onto a context you choose. They ship in the `ReactiveUI.Primitives.Async` package.

| Operator | What it does | Sync and async predicate forms |
|---|---|---|
| `WitnessOn(target)` | Moves observer callbacks onto an async context, a synchronization context, a task scheduler or a sequencer. | - |
| `ObserveOnSafe(target)` | Works like `WitnessOn`, and leaves the stream alone when the target is null. | - |
| `ObserveOnIf(condition, target)` | Switches context only when the condition is true. | - |
| `Yield()` | Yields before each value, so a fast producer cannot starve the consumer. | - |
| `IsSameAsCurrentAsyncContext()` | Tells you whether that context is the one running right now. | - |

```csharp
var values = await SignalAsync.Range(1, 2)
    .Tap(n => Console.WriteLine("saw " + n))
    .ToListAsync();
// saw 1
// saw 2
```

```csharp
var shared = SignalAsync.Interval(TimeSpan.FromSeconds(1)).Publish().RefCount();

await using var a = await shared.SubscribeAsync(t => Console.WriteLine("a " + t));
await using var b = await shared.SubscribeAsync(t => Console.WriteLine("b " + t));
// a 1
// b 1
// one timer feeds both subscribers
```

## Subjects and stateful signals

Some types are both a source and a sink. You push values into them, and subscribers read those values out.
This document calls such a type a **signal**. A **subscriber** is code that receives the values a signal sends.
Every signal below lives in the `ReactiveUI.Primitives.Signals` namespace unless a row says otherwise.

A signal sends three kinds of notification. `OnNext` carries a value. `OnError` carries an exception and ends the
signal. `OnCompleted` ends the signal with no error. The last two are **terminal**, because nothing follows them.

| Type | What it is | When you reach for it |
|---|---|---|
| `Signal<T>` | The plain signal. It passes each value straight to the current subscribers. `SubscribeAction(Action<T>)` subscribes with a plain action. | You want a simple hub. One piece of code pushes values, several pieces listen. |
| `BehaviorSignal<T>` | Keeps the most recent value and replays it to each new subscriber. Exposes `Value` and `TryGetValue`. | A late subscriber must see the current value at once, not wait for the next one. |
| `StateSignal<T>` | Holds a value you can read and write through `Value`. Writing sends the value to subscribers. Also offers `Changed`, `Refresh()`, `TryGetValue` and `ToReadOnlyState(selector)`, which returns a `ProjectedReadOnlyState<T, TResult>`. | You are modelling a piece of state, such as a property on a view model. |
| `ReplaySignal<T>` | Buffers past values and replays them to each new subscriber. You bound the buffer by count, by age, or by both. | A new subscriber needs recent history, not just the latest value. |
| `SerializedSignal<T>` | Wraps another signal. Producers on any number of threads deliver one at a time. | Several threads push into the same signal. |
| `CurrentValueSignal<T>` | Reads a value that something else owns. It emits on subscribe and again on every change. Observable only. | You are bridging a value that already has its own change notification, such as a control property. |
| `AsyncSignal<T>` | Records the latest value and replays it when the signal completes. Offers `IsCompleted`, `Value`, `GetAwaiter`, `GetResult` and `RemoveObserver`, so you can await it. | You want one final answer, and you want to `await` it. |
| `ScheduledSignal<T>` | Sends its notifications through an `ISequencer`. | Subscribers must run on a particular thread, such as a UI thread. |
| `DelayableNotificationSignal<T>` | Passes values through while a test you supply says "not delayed". It buffers them while that test says "delayed", then `Flush()` emits the batch with duplicates removed. Build one with `Signal.Delayable`. | You want to hold back a burst of notifications and release it as one batch. |
| `PrioritySemaphoreSignal<T>` | Forwards at most `MaximumCount` values at a time and releases the rest in priority order. `MaximumCount` is settable, and `Release()` frees one slot. `T` must implement `IComparable<T>`. | You need to cap how much work is in flight and run the most important item first. |
| `CommandSignal<TResult>` | An action you can run, exposed as a signal. Offers `CanRun`, `IsRunning` (a `StateSignal<bool>`), `Results`, `Faults` and `ExecuteAsync([CancellationToken])`, which returns a `CommandExecution<TResult>`. | A button or a menu item runs an operation, and the screen tracks whether it may run and whether it is running. |
| `ITaskSignal<T>` | What `Signal.FromTask` hands back. It carries the task's result as a signal, and adds `CancellationTokenSource`, `IsCancellationRequested`, `Source` and `GetOperationCanceled(observer)`. Observable only. | You started work through `Signal.FromTask` and need to cancel it or read why it stopped. |
| `ReadOnlyState<T>` | A read-only view of a latest value, built from a source and an initial value. Offers `Value` and `Changed`. | You want to hand out state that callers can read but not write. |
| `CurrentValueSubject<T>` | Keeps the latest value and replays it to new subscribers. Lives in `ReactiveUI.Primitives.Extensions`. | You want a latest-value signal from the extensions surface. |

### Worked examples

These five cover most day-to-day work.

`Signal<T>` sends each value to whoever is listening at the time. A subscriber that arrives later misses what it
missed.

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;

var ticks = new Signal<int>();

using var subscription = ticks.Subscribe(value => Console.WriteLine(value));

ticks.OnNext(1);
ticks.OnNext(2);
ticks.OnCompleted();
```

`BehaviorSignal<T>` hands the current value to every new subscriber. You give it a starting value.

```csharp
var temperature = new BehaviorSignal<double>(21.5);

temperature.OnNext(26.2);

// Prints 26.2 straight away, then every later value.
using var subscription = temperature.Subscribe(value => Console.WriteLine(value));

Console.WriteLine(temperature.Value); // 26.2
```

`StateSignal<T>` is the one to use for state you own. You set `Value`, and subscribers see the new value. Every
assignment notifies, even when you assign the value it already holds.

```csharp
var temperature = new StateSignal<double>(21.5);
var status = temperature.ToReadOnlyState(value => value >= 25.0 ? "warm" : "normal");

using var subscription = status.Changed.Subscribe(Console.WriteLine);

temperature.Value = 26.2; // prints "warm"
temperature.Refresh();    // sends the current value again
```

`ReplaySignal<T>` keeps a buffer. Give it a buffer size and a window. Pass `TimeSpan.MaxValue` when you do not
want to drop values by age.

```csharp
var history = new ReplaySignal<string>(bufferSize: 2, window: TimeSpan.MaxValue);

history.OnNext("A");
history.OnNext("B");
history.OnNext("C");

// Replays B and C, then sends later values.
using var subscription = history.Subscribe(Console.WriteLine);
```

`SerializedSignal<T>` is what you wrap around a signal that several threads push into. It delivers one notification
at a time. The parameterless constructor wraps a new `Signal<T>` for you.

```csharp
var readings = new SerializedSignal<int>();

using var subscription = readings.Subscribe(value => Console.WriteLine(value));

Parallel.For(0, 100, readings.OnNext);
```

### The async subject family

The `ReactiveUI.Primitives.Async` package ships signals that deliver through `ValueTask` instead of a plain method
call. They live in the `ReactiveUI.Primitives.Async` namespace.

You pick between them on two questions. First, do your producers push one at a time, or do several threads push at
once? Choose a `Serial` type for one at a time, and a `Concurrent` type when several threads push. Second, does a
new subscriber need the latest value? Choose a `ReplayLatest` type when it does.

That gives you `SerialSignalAsync`, `ConcurrentSignalAsync`, `SerialReplayLatestSignalAsync` and
`ConcurrentReplayLatestSignalAsync`. Each one has a `Stateless` variant: `SerialStatelessSignalAsync`,
`ConcurrentStatelessSignalAsync`, `SerialStatelessReplayLatestSignalAsync` and
`ConcurrentStatelessReplayLatestSignalAsync`.

## Scheduling work

An `ISequencer` decides when scheduled work runs and which thread runs it. You hand one to any operator or signal
that deals with time or threads. Sequencers live in the `ReactiveUI.Primitives.Concurrency` namespace.

The library defines its own scheduling interface for three reasons. The core package depends on no UI framework and
no other reactive library, so it stays small. A sequencer measures time with a monotonic timestamp, so a clock
change cannot disturb scheduled work. A test can swap in a sequencer that controls time, so a time-based test needs
no real waiting.

An `ISequencer` schedules an `IWorkItem` to run now or at a timestamp. `SequencerExtensions` adds the `Schedule`
conveniences that take an action, with or without a delay.

| Sequencer | What it does |
|---|---|
| `Sequencer.CurrentThread` | Queues work on the calling thread and runs it in order. Also available as `CurrentThreadSequencer.Instance`. |
| `Sequencer.Immediate` | Runs the work right away on the calling thread. Also available as `ImmediateSequencer.Instance`. |
| `Sequencer.Default` | The default choice for background work. It is `TaskPoolSequencer.Default`. |
| `TaskPoolSequencer` | Runs work through a `TaskFactory`. Use `TaskPoolSequencer.Instance`, or pass your own factory. |
| `ThreadPoolSequencer` | Runs work on the thread pool. Use `ThreadPoolSequencer.Instance`. |
| `SynchronizationContextSequencer` | Posts work to a `SynchronizationContext`. Use `SynchronizationContextSequencer.Current`, or pass a context. |
| `WasmSequencer` | Runs work on a browser's single-threaded event loop. Use `WasmSequencer.Default`. |
| `VirtualClock` | Controls time in a test, using `DateTimeOffset` and `TimeSpan`. You advance the clock, so nothing sleeps. |
| `VirtualTimeSequencer<TAbsolute, TRelative>` | Controls time in a test using your own clock types. |

Use a virtual sequencer for a time-based test. Do not sleep a real thread.

### Platform sequencers

Each UI framework gets its own package, so the core package pulls in none of them.

| Sequencer | Framework | Package |
|---|---|---|
| `DispatcherSequencer` | WPF | `ReactiveUI.Primitives.Wpf` |
| `ControlSequencer` | Windows Forms | `ReactiveUI.Primitives.WinForms` |
| `DispatcherQueueSequencer` | WinUI | `ReactiveUI.Primitives.WinUI` |
| `AvaloniaScheduler` | Avalonia | `ReactiveUI.Primitives.Avalonia` |
| `MauiDispatcherSequencer` | MAUI | `ReactiveUI.Primitives.Maui` |
| `BlazorRendererSequencer` | Blazor | `ReactiveUI.Primitives.Blazor` |
| `HandlerSequencer` | Android | `ReactiveUI.Primitives`, on the Android targets |
| `NSRunloopSequencer` | Apple platforms | `ReactiveUI.Primitives`, on the Apple targets |

`ObserveOn` moves notifications onto a sequencer. Values arrive on that sequencer's thread, so you can touch the
screen from your subscriber.

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

var readings = new Signal<double>();
var uiSequencer = new DispatcherSequencer(Dispatcher.CurrentDispatcher);

using var subscription = readings
    .ObserveOn(uiSequencer)
    .Subscribe(value => Console.WriteLine(value));
```

`WasmSequencer`, `DispatcherQueueSequencer`, `MauiDispatcherSequencer` and `BlazorRendererSequencer` behave the
same way through their own framework. Each one batches ready work into a single posted drain, so a burst of values
costs one trip to the UI thread rather than one trip per value.

## Disposing subscriptions

`Subscribe` returns an `IDisposable`. Dispose it and your subscriber detaches. No later notification reaches it.
The signal itself keeps running, and its other subscribers keep receiving values.

A `using` statement is enough for a subscription that lives as long as the enclosing method. For anything longer,
keep the handle in a field, or collect it with one of the types below.

The disposable primitives ship in the `ReactiveUI.Disposables` package, in the `ReactiveUI.Primitives.Disposables`
namespace. Note that the namespace and the package name differ.

| Type | What it does |
|---|---|
| `Scope` | A set of factory methods. `Scope.Empty` does nothing. `Scope.Create(action)` runs your action on dispose. `Scope.Create(state, action)` does the same without capturing a closure. `Scope.Combine(...)` joins several into one. |
| `DisposableSet` | A group of disposables that holds its first entries inline. It is a `record struct`, so an owner can hold several disposables without allocating a container. |
| `MultipleDisposable` | A group you can add to and remove from. It implements `ICollection<IDisposable>`. Disposing it disposes everything inside. |
| `Pocket` | The same collection behaviour as `MultipleDisposable`, built on `DisposableSet`. |
| `DisposableBag` | An add-only group for the "collect now, dispose once" case. It disposes each entry exactly once, in the order you added them. |
| `Slot` | Holds one disposable. Assigning a new one disposes the one it replaces. |
| `SingleDisposable` | Holds one disposable that you assign once. |
| `MutableDisposable` | Exposes a settable `Disposable` property. Setting it disposes the previous value. |
| `ActionDisposable` | Runs an `Action` the first time you dispose it. |
| `BooleanDisposable` | Sets `IsDisposed` to true and nothing else. Use it as a cheap "should I stop?" flag. |
| `CancellationDisposable` | Cancels a `CancellationTokenSource` on dispose and exposes its `Token`. |
| `EmptyDisposable` | Does nothing. Use the shared `EmptyDisposable.Instance`. |

`DisposeWith` adds a subscription to a `MultipleDisposable` and gives the subscription straight back, so you can
chain it onto the call that created it. Dispose the group and every subscription in it detaches.

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

var temperature = new StateSignal<double>(21.5);
var pressure = new StateSignal<double>(1013.0);

using var subscriptions = new MultipleDisposable();

temperature.Changed
    .Subscribe(value => Console.WriteLine($"temperature {value}"))
    .DisposeWith(subscriptions);

pressure.Changed
    .Subscribe(value => Console.WriteLine($"pressure {value}"))
    .DisposeWith(subscriptions);

// Leaving the scope disposes both subscriptions.
```

A warning about the `record struct` types, which include `DisposableSet`. Hold one in a field and use it in place.
Copying one gives you a second, separate set, and disposing the copy leaves the original untouched.

## Threading, errors and disposal rules

These are the rules to keep in mind. They decide what your code sees when something goes wrong.

**A signal delivers to one subscriber at a time, and holds no lock while your code runs.** A signal locks only
long enough to update its own state, such as its subscriber list or its buffer. It releases that lock before it
calls you. So your subscriber can push into another signal, take its own lock, or block, without deadlocking the
signal that called it.

**Ordering across threads is a choice you make.** `Signal<T>` does not order concurrent producers. Two threads that
call `OnNext` at the same time reach your subscriber at the same time. Wrap it in `SerializedSignal<T>` when you
need one notification at a time. `ReplaySignal<T>` orders its notifications the same way, so each subscriber sees
its replay first and every later value once, in the order they were sent.

**When your subscriber throws, the exception travels back to the code that pushed the value.** A signal does not
catch it. The call to `OnNext` throws, and the subscribers after yours in the list never see that value. So catch
exceptions inside your subscriber, and keep the work there short.

**When you push to a disposed signal, you get an `ObjectDisposedException`.** This covers `OnNext`, `OnError`,
`OnCompleted` and `Subscribe`. Reading `Value` on a disposed `BehaviorSignal<T>` or `StateSignal<T>` throws the
same exception, and `TryGetValue` returns false instead. `CurrentValueSubject<T>` is the exception to the rule: it
ignores a value pushed after disposal.

**A terminal notification ends the signal for good.** After `OnCompleted` or `OnError`, the signal drops every
later value. A subscriber that arrives after that point receives the terminal notification at once, rather than
waiting for something that never comes.

**`OnError` needs a real exception.** Pass `null` and you get an `ArgumentNullException`.

**A value-only subscriber rethrows a terminal error to the producer.** `Subscribe(onNext)` has nowhere to deliver
an error, so the error surfaces on the thread that called `OnError`. Pass an error handler,
`Subscribe(onNext, onError)`, whenever the signal can fail.

**Disposing a group disposes what is inside it.** Disposing a `MultipleDisposable`, `Pocket`, `DisposableBag` or
`Slot` disposes the disposables it holds. Disposing any of them twice is safe, and each entry is disposed once.
Adding to a group that is already disposed disposes the incoming disposable straight away, so a subscription can
never outlive the group you handed it to.

## Why not System.Reactive or R3?

System.Reactive is the original Rx library for .NET. It is the reason `IObservable<T>` exists. It is mature and widely
used. Its weak point is speed. A typical operator chain allocates several objects per operator and per value. That cost
grows under heavy load.

R3 is a newer library aimed at that weak point. R3 is fast. It reaches that speed partly by replacing `IObservable<T>`
with its own `Observable<T>` type. That swap breaks existing code. It also breaks the wider ecosystem built on
`IObservable<T>`.

We wanted the speed without the break. So we kept `IObservable<T>`, the interface .NET already ships. Our benchmarks
told us the interface was not the bottleneck. The cost lived in how the operators were written. So we kept the familiar
contract and rebuilt the operators as low-allocation sinks. See
[Why the operators are built this way](#why-the-operators-are-built-this-way).

This keeps your change small if you already use `IObservable<T>`. You keep the contract and the mental model. You gain
the lower allocation profile. When you need full System.Reactive or R3 behaviour, the `.Reactive` package variants and
the R3 source-generator bridges cover those boundaries.

### Two ways to build an operator

System.Reactive and R3 build operators from a small set of general parts. `Synchronize` shows the idea well. It is one
operator, and you compose it with any other operator when you need values delivered one at a time. That design keeps the
library small. It also reads well in a chain.

This library takes the other path. Each operator has its own sink. A sink that needs to deliver one value at a time
builds that in. You get more types as a result. You also get a sink that does its own job with no general layer between
it and your code.

Neither path is wrong. They trade different things. The composed path costs less surface area. The dedicated path costs
more classes and pays you back in speed and allocations. We chose speed, and we accepted the extra classes to get it.

### Where we could not stay on the standard types

Keeping `IObservable<T>` and `IObserver<T>` was easy. Both ship in .NET itself. Two related types do not, so we had to
make a call.

The first is the scheduler. A scheduler decides when and on which thread work runs. .NET has no scheduler type of its
own. The standard one, `IScheduler`, lives in System.Reactive. Using it would pull System.Reactive back in as a runtime
dependency, and that is the dependency we set out to avoid. So the lean library defines its own small scheduling
contract, `ISequencer`.

The second is `Unit`. `Unit` means "a value carrying no information". You use it for streams that report that something
happened but carry no data. .NET has no such type. The common `Unit` also lives in System.Reactive. So the lean library
defines its own, `RxVoid`.

These two types are the only places the lean surface departs from the System.Reactive shape. The `.Reactive` package
variants close that gap. They recompile the same source with `ISequencer` mapped to `IScheduler` and `RxVoid` mapped to
`System.Reactive.Unit`. Code that already speaks System.Reactive sees the types it expects.

Disposal groups are a third seam, and the shared types cannot close it on their own. `MultipleDisposable` ships in the
dependency-free `ReactiveUI.Disposables` package, so it cannot name `CompositeDisposable`.
`ReactiveUI.Primitives.Reactive` adds `ContainerDisposable` for that job. A `ContainerDisposable` is a
`MultipleDisposable` that converts implicitly to a `CompositeDisposable` it owns and disposes. Hand one to `DisposeWith`,
to a library that takes a `CompositeDisposable`, or to your own helper, and it works. Anything you register through the
composite is disposed with the container.

## Observable event source generation

A source generator is a compiler component that writes extra C# code into your project at build time.
`ReactiveUI.Primitives.ObservableEvents` is a source generator that turns .NET events into observables. It depends on no
particular observable implementation. It inspects your project and emits adapters for the first provider it finds:

- `ReactiveUI.Primitives.Signals.Signal` and `RxVoid` for lean Primitives projects.
- `ReactiveUI.Primitives.Reactive.Signals.Signal` and `System.Reactive.Unit` for `.Reactive` projects.
- `System.Reactive.Linq.Observable` and `System.Reactive.Unit` for System.Reactive projects that reference no
  ReactiveUI.Primitives package.

Install it alongside the observable provider your application already uses:

```bash
dotnet add package ReactiveUI.Primitives.ObservableEvents
```

To generate instance events, call `Events()` once on the object that declares them. The generator replaces that call
with a strongly typed wrapper. Each property on the wrapper subscribes and unsubscribes from the matching public event:

```csharp
using ReactiveUI.Primitives.ObservableEvents;

IObservable<EventArgs> changes = viewModel.Events().Changed;
```

To generate public static events, add an assembly attribute. The generator writes static observable properties on
`RxEvents` in the namespace of the type that declares the events. Property names prefix each host and event identifier
with its length, so type, nesting, and event-name boundaries cannot collide:

```csharp
[assembly: ReactiveUI.Primitives.ObservableEvents.GenerateStaticEventObservables(typeof(AppEvents))]

IObservable<string> messages = RxEvents.T9AppEvents7Message;
```

The value type of each observable follows the delegate. A delegate with no parameters gives `RxVoid` or `Unit`. One
parameter gives that parameter. A conventional `(object sender, TEventArgs args)` event gives the event-args parameter.
Any other multi-parameter delegate gives a named tuple. Delegates that return `void`, `Task`, or `ValueTask` all work.
An unsupported signature reports `RXOE003`. A missing provider reports `RXOE001`. An empty request reports `RXOE002`.

## Source-generator bridge behavior

R3 and R3Async bridge generation is opt-in. Install the standalone analyzer package:

```bash
dotnet add package ReactiveUI.Primitives.R3Bridge.Generator
```

The package ships one analyzer assembly, `ReactiveUI.Primitives.R3Bridge.Generator.dll`. Neither
`ReactiveUI.Primitives` nor `ReactiveUI.Primitives.Async` contains it, and neither depends on it. Add the generator
package only to projects that need generated R3 or R3Async bridge methods.

The assembly holds two generators:

- `R3BridgeGenerator` for R3 `Observable<T>` boundaries and R3-to-Primitives.Async adapters.
- `R3AsyncBridgeGenerator` for R3Async `AsyncObservable<T>` boundaries.

The generator stamps your assembly with an assembly metadata attribute:

```csharp
[assembly: System.Reflection.AssemblyMetadata("ReactiveUI.Primitives.R3Bridge.Generator", "0.1.0")]
```

It generates no custom marker attribute type. That keeps generated type identities unique across project-reference and
`InternalsVisibleTo` builds. It also avoids the CS0436 warning you get when two compilations generate the same internal
marker type.

The generator emits bridge extension methods only when your project already references the matching external symbols:

- The R3 bridge checks for `R3.Observable<T>`, `R3.Observer<T>`, and `R3.Result`.
- The R3-to-Primitives.Async bridge checks for the same R3 symbols plus
  `ReactiveUI.Primitives.Async.IObservableAsync<T>`.
- The R3Async bridge checks for `R3Async.AsyncObservable<T>`, `R3Async.AsyncObserver<T>`, `R3Async.Result`, and
  `ReactiveUI.Primitives.Async.IObservableAsync<T>`.

Generated code lands in the `ReactiveUI.Primitives.R3Bridge` namespace.

Generated R3 bridge methods:

- `AsPrimitivesSignal<T>(this R3.Observable<T> source)`
- `AsR3Observable<T>(this System.IObservable<T> source)`
- `AsPrimitivesAsyncObservable<T>(this R3.Observable<T> source)` when you reference `ReactiveUI.Primitives.Async`
- `AsR3Observable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when you reference
  `ReactiveUI.Primitives.Async`

Generated R3Async bridge methods:

- `AsPrimitivesAsyncObservable<T>(this R3Async.AsyncObservable<T> source)` when you reference R3Async and
  `ReactiveUI.Primitives.Async`
- `AsR3AsyncObservable<T>(this ReactiveUI.Primitives.Async.IObservableAsync<T> source)` when you reference R3Async and
  `ReactiveUI.Primitives.Async`

Use the R3 bridge when your project references R3 and the generator package:

```bash
dotnet add package ReactiveUI.Primitives
dotnet add package ReactiveUI.Primitives.R3Bridge.Generator
dotnet add package R3
```

```csharp
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.R3Bridge;
using ReactiveUI.Primitives.Signals;

// R3.Observable<int> r3Source = ...;
IObservable<int> primitivesSource = r3Source.AsPrimitivesSignal();
R3.Observable<int> r3Again = Signal.Sequence(1, 3).AsR3Observable();
```

Use the R3 async bridge when your project references R3, `ReactiveUI.Primitives.Async`, and the generator package:

```bash
dotnet add package ReactiveUI.Primitives.Async
dotnet add package ReactiveUI.Primitives.R3Bridge.Generator
dotnet add package R3
```

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge;

// R3.Observable<int> r3Source = ...;
IObservableAsync<int> primitivesAsync = r3Source.AsPrimitivesAsyncObservable();
R3.Observable<int> r3Again = primitivesAsync.AsR3Observable();
```

Use the R3Async bridge when your project references R3Async, `ReactiveUI.Primitives.Async`, and the generator package:

```bash
dotnet add package ReactiveUI.Primitives.Async
dotnet add package ReactiveUI.Primitives.R3Bridge.Generator
dotnet add package R3Async
```

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge;

// R3Async.AsyncObservable<int> r3AsyncSource = ...;
IObservableAsync<int> primitivesAsync = r3AsyncSource.AsPrimitivesAsyncObservable();
R3Async.AsyncObservable<int> r3AsyncAgain = primitivesAsync.AsR3AsyncObservable();
```

These R3 snippets are migration shapes. They work only when your application references R3 or R3Async and opts into
`ReactiveUI.Primitives.R3Bridge.Generator`. ReactiveUI.Primitives itself carries no R3 or R3Async runtime dependency.
System.Reactive interop lives in the `.Reactive` package variants, which recompile the same Primitives APIs against
System.Reactive `Unit` and `IScheduler`.

## Moving from System.Reactive

ReactiveUI.Primitives is not a clone of System.Reactive. It keeps the standard `IObservable<T>` contracts. It favours a
smaller runtime, explicit state types, and Primitives naming. Migrate one slice at a time: factories first, then
subject and state types, then operators and schedulers.

Your project may need to keep System.Reactive `Unit` or `IScheduler` in its public surface. Use
`ReactiveUI.Primitives.Reactive` or `ReactiveUI.Primitives.Async.Reactive` for that. `ReactiveUI.Primitives.Reactive`
includes the helpers from `ReactiveUI.Primitives.Extensions.Reactive`. To drop those public System.Reactive types
instead, use the lean packages and the mappings below.

### Track 1: move an existing project

Take this track when the project should stop exposing System.Reactive types and move to the lean ReactiveUI.Primitives
package family.

1. List your references and public API. Mark every project that exposes `System.Reactive.Unit`, `IScheduler`,
   `IObservable<T>` extension methods, UI schedulers, `Subject<T>` types, or ReactiveUI.Extensions helpers.
2. Add the lean packages the project needs:

```bash
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Async
```

3. Add a UI integration package only when the project owns UI-thread dispatch:

```bash
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Wpf
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.WinForms
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.WinUI
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Blazor
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Avalonia
dotnet add xyz/xyz.csproj package ReactiveUI.Primitives.Maui
```

4. Convert boundary types one at a time. `System.Reactive.Unit` becomes `RxVoid`. `IScheduler` becomes `ISequencer`. Rx
   subjects become `Signal<T>`, `StateSignal<T>`, `ReplaySignal<T>`, or `FinalSignal<T>`. Composite disposable types
   become `MultipleDisposable`, `Pocket`, `Slot`, or `AssignmentSlot`.
5. Keep the code compiling on the first pass with the Rx-name compatibility layer: `Select`, `Where`, `Aggregate`,
   `Scan`, `Merge`, `Concat`, `CombineLatest`, `SelectMany`, and related aliases. Then move hot paths to the Primitives
   names `Map`, `Keep`, `Reduce`, `Fold`, `Blend`, `Chain`, `SyncLatest`, and `FlatMap` where they read better.
6. Replace scheduler construction and test scheduling. Use `Sequencer.Immediate`, `Sequencer.CurrentThread`,
   `ThreadPoolSequencer.Instance`, `TaskPoolSequencer.Instance`, a UI sequencer, or `VirtualClock`.
7. Remove the `System.Reactive` and `ReactiveUI.Extensions` package references last. Wait until the project builds
   without `System.Reactive.Linq`, `System.Reactive.Subjects`, `System.Reactive.Disposables`, and
   `System.Reactive.Concurrency` imports.
8. Run the tests and the package and API approval checks. For time-sensitive tests, use virtual time instead of real
   sleeps.

### Track 2: add a new `xyz.Reactive` project

Take this track when your consumers need source compatibility with Rx while your implementation moves to
ReactiveUI.Primitives. Keep or create a lean `xyz` package. Add an `xyz.Reactive` package that references the
`.Reactive` Primitives range.

1. Move shared implementation files into a source folder that both projects link.
2. In that shared source, use the neutral names `RxVoid` and `ISequencer`. The lean project binds them to
   ReactiveUI.Primitives types. The `.Reactive` project binds them to `System.Reactive.Unit` and
   `System.Reactive.Concurrency.IScheduler`.
3. Gate the namespace when the public namespace must differ:

```csharp
#if REACTIVE_SHIM
namespace xyz.Reactive;
#else
namespace xyz;
#endif
```

4. Reference the `.Reactive` packages from `xyz.Reactive`:

```bash
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Async.Reactive
```

5. Add a reactive UI package only when the project exposes UI scheduling:

```bash
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Wpf.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.WinForms.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.WinUI.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Blazor.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Avalonia.Reactive
dotnet add xyz.Reactive/xyz.Reactive.csproj package ReactiveUI.Primitives.Maui.Reactive
```

6. Define `REACTIVE_SHIM` in the reactive project and alias the System.Reactive types. Skip this step if your
   `Directory.Build.props` already does it:

```xml
<PropertyGroup>
  <DefineConstants>$(DefineConstants);REACTIVE_SHIM</DefineConstants>
</PropertyGroup>
<ItemGroup>
  <Using Include="System.Reactive.Unit" Alias="RxVoid" />
  <Using Include="System.Reactive.Concurrency.IScheduler" Alias="ISequencer" />
</ItemGroup>
```

7. Change no source in the first `xyz.Reactive` pass. Keep the Rx names `Select`, `Where`, `SelectMany`,
   `CombineLatest`, `Merge`, `Concat`, `Throttle`, and `WithLatestFrom` where compatibility matters. The `.Reactive`
   packages supply those names over the Primitives implementation.
8. Build both packages side by side. `xyz` should carry no System.Reactive runtime dependency. `xyz.Reactive` should
   keep its System.Reactive-facing APIs for existing consumers.

### Factory mapping

| System.Reactive                     | ReactiveUI.Primitives                                                            | Notes                                                          |
|-------------------------------------|----------------------------------------------------------------------------------|----------------------------------------------------------------|
| `Observable.Return(value)`          | `Signal.Emit(value)` or `Signal.Return(value)`                                   | Emits one value and completes.                                 |
| `Observable.Empty<T>()`             | `Signal.None<T>()` or `Signal.Empty<T>()`                                        | Completes immediately.                                         |
| `Observable.Never<T>()`             | `Signal.Silent<T>()`, `Signal.Silent<T>(witness)`, or `Signal.Never<T>()`        | Non-terminating signal; witness overload helps type inference. |
| `Observable.Throw<T>(ex)`           | `Signal.Fail<T>(ex)` or `Signal.Throw<T>(ex)`                                    | Emits terminal error.                                          |
| `Observable.Range(start, count)`    | `Signal.Sequence(start, count)` or `Signal.Range(start, count)`                  | Optional scheduler overload exists.                            |
| `Observable.Repeat(value)`          | `Signal.Loop(value)` or `Signal.Repeat(value)`                                   | Indefinite repeat.                                             |
| `Observable.Repeat(value, count)`   | `Signal.Loop(value, count)` or `Signal.Repeat(value, count)`                     | Fixed repeat.                                                  |
| `Observable.Defer(factory)`         | `Signal.Lazy(factory)` or `Signal.Defer(factory)`                                | Create source per subscription.                                |
| `Observable.FromAsync(...)`         | `Signal.FromAsync(...)`                                                          | Invoke a task factory per subscription.                        |
| `Observable.Create<T>(...)`         | `Signal.Create<T>(...)` or `Signal.CreateSafe<T>(...)`                           | Prefer `CreateSafe` for general custom sources.                |
| `Observable.Using(...)`             | `Signal.Use(...)` or `Signal.Using(...)`                                         | Resource scoped to subscription.                               |
| `Observable.Timer(dueTime)`         | `Signal.After(dueTime)` or `Signal.Timer(dueTime)`                               | Emits `long` tick `0`. `Timer` also takes a `DateTimeOffset`.  |
| `Observable.Timer(dueTime, period)` | `Signal.After(dueTime, period)` or `Signal.Timer(dueTime, period)`               | Periodic `long` ticks.                                         |
| `Observable.Interval(period)`       | `Signal.Pulse(period)`, `Signal.Every(period)`, or `Signal.Interval(period)`     | Repeating ticks.                                               |
| `ToObservable()` from enumerable    | `Signal.FromEnumerable(values)`, `values.ToSignal()`, or `values.ToObservable()` | Cancellation-token overloads are available.                    |
| task conversion                     | `Signal.FromTask(task)`                                                          | Function-based task signals also exist.                        |
| `Observable.Generate(...)`          | `Signal.Generate(state, condition, iterate, selector)`                           | Unfolds a state into a sequence.                               |
| `Observable.If(condition, then)`    | `Signal.If(condition, then)` or `Signal.If(condition, then, else)`               | Chooses a source per subscription.                             |
| `Observable.Case(selector, map)`    | `Signal.Case(selector, sources)` or `Signal.Case(selector, sources, default)`    | Chooses a source by key per subscription.                      |
| `Observable.Concat(sources)`        | `Signal.Concat(sources)`                                                         | Subscribes to each source in turn.                             |
| `Observable.Merge(sources)`         | `Signal.Merge(sources)`                                                          | Subscribes to every source at once.                            |
| `Observable.Switch(sources)`        | `Signal.Switch(sources)`                                                         | Follows the most recent inner source.                          |
| `Observable.OnErrorResumeNext(...)` | `Signal.OnErrorResumeNext(sources)`                                              | Continues with the next source after an error.                 |

### Subject and state mapping

| System.Reactive                    | ReactiveUI.Primitives             | Migration detail                                                 |
|------------------------------------|-----------------------------------|------------------------------------------------------------------|
| `new Subject<T>()`                 | `new Signal<T>()`                 | Use `OnNext`, `OnError`, `OnCompleted`, and `Subscribe`.         |
| `new BehaviorSubject<T>(initial)`  | `new StateSignal<T>(initial)`     | Keeps `Value` getter/setter and emits changes through `Changed`. |
| mutable reactive property          | `new StateSignal<T>(initial)`     | Set `Value` to emit. Use `Changed` for observable state stream.  |
| `new ReplaySubject<T>()`           | `new ReplaySignal<T>()`           | Unbounded replay.                                                |
| `new ReplaySubject<T>(bufferSize)` | `new ReplaySignal<T>(bufferSize)` | Size-limited replay.                                             |
| `new ReplaySubject<T>(window)`     | `new ReplaySignal<T>(window)`     | Time-window replay.                                              |
| `new AsyncSubject<T>()`            | `new FinalSignal<T>()`            | Awaitable final-value signal shape.                              |

### Operator mapping

| System.Reactive                  | ReactiveUI.Primitives                                         | Notes                                                                                               |
|----------------------------------|---------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| `Select`                         | `Map`                                                         | Prefer `Map` for distinct Primitives style.                                                         |
| `Where`                          | `Keep`                                                        | Predicate filtering.                                                                                |
| `SelectMany`                     | `FlatMap`, `Bind`, or Rx-name `SelectMany`                    | Observable overloads preserve concurrent merge semantics; enumerable overloads flatten inline.      |
| `Aggregate`                      | `Reduce`                                                      | Emits final accumulated value on completion.                                                        |
| `Scan`                           | `Fold`                                                        | Emits every accumulated value.                                                                      |
| `Do`                             | `Tap`                                                         | Side effect while preserving values.                                                                |
| `Take` / `Skip`                  | `Take` / `Skip`                                               | Count-based overloads.                                                                              |
| `TakeWhile` / `SkipWhile`        | `TakeWhile` / `SkipWhile`                                     | Predicate-based.                                                                                    |
| `Distinct`                       | `Distinct`                                                    | Full seen-set distinct.                                                                             |
| `DistinctUntilChanged`           | `Unique`                                                      | Adjacent dedupe.                                                                                    |
| `OfType` / `Cast`                | `KeepType` / `CastTo`                                         | Object-source projections.                                                                          |
| `Materialize`                    | `Spark`                                                       | Converts notifications into `Spark<T>`.                                                             |
| `Dematerialize`                  | `Unspark`                                                     | Converts `Spark<T>` values back into notifications.                                                 |
| `Where` + `Select`               | `Choose`                                                      | Single fused sink; chooser returns `(HasValue, Value)` so a non-nullable value type can be skipped. |
| `Merge`                          | `Blend` or `Signal.Blend`                                     | Works over source-of-sources and params factories.                                                  |
| `Merge` + `DistinctUntilChanged` | `BlendUnique`                                                 | Single fused merge + adjacent dedupe over a params source set.                                      |
| `Concat`                         | `Chain` or `Signal.Chain`                                     | Sequential composition.                                                                             |
| `Amb`                            | `Race`                                                        | First source to produce a value or terminal signal wins.                                            |
| `Switch`                         | `SwitchTo`                                                    | Latest inner observable wins.                                                                       |
| `Select` + `Switch`              | `SwitchSelect`                                                | Filters null source values, projects each to an inner observable, and mirrors only the latest.      |
| `Zip`                            | `Pair` or `Signal.Pair`                                       | Pair values by index.                                                                               |
| `CombineLatest`                  | `SyncLatest`, Rx-name `CombineLatest`, or `Signal.SyncLatest` | Latest values after all sources have emitted; overloads support up to 16 total sources.             |
| `WithLatestFrom`                 | `Latch`                                                       | Left emission paired with latest right value.                                                       |
| `ForkJoin`                       | `ForkJoin`                                                    | Last values after completion.                                                                       |
| `Throttle`                       | `Calm` / `Stabilize`                                          | Quiet-period emission.                                                                              |
| `Sample`                         | `Probe`                                                       | Periodic latest-value sampling.                                                                     |
| `Delay`                          | `Shift`                                                       | Delay emitted values.                                                                               |
| `DelaySubscription`              | `DelayStart`                                                  | Delay source subscription.                                                                          |
| `Timeout`                        | `Expire`                                                      | Error on missing value before due time.                                                             |
| `Buffer(count)`                  | `Buffer(count)`                                               | Fixed-size buffers.                                                                                 |
| `SubscribeOn`                    | `SubscribeOn`                                                 | Schedule source subscription.                                                                       |
| `ToList` / `ToArray`             | `ToList` / `ToArray` or `CollectList` / `CollectArray`        | Signal results.                                                                                     |
| `FirstAsync` / `LastAsync`       | `FirstAsync` / `LastAsync`                                    | Task result.                                                                                        |
| `CountAsync` / `AnyAsync`        | `CountAsync` / `AnyAsync`                                     | Task-shaped terminal helpers, including cancellation overloads.                                     |

### Disposable mapping

| System.Reactive              | ReactiveUI.Primitives                   |
|------------------------------|-----------------------------------------|
| `Disposable.Create`          | `Disposable.Create`                     |
| `Disposable.Empty`           | `Disposable.Empty`                      |
| `BooleanDisposable`          | `BooleanDisposable`                     |
| `CancellationDisposable`     | `CancellationDisposable`                |
| `CompositeDisposable`        | `MultipleDisposable` or `Pocket`        |
| `SerialDisposable`           | `SingleReplaceableDisposable` or `Slot` |
| `SingleAssignmentDisposable` | `SingleDisposable` or `AssignmentSlot`  |
| `IDisposable.Dispose()`      | unchanged                               |

### Sequencer mapping

| System.Reactive scheduler concept  | ReactiveUI.Primitives scheduler                                |
|------------------------------------|----------------------------------------------------------------|
| `ImmediateScheduler.Instance`      | `Sequencer.Immediate` or `ImmediateSequencer.Instance`         |
| `CurrentThreadScheduler.Instance`  | `Sequencer.CurrentThread` or `CurrentThreadSequencer.Instance` |
| `ThreadPoolScheduler.Instance`     | `ThreadPoolSequencer.Instance`                                 |
| `TaskPoolScheduler.Default`        | `TaskPoolSequencer.Instance`                                   |
| synchronization-context scheduling | `SynchronizationContextSequencer`                              |
| WPF dispatcher scheduling          | `DispatcherSequencer` from `ReactiveUI.Primitives.Wpf`         |
| Windows Forms control scheduling   | `ControlSequencer` from `ReactiveUI.Primitives.WinForms`       |
| WinUI dispatcher queue scheduling  | `DispatcherQueueSequencer` from `ReactiveUI.Primitives.WinUI`  |
| Blazor renderer scheduling         | `BlazorRendererSequencer` from `ReactiveUI.Primitives.Blazor`  |
| Avalonia dispatcher scheduling     | `AvaloniaScheduler` from `ReactiveUI.Primitives.Avalonia`      |
| MAUI dispatcher scheduling         | `MauiDispatcherSequencer` from `ReactiveUI.Primitives.Maui`    |
| `TestScheduler` / virtual time     | `VirtualClock`                                                 |

### Migrating your tests

System.Reactive test code leans on `TestScheduler` and marble helpers. ReactiveUI.Primitives exposes virtual-time
primitives instead of cloning the full Rx testing API. Write tests that:

- Use `VirtualClock` for deterministic scheduling.
- Assert on values collected through `Subscribe` delegates.
- Dispose subscriptions explicitly.
- Use `CollectArrayAsync`, `CollectListAsync`, or `FirstAsync` when a task-shaped assertion reads better.

### Migration checklist

1. Replace subject construction with `Signal<T>`, `StateSignal<T>`, or `ReplaySignal<T>`, based on the behaviour you
   need.
2. Replace factories: `Observable.Return/Empty/Throw/Timer/Interval` becomes `Signal.Emit/None/Fail/After/Pulse`.
3. Replace hot-path operators with Primitives names: `Select -> Map`, `Where -> Keep`, `SelectMany -> FlatMap`,
   `Do -> Tap`, `Scan -> Fold`, `Aggregate -> Reduce`, `Amb -> Race`.
4. Replace composite and serial disposables with `MultipleDisposable` or `Pocket`, and `SingleReplaceableDisposable` or
   `Slot`.
5. Keep System.Reactive, R3, or R3Async at application boundaries only where you need them. Use the `.Reactive` package
   variants for System.Reactive public-surface compatibility, and the generated bridge methods for R3 and R3Async
   boundaries.
6. Run build, tests, pack, and `git diff --check` before you merge or publish.

## Moving from R3

R3 uses its own `Observable<T>` type and its own observer model. ReactiveUI.Primitives stays on the BCL
`IObservable<T>` shape so it interoperates at runtime.

| R3 concept            | ReactiveUI.Primitives equivalent                                                                                                                                                                       |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `R3.Observable<T>`    | BCL `IObservable<T>` from ReactiveUI.Primitives factories/operators.                                                                                                                                   |
| R3 subject            | `Signal<T>` / `StateSignal<T>` / `ReplaySignal<T>` depending on state/replay needs.                                                                                                                    |
| R3 `Select` / `Where` | `Map` / `Keep`.                                                                                                                                                                                        |
| R3 time operators     | `Signal.After`, `Signal.Pulse`, `Calm`, `Probe`, `Shift`, scheduler overloads.                                                                                                                         |
| R3 bridge             | Generated `AsPrimitivesSignal` / `AsR3Observable`; async bridge methods add `AsPrimitivesAsyncObservable` / `AsR3Observable` when R3 and `ReactiveUI.Primitives.Async` are referenced by the consumer. |

Use the generated bridge at boundaries only. Inside new code, prefer native ReactiveUI.Primitives operators.

## Moving from R3Async

`ReactiveUI.Primitives.Async` is the native async-observable package. Use it when your observer work is asynchronous,
when subscription and disposal need `ValueTask`, or when cancellation must flow through each notification. It differs
from R3Async in one main way: it uses `ReactiveUI.Primitives.Result` for completion.

The package set has no generated System.Reactive.Async bridge. Use `ReactiveUI.Primitives.Async.Reactive` when you need
async Primitives APIs compiled against System.Reactive `Unit` and `IScheduler`. Keep any other async-observable adapter
code at package or API edges.

| R3Async                            | ReactiveUI.Primitives.Async                      | Migration detail                                                                                     |
|------------------------------------|--------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `R3Async.AsyncObservable<T>`       | `IObservableAsync<T>` / `SignalAsync<T>`         | Use generated `AsPrimitivesAsyncObservable()` at external boundaries.                                |
| `R3Async.AsyncObserver<T>`         | `IObserverAsync<T>` / `WitnessAsync<T>`          | Use `WitnessAsync<T>` for custom observers that need disposal, cancellation, and concurrency checks. |
| `R3Async.Result`                   | `ReactiveUI.Primitives.Result`                   | Both carry success/failure; bridge adapters convert between them.                                    |
| `OnErrorResumeAsync`               | `OnErrorResumeAsync`                             | Same error-resume concept; Primitives passes the active `CancellationToken`.                         |
| `OnCompletedAsync(R3Async.Result)` | `OnCompletedAsync(ReactiveUI.Primitives.Result)` | Completion remains result-based.                                                                     |

```csharp
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge;

// R3Async.AsyncObservable<int> r3AsyncSource = ...;
IObservableAsync<int> native = r3AsyncSource.AsPrimitivesAsyncObservable();
R3Async.AsyncObservable<int> external = native.AsR3AsyncObservable();
```

Keep R3Async bridge conversions at package or API edges. Inside your application or library, use `SignalAsync`
factories, `IObservableAsync<T>` operators, and `IObserverAsync<T>` observers directly.

## Moving from ReactiveUI.Extensions

`ReactiveUI.Primitives` is the migration target for the non-async helpers from `ReactiveUI.Extensions`. The helpers live
in the `ReactiveUI.Primitives.Extensions` namespace. They keep their names where those names describe the behaviour and
do not collide with the core Primitives vocabulary. Scheduling overloads take `ISequencer` instead of a System.Reactive
scheduler.

| ReactiveUI.Extensions usage                                                             | ReactiveUI.Primitives usage                                                                  |
|-----------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| `WhereIsNotNull`, `SkipWhileNull`, `WhereTrue`, `WhereFalse`, `Not`                     | Same names over BCL `IObservable<T>`.                                                        |
| `WhereSelect`, `SelectConstant`, `TrySelect`, `SelectManyThen`, `Pairwise`, `Partition` | Same helper names; implemented with direct observers and fused operator shapes where useful. |
| `SyncTimer`, `ObserveOnIf`, `Schedule`, `ScheduleSafe`, throttle/debounce helpers       | Same helper names; use `ISequencer` overloads for scheduling.                                |
| `CatchIgnore`, `CatchAndReturn`, `CatchReturn`, retry helpers                           | Same helper names; no System.Reactive dependency.                                            |
| `SubscribeAsync`, `SelectAsync`, `SelectLatestAsync`, `DropIfBusy`                      | Same BCL observable helper names for Task/ValueTask interop.                                 |
| `RunAll`, `BufferUntil`, `FirstMatchFromCandidates`, `ToHotTask`, `ToHotValueTask`      | Same helper names; backed by ReactiveUI.Primitives runtime utilities.                        |

For async-native streams, prefer `ReactiveUI.Primitives.Async` and its `IObservableAsync<T>` operators. For BCL
observable helpers, migrate to `ReactiveUI.Primitives`. Your existing `ReactiveUI.Primitives.Extensions` imports keep
working.

## Benchmarks

The benchmarks live in `src/benchmarks/ReactiveUI.Primitives.Benchmarks`. They measure two things per scenario: how long
a call takes, and how many bytes it allocates. Each scenario runs the ReactiveUI.Primitives version against the closest
System.Reactive, R3, and ReactiveUI.Extensions version of the same work. The benchmark project references
System.Reactive, System.Reactive.Async 6.0.0-alpha.18, R3, and ReactiveUI.Extensions so it can compare them. The
production packages reference none of those.

Run the full suite:

```powershell
dotnet run --project src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj --framework net10.0 --configuration Release --no-restore -- --filter "*" --join --launchCount 1 --warmupCount 1 --iterationCount 3
```

Narrow the run with the `--filter` argument. BenchmarkDotNet writes the results to `BenchmarkDotNet.Artifacts/results`
as a GitHub-markdown report, an HTML report, and a CSV file. Read the CSV when you want to compare a run against an
earlier one.

The run of 2026-06-08 executed 617 benchmarks on .NET 10.0.8 under Windows 11, with no failed benchmark process. Those
617 rows cover 238 ReactiveUI.Primitives and ReactiveUI.Primitives.Async cases, 157 System.Reactive cases, 132 R3 cases,
and 90 ReactiveUI.Extensions cases. Across that run, ReactiveUI.Primitives is faster than System.Reactive in 151 of 157
comparisons, faster than R3 in 131 of 132 comparisons, and faster than ReactiveUI.Extensions 4.0.0 in 58 of 90
comparisons.

These rows show the shape of the results. Each cell reads `Mean / Allocated`, and `NA` means no alternative exists for
that scenario. The last four rows are places where an alternative matches or beats ReactiveUI.Primitives.

| Scenario                     |    ReactiveUI.Primitives |          System.Reactive |                      R3 |    ReactiveUI.Extensions |
|------------------------------|-------------------------:|-------------------------:|------------------------:|-------------------------:|
| `FirstAsync`                 |         5.9440 ns / 56 B |   2,582.4415 ns / 2792 B |      77.0095 ns / 208 B |                       NA |
| `CombineLatest`              |       41.1023 ns / 192 B |   3,327.7110 ns / 2824 B |     689.0555 ns / 344 B |                       NA |
| `ObserveOnImmediate`         |        27.0528 ns / 96 B | 17,127.3834 ns / 11307 B |     993.3502 ns / 432 B |                       NA |
| `DelayRange`                 |      165.0811 ns / 536 B |  6,285.0703 ns / 39584 B |  2,091.7877 ns / 2200 B |                       NA |
| `SubscribeDispose64`         |   3,743.0097 ns / 4360 B |  4,234.4889 ns / 38472 B |  3,772.4909 ns / 6728 B |                       NA |
| `ThrottleOnScheduler`        |   1,835.5199 ns / 2400 B |                       NA |                      NA | 30,618.2012 ns / 16366 B |
| `SignalMulticast4`           |    3,417.1940 ns / 600 B |    3,268.6291 ns / 728 B |   7,277.5419 ns / 608 B |                       NA |
| `SubjectSubscribeDispose8`   |      352.1052 ns / 704 B |     318.3937 ns / 1288 B |     493.2540 ns / 904 B |                       NA |
| `ReadOnlyStateProjection`    |      103.6957 ns / 224 B |       96.4655 ns / 328 B |     177.4105 ns / 312 B |                       NA |
| `CombineLatestValuesAreAllTrue` |   209.2835 ns / 936 B |      373.2756 ns / 648 B |                      NA |     230.6682 ns / 1176 B |

Some scenarios measure too fast to time. BenchmarkDotNet reports a `ZeroMeasurement` warning for them, including
`Return`, `CompletedSpark`, `Never`-style subscriptions, and `SubscribeAndComplete`. That warning means the measured
duration matches the overhead of an empty method. Compare the `Allocated` column for those scenarios instead of the
mean.

## Repository layout

| Path                                              | Purpose                                                                                      |
|---------------------------------------------------|----------------------------------------------------------------------------------------------|
| `src/ReactiveUI.Primitives.slnx`                  | Solution entrypoint.                                                                         |
| `src/ReactiveUI.Disposables`                      | Disposable primitives shared by the package family.                                          |
| `src/ReactiveUI.Primitives.Core`                  | Type-agnostic core shared by lean and System.Reactive-flavoured Primitives leaves.           |
| `src/ReactiveUI.Primitives`                       | Default lean signal/operator/sequencer package, extension helpers, and platform sequencers.  |
| `src/ReactiveUI.Primitives.Reactive`              | System.Reactive-flavoured Primitives leaf including the Reactive extension helpers.          |
| `src/ReactiveUI.Primitives.Async.Core`            | Type-agnostic async core shared by async leaves.                                             |
| `src/ReactiveUI.Primitives.Async`                 | Lean async observable/signal package built on `IObservableAsync<T>` and `IObserverAsync<T>`. |
| `src/ReactiveUI.Primitives.Async.Reactive`        | System.Reactive-flavoured async Primitives leaf.                                             |
| `src/ReactiveUI.Primitives.Extensions.Core`       | Source-only extension-helper implementation linked into `ReactiveUI.Primitives.Core`; not a project or package. |
| `src/ReactiveUI.Primitives.Wpf`                   | Optional WPF dispatcher integration library.                                                 |
| `src/ReactiveUI.Primitives.Wpf.Reactive`          | Optional WPF dispatcher scheduler integration library for System.Reactive consumers.         |
| `src/ReactiveUI.Primitives.WinForms`              | Optional Windows Forms control integration library.                                          |
| `src/ReactiveUI.Primitives.WinForms.Reactive`     | Optional Windows Forms control scheduler integration library for System.Reactive consumers.  |
| `src/ReactiveUI.Primitives.WinUI`                 | Optional WinUI dispatcher queue integration library.                                         |
| `src/ReactiveUI.Primitives.WinUI.Reactive`        | Optional WinUI dispatcher queue scheduler integration library for System.Reactive consumers. |
| `src/ReactiveUI.Primitives.Blazor`                | Optional Blazor renderer integration library.                                                |
| `src/ReactiveUI.Primitives.Blazor.Reactive`       | Optional Blazor renderer scheduler integration library for System.Reactive consumers.        |
| `src/ReactiveUI.Primitives.Avalonia`              | Optional Avalonia dispatcher sequencer integration library.                                  |
| `src/ReactiveUI.Primitives.Avalonia.Reactive`     | Optional Avalonia dispatcher scheduler integration library for System.Reactive consumers.    |
| `src/ReactiveUI.Primitives.Maui`                  | Optional MAUI dispatcher integration library.                                                |
| `src/ReactiveUI.Primitives.Maui.Reactive`         | Optional MAUI dispatcher scheduler integration library for System.Reactive consumers.        |
| `src/ReactiveUI.Primitives.ObservableEvents`      | Standalone analyzer package for provider-aware observable event generation.                  |
| `src/ReactiveUI.Primitives.R3Bridge.Generator`    | Standalone analyzer package for optional R3 and R3Async bridge generation.                   |
| `src/Primitives.Shared`                           | Linked lean/Reactive synchronous source.                                                     |
| `src/Primitives.Async.Shared`                     | Linked lean/Reactive async source.                                                           |
| `src/Primitives.Extensions.Shared`                | Linked lean/Reactive Extensions source.                                                      |
| `src/tests`                                       | Microsoft Testing Platform/TUnit-style test projects.                                        |
| `src/benchmarks/ReactiveUI.Primitives.Benchmarks` | BenchmarkDotNet comparison harness.                                                          |

## For advanced users

This section lists the types the extension methods build for you, so you can construct them yourself and skip the
indirection. It assumes you know the library and want the concrete type, the embedded struct or the contract to
implement.

### Constructing operator types directly

Most operators have a concrete public type behind them in `ReactiveUI.Primitives.Advanced`. The constructor arguments
mirror the operator arguments, so `source.Map(selector)` and `new MapSignal<TSource, TResult>(source, selector)` build
the same thing.

| Type | Operator it backs |
|------|-------------------|
| `MapSignal` | `Map` / `Select` |
| `MapIndexedSignal` | `Map` with the element index |
| `MapWithSignal` | `Map` with caller state |
| `KeepSignal` | `Keep` / `Where` |
| `KeepWithSignal` | `Keep` with caller state |
| `TapSignal` | `Tap` / `Do` |
| `TapWithSignal` | `Tap` with caller state |
| `CastSignal` | `Cast` |
| `SelectManySignal` | `SelectMany` |
| `SelectManyEnumerableSignal` | `SelectMany` over an enumerable |
| `SelectManyResultSignal` | `SelectMany` with a result selector |
| `SwitchMapSignal` | `SwitchMap` |
| `SwitchSignal` | `Switch` |
| `MergeSignal` | `Merge` |
| `BlendSignal` | `Blend` |
| `EnumerableBlendSignal` | `Blend` over an enumerable of sources |
| `MaxConcurrentEnumerableBlendSignal` | `Blend` with a concurrency limit |
| `ChainSignal` | `Concat` |
| `TaskChainSignal` | `Concat` over tasks |
| `RaceSignal` | `Race` / `Amb` |
| `IgnoreValuesSignal` | `IgnoreValues` |
| `FinallySignal` | `Finally` |
| `RecoverSignal` | `Recover` / `Catch` |
| `ResumeSignal` | `Resume` |
| `OnErrorResumeNextSignal` | `OnErrorResumeNext` |
| `ExpireSignal` | `Expire` / `Timeout` |
| `RepeatSourceSignal` | `Repeat` over a source |
| `BufferSignal` | `Buffer` |
| `CollectSignal` | `Collect` |
| `EmitIfQuietSignal` | `EmitIfQuiet` |
| `SerializeSignal` | `Serialize` |
| `SynchronizeSignal` | `Synchronize` |
| `SynchronizeGateSignal` | `Synchronize` over a gate you own |
| `SynchronizeObjectSignal` | `Synchronize` over an object you own |
| `SparkSignal` | `Spark` / `Materialize` |
| `UnsparkSignal` | `Unspark` / `Dematerialize` |
| `TimeIntervalSignal` | `TimeInterval` |
| `LeadSignal<T>` | `Lead` |
| `IsEmptySignal` | `IsEmpty` |
| `ForkJoinSignal` | `ForkJoin` |
| `PairSignal` | `Pair` |
| `SyncLatestSignal` | `SyncLatest` |
| `RangeZipSignal` | `Zip` over a range, fused |
| `RangeCombineLatestSignal` | `CombineLatest` over a range, fused |
| `RangeWithLatestSignal` | `Latch` over a range, fused |
| `RangeForkJoinSignal` | `ForkJoin` over a range, fused |
| `RangeSyncLatestSignal` | `SyncLatest` over a range, fused |
| `PublishSelectorSignal` | `Publish` with a selector |
| `AutoConnectSignal` | `AutoConnect` |
| `AutoShareSignal` | `AutoShare` |
| `ConnectableSignal<T>` | `Publish` and its `Connect` handle |

Sources carry concrete types too: `AnonymousSignal<T>` behind `Signal.Create`, plus `ReturnSignal<T>`,
`EmptySignal<T>`, `ThrowSignal<T>`, `RangeSignal`, `RepeatSignal<T>`, `UnfoldSignal<TState, TResult>`,
`FromEnumerableSignal<T>`, `AsyncEnumerableSignal<T>`, `UseSignal<TResource, T>`, `EverySignal`, `AfterSignal`,
`StartSignal`, `FromAsyncSignal<T>` and `FromEventPatternSignal`. The constant fast paths
`ImmediateReturnSignal<T>`, `ImmutableEmptySignal<T>` and `ImmutableNeverSignal<T>` allocate nothing.

Every fused operator type is public and takes its sources through the constructor, so you can build one directly
instead of calling the operator.

```csharp
IObservable<int> viaOperator = source.Unique();
IObservable<int> viaType = new UniqueSignal<int>(source, EqualityComparer<int>.Default);
```

### Embedded state types

`DeliveryGateState`, `SerializedDelivery<T>`, `SerializedBroadcaster<T>`, `CurrentValueDelivery<T>`,
`WitnessAsyncState`, `DisposableSet` and `DispatchSequencerState` are record structs. You hold one as a non-readonly
field and call it in place. That is what keeps a sink allocation-free.

```csharp
public sealed class MySink<T>(IObserver<T> observer)
{
    // Non-readonly field, called in place.
    private SerializedDelivery<T> _delivery = new();

    public void OnNext(T value) => _delivery.OnNext(observer, value, new PendingDrain(this));
}
```

A copy is a separate gate, queue or set, and the copy silently loses notifications. Three ways to make one by accident:

```csharp
private readonly SerializedDelivery<T> _delivery = new();  // every call runs against a fresh copy
var delivery = _delivery;                                  // a separate queue
void Post(SerializedDelivery<T> delivery)                  // takes a copy; declare the parameter as ref
```

Nothing throws when you copy one. The queue you post to and the queue you flush are different queues, so the posted
values never reach the observer and a terminal notification never arrives. Keep the field non-readonly, pass it by
`ref`, and never assign it to a local.

### Building your own sink

- **`DeliveryGate`** - static class. It serializes deliveries to one downstream observer without holding a lock while
  the observer runs, so an observer that blocks on another producer's thread cannot deadlock it. Enter, deliver and
  exit on one thread, with no `await` in between.
- **`DeliveryGateState`** - the mutable state of one gate. Hold it as a non-readonly field and pass it by `ref` to
  every `DeliveryGate` call.
- **`SerializedDelivery<T>`** - allocation-free serialized delivery to one observer. `OnNext` delivers directly when
  nothing else is delivering, and the `Post` methods queue without delivering so you can fix the order under your own
  lock and call `Flush` after you release it.
- **`IDrainTarget`** - in `ReactiveUI.Primitives.Extensions`, a single `Drain()` method the gate calls to deliver
  queued notifications. Implement it on a struct so delivery allocates nothing.
- **`SerializedWitness<T>`** - the class form of `SerializedDelivery<T>`. It wraps an `IObserver<T>` so notifications
  from any number of threads reach it one at a time.
- **`SerializedBroadcaster<T>`** - the subscriber list a subject fans out to. Add, remove and post under your own lock
  so every subscriber sees the same order.
- **`SerializedBroadcast<T>`** - the batch a post returns. Call `Flush()` after you release the lock; the default value
  flushes nothing.
- **`CurrentValueDelivery<T>`** - serialized delivery of a value you re-read on each change. It conflates to the latest
  value under contention and skips a repeat when you supply a comparer.
- **`ICurrentValueReader<T>`** - supplies `Read()` for `CurrentValueDelivery<T>`. Implement it on a struct wrapping the
  owner so the read needs no delegate.
- **`CurrentValueWitness<T>`** - the class form of `CurrentValueDelivery<T>`. Attach your change hook, call `Start()`,
  then call `Changed()` from any thread.
- **`WitnessSubscription`** - one call that links a downstream async witness's teardown, subscribes the witness to its
  source, and returns the witness as the subscription handle. Disposing that handle tears down the source subscription
  too.
- **`TaskResultCompletionSource<T>`** - coordinates the terminal task result for an async terminal operator and
  disposes the owning subscription when the wait exits.

Take the gate, deliver, then exit. A delivery that throws releases the gate through `Reset`:

```csharp
private DeliveryGateState _gate;

public void Deliver(T value)
{
    if (DeliveryGate.TryEnter(ref _gate))
    {
        try
        {
            observer.OnNext(value);
        }
        catch
        {
            DeliveryGate.Reset(ref _gate);
            throw;
        }

        DeliveryGate.Exit(ref _gate, new PendingDrain(this));
        return;
    }

    Enqueue(value);
    DeliveryGate.Signal(ref _gate, new PendingDrain(this));
}

private readonly struct PendingDrain(MySink owner) : IDrainTarget
{
    public void Drain() => owner.DeliverQueued();
}
```

A subject posts under its own lock and flushes after it releases it, so no observer runs while you hold the lock:

```csharp
private readonly Lock _lock = new();
private SerializedBroadcaster<T> _subscribers;

public void OnNext(T value)
{
    SerializedBroadcast<T> batch;

    lock (_lock)
    {
        batch = _subscribers.PostNext(value);
    }

    batch.Flush();
}
```

Always flush the batch. A subscriber claimed by the post delivers nothing else until you do.

### Marker interfaces

Implement these on your own signal to let the operators take a faster path.

- **`IInlineSignal<T>`** - adds a delegate-based `Subscribe(onNext, onError, onCompleted)`, so a subscriber reaches
  your signal without allocating an observer.
- **`IRequireCurrentThread<T>`** - declares that you must be subscribed on the calling thread, which lets an operator
  skip a scheduling hop.
- **`IAsyncEnumerableBackedSignal<T>`** - exposes the underlying `IAsyncEnumerable<T>` and its token, so a consumer can
  enumerate you directly.
- **`IAggregator<T, TResult, TSelf>`** - the struct-based accumulator contract behind allocation-free counting.
  `CountAggregator<T>`, `LongCountAggregator<T>` and the `DistinctBy` variants implement it.

### Async witness contracts

You write a custom async operator sink by implementing `IWitnessAsync<T>`. Hold a `WitnessAsyncState` as a non-readonly
field and return it by `ref` from an explicit `IWitnessState.Witness`. Forward the three `IObserverAsync<T>` members and
`DisposeAsync` to `WitnessAsync`, then write only the three `Core` hooks. `WitnessAsync` runs the gating, cancellation
linking and disposal: it drops a notification when the witness is disposed or cancelled, or when another thread holds
the gate, and it keeps a hook failure away from the producer.

```csharp
public sealed class MyWitness<T>(IObserverAsync<T> downstream) : IWitnessAsync<T>
{
    private WitnessAsyncState _witness;

    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        downstream.OnNextAsync(value, cancellationToken);

    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => downstream.OnCompletedAsync(result);
}
```

Two calls that overlap from different threads are dropped and reported as `ConcurrentWitnessCallsException`. A
reentrant call on the thread already inside the witness runs.

### Writing a sequencer for a new dispatcher

A sequencer schedules `IWorkItem`s through `ISequencer`, either at once or at an absolute monotonic timestamp. To wrap
a dispatcher the library does not ship, implement `ISequencer` and embed a `DispatchSequencerState` as a non-readonly
field. Construct the state with your owning sequencer, a `Func<Action, bool>` that posts a callback to the dispatcher,
and the drain callback the dispatcher runs. The state queues ready work and keeps at most one drain pending, so a burst
of posts coalesces into one trip through the dispatcher.

Every type listed here also exists under `ReactiveUI.Primitives.Reactive.*` in the `ReactiveUI.Primitives.Reactive`
package, compiled against System.Reactive `Unit` and `IScheduler`.

## Contribute

ReactiveUI.Primitives is developed under an OSI-approved open source license, making it freely usable and distributable,
even for commercial use. We love the people who are involved in this project, and we would love to have you on board,
especially if you are just getting started or have never contributed to open-source before.

So here is to you, lovely person who wants to join us. This is how you can support us:

- [Answering questions on GitHub Discussions](https://github.com/reactiveui/Primitives/discussions)
- [Passing on knowledge and teaching the next generation of developers](https://ericsink.com/entries/dont_use_rxui.html)
- Submitting documentation updates where you see fit or lacking.
- Making contributions to the code base.

## Code of Conduct

We are dedicated to providing a welcoming and inclusive community. Please read and follow
our [Code of Conduct](CODE_OF_CONDUCT.md).

## License

ReactiveUI.Primitives is licensed under the [MIT License](LICENSE).
