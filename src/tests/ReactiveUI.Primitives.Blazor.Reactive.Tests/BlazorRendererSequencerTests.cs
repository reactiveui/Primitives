// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Components;
using ReactiveUI.Primitives.Blazor.Reactive.Components;
using ReactiveUI.Primitives.Blazor.Reactive.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Blazor.Reactive.Tests;

/// <summary>Tests for <see cref="BlazorRendererSequencer"/> as an <see cref="IScheduler"/> driven through a fake renderer delegate.</summary>
public sealed class BlazorRendererSequencerTests
{
    /// <summary>The failure reported by a rejected renderer operation.</summary>
    private const string RendererFailure = "renderer rejected";

    /// <summary>The upper bound for delayed work to complete on a loaded machine.</summary>
    private static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(30);

    /// <summary>The values an immediate burst produces, in the FIFO order asserted.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Verifies the constructor rejects a null renderer delegate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDelegate() =>
        await Assert.That(static () => new BlazorRendererSequencer((Func<Action, Task>)null!))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the constructor rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(static () => new BlazorRendererSequencer((Dispatcher)null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the dispatcher adapter extension rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ToSequencerRejectsNullDispatcher() =>
        await Assert.That(static () => ((Dispatcher)null!).ToSequencer()).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies a dispatcher-backed scheduler marshals and executes work.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatcherSchedulerExecutesWork()
    {
        FakeRenderer renderer = new();
        var scheduler = renderer.ToSequencer();
        var executed = false;

        _ = scheduler.Schedule(() => executed = true);

        await Assert.That(executed).IsTrue();
        await Assert.That(renderer.InvokeCount).IsEqualTo(1);
    }

    /// <summary>Verifies renderer-task faults reach the unhandled-exception handler instead of vanishing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task FaultedRendererTaskRoutesToHandler()
    {
        TaskCompletionSource<Exception> observed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException fault = new(RendererFailure);
        BlazorRendererSequencer scheduler = new(_ => Task.FromException(fault));
        scheduler.UnhandledExceptionHandler = ex => observed.TrySetResult(ex);

        _ = scheduler.Schedule(static () => { });

        await Assert.That(await observed.Task).IsSameReferenceAs(fault);
    }

    /// <summary>A successful renderer task needs no fault registration.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserveFaults_SuccessfulTask_SkipsRegistration()
    {
        BlazorRendererSequencer sequencer = new(static _ => Task.CompletedTask);
        var registered = false;

        sequencer.ObserveFaults(Task.CompletedTask, (_, _) => registered = true);

        await Assert.That(registered).IsFalse();
    }

    /// <summary>A pending renderer task retains its owner and forwards a later base exception.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserveFaults_PendingTask_ForwardsLaterFault()
    {
        BlazorRendererSequencer sequencer = new(static _ => Task.CompletedTask);
        TaskCompletionSource<bool> renderer = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<Exception> observed = [];
        sequencer.UnhandledExceptionHandler = observed.Add;
        Task? registeredTask = null;
        BlazorRendererSequencer? registeredOwner = null;
        sequencer.ObserveFaults(renderer.Task, (task, owner) =>
        {
            registeredTask = task;
            registeredOwner = owner;
        });

        await Assert.That(ReferenceEquals(registeredTask, renderer.Task)).IsTrue();
        await Assert.That(registeredOwner).IsSameReferenceAs(sequencer);
        await Assert.That(observed).IsEmpty();

        InvalidOperationException fault = new(RendererFailure);
        renderer.SetException(new AggregateException(fault));
        registeredOwner!.CompleteRendererTask(registeredTask!);

        await Assert.That(observed).HasSingleItem();
        await Assert.That(observed[0]).IsSameReferenceAs(fault);
    }

    /// <summary>Pending, successful, and canceled renderer tasks produce no fault notification.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompleteRendererTask_WithoutFault_IgnoresTask()
    {
        BlazorRendererSequencer sequencer = new(static _ => Task.CompletedTask);
        List<Exception> observed = [];
        sequencer.UnhandledExceptionHandler = observed.Add;
        TaskCompletionSource<bool> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.CompleteRendererTask(pending.Task);
        sequencer.CompleteRendererTask(Task.CompletedTask);
        sequencer.CompleteRendererTask(Task.FromCanceled(new(true)));

        await Assert.That(observed).IsEmpty();
    }

    /// <summary>A configured handler receives the fault; otherwise the fallback receives it.</summary>
    /// <param name="hasHandler">True when a renderer fault handler is configured.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task HandleFault_HandlerSelection_UsesOneDestination(bool hasHandler)
    {
        BlazorRendererSequencer sequencer = new(static _ => Task.CompletedTask);
        List<Exception> handled = [];
        List<Exception> rethrown = [];
        sequencer.UnhandledExceptionHandler = hasHandler ? handled.Add : null;
        InvalidOperationException fault = new(RendererFailure);

        sequencer.HandleFault(fault, rethrown.Add);

        await Assert.That(handled.Count).IsEqualTo(hasHandler ? 1 : 0);
        await Assert.That(rethrown.Count).IsEqualTo(hasHandler ? 0 : 1);
        await Assert.That((hasHandler ? handled : rethrown)[0]).IsSameReferenceAs(fault);
    }

    /// <summary>Verifies reactive component observation guards reject null inputs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ReactiveComponentObserveRejectsNullArguments()
    {
        TestReactiveComponent component = new();
        PassiveObservable<int> source = new();

        await Assert.That(() => component.ObserveSource<int>(null!, static _ => { }))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => component.ObserveSource(source, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies the default observed-error handler validates and wraps errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ReactiveComponentObservedErrorRejectsNullAndWrapsError()
    {
        TestReactiveComponent component = new();
        await Assert.That(() => component.NotifyObservedError(null!)).ThrowsExactly<ArgumentNullException>();

        InvalidOperationException error = new("observed");
        var caught = await Assert.That(() => component.NotifyObservedError(error))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(caught!.InnerException).IsSameReferenceAs(error);
    }

    /// <summary>Verifies tracking after disposal immediately disposes the incoming subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ReactiveComponentTrackAfterDisposeReturnsEmptyAndDisposesInput()
    {
        TestReactiveComponent component = new();
        var inputDisposed = false;
        component.Dispose();

        var tracked = component.TrackSubscription(new FlagDisposable(() => inputDisposed = true));

        await Assert.That(inputDisposed).IsTrue();
        await Assert.That(tracked).IsSameReferenceAs(EmptyDisposable.Instance);
        await Assert.That(component.IsDisposedState).IsTrue();
    }

    /// <summary>Verifies tracking before disposal returns the original subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ReactiveComponentTrackBeforeDisposeReturnsOriginalSubscription()
    {
        TestReactiveComponent component = new();
        var subscription = new FlagDisposable(static () => { });

        var tracked = component.TrackSubscription(subscription);

        await Assert.That(tracked).IsSameReferenceAs(subscription);
    }

    /// <summary>Verifies immediate work is marshalled through the renderer delegate and executed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleMarshalsThroughRenderer()
    {
        FakeRenderer renderer = new();
        BlazorRendererSequencer scheduler = new(renderer.InvokeAsync);
        var executed = false;

        _ = scheduler.Schedule(() => executed = true);

        await Assert.That(executed).IsTrue();
        await Assert.That(renderer.InvokeCount).IsGreaterThan(0);
    }

    /// <summary>Verifies a burst of immediate work items all execute in FIFO order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateBurstExecutesInOrder()
    {
        FakeRenderer renderer = new();
        BlazorRendererSequencer scheduler = new(renderer.InvokeAsync);
        List<int> values = [];

        foreach (var value in ExpectedBurst)
        {
            var captured = value;
            _ = scheduler.Schedule(() => values.Add(captured));
        }

        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    /// <summary>Verifies delayed work waits out its delay and then runs through the renderer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleRunsThroughRendererAfterTheDelay()
    {
        FakeRenderer renderer = new();
        BlazorRendererSequencer scheduler = new(renderer.InvokeAsync);
        TaskCompletionSource ran = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Func<IScheduler, TaskCompletionSource, IDisposable> action = static (_, completion) =>
        {
            completion.SetResult();
            return EmptyDisposable.Instance;
        };

        using var handle = scheduler.Schedule(ran, TimeSpan.FromMilliseconds(1), action);

        await Assert.That(ran.Task).CompletesWithin(CompletionTimeout);
        await Assert.That(renderer.InvokeCount).IsGreaterThan(0);
    }

    /// <summary>Verifies cancelling queued work twice suppresses it and releases nothing twice.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task CancellingQueuedWorkTwiceSuppressesIt()
    {
        List<Action> posted = [];
        BlazorRendererSequencer scheduler = new(drain =>
        {
            posted.Add(drain);
            return Task.CompletedTask;
        });
        var ran = false;

        var item = scheduler.Schedule(0, (_, _) =>
        {
            ran = true;
            return EmptyDisposable.Instance;
        });
        item.Dispose();
        item.Dispose();
        posted[0]();

        await Assert.That(ran).IsFalse();
    }

    /// <summary>Fake renderer that runs marshalled work synchronously and records how often it was invoked.</summary>
    private sealed class FakeRenderer : Dispatcher
    {
        /// <summary>Gets the number of times <see cref="InvokeAsync(Action)"/> was called.</summary>
        public int InvokeCount { get; private set; }

        /// <inheritdoc/>
        public override bool CheckAccess() => true;

        /// <inheritdoc/>
        public override Task InvokeAsync(Action workItem)
        {
            InvokeCount++;
            workItem();
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public override Task InvokeAsync(Func<Task> workItem) => workItem();

        /// <inheritdoc/>
        public override Task<TResult> InvokeAsync<TResult>(Func<TResult> workItem) => Task.FromResult(workItem());

        /// <inheritdoc/>
        public override Task<TResult> InvokeAsync<TResult>(Func<Task<TResult>> workItem) => workItem();
    }

    /// <summary>Test component that exposes protected reactive component members.</summary>
    private sealed class TestReactiveComponent : ReactiveComponentBase
    {
        /// <summary>Gets a value indicating whether this component is disposed.</summary>
        public bool IsDisposedState => IsDisposed;

        /// <summary>Calls the protected observe method.</summary>
        /// <typeparam name="T">The observed value type.</typeparam>
        /// <param name="source">The source sequence.</param>
        /// <param name="onNext">The value callback.</param>
        /// <returns>The tracked subscription.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ObserveSource<T>(IObservable<T> source, Action<T> onNext) => Observe(source, onNext);

        /// <summary>Calls the protected track method.</summary>
        /// <param name="subscription">The subscription to track.</param>
        /// <returns>The tracked subscription.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable TrackSubscription(IDisposable subscription) => Track(subscription);

        /// <summary>Calls the protected observed-error handler.</summary>
        /// <param name="error">The observed error.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void NotifyObservedError(Exception error) => OnObservedError(error);
    }

    /// <summary>Observable that records subscriptions without producing signals.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class PassiveObservable<T> : IObservable<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(IObserver<T> observer) => EmptyDisposable.Instance;
    }

    /// <summary>Disposable that invokes a callback once.</summary>
    private sealed class FlagDisposable : IDisposable
    {
        /// <summary>The callback to invoke on dispose.</summary>
        private readonly Action _onDispose;

        /// <summary>A latch set to one once dispose has run.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="FlagDisposable"/> class.</summary>
        /// <param name="onDispose">The dispose callback.</param>
        public FlagDisposable(Action onDispose) => _onDispose = onDispose;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _onDispose();
        }
    }
}
