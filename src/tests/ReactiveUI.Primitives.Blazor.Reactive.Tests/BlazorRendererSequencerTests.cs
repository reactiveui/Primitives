// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using Microsoft.AspNetCore.Components;
using ReactiveUI.Primitives.Blazor.Reactive.Components;
using ReactiveUI.Primitives.Blazor.Reactive.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Blazor.Reactive.Tests;

/// <summary>Tests for <see cref="BlazorRendererSequencer"/> as an <see cref="IScheduler"/> driven through a fake renderer delegate.</summary>
public sealed class BlazorRendererSequencerTests
{
    /// <summary>Expected values produced by an immediate burst, used to verify FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Verifies the constructor rejects a null renderer delegate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDelegate() =>
        await Assert.That(() => new BlazorRendererSequencer((Func<Action, Task>)null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the constructor rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(() => new BlazorRendererSequencer((Dispatcher)null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the dispatcher adapter extension rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ToSequencerRejectsNullDispatcher() =>
        await Assert.That(() => ((Dispatcher)null!).ToSequencer()).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies a dispatcher-backed scheduler marshals and executes work.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatcherSchedulerExecutesWork()
    {
        var scheduler = Dispatcher.CreateDefault().ToSequencer();
        TaskCompletionSource<bool> executed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scheduler.Schedule(() => executed.TrySetResult(true));

        await Assert.That(await executed.Task.WaitAsync(TimeSpan.FromSeconds(5))).IsTrue();
    }

    /// <summary>Verifies renderer-task faults reach the unhandled-exception handler instead of vanishing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task FaultedRendererTaskRoutesToHandler()
    {
        TaskCompletionSource<Exception> observed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException fault = new("renderer rejected");
        BlazorRendererSequencer scheduler = new(_ => Task.FromException(fault));
        scheduler.UnhandledExceptionHandler = ex => observed.TrySetResult(ex);

        _ = scheduler.Schedule(static () => { });

        await Assert.That(await observed.Task.WaitAsync(TimeSpan.FromSeconds(5))).IsSameReferenceAs(fault);
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

        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default);
    }

    /// <summary>Fake renderer that runs marshalled work synchronously and records how often it was invoked.</summary>
    private sealed class FakeRenderer
    {
        /// <summary>Gets the number of times <see cref="InvokeAsync(Action)"/> was called.</summary>
        public int InvokeCount { get; private set; }

        /// <summary>Runs the supplied work synchronously, mimicking <c>ComponentBase.InvokeAsync</c>.</summary>
        /// <param name="action">The work to run.</param>
        /// <returns>A completed task.</returns>
        public Task InvokeAsync(Action action)
        {
            InvokeCount++;
            action();
            return Task.CompletedTask;
        }
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
        public IDisposable ObserveSource<T>(IObservable<T> source, Action<T> onNext) => Observe(source, onNext);

        /// <summary>Calls the protected track method.</summary>
        /// <param name="subscription">The subscription to track.</param>
        /// <returns>The tracked subscription.</returns>
        public IDisposable TrackSubscription(IDisposable subscription) => Track(subscription);

        /// <summary>Calls the protected observed-error handler.</summary>
        /// <param name="error">The observed error.</param>
        public void NotifyObservedError(Exception error) => OnObservedError(error);
    }

    /// <summary>Observable that records subscriptions without producing signals.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class PassiveObservable<T> : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer) => EmptyDisposable.Instance;
    }

    /// <summary>Disposable that invokes a callback once.</summary>
    private sealed class FlagDisposable : IDisposable
    {
        /// <summary>The callback to invoke on dispose.</summary>
        private readonly Action _onDispose;

        /// <summary>A value indicating whether dispose already ran.</summary>
        private bool _disposed;

        /// <summary>Initializes a new instance of the <see cref="FlagDisposable"/> class.</summary>
        /// <param name="onDispose">The dispose callback.</param>
        public FlagDisposable(Action onDispose) => _onDispose = onDispose;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _onDispose();
        }
    }
}
