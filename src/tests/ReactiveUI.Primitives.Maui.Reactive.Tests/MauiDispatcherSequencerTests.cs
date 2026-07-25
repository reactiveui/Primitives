// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using Microsoft.Maui.Dispatching;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Maui.Reactive.Tests;

/// <summary>
/// Tests for <see cref="MauiDispatcherSequencer"/> as an <see cref="IScheduler"/>, exercised through a fake
/// <see cref="IDispatcher"/> so the immediate and time-based dispatch paths run deterministically on any platform.
/// </summary>
public sealed class MauiDispatcherSequencerTests
{
    /// <summary>Expected values produced by an immediate burst, used to verify FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Verifies the constructor rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(static () => new MauiDispatcherSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the dispatcher extension method validates and adapts dispatchers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ToSequencerValidatesAndAdaptsDispatcher()
    {
        const IDispatcher nullDispatcher = null!;
        await Assert.That(static () => nullDispatcher.ToSequencer()).ThrowsExactly<ArgumentNullException>();

        FakeDispatcher dispatcher = new();
        var scheduler = dispatcher.ToSequencer();

        await Assert.That(scheduler).IsNotNull();
    }

    /// <summary>Verifies immediate work is marshalled through <see cref="IDispatcher.Dispatch(Action)"/> and executed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleDispatchesAndExecutes()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer scheduler = new(dispatcher);
        var executed = false;

        _ = scheduler.Schedule(() => executed = true);

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchCount).IsGreaterThan(0);
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies work due in the future routes through <see cref="IDispatcher.DispatchDelayed(TimeSpan, Action)"/>
    /// with a positive delay, and runs on the dispatcher without the shared-timer marshal hop.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleUsesDispatchDelayed()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer scheduler = new(dispatcher);
        var executed = false;

        _ = scheduler.Schedule(TimeSpan.FromSeconds(1), () => executed = true);

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(1);
        await Assert.That(dispatcher.LastDelay).IsGreaterThan(TimeSpan.Zero);
    }

    /// <summary>Verifies a zero due time takes the immediate path rather than the delayed timer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ZeroDueTimeUsesImmediatePath()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer scheduler = new(dispatcher);
        var executed = false;

        _ = scheduler.Schedule(TimeSpan.Zero, () => executed = true);

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchCount).IsGreaterThan(0);
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(0);
    }

    /// <summary>Verifies a burst of immediate work items all execute in FIFO order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateBurstExecutesInOrder()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer scheduler = new(dispatcher);
        List<int> values = [];

        foreach (var value in ExpectedBurst)
        {
            var captured = value;
            _ = scheduler.Schedule(() => values.Add(captured));
        }

        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default);
    }

    /// <summary>Fake MAUI dispatcher that runs marshalled work synchronously and records how it was dispatched.</summary>
    private sealed class FakeDispatcher : IDispatcher
    {
        /// <summary>Gets the number of times <see cref="Dispatch(Action)"/> was called.</summary>
        public int DispatchCount { get; private set; }

        /// <summary>Gets the number of times <see cref="DispatchDelayed(TimeSpan, Action)"/> was called.</summary>
        public int DispatchDelayedCount { get; private set; }

        /// <summary>Gets the delay passed to the most recent <see cref="DispatchDelayed(TimeSpan, Action)"/> call.</summary>
        public TimeSpan LastDelay { get; private set; }

        /// <inheritdoc/>
        public bool IsDispatchRequired => true;

        /// <inheritdoc/>
        public bool Dispatch(Action action)
        {
            DispatchCount++;
            action();
            return true;
        }

        /// <inheritdoc/>
        public bool DispatchDelayed(TimeSpan delay, Action action)
        {
            DispatchDelayedCount++;
            LastDelay = delay;
            action();
            return true;
        }

        /// <inheritdoc/>
        public IDispatcherTimer CreateTimer() => new FakeDispatcherTimer();

        /// <summary>Fake dispatcher timer that fires its tick immediately on start.</summary>
        private sealed class FakeDispatcherTimer : IDispatcherTimer
        {
            /// <inheritdoc/>
            public event EventHandler? Tick;

            /// <inheritdoc/>
            public TimeSpan Interval { get; set; }

            /// <inheritdoc/>
            public bool IsRepeating { get; set; }

            /// <inheritdoc/>
            public bool IsRunning { get; private set; }

            /// <inheritdoc/>
            public void Start()
            {
                IsRunning = true;
                Tick?.Invoke(this, EventArgs.Empty);
            }

            /// <inheritdoc/>
            public void Stop() => IsRunning = false;
        }
    }
}
