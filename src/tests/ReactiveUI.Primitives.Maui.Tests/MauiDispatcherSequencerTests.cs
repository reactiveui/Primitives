// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Maui.Dispatching;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Maui.Tests;

/// <summary>
/// Tests for <see cref="MauiDispatcherSequencer"/>, exercised through a fake <see cref="IDispatcher"/>
/// so the immediate and time-based dispatch paths run deterministically on any platform.
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
        await Assert.That(static () => nullDispatcher!.ToSequencer()).ThrowsExactly<ArgumentNullException>();

        FakeDispatcher dispatcher = new();
        var sequencer = dispatcher.ToSequencer();

        await Assert.That(sequencer).IsNotNull();
    }

    /// <summary>Verifies immediate work is marshalled through <see cref="IDispatcher.Dispatch(Action)"/> and executed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleDispatchesAndExecutes()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var executed = false;

        sequencer.Schedule(new DelegateWorkItem(() => executed = true));

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchCount).IsGreaterThan(0);
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies work due in the future routes through <see cref="IDispatcher.DispatchDelayed(TimeSpan, Action)"/>
    /// with a positive delay, and runs on the dispatcher without the thread-pool marshal hop.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleUsesDispatchDelayed()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var executed = false;

        var due = sequencer.Timestamp + Stopwatch.Frequency; // ~1 second into the future.
        sequencer.Schedule(new DelegateWorkItem(() => executed = true), due);

        await Assert.That(executed).IsTrue();
        await Assert.That(dispatcher.DispatchDelayedCount).IsEqualTo(1);
        await Assert.That(dispatcher.LastDelay).IsGreaterThan(TimeSpan.Zero);
    }

    /// <summary>Verifies a due timestamp at or before now takes the immediate path rather than the delayed timer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task PastDueTimestampUsesImmediatePath()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var executed = false;

        var due = sequencer.Timestamp - Stopwatch.Frequency; // already elapsed.
        sequencer.Schedule(new DelegateWorkItem(() => executed = true), due);

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
        MauiDispatcherSequencer sequencer = new(dispatcher);
        List<int> values = [];

        foreach (var value in ExpectedBurst)
        {
            var captured = value;
            sequencer.Schedule(new DelegateWorkItem(() => values.Add(captured)));
        }

        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default);
    }

    /// <summary>Verifies the sequencer surfaces the shared dispatch clock through both clock properties.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ClockPropertiesReportTheSharedDispatchClock()
    {
        FakeDispatcher dispatcher = new();
        MauiDispatcherSequencer sequencer = new(dispatcher);
        var before = sequencer.Timestamp;

        await Assert.That(sequencer.Now).IsGreaterThan(DateTimeOffset.MinValue);
        await Assert.That(sequencer.Timestamp).IsGreaterThanOrEqualTo(before);
    }

    /// <summary>Work item that invokes a delegate when executed.</summary>
    private sealed class DelegateWorkItem : IWorkItem
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action;

        /// <summary>Initializes a new instance of the <see cref="DelegateWorkItem"/> class.</summary>
        /// <param name="action">The action to run on execution.</param>
        public DelegateWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _action();
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
