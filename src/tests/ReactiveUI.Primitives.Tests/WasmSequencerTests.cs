// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="WasmSequencer"/>.</summary>
public sealed class WasmSequencerTests
{
    /// <summary>Timestamp ticks per second of a provider that counts milliseconds.</summary>
    private const long MillisecondFrequency = 1000;

    /// <summary>Expected values produced by an immediate burst, in FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>The delay used for scheduled work.</summary>
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    /// <summary>Verifies the shared instance is a singleton.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultReturnsSingleton() =>
        await Assert.That(WasmSequencer.Default).IsSameReferenceAs(WasmSequencer.Default);

    /// <summary>Verifies immediate scheduling rejects a null work item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleRejectsNullItem()
    {
        await Assert.That(static () => WasmSequencer.Default.Schedule(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => WasmSequencer.Default.Schedule(null!, 0L)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies the clock properties are sane.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ClockAdvances()
    {
        var sequencer = WasmSequencer.Default;
        var before = sequencer.Timestamp;

        await Assert.That(sequencer.Now).IsGreaterThan(DateTimeOffset.MinValue);
        await Assert.That(sequencer.Timestamp).IsGreaterThanOrEqualTo(before);
    }

    /// <summary>Immediate work executes when its queued drain runs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutes()
    {
        Queue<Action> drains = new();
        using var sequencer = CreateSequencer(drains);
        var executed = false;
        sequencer.Schedule(new DelegateWorkItem(() => executed = true));
        await Assert.That(executed).IsFalse();
        drains.Dequeue()();
        await Assert.That(executed).IsTrue();
    }

    /// <summary>Verifies a burst of immediate work executes in FIFO order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateBurstExecutesInOrder()
    {
        Queue<Action> drains = new();
        using var sequencer = CreateSequencer(drains);
        List<int> values = [];
        foreach (var value in ExpectedBurst)
        {
            sequencer.Schedule(new DelegateWorkItem(() => values.Add(value)));
        }

        await Assert.That(drains.Count).IsEqualTo(1);
        drains.Dequeue()();
        await Assert.That(values.SequenceEqual(ExpectedBurst)).IsTrue();
    }

    /// <summary>Delayed work enters the drain only when the delay callback runs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutesAfterDue()
    {
        Queue<Action> drains = new();
        ManualSequencer delays = new();
        using var sequencer = CreateSequencer(drains, delays);
        var executed = false;
        sequencer.Schedule(new DelegateWorkItem(() => executed = true), long.MaxValue);
        await Assert.That(drains.Count).IsEqualTo(0);
        await Assert.That(executed).IsFalse();
        delays.RunPending();
        await Assert.That(drains.Count).IsEqualTo(1);
        await Assert.That(executed).IsFalse();
        drains.Dequeue()();
        await Assert.That(executed).IsTrue();
    }

    /// <summary>Past-due work uses the immediate queue.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task PastDueScheduleExecutes()
    {
        Queue<Action> drains = new();
        using var sequencer = CreateSequencer(drains);
        var executed = false;
        sequencer.Schedule(new DelegateWorkItem(() => executed = true), long.MinValue);
        drains.Dequeue()();
        await Assert.That(executed).IsTrue();
    }

    /// <summary>Verifies a cancelled work item never executes while later work runs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task CancelledItemIsSkipped()
    {
        Queue<Action> drains = new();
        using var sequencer = CreateSequencer(drains);
        var cancelledRan = false;
        var markerRan = false;
        CancellableWorkItem cancelled = new(() => cancelledRan = true);
        cancelled.Dispose();
        sequencer.Schedule(cancelled);
        sequencer.Schedule(new DelegateWorkItem(() => markerRan = true));
        drains.Dequeue()();
        await Assert.That(cancelledRan).IsFalse();
        await Assert.That(markerRan).IsTrue();
    }

    /// <summary>Verifies disposing a fresh sequencer releases its drain timer and is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeReleasesDrainTimerAndIsIdempotent()
    {
        WasmSequencer sequencer = new();

        sequencer.Dispose();

        await Assert.That(sequencer.Dispose).ThrowsNothing();
    }

    /// <summary>A disposed sequencer rejects immediate and delayed work.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleAfterDisposeThrowsObjectDisposedException()
    {
        Queue<Action> drains = new();
        var sequencer = CreateSequencer(drains);
        var ran = 0;
        DelegateWorkItem immediate = new(() => ran++);
        DelegateWorkItem delayed = new(() => ran++);
        sequencer.Dispose();
        await Assert.That(() => sequencer.Schedule(immediate)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => sequencer.Schedule(delayed, long.MaxValue)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(drains.Count).IsEqualTo(0);
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>The ready queue releases work received after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ScheduleReadyAfterDisposeReleasesTheItem()
    {
        Queue<Action> drains = new();
        var sequencer = CreateSequencer(drains);
        var ran = 0;
        CancellableWorkItem item = new(() => ran++);
        sequencer.Dispose();
        sequencer.ScheduleReady(item);
        await Assert.That(item.IsDisposed).IsTrue();
        await Assert.That(drains.Count).IsEqualTo(0);
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>A delay callback releases work when the destination sequencer is disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeReleasesDelayedWorkThatComesDueAfterwards()
    {
        Queue<Action> drains = new();
        ManualSequencer delays = new();
        var sequencer = CreateSequencer(drains, delays);
        var ran = 0;
        var released = 0;
        CancellableWorkItem delayed = new(() => ran++, () => released++);
        sequencer.Schedule(delayed, long.MaxValue);
        sequencer.Dispose();
        delays.RunPending();
        await Assert.That(released).IsEqualTo(1);
        await Assert.That(delayed.IsDisposed).IsTrue();
        await Assert.That(drains.Count).IsEqualTo(0);
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>Canceled delayed work never reaches the ready queue.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedItemCancelledBeforeItIsDueIsSkippedByTheMarshalStep()
    {
        Queue<Action> drains = new();
        ManualSequencer delays = new();
        using var sequencer = CreateSequencer(drains, delays);
        var ran = 0;
        CancellableWorkItem delayed = new(() => ran++);
        sequencer.Schedule(delayed, long.MaxValue);
        delayed.Dispose();
        delays.RunPending();
        await Assert.That(delayed.IsDisposed).IsTrue();
        await Assert.That(drains.Count).IsEqualTo(0);
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>Disposal suppresses delayed work without a cancellation handle.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeDropsDelayedNonDisposableWorkThatComesDueAfterwards()
    {
        Queue<Action> drains = new();
        ManualSequencer delays = new();
        var sequencer = CreateSequencer(drains, delays);
        var ran = 0;
        sequencer.Schedule(new DelegateWorkItem(() => ran++), long.MaxValue);
        sequencer.Dispose();
        delays.RunPending();
        await Assert.That(drains.Count).IsEqualTo(0);
        await Assert.That(ran).IsEqualTo(0);
    }

    /// <summary>A sequencer rejects a missing provider.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ProviderConstructorRejectsANullProvider() =>
        await Assert.That(static () => new WasmSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>The current time and timestamp follow the provider.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task NowAndTimestampFollowTheProvider()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        using WasmSequencer sequencer = new(clock);
        var start = sequencer.Timestamp;

        clock.AdvanceBy(OneSecond);

        await Assert.That(sequencer.Now).IsEqualTo(DateTimeOffset.UnixEpoch + OneSecond);
        await Assert.That(sequencer.Timestamp - start).IsEqualTo(Sequencer.ToTimestampDelta(OneSecond));
    }

    /// <summary>Immediate work runs on the provider's next timer turn.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateWorkRunsWhenTheProviderFiresItsDrainTimer()
    {
        VirtualClock clock = new();
        using WasmSequencer sequencer = new(clock);
        RecordingWorkItem item = new();
        RecordingWorkItem cancelled = new();
        cancelled.Dispose();

        sequencer.Schedule(cancelled);
        sequencer.Schedule(item);

        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(1));

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
        await Assert.That(cancelled.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>Delayed work runs when the provider reaches its timestamp and not before.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedWorkRunsExactlyWhenTheProviderReachesItsTimestamp()
    {
        VirtualClock clock = new();
        using WasmSequencer sequencer = new(clock);
        RecordingWorkItem item = new();

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        clock.AdvanceBy(OneSecond - TimeSpan.FromTicks(1));

        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(1));

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Delayed work cancelled before its timestamp never runs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedWorkCancelledBeforeItsTimestampDoesNotRun()
    {
        VirtualClock clock = new();
        using WasmSequencer sequencer = new(clock);
        RecordingWorkItem item = new();

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        item.Dispose();
        clock.AdvanceBy(OneSecond);

        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>A provider counting at another frequency drives delayed work at the right wall time.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ProviderWithAnotherFrequencyDrivesDelayedWork()
    {
        VirtualClock clock = new();
        ScaledTimeProvider provider = new(clock, MillisecondFrequency);
        using WasmSequencer sequencer = new(provider);
        RecordingWorkItem item = new();

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        clock.AdvanceBy(OneSecond - TimeSpan.FromMilliseconds(1));

        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromMilliseconds(1));

        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Disposal cancels queued immediate work before the provider's drain timer fires.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeCancelsQueuedWorkBeforeTheDrainTimerFires()
    {
        VirtualClock clock = new();
        WasmSequencer sequencer = new(clock);
        RecordingWorkItem item = new();

        sequencer.Schedule(item);
        sequencer.Dispose();
        clock.AdvanceBy(OneSecond);

        await Assert.That(item.IsDisposed).IsTrue();
        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>Creates a sequencer whose event loop is driven explicitly.</summary>
    /// <param name="drains">The queued drain callbacks.</param>
    /// <param name="delays">The delayed callback queue.</param>
    /// <returns>The isolated sequencer.</returns>
    private static WasmSequencer CreateSequencer(Queue<Action> drains, ManualSequencer? delays = null) =>
        new(
            drain =>
            {
                drains.Enqueue(drain);
                return true;
            },
            (delays ?? new ManualSequencer()).Schedule);

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

    /// <summary>Cancellable work item that reports disposal to the sequencer.</summary>
    /// <param name="action">The action to run on execution.</param>
    /// <param name="onDisposed">An optional callback invoked the instant the item is disposed.</param>
    private sealed class CancellableWorkItem(Action action, Action? onDisposed = null) : IWorkItem, IsDisposed
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action = action;

        /// <summary>Reports disposal.</summary>
        private readonly Action? _onDisposed = onDisposed;

        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            IsDisposed = true;
            _onDisposed?.Invoke();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _action();
    }
}
