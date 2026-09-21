// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies delayed signal operator behavior.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>Verifies shift uses one ordered drain for a burst of delayed notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftUsesSingleSerializedDrainForQueuedNotifications()
    {
        var dueTime = TimeSpan.FromTicks(Ten);
        RecordingSequencer sequencer = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        RecordingWitness<int> observer = new();
        using var subscription = source.Shift(dueTime, sequencer).Subscribe(observer);

        source.OnNext(One);
        source.OnNext(Two);
        source.OnCompleted();

        await Assert.That(sequencer.ScheduledCount).IsEqualTo(One);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);

        sequencer.AdvanceBy(dueTime);
        sequencer.RunNext();

        await Assert.That(observer.Values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(One);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(sequencer.ScheduledCount).IsEqualTo(0);
    }

    /// <summary>An inline drain retains the successor it schedules before the initial scheduling call returns.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftRetainsTheDrainTimerArmedByAnInlineTick()
    {
        var dueTime = TimeSpan.FromTicks(Ten);
        FirstInlineSequencer sequencer = new(TimeSpan.Zero);
        Signal<int> source = new();
        RecordingWitness<int> observer = new();
        using var subscription = source.Shift(dueTime, sequencer).Subscribe(observer);

        source.OnNext(One);
        await Assert.That(observer.Values.Count).IsEqualTo(0);

        sequencer.Advance(dueTime);
        sequencer.RunPending();

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
    }

    /// <summary>Disposing during delivery still delivers the notifications that were already due, including the terminal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftDisposalClaimDuringDeliveryStillDeliversTheNotificationsAlreadyDue()
    {
        var dueTime = TimeSpan.FromTicks(One);
        RecordingSequencer sequencer = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        var completed = 0;
        var claimed = false;
        LinqExtensions.ShiftCoordinator<int>? coordinator = null;
        var observer = new DelegateWitness<int>(
            value =>
            {
                values.Add(value);
                claimed |= coordinator!.TryBeginDispose();
            },
            static _ => { },
            () => completed++);
        coordinator = new(source, dueTime, sequencer, observer);
        using var subscription = coordinator.Run();
        source.OnNext(One);
        source.OnNext(Two);
        source.OnCompleted();
        sequencer.AdvanceBy(dueTime);
        sequencer.RunNext();
        coordinator.ReleaseSubscriptions();
        await Assert.That(claimed).IsTrue();
        await Assert.That(coordinator.TryBeginDispose()).IsFalse();
        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
    }

    /// <summary>Verifies a delayed error is forwarded after the queued values that precede it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftForwardsDelayedErrorAfterQueuedValues()
    {
        var dueTime = TimeSpan.FromTicks(Ten);
        RecordingSequencer sequencer = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        RecordingWitness<int> observer = new();
        InvalidOperationException failure = new("boom");
        using var subscription = source.Shift(dueTime, sequencer).Subscribe(observer);

        source.OnNext(One);
        source.OnError(failure);

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);

        sequencer.AdvanceBy(dueTime);
        sequencer.RunNext();

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(observer.Errors.Count).IsEqualTo(One);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(failure);
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(sequencer.ScheduledCount).IsEqualTo(0);
    }

    /// <summary>Shift drops the notifications a source delivers after its terminal one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftDropsNotificationsAfterTerminalNotification()
    {
        var dueTime = TimeSpan.FromTicks(Ten);
        RecordingSequencer sequencer = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        RecordingWitness<int> observer = new();
        using var subscription = source.Shift(dueTime, sequencer).Subscribe(observer);

        source.OnNext(One);
        source.OnCompleted();
        source.OnNext(Two);

        sequencer.AdvanceBy(dueTime);
        sequencer.RunNext();

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(One);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(sequencer.ScheduledCount).IsEqualTo(0);
    }

    /// <summary>Verifies the drain reschedules when the next queued notification is not yet due.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftReschedulesWhenNextNotificationIsNotYetDue()
    {
        var step = TimeSpan.FromTicks(Five);
        var dueTime = TimeSpan.FromTicks(Ten);
        RecordingSequencer sequencer = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        RecordingWitness<int> observer = new();
        using var subscription = source.Shift(dueTime, sequencer).Subscribe(observer);

        source.OnNext(One);
        sequencer.AdvanceBy(step);
        source.OnNext(Two);

        await Assert.That(sequencer.ScheduledCount).IsEqualTo(One);

        sequencer.AdvanceBy(step);
        sequencer.RunNext();

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(sequencer.ScheduledCount).IsEqualTo(One);

        sequencer.AdvanceBy(step);
        sequencer.RunNext();

        await Assert.That(observer.Values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(sequencer.ScheduledCount).IsEqualTo(0);
    }

    /// <summary>Verifies shift over a sequencer driven by a virtual clock used as a time provider delivers each value one due time after it arrived.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftOverATimeProviderSequencerDeliversEachValueOneDueTimeLater()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        var sequencer = InlineThreadPool.Create(clock);
        var step = TimeSpan.FromTicks(Five);
        var dueTime = TimeSpan.FromTicks(Ten);
        Signal<int> source = new();
        RecordingWitness<int> observer = new();
        using var subscription = source.Shift(dueTime, sequencer).Subscribe(observer);

        source.OnNext(One);
        clock.AdvanceBy(step);
        source.OnNext(Two);
        clock.AdvanceBy(step - TimeSpan.FromTicks(One));

        await Assert.That(observer.Values.Count).IsEqualTo(0);

        clock.AdvanceBy(TimeSpan.FromTicks(One));

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();

        clock.AdvanceBy(step);

        await Assert.That(observer.Values.SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Sequencer that records scheduled work for deterministic execution.</summary>
    private sealed class RecordingSequencer : ISequencer
    {
        /// <summary>The scheduled work items.</summary>
        private readonly Queue<IWorkItem> _items = [];

        /// <summary>Initializes a new instance of the <see cref="RecordingSequencer"/> class.</summary>
        /// <param name="now">The initial scheduler clock.</param>
        public RecordingSequencer(DateTimeOffset now) => Now = now;

        /// <inheritdoc/>
        public DateTimeOffset Now { get; private set; }

        /// <inheritdoc/>
        public long Timestamp => 0;

        /// <summary>Gets the number of scheduled work items.</summary>
        public int ScheduledCount => _items.Count;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item) => _items.Enqueue(item);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2318:Members should not have identical bodies",
            Justification =
                "Both Schedule overloads are required by the ISequencer contract and cannot forward to one another.")]
        public void Schedule(IWorkItem item, long dueTimestamp) => _items.Enqueue(item);

        /// <summary>Advances the scheduler clock without running queued work.</summary>
        /// <param name="time">The clock movement.</param>
        public void AdvanceBy(TimeSpan time) => Now += time;

        /// <summary>Runs the next scheduled work item.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RunNext() => _items.Dequeue().Execute();
    }
}
