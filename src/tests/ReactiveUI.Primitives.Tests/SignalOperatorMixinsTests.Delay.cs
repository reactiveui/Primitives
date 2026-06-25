// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Threading;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies delayed signal operator behavior.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>Observation window used to verify disposal waits for in-flight delivery.</summary>
    private const int DisposeObservationMilliseconds = 200;

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

    /// <summary>Verifies that dispose waits for in-flight delivery and blocks queued notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ShiftDisposeWaitsForInflightDeliveryAndDisallowsFurtherDelivery()
    {
        TimeSpan dueTime = TimeSpan.FromMilliseconds(One);
        Signal<int> source = new();
        using ManualResetEventSlim onNextEntered = new(false);
        using ManualResetEventSlim onNextRelease = new(false);
        ConcurrentQueue<int> values = [];
        var completed = 0;
        var delivered = 0;

        using var subscription = source
            .Shift(dueTime, TaskPoolSequencer.Instance)
            .Subscribe(
                value =>
                {
                    if (Interlocked.Increment(ref delivered) == 1)
                    {
                        onNextEntered.Set();
                    }

                    onNextRelease.Wait();
                    values.Enqueue(value);
                },
                _ => { },
                () => Interlocked.Increment(ref completed));

        source.OnNext(One);
        source.OnNext(Two);
        source.OnCompleted();

        await Assert.That(onNextEntered.Wait(TimeSpan.FromSeconds(1))).IsTrue();

        Task disposeTask = Task.Run(subscription.Dispose);
        Task disposeObservation = Task.Delay(TimeSpan.FromMilliseconds(DisposeObservationMilliseconds));
        var disposeCompleted = await Task.WhenAny(disposeTask, disposeObservation) == disposeTask;

        await Assert.That(disposeCompleted).IsFalse();

        onNextRelease.Set();
        await disposeTask;

        await Assert.That(values.ToArray().SequenceEqual([One])).IsTrue();

        await Assert.That(Volatile.Read(ref completed)).IsEqualTo(0);
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
        public void Schedule(IWorkItem item) => _items.Enqueue(item);

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp) => _items.Enqueue(item);

        /// <summary>Advances the scheduler clock without running queued work.</summary>
        /// <param name="time">The clock movement.</param>
        public void AdvanceBy(TimeSpan time) => Now += time;

        /// <summary>Runs the next scheduled work item.</summary>
        public void RunNext() => _items.Dequeue().Execute();
    }
}
