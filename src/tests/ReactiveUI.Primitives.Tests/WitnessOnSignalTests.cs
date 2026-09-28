// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the <c>WitnessOn</c> / <c>ObserveOn</c> signal, which queues source notifications and drains them
/// on a sequencer, and stops delivering as soon as the subscription is torn down.
/// </summary>
public sealed class WitnessOnSignalTests
{
    /// <summary>The first value pushed through the dispatch queue.</summary>
    private const int First = 1;

    /// <summary>The second value pushed through the dispatch queue.</summary>
    private const int Second = 2;

    /// <summary>A single virtual tick, enough to drain the sequencer queue.</summary>
    private static readonly TimeSpan SingleTick = TimeSpan.FromTicks(1);

    /// <summary>The values expected when only the first queued notification is delivered.</summary>
    private static readonly int[] ExpectedFirstOnly = [First];

    /// <summary>The dispatch signal drives its source on the current thread and defers delivery to the sequencer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatchDefersDeliveryToTheSequencer()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        RecordingWitness<int> witness = new();

        var dispatched = source.WitnessOn(clock);
        await Assert.That(((IRequireCurrentThread<int>)dispatched).IsRequiredSubscribeOnCurrentThread()).IsTrue();

        using var subscription = dispatched.Subscribe(witness);
        source.OnNext(First);
        source.OnNext(Second);

        await Assert.That(witness.Values.Count).IsEqualTo(0);

        clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Values.SequenceEqual([First, Second])).IsTrue();

        source.OnCompleted();
        clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Completed).IsEqualTo(1);
    }

    /// <summary>The dispatch signal forwards a source error through the sequencer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatchForwardsSourceErrorsThroughTheSequencer()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        RecordingWitness<int> witness = new();
        InvalidOperationException expected = new("dispatch");

        using var subscription = source.WitnessOn(clock).Subscribe(witness);
        source.OnError(expected);

        await Assert.That(witness.Errors.Count).IsEqualTo(0);

        clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Errors.Count).IsEqualTo(1);
        await Assert.That(witness.Errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>A drain that is torn down mid-flight abandons the notifications queued behind it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatchAbandonsQueuedNotificationsWhenDisposedMidDrain()
    {
        VirtualClock clock = new();
        Signal<int> source = new();
        List<int> observed = [];
        IDisposable? subscription = null;

        subscription = source.WitnessOn(clock).Subscribe(value =>
        {
            observed.Add(value);
            subscription?.Dispose();
        });

        source.OnNext(First);
        source.OnNext(Second);

        clock.AdvanceBy(SingleTick);

        // The first value tore the subscription down, so the second must never leave the queue.
        await Assert.That(observed.SequenceEqual(ExpectedFirstOnly)).IsTrue();
    }

    /// <summary>A torn-down dispatch signal refuses to queue any further source notification.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatchIgnoresSourceNotificationsAfterDisposal()
    {
        VirtualClock clock = new();
        IObserver<int>? upstream = null;
        RecordingWitness<int> witness = new();

        var subscription = new ScriptedObservable<int>(observer => upstream = observer)
            .WitnessOn(clock)
            .Subscribe(witness);
        subscription.Dispose();

        upstream!.OnNext(First);
        upstream.OnCompleted();
        clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Completed).IsEqualTo(0);
    }

    /// <summary>A notification raised on the owning thread with nothing waiting is delivered inline.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OwnerDeliversInlineOnTheOwningThread()
    {
        OwnedClock clock = new() { Owned = true };
        Signal<int> source = new();
        RecordingWitness<int> witness = new();

        using var subscription = source.WitnessOnOwner(clock).Subscribe(witness);
        source.OnNext(First);
        source.OnCompleted();

        await Assert.That(witness.Values.SequenceEqual(ExpectedFirstOnly)).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
        await Assert.That(clock.Scheduled).IsEqualTo(0);
    }

    /// <summary>A notification raised off the owning thread is dispatched through the sequencer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OwnerDispatchesOffTheOwningThread()
    {
        OwnedClock clock = new();
        Signal<int> source = new();
        RecordingWitness<int> witness = new();

        using var subscription = new WitnessOnSignal<int>(source, clock, clock).Subscribe(witness);
        source.OnNext(First);

        await Assert.That(witness.Values.Count).IsEqualTo(0);

        clock.Clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Values.SequenceEqual(ExpectedFirstOnly)).IsTrue();
    }

    /// <summary>Once a notification waits, one raised on the owning thread queues behind it rather than overtaking it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OwnedNotificationQueuesBehindAWaitingOne()
    {
        OwnedClock clock = new();
        Signal<int> source = new();
        RecordingWitness<int> witness = new();

        using var subscription = source.WitnessOnOwner(clock).Subscribe(witness);
        source.OnNext(First);
        clock.Owned = true;
        source.OnNext(Second);

        await Assert.That(witness.Values.Count).IsEqualTo(0);

        clock.Clock.AdvanceBy(SingleTick);

        await Assert.That(witness.Values.SequenceEqual([First, Second])).IsTrue();
    }

    /// <summary>A notification raised while an inline delivery runs waits until that delivery returns.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReentrantNotificationFollowsTheInlineDelivery()
    {
        OwnedClock clock = new() { Owned = true };
        Signal<int> source = new();
        List<string> log = [];

        using var subscription = source.WitnessOnOwner(clock).Subscribe(value =>
        {
            log.Add($"start {value}");
            if (value == First)
            {
                source.OnNext(Second);
            }

            log.Add($"end {value}");
        });
        source.OnNext(First);

        await Assert.That(log.SequenceEqual(["start 1", "end 1", "start 2", "end 2"])).IsTrue();
        await Assert.That(clock.Scheduled).IsEqualTo(0);
    }

    /// <summary>A sequencer over a virtual clock whose thread ownership the test sets.</summary>
    private sealed class OwnedClock : ISequencer, IThreadAffineSequencer
    {
        /// <summary>Gets the clock that runs the dispatched work.</summary>
        public VirtualClock Clock { get; } = new();

        /// <summary>Gets or sets a value indicating whether the calling thread owns the sequencer.</summary>
        public bool Owned { get; set; }

        /// <summary>Gets the number of work items dispatched through the clock.</summary>
        public int Scheduled { get; private set; }

        /// <inheritdoc/>
        public DateTimeOffset Now => Clock.Now;

        /// <inheritdoc/>
        public long Timestamp => Clock.Timestamp;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CheckAccess() => Owned;

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            Scheduled++;
            Clock.Schedule(item);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Clock.Schedule(item, dueTimestamp);
    }
}
