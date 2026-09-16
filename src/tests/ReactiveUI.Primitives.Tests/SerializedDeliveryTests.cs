// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Extensions;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the re-entrant mode and stopping of the embeddable serialized delivery.</summary>
public sealed class SerializedDeliveryTests
{
    /// <summary>The second value a test pushes.</summary>
    private const int Second = 2;

    /// <summary>In re-entrant mode, a value raised by the observer on the delivering thread is delivered before the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ReentrantModeDeliversANestedValueInside()
    {
        List<string> log = [];
        DeliveryOwner? owner = null;
        owner = new(
            new CallbackRecordingWitness<int>(value =>
            {
                log.Add($"start{value}");
                if (value == 1)
                {
                    owner!.OnNext(Second);
                }

                log.Add($"end{value}");
            }),
            reentrant: true);

        owner.OnNext(1);
        owner.OnNext(Second + 1);

        await Assert.That(string.Join(",", log)).IsEqualTo("start1,start2,end2,end1,start3,end3");
    }

    /// <summary>In re-entrant mode, a nested value that throws leaves the outer delivery able to deliver the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ReentrantNestedThrowLeavesTheDeliveryUsable()
    {
        List<int> values = [];
        DeliveryOwner? owner = null;
        owner = new(
            new CallbackRecordingWitness<int>(value =>
            {
                values.Add(value);
                if (value == Second)
                {
                    throw new InvalidOperationException("nested");
                }

                if (value == 1)
                {
                    _ = Assert.Throws<InvalidOperationException>(() => owner!.OnNext(Second));
                }
            }),
            reentrant: true);

        owner.OnNext(1);
        owner.OnNext(Second + 1);

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2,3");
    }

    /// <summary>Values raised while the initial value is being read are delivered after it, in both delivery modes.</summary>
    /// <param name="reentrant">Whether nested values are delivered inside.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StartDeliversTheInitialValueBeforeValuesRaisedDuringTheRead(bool reentrant)
    {
        RecordingWitness<int> downstream = new();
        DeliveryOwner owner = new(downstream, reentrant);

        owner.Start(() =>
        {
            owner.OnNext(1);
            owner.OnNext(Second);
            return 0;
        });

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("0,1,2");
    }

    /// <summary>A reader that throws fails the start and leaves the delivery able to deliver the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StartWithAThrowingReaderLeavesTheDeliveryUsable()
    {
        RecordingWitness<int> downstream = new();
        DeliveryOwner owner = new(downstream, reentrant: false);

        _ = Assert.Throws<InvalidOperationException>(() => owner.Start(static () => throw new InvalidOperationException("read")));
        owner.OnNext(Second);

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("2");
    }

    /// <summary>Starting a stopped delivery neither reads nor delivers.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StartAfterStopNeitherReadsNorDelivers()
    {
        RecordingWitness<int> downstream = new();
        DeliveryOwner owner = new(downstream, reentrant: false);
        var reads = 0;

        owner.Stop();
        owner.Start(() =>
        {
            reads++;
            return 1;
        });

        await Assert.That(reads).IsEqualTo(0);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }

    /// <summary>An initial value read while values are already queued is delivered after them.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StartBehindQueuedValuesDeliversTheInitialValueLast()
    {
        RecordingWitness<int> downstream = new();
        DeliveryOwner owner = new(downstream, reentrant: false);

        _ = owner.Post(1);
        owner.Start(static () => Second);

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1,2");
    }

    /// <summary>Starting from inside a running delivery queues the initial value behind that delivery.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StartDuringADeliveryQueuesTheInitialValueBehindIt()
    {
        List<int> values = [];
        DeliveryOwner? owner = null;
        owner = new(
            new CallbackRecordingWitness<int>(value =>
            {
                values.Add(value);
                if (value == 1)
                {
                    owner!.Start(static () => Second);
                }
            }),
            reentrant: false);

        owner.OnNext(1);

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
    }

    /// <summary>Values delivered on a claimed delivery arrive in order, followed by anything queued while they were delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClaimedValuesArriveInOrderBeforeValuesQueuedDuringThem()
    {
        List<int> values = [];
        DeliveryOwner? owner = null;
        owner = new(
            new CallbackRecordingWitness<int>(value =>
            {
                values.Add(value);
                if (value == 1)
                {
                    owner!.OnNext(Second + 1);
                }
            }),
            reentrant: false);

        var claimed = owner.TryClaim();
        owner.DeliverClaimed([1, Second]);

        await Assert.That(claimed).IsTrue();
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2,3");
    }

    /// <summary>Stopping during a claimed multi-value delivery skips the values not yet delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StopDuringClaimedValuesSkipsTheRest()
    {
        List<int> values = [];
        DeliveryOwner? owner = null;
        owner = new(
            new CallbackRecordingWitness<int>(value =>
            {
                values.Add(value);
                owner!.Stop();
            }),
            reentrant: false);

        _ = owner.TryClaim();
        owner.DeliverClaimed([1, Second]);

        await Assert.That(string.Join(",", values)).IsEqualTo("1");
    }

    /// <summary>An observer that throws during claimed values releases the delivery for the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThrowDuringClaimedValuesReleasesTheDelivery()
    {
        List<int> values = [];
        DeliveryOwner owner = new(
            new CallbackRecordingWitness<int>(value =>
            {
                values.Add(value);
                if (value == 1)
                {
                    throw new InvalidOperationException("claimed");
                }
            }),
            reentrant: false);

        _ = owner.TryClaim();
        _ = Assert.Throws<InvalidOperationException>(() => owner.DeliverClaimed([1, Second]));
        owner.OnNext(Second + 1);

        await Assert.That(string.Join(",", values)).IsEqualTo("1,3");
    }

    /// <summary>Stopping drops queued notifications and refuses everything after.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StopDropsQueuedNotifications()
    {
        RecordingWitness<int> downstream = new();
        DeliveryOwner owner = new(downstream, reentrant: false);

        var queued = owner.Post(1);
        _ = owner.PostCompleted();
        owner.Stop();
        var queuedAfterStop = owner.Post(Second);
        owner.Flush();
        owner.OnNext(Second);

        await Assert.That(queued).IsTrue();
        await Assert.That(queuedAfterStop).IsFalse();
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(0);
        await Assert.That(owner.IsTerminated).IsTrue();
    }

    /// <summary>An owner embedding the delivery, as an operator would.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="reentrant">Whether nested values are delivered inside.</param>
    private sealed class DeliveryOwner(IObserver<int> observer, bool reentrant)
    {
        /// <summary>The embedded delivery.</summary>
        private SerializedDelivery<int> _delivery = new(reentrant);

        /// <summary>Gets a value indicating whether delivery has terminated.</summary>
        public bool IsTerminated => _delivery.IsTerminated;

        /// <summary>Gets the downstream observer.</summary>
        private IObserver<int> Observer => observer;

        /// <summary>Delivers a value.</summary>
        /// <param name="value">The value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(int value) => _delivery.OnNext(observer, value, new OwnerDrain(this));

        /// <summary>Claims the delivery.</summary>
        /// <returns>Whether the delivery was claimed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryClaim() => _delivery.TryClaim();

        /// <summary>Delivers values on the claimed delivery.</summary>
        /// <param name="values">The values.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DeliverClaimed(int[] values) => _delivery.DeliverClaimed(observer, values, new OwnerDrain(this));

        /// <summary>Reads and delivers the initial value.</summary>
        /// <param name="read">Reads the initial value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Start(Func<int> read) => _delivery.Start(observer, new DelegateReader(read), new OwnerDrain(this));

        /// <summary>Queues a value.</summary>
        /// <param name="value">The value.</param>
        /// <returns>Whether it was queued.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(int value) => _delivery.Post(value);

        /// <summary>Queues completion.</summary>
        /// <returns>Whether it was queued.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool PostCompleted() => _delivery.PostCompleted();

        /// <summary>Delivers queued notifications.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush() => _delivery.Flush(new OwnerDrain(this));

        /// <summary>Stops delivery.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Stop() => _delivery.Stop();

        /// <summary>Reads the initial value through a delegate.</summary>
        /// <param name="ReadValue">Reads the value.</param>
        private readonly record struct DelegateReader(Func<int> ReadValue) : ICurrentValueReader<int>
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Read() => ReadValue();
        }

        /// <summary>Drains the owner's queued notifications.</summary>
        /// <param name="Owner">The owner.</param>
        private readonly record struct OwnerDrain(DeliveryOwner Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner.Observer);
        }
    }
}
