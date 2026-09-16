// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the observer wrapper that serializes notifications without holding a lock while the observer runs.</summary>
[DebuggerDisplay("SerializedWitnessTests")]
public sealed class SerializedWitnessTests
{
    /// <summary>The number of producers in the contention test.</summary>
    private const int Producers = 4;

    /// <summary>The number of values each producer pushes in the contention test.</summary>
    private const int ValuesPerProducer = 2_000;

    /// <summary>The second value a test pushes.</summary>
    private const int Second = 2;

    /// <summary>The third value a test pushes.</summary>
    private const int Third = 3;

    /// <summary>An observer that marshals synchronously to a producer's thread is not deadlocked by that producer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverMarshallingToAProducerThreadDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        ConcurrentQueue<int> values = new();
        SerializedWitness<int> witness = new(new CallbackRecordingWitness<int>(value =>
        {
            values.Enqueue(value);
            if (Environment.CurrentManagedThreadId == dispatcher.ManagedThreadId || inside.IsSet)
            {
                return;
            }

            inside.Set();
            dispatcher.Invoke(static () => { });
        }));

        dispatcher.Post(() =>
        {
            inside.Wait();
            witness.OnNext(Second);
        });
        var worker = BackgroundThread.Start(() => witness.OnNext(1));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
    }

    /// <summary>A value pushed by the observer itself is delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValueRaisedByTheObserverIsDeliveredAfterItReturns()
    {
        List<string> log = [];
        SerializedWitness<int>? witness = null;
        witness = new(new CallbackRecordingWitness<int>(value =>
        {
            log.Add($"start{value}");
            if (value == 1)
            {
                witness!.OnNext(Second);
            }

            log.Add($"end{value}");
        }));

        witness.OnNext(1);

        await Assert.That(string.Join(",", log)).IsEqualTo("start1,end1,start2,end2");
    }

    /// <summary>Producers on separate threads never overlap downstream, and each producer's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConcurrentProducersDeliverEveryValueInOrderWithoutOverlap()
    {
        var lastSeen = new int[Producers];
        Array.Fill(lastSeen, -1);
        var inFlight = 0;
        var overlaps = 0;
        var outOfOrder = 0;
        var delivered = 0;
        SerializedWitness<(int Producer, int Sequence)> witness = new(new CallbackRecordingWitness<(int Producer, int Sequence)>(item =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            if (item.Sequence != lastSeen[item.Producer] + 1)
            {
                outOfOrder++;
            }

            lastSeen[item.Producer] = item.Sequence;
            delivered++;
            _ = Interlocked.Decrement(ref inFlight);
        }));

        var producers = new Task<int>[Producers];
        for (var p = 0; p < Producers; p++)
        {
            var producer = p;
            producers[p] = BackgroundThread.Start(() =>
            {
                for (var sequence = 0; sequence < ValuesPerProducer; sequence++)
                {
                    witness.OnNext((producer, sequence));
                }
            });
        }

        await Task.WhenAll(producers);

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(delivered).IsEqualTo(Producers * ValuesPerProducer);
    }

    /// <summary>Posted notifications are held until a flush delivers them in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PostedNotificationsWaitForFlush()
    {
        RecordingWitness<int> downstream = new();
        SerializedWitness<int> witness = new(downstream);

        var queuedFirst = witness.Post(1);
        _ = witness.Post(Second);
        var queuedCompletion = witness.PostCompleted();
        var queuedAfterCompletion = witness.Post(Third);
        var valuesBeforeFlush = downstream.Values.Count;
        witness.Flush();

        await Assert.That(queuedFirst).IsTrue();
        await Assert.That(queuedCompletion).IsTrue();
        await Assert.That(queuedAfterCompletion).IsFalse();
        await Assert.That(valuesBeforeFlush).IsEqualTo(0);
        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1,2");
        await Assert.That(downstream.Completed).IsEqualTo(1);
        await Assert.That(witness.IsTerminated).IsTrue();
    }

    /// <summary>Only the first terminal notification is delivered, and no value follows it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FirstTerminalNotificationWins()
    {
        RecordingWitness<int> downstream = new();
        SerializedWitness<int> witness = new(downstream);
        InvalidOperationException expected = new("first");

        witness.OnError(expected);
        witness.OnCompleted();
        witness.OnError(new InvalidOperationException("second"));
        witness.OnNext(1);

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completed).IsEqualTo(0);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }

    /// <summary>An observer that throws on a direct delivery leaves the witness able to deliver the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverThrowingOnDirectDeliveryLeavesTheWitnessUsable()
    {
        List<int> values = [];
        SerializedWitness<int> witness = new(new CallbackRecordingWitness<int>(value =>
        {
            if (value == 1)
            {
                throw new InvalidOperationException("observer");
            }

            values.Add(value);
        }));

        await Assert.That(() => witness.OnNext(1)).Throws<InvalidOperationException>();
        witness.OnNext(Second);

        await Assert.That(string.Join(",", values)).IsEqualTo("2");
    }

    /// <summary>An error raised while another thread delivers follows the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ErrorRaisedDuringDeliveryFollowsTheQueuedValues()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        List<int> values = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            values.Add(value);
            if (value != 1)
            {
                return;
            }

            inside.Set();
            release.Wait();
        });
        SerializedWitness<int> witness = new(downstream);
        InvalidOperationException expected = new("witness");

        var owner = BackgroundThread.Start(() => witness.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => witness.OnNext(Second));
        await BackgroundThread.Start(() => witness.OnNext(Third));
        await BackgroundThread.Start(() => witness.OnError(expected));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2,3");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }

    /// <summary>The constructor and the error entry points reject null.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NullArgumentsAreRejected()
    {
        SerializedWitness<int> witness = new(new RecordingWitness<int>());

        await Assert.That(static () => new SerializedWitness<int>(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => witness.PostError(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => witness.OnError(null!)).Throws<ArgumentNullException>();
    }
}
