// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the <c>Serialize</c> operator, which delivers notifications one at a time without holding a lock while the observer runs.</summary>
public sealed class SerializeSignalTests
{
    /// <summary>The number of producers in the contention test.</summary>
    private const int Producers = 4;

    /// <summary>The number of values each producer pushes in the contention test.</summary>
    private const int ValuesPerProducer = 2_000;

    /// <summary>The second value a test pushes.</summary>
    private const int Second = 2;

    /// <summary>The operator forwards the source values and completion unchanged.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ForwardsTheSourceSequence()
    {
        RecordingWitness<int> downstream = new();
        ScriptedObservable<int> source = new(static observer =>
        {
            observer.OnNext(1);
            observer.OnNext(Second);
            observer.OnCompleted();
        });

        using var subscription = source.Serialize().Subscribe(downstream);

        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("1,2");
        await Assert.That(downstream.Completed).IsEqualTo(1);
    }

    /// <summary>An observer that marshals synchronously to a producer's thread is not deadlocked by that producer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverMarshallingToAProducerThreadDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        ConcurrentQueue<int> values = new();
        IObserver<int>? producer = null;
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            values.Enqueue(value);
            if (Environment.CurrentManagedThreadId == dispatcher.ManagedThreadId || inside.IsSet)
            {
                return;
            }

            inside.Set();
            dispatcher.Invoke(static () => { });
        });
        using var subscription = new ScriptedObservable<int>(observer => producer = observer).Serialize().Subscribe(downstream);

        dispatcher.Post(() =>
        {
            inside.Wait();
            producer!.OnNext(Second);
        });
        var worker = BackgroundThread.Start(() => producer!.OnNext(1));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
    }

    /// <summary>A value the source pushes from inside the observer is delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValueRaisedByTheObserverIsDeliveredAfterItReturns()
    {
        List<string> log = [];
        IObserver<int>? producer = null;
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            log.Add($"start{value}");
            if (value == 1)
            {
                producer!.OnNext(Second);
            }

            log.Add($"end{value}");
        });
        using var subscription = new ScriptedObservable<int>(observer => producer = observer).Serialize().Subscribe(downstream);

        producer!.OnNext(1);

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
        IObserver<(int Producer, int Sequence)>? source = null;
        CallbackRecordingWitness<(int Producer, int Sequence)> downstream = new(item =>
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
        });
        using var subscription = new ScriptedObservable<(int Producer, int Sequence)>(observer => source = observer)
            .Serialize()
            .Subscribe(downstream);

        var producers = new Task<int>[Producers];
        for (var p = 0; p < Producers; p++)
        {
            var producer = p;
            producers[p] = BackgroundThread.Start(() =>
            {
                for (var sequence = 0; sequence < ValuesPerProducer; sequence++)
                {
                    source!.OnNext((producer, sequence));
                }
            });
        }

        await Task.WhenAll(producers);

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(delivered).IsEqualTo(Producers * ValuesPerProducer);
    }

    /// <summary>Only the first terminal notification is delivered, and no value follows it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FirstTerminalNotificationWins()
    {
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("first");
        ScriptedObservable<int> source = new(observer =>
        {
            observer.OnError(expected);
            observer.OnCompleted();
            observer.OnNext(1);
        });

        using var subscription = source.Serialize().Subscribe(downstream);

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completed).IsEqualTo(0);
        await Assert.That(downstream.Values.Count).IsEqualTo(0);
    }

    /// <summary>A completion queued behind a running delivery is still delivered when the subscription is disposed before it drains.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompletionQueuedBeforeDisposeIsStillDelivered()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<int>? producer = null;
        CallbackRecordingWitness<int> downstream = new(_ =>
        {
            inside.Set();
            release.Wait();
        });
        var subscription = new ScriptedObservable<int>(observer => producer = observer).Serialize().Subscribe(downstream);

        var owner = BackgroundThread.Start(() => producer!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => producer!.OnCompleted());
        subscription.Dispose();
        release.Set();
        await owner;

        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>Disposing the subscription releases the upstream subscription exactly once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeReleasesTheUpstreamSubscriptionOnce()
    {
        RecordingDisposable upstream = new();
        var subscription = new ScriptedObservable<int>(static _ => { }, upstream).Serialize().Subscribe(new RecordingWitness<int>());

        subscription.Dispose();
        subscription.Dispose();

        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The operator rejects a null source and the subscription rejects a null observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NullArgumentsAreRejected()
    {
        var serialized = new ScriptedObservable<int>(static _ => { }).Serialize();

        await Assert.That(static () => ((IObservable<int>)null!).Serialize()).Throws<ArgumentNullException>();
        await Assert.That(() => serialized.Subscribe(null!)).Throws<ArgumentNullException>();
    }
}
