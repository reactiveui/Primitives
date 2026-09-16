// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the signal that serializes notifications from any thread into its subscribers.</summary>
[DebuggerDisplay("SerializedSignalTests")]
public sealed class SerializedSignalTests
{
    /// <summary>The number of producers in the contention test.</summary>
    private const int Producers = 4;

    /// <summary>The number of values each producer pushes in the contention test.</summary>
    private const int ValuesPerProducer = 2_000;

    /// <summary>The second value a test pushes.</summary>
    private const int Second = 2;

    /// <summary>Producers on separate threads reach every subscriber without overlapping deliveries.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConcurrentProducersReachEverySubscriberWithoutOverlap()
    {
        using var signal = Signal.Serialized<int>();
        var inFlight = 0;
        var overlaps = 0;
        var firstDelivered = 0;
        var secondDelivered = 0;
        using var first = signal.Subscribe(new CallbackRecordingWitness<int>(_ =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            firstDelivered++;
            _ = Interlocked.Decrement(ref inFlight);
        }));
        using var second = signal.Subscribe(new CallbackRecordingWitness<int>(_ => secondDelivered++));

        var producers = new Task<int>[Producers];
        for (var p = 0; p < Producers; p++)
        {
            producers[p] = BackgroundThread.Start(() =>
            {
                for (var i = 0; i < ValuesPerProducer; i++)
                {
                    signal.OnNext(i);
                }
            });
        }

        await Task.WhenAll(producers);

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(firstDelivered).IsEqualTo(Producers * ValuesPerProducer);
        await Assert.That(secondDelivered).IsEqualTo(Producers * ValuesPerProducer);
    }

    /// <summary>A subscriber that marshals synchronously to a producer's thread is not deadlocked by that producer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SubscriberMarshallingToAProducerThreadDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        using var signal = Signal.Serialized<int>();
        ConcurrentQueue<int> values = new();
        using var subscription = signal.Subscribe(new CallbackRecordingWitness<int>(value =>
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
            signal.OnNext(Second);
        });
        var worker = BackgroundThread.Start(() => signal.OnNext(1));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
    }

    /// <summary>Completion raised while another thread delivers follows the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompletionRaisedDuringDeliveryFollowsTheQueuedValues()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        using var signal = Signal.Serialized<int>();
        List<int> values = [];
        CallbackRecordingWitness<int> subscriber = new(value =>
        {
            values.Add(value);
            if (value != 1)
            {
                return;
            }

            inside.Set();
            release.Wait();
        });
        using var subscription = signal.Subscribe(subscriber);

        var owner = BackgroundThread.Start(() => signal.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => signal.OnNext(Second));
        await BackgroundThread.Start(signal.OnCompleted);
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(subscriber.IsCompleted).IsTrue();
    }

    /// <summary>A serialized signal over an existing signal forwards subscriptions, state and disposal to it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WrappedSignalKeepsSubscriptionsStateAndDisposal()
    {
        Signal<int> inner = new();
        var signal = Signal.Serialized(inner);
        RecordingWitness<int> subscriber = new();

        var hadObservers = signal.HasObservers;
        var subscription = signal.Subscribe(subscriber);
        var hasObservers = signal.HasObservers && inner.HasObservers;
        signal.OnNext(1);
        subscription.Dispose();
        signal.Dispose();

        await Assert.That(hadObservers).IsFalse();
        await Assert.That(hasObservers).IsTrue();
        await Assert.That(string.Join(",", subscriber.Values)).IsEqualTo("1");
        await Assert.That(signal.IsDisposed).IsTrue();
        await Assert.That(inner.IsDisposed).IsTrue();
        await Assert.That(static () => new SerializedSignal<int>(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>An error reaches the subscribers of the wrapped signal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ErrorReachesSubscribers()
    {
        using SerializedSignal<int> signal = new();
        RecordingWitness<int> subscriber = new();
        InvalidOperationException expected = new("signal");
        using var subscription = signal.Subscribe(subscriber);

        signal.OnError(expected);

        await Assert.That(subscriber.Errors.Count).IsEqualTo(1);
        await Assert.That(subscriber.Errors[0]).IsSameReferenceAs(expected);
    }
}
