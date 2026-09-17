// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests merge completion tracking and delivery serialization across inner sources.</summary>
[DebuggerDisplay("MergeCoordinatorTests")]
public sealed class MergeCoordinatorTests
{
    /// <summary>The number of sources in the contention test.</summary>
    private const int Sources = 4;

    /// <summary>The number of values each source produces in the contention test.</summary>
    private const int ValuesPerSource = 2_000;

    /// <summary>The second value a test pushes.</summary>
    private const int Second = 2;

    /// <summary>The third value a test pushes.</summary>
    private const int Third = 3;

    /// <summary>Repeated completion cannot consume another inner source's active count.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RepeatedInnerCompletionKeepsTheSiblingActive()
    {
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        RecordingWitness<int> downstream = new();
        ScriptedObservable<int> leftSource = new(observer => left = observer);
        ScriptedObservable<int> rightSource = new(observer => right = observer);

        using var subscription = new MergeCoordinator<int>(downstream).Run([leftSource, rightSource]);

        left!.OnCompleted();
        left.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(0);

        right!.OnCompleted();
        await Assert.That(downstream.Completed).IsEqualTo(1);
        await Assert.That(right).IsNotNull();
    }

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes a value
    /// through a sibling source, and both values are delivered in order.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        IObserver<int>? left = null;
        IObserver<int>? right = null;
        ConcurrentQueue<int> values = new();
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

        using var subscription = new MergeCoordinator<int>(downstream).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(observer => right = observer));

        dispatcher.Post(() =>
        {
            inside.Wait();
            right!.OnNext(Second);
        });
        var worker = BackgroundThread.Start(() => left!.OnNext(1));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
    }

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValueRaisedByTheObserverIsDeliveredAfterItReturns()
    {
        IObserver<int>? left = null;
        List<string> log = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            log.Add($"start{value}");
            if (value == 1)
            {
                left!.OnNext(Second);
            }

            log.Add($"end{value}");
        });

        using var subscription = new MergeCoordinator<int>(downstream).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(static _ => { }));
        left!.OnNext(1);

        await Assert.That(string.Join(",", log)).IsEqualTo("start1,end1,start2,end2");
    }

    /// <summary>Sources pushing from separate threads never overlap downstream, and each source's values arrive in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap()
    {
        var observers = new IObserver<(int Source, int Sequence)>[Sources];
        var sources = new IObservable<(int Source, int Sequence)>[Sources];
        for (var s = 0; s < Sources; s++)
        {
            var index = s;
            sources[s] = new ScriptedObservable<(int Source, int Sequence)>(observer => observers[index] = observer);
        }

        var lastSeen = new int[Sources];
        Array.Fill(lastSeen, -1);
        var inFlight = 0;
        var overlaps = 0;
        var outOfOrder = 0;
        var delivered = 0;
        CallbackRecordingWitness<(int Source, int Sequence)> downstream = new(item =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            if (item.Sequence != lastSeen[item.Source] + 1)
            {
                outOfOrder++;
            }

            lastSeen[item.Source] = item.Sequence;
            delivered++;
            _ = Interlocked.Decrement(ref inFlight);
        });

        using var subscription = new MergeCoordinator<(int Source, int Sequence)>(downstream).Run(sources);
        var producers = new Task<int>[Sources];
        for (var s = 0; s < Sources; s++)
        {
            var source = s;
            producers[s] = BackgroundThread.Start(() =>
            {
                for (var sequence = 0; sequence < ValuesPerSource; sequence++)
                {
                    observers[source].OnNext((source, sequence));
                }
            });
        }

        await Task.WhenAll(producers);

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(delivered).IsEqualTo(Sources * ValuesPerSource);
    }

    /// <summary>An observer that throws on a direct delivery leaves the merge able to deliver the next value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ObserverThrowingOnDirectDeliveryLeavesTheMergeUsable()
    {
        IObserver<int>? left = null;
        List<int> values = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            if (value == 1)
            {
                throw new InvalidOperationException("observer");
            }

            values.Add(value);
        });

        using var subscription = new MergeCoordinator<int>(downstream).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(static _ => { }));

        await Assert.That(() => left!.OnNext(1)).Throws<InvalidOperationException>();
        left!.OnNext(Second);

        await Assert.That(string.Join(",", values)).IsEqualTo("2");
    }

    /// <summary>A value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValuePushedWhileTheTerminalWaitsIsDropped()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        IObserver<int>? third = null;
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
        InvalidOperationException expected = new("merge");

        using var subscription = new MergeCoordinator<int>(downstream).Run(
        [
            new ScriptedObservable<int>(observer => first = observer),
            new ScriptedObservable<int>(observer => second = observer),
            new ScriptedObservable<int>(observer => third = observer),
        ]);
        var owner = BackgroundThread.Start(() => first!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => second!.OnError(expected));
        await BackgroundThread.Start(() => third!.OnNext(Second));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }

    /// <summary>An error raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ErrorRaisedDuringDeliveryFollowsTheQueuedValues()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<int>? left = null;
        IObserver<int>? right = null;
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
        InvalidOperationException expected = new("merge");

        using var subscription = new MergeCoordinator<int>(downstream).Run(
            new ScriptedObservable<int>(observer => left = observer),
            new ScriptedObservable<int>(observer => right = observer));
        var owner = BackgroundThread.Start(() => left!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => right!.OnNext(Second));
        await BackgroundThread.Start(() => right!.OnNext(Third));
        await BackgroundThread.Start(() => right!.OnError(expected));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2,3");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }
}
