// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Asserts the delivery contract of operators that concatenate inner sources: deliveries never overlap, no lock is held
/// while the observer runs, inner sources are subscribed one at a time in order, and terminals follow the queued values.
/// </summary>
internal static class ChainDeliveryAssertions
{
    /// <summary>The second value a scenario pushes.</summary>
    private const int Second = 2;

    /// <summary>The third value a scenario pushes.</summary>
    private const int Third = 3;

    /// <summary>The number of inner sources in the contention scenario.</summary>
    private const int ContendedInners = 200;

    /// <summary>The number of values each inner source produces in the contention scenario.</summary>
    private const int ValuesPerInner = 5;

    /// <summary>How long the contention scenario waits for the next inner subscription.</summary>
    private static readonly TimeSpan SubscriptionWait = TimeSpan.FromSeconds(5);

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread completes the active
    /// inner source, pushes a new inner source and completes the outer source; everything is delivered in order.
    /// </summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ObserverMarshallingWhileTheOtherThreadAdvancesTheChainDoesNotDeadlock(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
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

        using var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        dispatcher.Post(() =>
        {
            inside.Wait();
            first!.OnCompleted();
            outer.OnNext(EmitAndComplete(Second));
            outer.OnCompleted();
        });
        var worker = BackgroundThread.Start(() => first!.OnNext(1));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ValueRaisedByTheObserverIsDeliveredAfterItReturns(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? inner = null;
        List<string> log = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            log.Add($"start{value}");
            if (value == 1)
            {
                inner!.OnNext(Second);
            }

            log.Add($"end{value}");
        });

        using var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => inner = observer));
        inner!.OnNext(1);

        await Assert.That(string.Join(",", log)).IsEqualTo("start1,end1,start2,end2");
    }

    /// <summary>
    /// Values and the next inner source raised while another thread delivers are queued in order, and outer completion
    /// raised from a third thread is delivered only after them.
    /// </summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task CompletionRaisedDuringDeliveryFollowsTheQueuedValues(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);

        using var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        outer.OnNext(EmitAndComplete(Third));
        var owner = BackgroundThread.Start(() => first!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() =>
        {
            first!.OnNext(Second);
            first.OnCompleted();
        });
        await BackgroundThread.Start(outer.OnCompleted);
        await Assert.That(downstream.IsCompleted).IsFalse();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2,3");
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>
    /// An outer error raised while another thread delivers follows the queued values, and later values, completions and
    /// inner sources are dropped without subscribing the queued inner source.
    /// </summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task OuterErrorRaisedDuringDeliveryFollowsTheQueuedValuesAndStopsTheChain(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
        var subscribed = 0;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("outer");
        ScriptedObservable<int> counted = new(_ => subscribed++);

        using var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        outer.OnNext(counted);
        var owner = BackgroundThread.Start(() => first!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => first!.OnNext(Second));
        await BackgroundThread.Start(() => outer.OnError(expected));
        await BackgroundThread.Start(() =>
        {
            first!.OnNext(Third);
            first.OnCompleted();
            outer.OnNext(counted);
            outer.OnCompleted();
        });
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completions).IsEqualTo(0);
        await Assert.That(subscribed).IsEqualTo(0);
    }

    /// <summary>
    /// An inner error raised while another thread delivers follows the queued values, wins over a later error, and
    /// suppresses the outer completion that follows it.
    /// </summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task InnerErrorRaisedDuringDeliveryFollowsTheQueuedValues(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("inner");

        using var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        var owner = BackgroundThread.Start(() => first!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() =>
        {
            first!.OnNext(Second);
            first.OnError(expected);
        });
        await BackgroundThread.Start(() =>
        {
            outer.OnError(new InvalidOperationException("late"));
            outer.OnCompleted();
        });
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completions).IsEqualTo(0);
    }

    /// <summary>
    /// With inner sources pushed from one thread and completed from another, each inner source is subscribed only after
    /// the previous one completes, in source order, and every value is delivered in order without overlap.
    /// </summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task InnerSourcesAreSubscribedOneAtATimeInOrderUnderContention(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        using BlockingCollection<(int Index, IObserver<int> Observer)> subscriptions = [];
        IObserver<IObservable<int>>? outer = null;
        var activeInners = 0;
        var overlappingInners = 0;
        var misorderedInners = 0;
        var inFlight = 0;
        var overlaps = 0;
        var outOfOrder = 0;
        var last = -1;
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            if (value != last + 1)
            {
                outOfOrder++;
            }

            last = value;
            _ = Interlocked.Decrement(ref inFlight);
        });

        using var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        var producer = BackgroundThread.Start(() =>
        {
            for (var i = 0; i < ContendedInners; i++)
            {
                var index = i;
                outer!.OnNext(new ScriptedObservable<int>(observer =>
                {
                    if (Interlocked.Increment(ref activeInners) != 1)
                    {
                        _ = Interlocked.Increment(ref overlappingInners);
                    }

                    subscriptions.Add((index, observer));
                }));
            }

            outer!.OnCompleted();
        });
        var completer = BackgroundThread.Start(() =>
            misorderedInners = CompleteInnersInOrder(subscriptions, () => _ = Interlocked.Decrement(ref activeInners)));

        await Task.WhenAll(producer, completer);

        await Assert.That(overlappingInners).IsEqualTo(0);
        await Assert.That(misorderedInners).IsEqualTo(0);
        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(last).IsEqualTo((ContendedInners * ValuesPerInner) - 1);
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>
    /// Completion raised before disposal while another thread delivers is still delivered once, and disposal does not wait
    /// for the delivering thread.
    /// </summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task TerminalRaisedBeforeDisposeIsStillDelivered(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);

        var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        var owner = BackgroundThread.Start(() => first!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() =>
        {
            first!.OnCompleted();
            outer.OnCompleted();
        });
        var disposer = BackgroundThread.Start(subscription.Dispose);
        await Assert.That(await BackgroundThread.FinishesPromptly(disposer)).IsTrue();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1");
        await Assert.That(downstream.Completions).IsEqualTo(1);
        await Assert.That(downstream.Error).IsNull();
    }

    /// <summary>Values, terminals and inner sources raised after disposal are dropped, and the queued inner source is never subscribed.</summary>
    /// <param name="subscribeChain">Subscribes the concatenating operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task NotificationsRaisedAfterDisposeAreDropped(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeChain)
    {
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
        var subscribed = 0;
        RecordingWitness<int> downstream = new();
        ScriptedObservable<int> counted = new(_ => subscribed++);

        var subscription = subscribeChain(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        outer.OnNext(counted);
        subscription.Dispose();
        first!.OnNext(1);
        first.OnCompleted();
        outer.OnNext(counted);
        first.OnError(new InvalidOperationException("after dispose"));
        outer.OnCompleted();

        await Assert.That(downstream.Values.Count).IsEqualTo(0);
        await Assert.That(downstream.Errors.Count).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(0);
        await Assert.That(subscribed).IsEqualTo(0);
    }

    /// <summary>Takes each inner subscription as it arrives, emits its values, marks it inactive and completes it.</summary>
    /// <param name="subscriptions">The inner subscriptions, in the order they were made.</param>
    /// <param name="onInnerDone">Runs after an inner source's values and before its completion.</param>
    /// <returns>The number of inner sources subscribed out of source order.</returns>
    private static int CompleteInnersInOrder(BlockingCollection<(int Index, IObserver<int> Observer)> subscriptions, Action onInnerDone)
    {
        var misordered = 0;
        for (var i = 0; i < ContendedInners && subscriptions.TryTake(out var inner, SubscriptionWait); i++)
        {
            misordered += inner.Index == i ? 0 : 1;
            for (var k = 0; k < ValuesPerInner; k++)
            {
                inner.Observer.OnNext((inner.Index * ValuesPerInner) + k);
            }

            onInnerDone();
            inner.Observer.OnCompleted();
        }

        return misordered;
    }

    /// <summary>Creates a source that emits one value and completes on subscription.</summary>
    /// <param name="value">The value to emit.</param>
    /// <returns>The source.</returns>
    private static ScriptedObservable<int> EmitAndComplete(int value) =>
        new(observer =>
        {
            observer.OnNext(value);
            observer.OnCompleted();
        });
}
