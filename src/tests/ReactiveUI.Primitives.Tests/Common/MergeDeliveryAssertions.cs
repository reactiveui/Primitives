// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Asserts the delivery contract of operators that merge or switch between concurrent sources: deliveries never overlap,
/// no lock is held while the observer runs, and queued notifications keep their arrival order.
/// </summary>
internal static class MergeDeliveryAssertions
{
    /// <summary>The number of sources in the contention scenario.</summary>
    private const int ContendedSources = 4;

    /// <summary>The number of values each source produces in the contention scenario.</summary>
    private const int ValuesPerSource = 2_000;

    /// <summary>The multiplier that packs a source index and a sequence number into one value.</summary>
    private const int SourceScale = 100_000;

    /// <summary>The second value a scenario pushes.</summary>
    private const int Second = 2;

    /// <summary>The third value a scenario pushes.</summary>
    private const int Third = 3;

    /// <summary>The multiplier a pairing selector applies to the left value.</summary>
    private const int PairScale = 100;

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes a value through
    /// a sibling source, and both values are delivered in order.
    /// </summary>
    /// <param name="subscribe">Subscribes the operator under test over the sources.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ObserverMarshallingToAnotherSourceThreadDoesNotDeadlock(Func<IObservable<int>[], IObserver<int>, IDisposable> subscribe)
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
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
        var observers = new IObserver<int>[Second];

        using var subscription = subscribe(CaptureSources(observers), downstream);
        dispatcher.Post(() =>
        {
            inside.Wait();
            observers[1].OnNext(Second);
        });
        var worker = BackgroundThread.Start(() => observers[0].OnNext(1));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
    }

    /// <summary>A value pushed by the observer itself is delivered after the observer returns, not inside it.</summary>
    /// <param name="subscribe">Subscribes the operator under test over the sources.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ValueRaisedByTheObserverIsDeliveredAfterItReturns(Func<IObservable<int>[], IObserver<int>, IDisposable> subscribe)
    {
        var observers = new IObserver<int>[Second];
        List<string> log = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            log.Add($"start{value}");
            if (value == 1)
            {
                observers[0].OnNext(Second);
            }

            log.Add($"end{value}");
        });

        using var subscription = subscribe(CaptureSources(observers), downstream);
        observers[0].OnNext(1);

        await Assert.That(string.Join(",", log)).IsEqualTo("start1,end1,start2,end2");
    }

    /// <summary>Sources pushing from separate threads never overlap downstream, and each source's values arrive in order.</summary>
    /// <param name="subscribe">Subscribes the operator under test over the sources.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ConcurrentSourcesDeliverEveryValueInOrderWithoutOverlap(Func<IObservable<int>[], IObserver<int>, IDisposable> subscribe)
    {
        var observers = new IObserver<int>[ContendedSources];
        var lastSeen = new int[ContendedSources];
        Array.Fill(lastSeen, -1);
        var inFlight = 0;
        var overlaps = 0;
        var outOfOrder = 0;
        var delivered = 0;
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            var source = value / SourceScale;
            var sequence = value % SourceScale;
            if (sequence != lastSeen[source] + 1)
            {
                outOfOrder++;
            }

            lastSeen[source] = sequence;
            delivered++;
            _ = Interlocked.Decrement(ref inFlight);
        });

        using var subscription = subscribe(CaptureSources(observers), downstream);
        var producers = new Task<int>[ContendedSources];
        for (var s = 0; s < ContendedSources; s++)
        {
            var source = s;
            producers[s] = BackgroundThread.Start(() =>
            {
                for (var sequence = 0; sequence < ValuesPerSource; sequence++)
                {
                    observers[source].OnNext((source * SourceScale) + sequence);
                }
            });
        }

        await Task.WhenAll(producers);

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(delivered).IsEqualTo(ContendedSources * ValuesPerSource);
    }

    /// <summary>An error raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <param name="subscribe">Subscribes the operator under test over the sources.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ErrorRaisedDuringDeliveryFollowsTheQueuedValues(Func<IObservable<int>[], IObserver<int>, IDisposable> subscribe)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        var observers = new IObserver<int>[Second];
        List<int> values = [];
        var downstream = BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("merge");

        using var subscription = subscribe(CaptureSources(observers), downstream);
        var owner = BackgroundThread.Start(() => observers[0].OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => observers[1].OnNext(Second));
        await BackgroundThread.Start(() => observers[1].OnNext(Third));
        await BackgroundThread.Start(() => observers[1].OnError(expected));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2,3");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }

    /// <summary>A value pushed while a terminal notification waits for the delivering thread is dropped.</summary>
    /// <param name="subscribe">Subscribes the operator under test over the sources.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ValuePushedWhileTheTerminalWaitsIsDropped(Func<IObservable<int>[], IObserver<int>, IDisposable> subscribe)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        var observers = new IObserver<int>[Third];
        List<int> values = [];
        var downstream = BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("merge");

        using var subscription = subscribe(CaptureSources(observers), downstream);
        var owner = BackgroundThread.Start(() => observers[0].OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => observers[1].OnError(expected));
        await BackgroundThread.Start(() => observers[Second].OnNext(Second));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }

    /// <summary>
    /// Adapts a switching operator to the merge scenarios: the outer source yields one inner source that forwards every
    /// scenario source into the same inner subscriber.
    /// </summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A subscribe function for the merge scenarios.</returns>
    internal static Func<IObservable<int>[], IObserver<int>, IDisposable> OverSharedInner(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch) =>
        (sources, observer) => subscribeSwitch(
            new ScriptedObservable<IObservable<int>>(outer => outer.OnNext(new ScriptedObservable<int>(inner =>
            {
                foreach (var source in sources)
                {
                    source.Subscribe(inner).Dispose();
                }
            }))),
            observer);

    /// <summary>
    /// Notifications from a superseded inner source raised while another thread delivers are dropped, and the switch
    /// completes only once the outer source and the current inner source have completed.
    /// </summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task SupersededInnerNotificationsRaisedDuringDeliveryAreDropped(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        List<int> values = [];
        var downstream = BlockOnFirstValue(values, inside, release);

        using var subscription = subscribeSwitch(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => first = observer));
        var owner = BackgroundThread.Start(() => first!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => outer.OnNext(new ScriptedObservable<int>(observer => second = observer)));
        await BackgroundThread.Start(() =>
        {
            first!.OnNext(Third);
            first.OnCompleted();
            first.OnError(new InvalidOperationException("superseded"));
            second!.OnNext(Second);
        });
        release.Set();
        await owner;
        outer.OnCompleted();

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Error).IsNull();
        await Assert.That(downstream.IsCompleted).IsFalse();

        second!.OnCompleted();
        await Assert.That(downstream.IsCompleted).IsTrue();
    }

    /// <summary>Completion raised while another thread delivers is delivered after the values queued before it.</summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task OuterCompletionRaisedDuringDeliveryFollowsTheQueuedValues(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? inner = null;
        List<int> values = [];
        var downstream = BlockOnFirstValue(values, inside, release);

        using var subscription = subscribeSwitch(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => inner = observer));
        var owner = BackgroundThread.Start(() => inner!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() =>
        {
            inner!.OnNext(Second);
            inner.OnCompleted();
        });
        await BackgroundThread.Start(outer.OnCompleted);
        await Assert.That(downstream.IsCompleted).IsFalse();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.IsCompleted).IsTrue();
    }

    /// <summary>An outer error raised while another thread delivers follows the queued values, and later inner values are dropped.</summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task OuterErrorRaisedDuringDeliveryFollowsTheQueuedValues(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? inner = null;
        List<int> values = [];
        var downstream = BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("outer");

        using var subscription = subscribeSwitch(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => inner = observer));
        var owner = BackgroundThread.Start(() => inner!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() => inner!.OnNext(Second));
        await BackgroundThread.Start(() => outer.OnError(expected));
        await BackgroundThread.Start(() => inner!.OnNext(Third));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }

    /// <summary>
    /// When an earlier switch's inner subscription returns after a later switch has installed its inner, the earlier
    /// subscription is disposed and its values dropped, and the later one stays subscribed.
    /// </summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task OverlappingSwitchesKeepOnlyTheNewestInnerSubscribed(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch)
    {
        using ManualResetEventSlim subscribing = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? slowInner = null;
        IObserver<int>? fastInner = null;
        RecordingDisposable slowSubscription = new();
        RecordingDisposable fastSubscription = new();
        RecordingWitness<int> downstream = new();

        using var subscription = subscribeSwitch(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        var slow = BackgroundThread.Start(() => outer!.OnNext(new ScriptedObservable<int>(
            observer =>
            {
                slowInner = observer;
                subscribing.Set();
                release.Wait();
            },
            slowSubscription)));
        subscribing.Wait();
        await BackgroundThread.Start(() => outer!.OnNext(new ScriptedObservable<int>(observer => fastInner = observer, fastSubscription)));
        release.Set();
        await slow;
        slowInner!.OnNext(1);
        fastInner!.OnNext(Second);

        await Assert.That(slowSubscription.DisposeCount).IsEqualTo(1);
        await Assert.That(fastSubscription.DisposeCount).IsEqualTo(0);
        await Assert.That(string.Join(",", downstream.Values)).IsEqualTo("2");
    }

    /// <summary>
    /// Completion raised before disposal while another thread delivers is still delivered once, after the values queued
    /// ahead of it, and disposal does not wait for it; notifications raised after disposal are dropped.
    /// </summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task TerminalRaisedBeforeDisposeIsStillDelivered(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<IObservable<int>>? outer = null;
        IObserver<int>? inner = null;
        List<int> values = [];
        var downstream = BlockOnFirstValue(values, inside, release);

        var subscription = subscribeSwitch(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => inner = observer));
        var owner = BackgroundThread.Start(() => inner!.OnNext(1));
        inside.Wait();
        await BackgroundThread.Start(() =>
        {
            inner!.OnNext(Second);
            inner.OnCompleted();
            outer.OnCompleted();
        });
        var disposer = BackgroundThread.Start(subscription.Dispose);
        await Assert.That(await BackgroundThread.FinishesPromptly(disposer)).IsTrue();
        inner!.OnNext(Third);
        inner.OnError(new InvalidOperationException("after dispose"));
        outer.OnError(new InvalidOperationException("after dispose"));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Completions).IsEqualTo(1);
        await Assert.That(downstream.Error).IsNull();
    }

    /// <summary>An inner subscription that returns after the switch was disposed is disposed at once.</summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task InnerSubscribedWhileDisposingIsDisposed(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch)
    {
        IObserver<IObservable<int>>? outer = null;
        RecordingDisposable innerSubscription = new();

        var subscription = subscribeSwitch(new ScriptedObservable<IObservable<int>>(observer => outer = observer), new RecordingWitness<int>());
        outer!.OnNext(new ScriptedObservable<int>(_ => subscription.Dispose(), innerSubscription));

        await Assert.That(innerSubscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>An inner source that fails while it is being subscribed delivers the error and has its subscription disposed.</summary>
    /// <param name="subscribeSwitch">Subscribes the switching operator under test over an outer source.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task InnerFailingWhileSubscribingIsDisposed(Func<IObservable<IObservable<int>>, IObserver<int>, IDisposable> subscribeSwitch)
    {
        IObserver<IObservable<int>>? outer = null;
        RecordingDisposable innerSubscription = new();
        RecordingWitness<int> downstream = new();
        InvalidOperationException expected = new("inner");

        using var subscription = subscribeSwitch(new ScriptedObservable<IObservable<int>>(observer => outer = observer), downstream);
        outer!.OnNext(new ScriptedObservable<int>(observer => observer.OnError(expected), innerSubscription));

        await Assert.That(downstream.Errors.Count).IsEqualTo(1);
        await Assert.That(downstream.Errors[0]).IsSameReferenceAs(expected);
        await Assert.That(innerSubscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>
    /// Primes a combining operator, then has a worker push a value whose delivery marshals to a dispatcher thread while
    /// that thread pushes another source, and asserts the worker finishes and the expected results are delivered.
    /// </summary>
    /// <param name="combined">The combined sequence under test.</param>
    /// <param name="prime">Pushes the notifications that precede the worker's push.</param>
    /// <param name="workerPush">The worker thread's push.</param>
    /// <param name="dispatcherPush">The dispatcher thread's push while the worker is delivering.</param>
    /// <param name="expected">The results expected, in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task CombinedObserverMarshallingDoesNotDeadlock(IObservable<int> combined, Action prime, Action workerPush, Action dispatcherPush, string expected)
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
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

        using var subscription = combined.Subscribe(downstream);
        prime();
        dispatcher.Post(() =>
        {
            inside.Wait();
            dispatcherPush();
        });
        var worker = BackgroundThread.Start(workerPush);

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo(expected);
    }

    /// <summary>
    /// Primes a combining operator whose result selector marshals to a dispatcher thread, has a worker push the value
    /// that runs the selector while that thread pushes another source, and asserts the worker finishes and the expected
    /// results are delivered. The selector returns the left value times one hundred plus the right value.
    /// </summary>
    /// <param name="combine">Creates the combined sequence under test over the supplied selector.</param>
    /// <param name="prime">Pushes the notifications that precede the worker's push.</param>
    /// <param name="workerPush">The worker thread's push.</param>
    /// <param name="dispatcherPush">The dispatcher thread's push while the selector runs.</param>
    /// <param name="expected">The results expected, in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task CombinedSelectorMarshallingDoesNotDeadlock(
        Func<Func<int, int, int>, IObservable<int>> combine,
        Action prime,
        Action workerPush,
        Action dispatcherPush,
        string expected)
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        ConcurrentQueue<int> values = new();
        var combined = combine((left, right) =>
        {
            if (Environment.CurrentManagedThreadId != dispatcher.ManagedThreadId && !inside.IsSet)
            {
                inside.Set();
                dispatcher.Invoke(static () => { });
            }

            return (left * PairScale) + right;
        });

        using var subscription = combined.Subscribe(new CallbackRecordingWitness<int>(values.Enqueue));
        prime();
        dispatcher.Post(() =>
        {
            inside.Wait();
            dispatcherPush();
        });
        var worker = BackgroundThread.Start(workerPush);

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo(expected);
    }

    /// <summary>
    /// Blocks a delivering thread inside the observer, queues two pushes from other threads behind it, then asserts every
    /// result arrives in push order.
    /// </summary>
    /// <param name="combined">The combined sequence under test.</param>
    /// <param name="prime">Pushes the notifications that precede the owner's push.</param>
    /// <param name="ownerPush">The push whose result blocks the delivering thread.</param>
    /// <param name="firstQueuedPush">The first push queued behind the delivery.</param>
    /// <param name="secondQueuedPush">The second push queued behind the delivery.</param>
    /// <param name="expected">The results expected, in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task CombinedValuesQueuedDuringDeliveryFollowInOrder(
        IObservable<int> combined,
        Action prime,
        Action ownerPush,
        Action firstQueuedPush,
        Action secondQueuedPush,
        string expected)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        List<int> values = [];
        var downstream = BlockOnFirstDelivery(values, inside, release);

        using var subscription = combined.Subscribe(downstream);
        prime();
        var owner = BackgroundThread.Start(ownerPush);
        inside.Wait();
        await BackgroundThread.Start(firstQueuedPush);
        await BackgroundThread.Start(secondQueuedPush);
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo(expected);
    }

    /// <summary>
    /// Blocks a delivering thread inside the observer, requests an error from a second thread and pushes a value from a
    /// third, then asserts only the blocked result and the error are delivered.
    /// </summary>
    /// <param name="combined">The combined sequence under test.</param>
    /// <param name="prime">Pushes the notifications that precede the owner's push.</param>
    /// <param name="ownerPush">The push whose result blocks the delivering thread.</param>
    /// <param name="errorSource">Returns the source observer that raises the error, once subscribed.</param>
    /// <param name="latePush">The push made while the error waits.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task CombinedValuePushedWhileTheTerminalWaitsIsDropped(
        IObservable<int> combined,
        Action prime,
        Action ownerPush,
        Func<IObserver<int>> errorSource,
        Action latePush)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        List<int> values = [];
        var downstream = BlockOnFirstDelivery(values, inside, release);
        InvalidOperationException expected = new("combine");

        using var subscription = combined.Subscribe(downstream);
        prime();
        var owner = BackgroundThread.Start(ownerPush);
        inside.Wait();
        await BackgroundThread.Start(() => errorSource().OnError(expected));
        await BackgroundThread.Start(latePush);
        release.Set();
        await owner;

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
    }

    /// <summary>
    /// Blocks a delivering thread inside the observer, queues pushes and then an error from other threads behind it, and
    /// asserts the queued results are delivered before the error.
    /// </summary>
    /// <param name="combined">The combined sequence under test.</param>
    /// <param name="prime">Pushes the notifications that precede the owner's push.</param>
    /// <param name="ownerPush">The push whose result blocks the delivering thread.</param>
    /// <param name="queuedPushes">The notifications queued behind the delivery before the error.</param>
    /// <param name="errorSource">Returns the source observer that raises the error, once subscribed.</param>
    /// <param name="expected">The results expected before the error, in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task CombinedErrorRaisedDuringDeliveryFollowsTheQueuedValues(
        IObservable<int> combined,
        Action prime,
        Action ownerPush,
        Action queuedPushes,
        Func<IObserver<int>> errorSource,
        string expected)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        List<int> values = [];
        var downstream = BlockOnFirstDelivery(values, inside, release);
        InvalidOperationException error = new("combine");

        using var subscription = combined.Subscribe(downstream);
        prime();
        var owner = BackgroundThread.Start(ownerPush);
        inside.Wait();
        await BackgroundThread.Start(queuedPushes);
        await BackgroundThread.Start(() => errorSource().OnError(error));
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo(expected);
        await Assert.That(downstream.Error).IsSameReferenceAs(error);
    }

    /// <summary>
    /// Blocks a delivering thread inside the observer, queues notifications that end in completion from another thread,
    /// and asserts completion waits for the blocked delivery and follows the queued results.
    /// </summary>
    /// <param name="combined">The combined sequence under test.</param>
    /// <param name="prime">Pushes the notifications that precede the owner's push.</param>
    /// <param name="ownerPush">The push whose result blocks the delivering thread.</param>
    /// <param name="queuedNotifications">The notifications queued behind the delivery, including the completions.</param>
    /// <param name="expected">The results expected before completion, in order.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task CombinedCompletionRaisedDuringDeliveryFollowsTheQueuedValues(
        IObservable<int> combined,
        Action prime,
        Action ownerPush,
        Action queuedNotifications,
        string expected)
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        List<int> values = [];
        var downstream = BlockOnFirstDelivery(values, inside, release);

        using var subscription = combined.Subscribe(downstream);
        prime();
        var owner = BackgroundThread.Start(ownerPush);
        inside.Wait();
        await BackgroundThread.Start(queuedNotifications);
        await Assert.That(downstream.IsCompleted).IsFalse();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo(expected);
        await Assert.That(downstream.IsCompleted).IsTrue();
    }

    /// <summary>
    /// Primes the sources, asserts the observer's throw on the first result propagates, then asserts the next result is
    /// still delivered.
    /// </summary>
    /// <param name="combined">The combined sequence under test.</param>
    /// <param name="prime">Pushes the notifications that precede the throwing push.</param>
    /// <param name="throwingPush">The push whose result the observer throws on.</param>
    /// <param name="nextPush">The push whose result must still be delivered.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ThrowingObserverLeavesCombinationUsable(IObservable<int> combined, Action prime, Action throwingPush, Action nextPush)
    {
        var calls = 0;
        List<int> values = [];
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            calls++;
            if (calls == 1)
            {
                throw new InvalidOperationException("observer");
            }

            values.Add(value);
        });

        using var subscription = combined.Subscribe(downstream);
        prime();

        await Assert.That(throwingPush).Throws<InvalidOperationException>();
        nextPush();

        await Assert.That(values.Count).IsEqualTo(1);
    }

    /// <summary>
    /// Both sides of a latest-value combination pushing from separate threads never overlap downstream, each side's
    /// latest value never moves backwards, and every update produces a combination.
    /// </summary>
    /// <param name="subscribe">Subscribes the operator under test over the left and right sources.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task LatestSourcesDeliverEveryCombinationWithoutOverlap(Func<IObservable<int>, IObservable<int>, IObserver<(int Left, int Right)>, IDisposable> subscribe)
    {
        var observers = new IObserver<int>[Second];
        var sources = CaptureSources(observers);
        var inFlight = 0;
        var overlaps = 0;
        var delivered = 0;
        var lastLeft = -1;
        var lastRight = -1;
        var outOfOrder = 0;
        CallbackRecordingWitness<(int Left, int Right)> downstream = new(pair =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            if (pair.Left < lastLeft || pair.Right < lastRight)
            {
                outOfOrder++;
            }

            lastLeft = pair.Left;
            lastRight = pair.Right;
            delivered++;
            _ = Interlocked.Decrement(ref inFlight);
        });

        using var subscription = subscribe(sources[0], sources[1], downstream);
        observers[0].OnNext(0);
        observers[1].OnNext(0);

        await Task.WhenAll(
            BackgroundThread.Start(() => PushSequence(observers[0])),
            BackgroundThread.Start(() => PushSequence(observers[1])));

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(delivered).IsEqualTo(1 + (ValuesPerSource * Second));
        await Assert.That((lastLeft, lastRight)).IsEqualTo((ValuesPerSource, ValuesPerSource));
    }

    /// <summary>
    /// Both sides of a pair-by-index combination pushing from separate threads never overlap downstream, every pair joins
    /// the values at the same index, and every index produces a pair.
    /// </summary>
    /// <param name="subscribe">Subscribes the operator under test over the left and right sources.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    internal static async Task ZippedSourcesDeliverEveryPairInOrderWithoutOverlap(Func<IObservable<int>, IObservable<int>, IObserver<(int Left, int Right)>, IDisposable> subscribe)
    {
        var observers = new IObserver<int>[Second];
        var sources = CaptureSources(observers);
        var inFlight = 0;
        var overlaps = 0;
        var mismatched = 0;
        var delivered = 0;
        CallbackRecordingWitness<(int Left, int Right)> downstream = new(pair =>
        {
            if (Interlocked.Increment(ref inFlight) != 1)
            {
                overlaps++;
            }

            delivered++;
            if (pair.Left != delivered || pair.Right != delivered)
            {
                mismatched++;
            }

            _ = Interlocked.Decrement(ref inFlight);
        });

        using var subscription = subscribe(sources[0], sources[1], downstream);

        await Task.WhenAll(
            BackgroundThread.Start(() => PushSequence(observers[0])),
            BackgroundThread.Start(() => PushSequence(observers[1])));

        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(mismatched).IsEqualTo(0);
        await Assert.That(delivered).IsEqualTo(ValuesPerSource);
    }

    /// <summary>Creates an observer that records values and blocks the first delivery until released.</summary>
    /// <param name="values">Receives the delivered values.</param>
    /// <param name="inside">Set once the first delivery is running.</param>
    /// <param name="release">Releases the first delivery.</param>
    /// <returns>The observer.</returns>
    internal static CallbackRecordingWitness<int> BlockOnFirstValue(List<int> values, ManualResetEventSlim inside, ManualResetEventSlim release) =>
        new(value =>
        {
            values.Add(value);
            if (value != 1)
            {
                return;
            }

            inside.Set();
            release.Wait();
        });

    /// <summary>Pushes an ascending sequence from one up to the contention count.</summary>
    /// <param name="observer">The source observer to push into.</param>
    private static void PushSequence(IObserver<int> observer)
    {
        for (var value = 1; value <= ValuesPerSource; value++)
        {
            observer.OnNext(value);
        }
    }

    /// <summary>Creates an observer that records values and blocks the first delivery until released.</summary>
    /// <param name="values">Receives the delivered values.</param>
    /// <param name="inside">Set once the first delivery is running.</param>
    /// <param name="release">Releases the first delivery.</param>
    /// <returns>The observer.</returns>
    private static CallbackRecordingWitness<int> BlockOnFirstDelivery(List<int> values, ManualResetEventSlim inside, ManualResetEventSlim release) =>
        new(value =>
        {
            values.Add(value);
            if (inside.IsSet)
            {
                return;
            }

            inside.Set();
            release.Wait();
        });

    /// <summary>Creates one source per observer slot, each storing its subscriber in that slot.</summary>
    /// <param name="observers">The slots that receive each source's subscriber.</param>
    /// <returns>The sources.</returns>
    private static IObservable<int>[] CaptureSources(IObserver<int>[] observers)
    {
        var sources = new IObservable<int>[observers.Length];
        for (var i = 0; i < sources.Length; i++)
        {
            var index = i;
            sources[i] = new ScriptedObservable<int>(observer => observers[index] = observer);
        }

        return sources;
    }
}
