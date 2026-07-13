// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Covers the consistent <c>if (_done) return;</c> after-terminal guards on the
/// remaining sync operators that share the pattern but lacked dedicated coverage —
/// <c>RetryWithDelay</c>, <c>OnErrorRetry</c>, <c>TakeUntilInclusive</c>, <c>SwitchIfEmpty</c>,
/// <c>ThrottleOnScheduler</c>, <c>BufferUntilIdle</c>, <c>ObserveOnIf</c>. Each test drives a
/// <see cref = "SyncDirectSource{T}"/> through one terminal event, then pushes additional
/// notifications past the terminal to verify the guard silently drops them.</summary>
public class OperatorAfterTerminalGuardTests
{
    /// <summary>Settle window used to let scheduler-marshalled tests fire any racing emission.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Tick window for fast-scheduler tests.</summary>
    private const int TickWindow = 100;

    /// <summary>Multiplier used to advance past the tick window in settle assertions.</summary>
    private const int SettleMultiplier = 2;

    /// <summary>Second sentinel value used in after-terminal pushes.</summary>
    private const int SecondValue = 2;

    /// <summary>Guard timeout so a hung rendezvous fails this test rather than stalling the run.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies <c>OnErrorRetry</c>'s sink silently drops events after a downstream
    /// completion has set the <c>_disposed</c> latch — and that a second dispose hits the
    /// <c>Interlocked.Exchange != 0</c> idempotency guard in <see cref = "IDisposable.Dispose"/>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryForeverEventsAfterDispose_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        List<int> values = [];
        var completed = false;
        var sub = source.OnErrorRetry().Subscribe(values.Add, () => completed = true);
        source.Observer.OnCompleted();

        // First dispose latches _disposed in the retry sink.
        sub.Dispose();

        // Second dispose exercises the Interlocked.Exchange idempotency guard.
        sub.Dispose();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        await Assert.That(completed).IsTrue();
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies that <c>RetryWithDelay</c>'s sink silently drops a source error
    /// arriving after dispose — exercises the <c>if (_disposed) return;</c> guard in OnError.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithDelaySourceErrorAfterDispose_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        Exception? caught = null;
        var sub = source.RetryForeverWithDelay(TimeSpan.FromMilliseconds(SettleDelayMilliseconds)).Subscribe(
            static _ => { },
            ex => caught = ex);
        sub.Dispose();
        source.Observer.OnError(new InvalidOperationException("after-dispose"));

        // The sink's _disposed guard short-circuits, so the downstream onError handler is not invoked
        // (no retry, no terminal forwarded).
        await Assert.That(caught).IsNull();
    }

    /// <summary>Exercises <c>RetryWithDelay.SubscribeToSource</c>'s <c>_disposed</c> guard —
    /// when the source errors and schedules a delayed re-subscribe, then the subscription is
    /// disposed before the delay elapses, the scheduled callback invokes <c>SubscribeToSource</c>
    /// which sees <c>_disposed == true</c> and returns at the guard rather than re-subscribing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithDelayDisposedDuringDelay_ThenSubscribeToSourceGuardSkipsRetry()
    {
        const int LongDelayMs = 250;
        var subscribeCount = 0;
        var source = Observable.Create<int>(o =>
        {
            subscribeCount++;
            o.OnError(new InvalidOperationException("retry-after-dispose"));
            return EmptyDisposable.Instance;
        });
        var sub = source.RetryForeverWithDelay(TimeSpan.FromMilliseconds(LongDelayMs)).Subscribe(static _ => { });

        // First subscribe ran; source errored synchronously and a retry has been scheduled.
        sub.Dispose();

        // Wait past the delay window so the scheduled callback fires while _disposed = true,
        // hitting the SubscribeToSource _disposed guard rather than re-subscribing.
        await Task.Delay(LongDelayMs + LongDelayMs);
        await Assert.That(subscribeCount).IsEqualTo(1);
    }

    /// <summary>Exercises <c>RetryWithBackoff.SubscribeToSource</c>'s <c>_disposed</c> guard — same shape as the RetryWithDelay variant.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffDisposedDuringDelay_ThenSubscribeToSourceGuardSkipsRetry()
    {
        const int LongDelayMs = 250;
        const int RetryAttempts = 10;
        var subscribeCount = 0;
        var source = Observable.Create<int>(o =>
        {
            subscribeCount++;
            o.OnError(new InvalidOperationException("retry-after-dispose"));
            return EmptyDisposable.Instance;
        });
        var sub = source.OnErrorRetry<int, InvalidOperationException>(
            static _ => { },
            RetryAttempts,
            TimeSpan.FromMilliseconds(LongDelayMs),
            TaskPoolSequencer.Default).Subscribe(static _ => { });
        sub.Dispose();
        await Task.Delay(LongDelayMs + LongDelayMs);
        await Assert.That(subscribeCount).IsEqualTo(1);
    }

    /// <summary>Verifies <c>TakeUntilInclusive</c>'s after-terminal sink guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeUntilInclusiveEventsAfterTerminated_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.TakeUntil(static x => x > 0).Subscribe(values.Add, () => completedCount++);

        // Predicate triggers on the first positive value, sets _done.
        source.Observer.OnNext(1);
        source.Observer.OnNext(SecondValue);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsCollectionEqualTo([1]);
    }

    /// <summary>Verifies <c>SwitchIfEmpty</c>'s after-terminal sink guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchIfEmptyEventsAfterTerminated_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        Subject<int> fallback = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.SwitchIfEmpty(fallback).Subscribe(values.Add, () => completedCount++);
        source.Observer.OnNext(1);
        source.Observer.OnCompleted();
        source.Observer.OnNext(SecondValue);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsCollectionEqualTo([1]);
    }

    /// <summary>Verifies <c>ThrottleOnScheduler</c>'s post-completion sink guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnSchedulerEventsAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.ThrottleOnScheduler(TimeSpan.FromTicks(TickWindow), scheduler)
            .Subscribe(values.Add, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies <c>DetectStale</c>'s post-completion <c>OnNext</c> guard — values
    /// arriving after the upstream completed are dropped at the <c>_state.Done</c> check.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDetectStaleEventsAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        List<Stale<int>> values = [];
        var completedCount = 0;
        using var sub = source.DetectStale(TimeSpan.FromTicks(TickWindow), scheduler)
            .Subscribe(values.Add, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies <c>DropIfBusy</c>'s post-completion <c>OnNext</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDropIfBusyEventsAfterCompleted_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.DropIfBusy(static _ => default).Subscribe(values.Add, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Exercises <c>WhileObservable.Iterate</c>'s <c>_disposed</c> guard — when the
    /// downstream consumer's <c>OnNext</c> callback disposes the subscription captured via
    /// a single-assignment slot, the post-action call back to <c>Iterate</c> sees
    /// <c>_disposed == 1</c> and returns at the guard rather than re-entering RunActionAndContinue.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileDownstreamDisposesInsideOnNext_ThenIterateGuardSkipsNextPredicate()
    {
        // The scheduler indirection lets us defer the first iteration to after Subscribe has
        // returned (so the SingleAssignmentDisposable can capture the subscription), then run
        // the inner iterations synchronously enough that the OnNext-side dispose hits before
        // the second Iterate evaluates the predicate.
        VirtualClock scheduler = new();
        var actionCalls = 0;
        SingleAssignmentDisposable sub = new();
        sub.Disposable = ReactiveExtensions.While(static () => true, () => actionCalls++, scheduler)
            .Subscribe(_ => sub.Dispose());
        scheduler.AdvanceBy(1);
        await Assert.That(actionCalls).IsEqualTo(1);
    }

    /// <summary>Exercises <c>ThrottleDistinct</c>'s scheduled-emit done guard — when the source
    /// completes between a value being received and the throttle window elapsing, the
    /// scheduled <c>Emit</c> callback sees <c>_state.Done == true</c> and returns without
    /// forwarding the buffered value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctSourceCompletesBeforeEmitWindow_ThenScheduledEmitDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.ThrottleDistinct(TimeSpan.FromTicks(TickWindow), scheduler)
            .Subscribe(values.Add, () => completedCount++);
        source.Observer.OnNext(1);
        source.Observer.OnCompleted();
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Exercises the <c>SampleLatest</c> trigger-error post-terminal guard — when the
    /// source has already errored (setting <c>_done = true</c>), a subsequent error on the
    /// trigger observer hits the <c>if (_done) return;</c> guard inside the trigger's
    /// OnError delegate.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestTriggerErrorsAfterSourceErrored_ThenDroppedByDoneGuard()
    {
        Subject<int> source = new();
        Subject<object> trigger = new();
        Exception? caught = null;
        InvalidOperationException sourceError = new("source");
        using var sub = source.SampleLatest(trigger).Subscribe(
            static _ => { },
            ex => caught = ex);
        source.OnError(sourceError);
        trigger.OnError(new InvalidOperationException("trigger"));
        await Assert.That(caught).IsSameReferenceAs(sourceError);
    }

    /// <summary>Verifies <c>SampleLatest</c>'s post-completion <c>Sample</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestSampledAfterCompleted_ThenNoOp()
    {
        SyncDirectSource<int> source = new();
        Subject<object> sampler = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.SampleLatest(sampler).Subscribe(values.Add, () => completedCount++);
        source.Observer.OnNext(1);
        source.Observer.OnCompleted();
        sampler.OnNext(new());
        await Assert.That(completedCount).IsEqualTo(1);
    }

    /// <summary>Verifies <c>Heartbeat</c>'s post-completion <c>OnNext</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHeartbeatEventsAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        var completedCount = 0;
        using var sub = source.Heartbeat(TimeSpan.FromTicks(TickWindow), scheduler).Subscribe(
            static _ => { },
            () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(completedCount).IsEqualTo(1);
    }

    /// <summary>Exercises <c>Heartbeat</c>'s <c>ScheduleHeartbeats</c> <c>_done</c> guard —
    /// when the source completes synchronously during <c>source.Subscribe(sink)</c>, the sink
    /// is marked done before <c>sink.Initialize()</c> runs, so the post-Initialize call to
    /// <c>ScheduleHeartbeats</c> returns at the <c>_done</c> check without arming the timer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHeartbeatSourceCompletesDuringSubscribe_ThenInitializeShortCircuits()
    {
        VirtualClock scheduler = new();
        var completedCount = 0;
        using var sub = Observable.Empty<int>().Heartbeat(TimeSpan.FromTicks(TickWindow), scheduler).Subscribe(
            static _ => { },
            () => completedCount++);
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(completedCount).IsEqualTo(1);
    }

    /// <summary>Verifies <c>DebounceUntil</c>'s post-completion sink guard — values arriving
    /// after the upstream has already completed are dropped at the <c>_state.Done</c> check
    /// inside <c>OnNext</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilEventsAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.DebounceUntil(TimeSpan.FromTicks(TickWindow), static _ => true, scheduler)
            .Subscribe(values.Add, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies <c>BufferUntilIdle</c>'s post-completion sink guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilIdleEventsAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        List<IList<int>> batches = [];
        var completedCount = 0;
        using var sub = source.BufferUntilIdle(TimeSpan.FromTicks(TickWindow), scheduler)
            .Subscribe(batches.Add, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(completedCount).IsEqualTo(1);
    }

    /// <summary>Verifies <c>ObserveOnIf</c>'s post-completion sink guard on the condition observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIfConditionEventsAfterCompleted_ThenDropped()
    {
        Subject<int> source = new();
        SyncDirectSource<bool> condition = new();
        var trueScheduler = Sequencer.Immediate;
        var falseScheduler = Sequencer.Immediate;
        List<int> values = [];
        var completedCount = 0;
        using var sub = source.ObserveOnIf(condition, trueScheduler, falseScheduler)
            .Subscribe(values.Add, () => completedCount++);

        // Drive the condition observer terminal, then push more events to hit the after-terminal guard.
        condition.Observer.OnCompleted();
        condition.Observer.OnNext(true);
        condition.Observer.OnError(new InvalidOperationException("late"));
        condition.Observer.OnCompleted();

        // Source still works because the operator multicasts via condition.
        source.OnNext(1);
        source.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
    }

    /// <summary>Verifies <c>RetryWithBackoff</c>'s sink silently drops a source error after dispose.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffSourceErrorAfterDispose_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        Exception? caught = null;
        var sub = source.RetryWithBackoff(1, TimeSpan.FromMilliseconds(SettleDelayMilliseconds)).Subscribe(
            static _ => { },
            ex => caught = ex);
        sub.Dispose();
        source.Observer.OnError(new InvalidOperationException("after-dispose"));
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies <c>WhileObservable</c>'s after-dispose guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileDisposedTwice_ThenSecondIsNoOp()
    {
        var ran = 0;
        var condition = true;
        var sub = ReactiveExtensions.While(
            () =>
            {
                if (!condition)
                {
                    return false;
                }

                condition = false;
                return true;
            },
            () => Interlocked.Increment(ref ran)).Subscribe(static _ => { });
        sub.Dispose();
        sub.Dispose();
        await Assert.That(ran).IsEqualTo(1);
    }

    /// <summary>Verifies <c>ScheduledSource</c>'s emit catch — when the side-effect action throws,
    /// the exception is forwarded as <c>OnError</c> on the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduledSourceActionThrows_ThenForwardsError()
    {
        VirtualClock scheduler = new();
        Subject<int> source = new();
        InvalidOperationException expected = new("action-failed");
        Exception? caught = null;
        using var sub = source.Schedule(TimeSpan.FromTicks(TickWindow), scheduler, _ => throw expected).Subscribe(
            static _ => { },
            ex => caught = ex);
        source.OnNext(1);
        scheduler.AdvanceBy(TickWindow * SettleMultiplier);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies the <c>SubscribeSynchronous</c> sink's null-callback branches —
    /// omitting <c>onError</c> and <c>onCompleted</c> covers the null-coalescing fast paths.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeSynchronousOmitsErrorAndCompletedCallbacks_ThenNullPathsTaken()
    {
        Subject<int> subject = new();
        TaskCompletionSource processed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SubscribeSynchronous(value =>
        {
            _ = processed.TrySetResult();
            return default;
        });
        subject.OnNext(1);
        await processed.Task.WaitAsync(GuardTimeout);

        // Subject silently terminates without invoking the optional callbacks.
        subject.OnError(new InvalidOperationException("ignored"));
        Subject<int> second = new();
        using var sub2 = second.SubscribeSynchronous(static _ => default);
        second.OnCompleted();
    }
}
