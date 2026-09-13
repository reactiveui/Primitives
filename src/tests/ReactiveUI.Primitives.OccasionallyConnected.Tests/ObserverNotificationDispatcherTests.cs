// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ObserverNotificationDispatcher{T}"/>.</summary>
public sealed partial class ObserverNotificationDispatcherTests
{
    /// <summary>Defines a one-item queue capacity.</summary>
    private const int OneItem = 1;

    /// <summary>Defines a two-item queue capacity.</summary>
    private const int TwoItems = 2;

    /// <summary>Defines a one-byte notification size.</summary>
    private const long OneByte = 1;

    /// <summary>Defines a two-byte notification size.</summary>
    private const long TwoBytes = 2;

    /// <summary>Defines an empty count.</summary>
    private const int None = 0;

    /// <summary>Defines the first notification value.</summary>
    private const int FirstValue = 1;

    /// <summary>Defines the second notification value.</summary>
    private const int SecondValue = 2;

    /// <summary>Defines an invalid zero byte size.</summary>
    private const long NoBytes = 0;

    /// <summary>Defines an invalid negative byte size.</summary>
    private const long NegativeBytes = -1;

    /// <summary>Defines a guard timeout for thread-pool callback assertions.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies local state notifications coalesce to the newest queued value instead of blocking publishers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestCoalescesQueuedStateAndRunsAsynchronously()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var first = dispatcher.PublishLatest(FirstValue, OneByte);
        var second = dispatcher.PublishLatest(SecondValue, OneByte);

        await Assert.That(first).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(second).IsEqualTo(ObserverNotificationPublishResult.Coalesced);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(scheduler.PendingCount).IsEqualTo(OneItem);

        scheduler.RunAll();

        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(SecondValue);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(OneItem);
    }

    /// <summary>Verifies remote event overflow disconnects only the slow observer with a typed error.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishEventOverflowDisconnectsOnlyTheSlowObserver()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var slow = new RecordingObserver<int>();
        var fast = new RecordingObserver<int>();
        using var slowSubscription = dispatcher.Subscribe(slow, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));
        using var fastSubscription = dispatcher.Subscribe(fast, new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.Disconnect));

        _ = dispatcher.PublishEvent(FirstValue, OneByte);
        var result = dispatcher.PublishEvent(SecondValue, OneByte);

        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Disconnected);
        await Assert.That(slow.Values).Count().IsEqualTo(OneItem);
        await Assert.That(slow.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(slow.Error).IsTypeOf<ObserverNotificationOverflowException>();
        await Assert.That(fast.Values).Count().IsEqualTo(TwoItems);
        await Assert.That(fast.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(fast.Values[1]).IsEqualTo(SecondValue);
        await Assert.That(fast.Error).IsNull();
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(OneItem);
    }

    /// <summary>Verifies remote event overflow disconnects even when the subscription permits state coalescing.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishEventOverflowDisconnectsCoalescingSubscription()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        _ = dispatcher.PublishEvent(FirstValue, OneByte);
        var result = dispatcher.PublishEvent(SecondValue, OneByte);

        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Disconnected);
        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.Error).IsTypeOf<ObserverNotificationOverflowException>();
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies an oversized notification is rejected by the byte bound and reports the typed overflow error.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishEventDisconnectsWhenNotificationExceedsByteCapacity()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        var result = dispatcher.PublishEvent(FirstValue, TwoBytes);

        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Disconnected);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Error).IsTypeOf<ObserverNotificationOverflowException>();
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies callback execution remains serialized when an observer publishes reentrantly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestSerializesReentrantCallbacks()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new ReentrantObserver(dispatcher);
        using var subscription = dispatcher.Subscribe(observer, new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.CoalesceLatest));

        _ = dispatcher.PublishLatest(FirstValue, OneByte);

        scheduler.RunAll();

        await Assert.That(observer.Values).Count().IsEqualTo(TwoItems);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.Values[1]).IsEqualTo(SecondValue);
        await Assert.That(observer.ConcurrentCallbacks).IsEqualTo(None);
        await Assert.That(observer.MaximumDepth).IsEqualTo(OneItem);
    }

    /// <summary>Verifies completion and disposal stop later notifications while allowing an already executing callback to finish.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CompleteAndDisposePreventLaterNotifications()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new DisposingObserver();
        using var subscription = dispatcher.Subscribe(observer, new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.CoalesceLatest));
        observer.Subscription = subscription;

        _ = dispatcher.PublishLatest(FirstValue, OneByte);
        scheduler.RunOne();
        _ = dispatcher.Complete();
        _ = dispatcher.PublishLatest(SecondValue, OneByte);
        scheduler.RunAll();

        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.CompletedCount).IsEqualTo(None);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies observer exceptions are reported and do not stop other subscriptions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ThrowingObserverIsIsolatedFromOtherObservers()
    {
        var scheduler = new ControlledObserverScheduler();
        var faults = new List<Exception>();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler, faults.Add);
        var throwing = new ThrowingObserver<int>();
        var recording = new RecordingObserver<int>();
        using var throwingSubscription = dispatcher.Subscribe(throwing, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));
        using var recordingSubscription = dispatcher.Subscribe(recording, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        _ = dispatcher.PublishEvent(FirstValue, OneByte);
        scheduler.RunAll();

        await Assert.That(faults).Count().IsEqualTo(OneItem);
        await Assert.That(faults[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(recording.Values).Count().IsEqualTo(OneItem);
        await Assert.That(recording.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(OneItem);
    }

    /// <summary>Verifies fault reporter exceptions cannot escape observer drain work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FaultReporterExceptionIsContainedWhenObserverThrows()
    {
        var scheduler = new ControlledObserverScheduler();
        using var listener = ThrowingTraceListener.Install();
        using var dispatcher = new ObserverNotificationDispatcher<int>(
            scheduler,
            static _ => throw new InvalidOperationException("reporter failed"));
        var throwing = new ThrowingObserver<int>();
        var recording = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(throwing, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));
        _ = dispatcher.Subscribe(recording, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        await Assert.That(dispatcher.HasFaultReporterFailure).IsFalse();
        _ = dispatcher.PublishEvent(FirstValue, OneByte);
        scheduler.RunAll();

        await Assert.That(recording.Values).Count().IsEqualTo(OneItem);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(OneItem);
        await Assert.That(listener.WriteCount).IsEqualTo(None);
        await Assert.That(dispatcher.HasFaultReporterFailure).IsTrue();
    }

    /// <summary>Verifies the default fault reporter contains observer failures without extra setup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DefaultFaultReporterContainsObserverFailure()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new ThrowingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        var result = dispatcher.PublishEvent(FirstValue, OneByte);
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies scheduler failures clear the affected subscription without requiring another publication.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SchedulerFailureClearsSubscriptionWithoutFurtherPublication()
    {
        var scheduler = new ControlledObserverScheduler { FailNextSchedule = true };
        using var listener = ThrowingTraceListener.Install();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var failed = dispatcher.PublishLatest(FirstValue, OneByte);
        scheduler.RunAll();
        var later = dispatcher.PublishLatest(SecondValue, OneByte);

        await Assert.That(failed).IsEqualTo(ObserverNotificationPublishResult.SchedulerRejected);
        await Assert.That(later).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
        await Assert.That(listener.WriteCount).IsEqualTo(None);
    }

    /// <summary>Verifies fault reporter exceptions cannot escape scheduler failure handling.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FaultReporterExceptionIsContainedWhenSchedulingFails()
    {
        var scheduler = new ControlledObserverScheduler { FailNextSchedule = true };
        using var dispatcher = new ObserverNotificationDispatcher<int>(
            scheduler,
            static _ => throw new InvalidOperationException("reporter failed"));
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.PublishLatest(FirstValue, OneByte);

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.SchedulerRejected);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies scheduler failure is returned without invoking a slow fault reporter on the publisher stack.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SchedulerFailureDoesNotInvokeFaultReporterOnPublisherStack()
    {
        var scheduler = new ControlledObserverScheduler { FailNextSchedule = true };
        var faultReports = None;
        using var dispatcher = new ObserverNotificationDispatcher<int>(
            scheduler,
            _ =>
            {
                faultReports++;
                throw new InvalidOperationException("reporter would block");
            });
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.PublishLatest(FirstValue, OneByte);

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.SchedulerRejected);
        await Assert.That(faultReports).IsEqualTo(None);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies terminal scheduler failure disconnects without leaving a later callback to drain.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SchedulerFailureDuringCompletionClearsQueuedTerminalNotification()
    {
        var scheduler = new ControlledObserverScheduler { FailNextSchedule = true };
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.Complete();
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.SchedulerRejected);
        await Assert.That(observer.CompletedCount).IsEqualTo(None);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies invalid subscription options and notification sizes fail before mutating dispatcher state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvalidOptionsAndSizesAreRejected()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);

        await Assert.That(() => dispatcher.Subscribe(new RecordingObserver<int>(), new(None, OneByte, ObserverNotificationOverflowMode.CoalesceLatest))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => dispatcher.PublishLatest(FirstValue, NoBytes)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => dispatcher.PublishEvent(FirstValue, NegativeBytes)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies dispatcher disposal clears active subscriptions and is idempotent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeClearsSubscriptionsAndStopsFuturePublication()
    {
        var scheduler = new ControlledObserverScheduler();
        var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        dispatcher.Dispose();
        dispatcher.Dispose();
        var result = dispatcher.PublishLatest(FirstValue, OneByte);

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
        await Assert.That(observer.Values).IsEmpty();
    }

    /// <summary>Verifies completion reaches observers and stops later subscriptions.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CompleteNotifiesObserversAndRejectsLaterSubscription()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.Complete();
        var stopped = dispatcher.Complete();

        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(stopped).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(observer.CompletedCount).IsEqualTo(OneItem);
        await Assert.That(() => dispatcher.Subscribe(new RecordingObserver<int>(), new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies normal completion preserves already accepted queued values before completing.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CompletePreservesAcceptedQueuedValueBeforeTerminalCallback()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        var queued = dispatcher.PublishEvent(FirstValue, OneByte);
        var completed = dispatcher.Complete();

        scheduler.RunAll();

        await Assert.That(queued).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(completed).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.CompletedCount).IsEqualTo(OneItem);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies disposing after queued completion prevents the terminal callback from running.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAfterQueuedCompletionPreventsCompletionCallback()
    {
        var scheduler = new ControlledObserverScheduler();
        var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.Complete();
        dispatcher.Dispose();
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(observer.CompletedCount).IsEqualTo(None);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies disposing after queued overflow prevents the overflow error callback from running.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAfterQueuedOverflowPreventsErrorCallback()
    {
        var scheduler = new ControlledObserverScheduler();
        var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        _ = dispatcher.PublishEvent(FirstValue, OneByte);
        var result = dispatcher.PublishEvent(SecondValue, OneByte);
        dispatcher.Dispose();
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Disconnected);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Error).IsNull();
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies disposal lets an already executing callback return naturally.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringRunningCallbackAllowsCallbackToReturn()
    {
        var scheduler = new ControlledObserverScheduler();
        var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new BlockingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.CoalesceLatest));

        _ = dispatcher.PublishLatest(FirstValue, OneByte);
        var drain = Task.Run(scheduler.RunOne);
        await observer.Entered.Task.WaitAsync(GuardTimeout);

        dispatcher.Dispose();
        observer.Release();
        await drain.WaitAsync(GuardTimeout);

        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies fault reaches observers and validates null errors.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FaultNotifiesObserversAndRejectsNullError()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        var error = new InvalidOperationException("terminal");
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        var result = dispatcher.Fault(error);
        var stopped = dispatcher.Fault(error);

        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(stopped).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(observer.Error).IsSameReferenceAs(error);
        Func<Exception, ObserverNotificationPublishResult> fault = dispatcher.Fault;
        var exception = Assert.ThrowsExactly<TargetInvocationException>(() => fault.DynamicInvoke([null]));
        await Assert.That(exception.InnerException).IsTypeOf<ArgumentNullException>();
    }

    /// <summary>Verifies a subscription disposed during publication receives no notification.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionDisposedDuringPublicationIsSkipped()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var first = new RecordingObserver<int>();
        var second = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(first, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));
        var secondSubscription = dispatcher.Subscribe(second, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));
        scheduler.AfterNextSchedule = secondSubscription.Dispose;

        var result = dispatcher.PublishLatest(FirstValue, OneByte);

        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(first.Values).Count().IsEqualTo(OneItem);
        await Assert.That(second.Values).IsEmpty();
    }

    /// <summary>Verifies a subscription disposed during terminal publication is skipped.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionDisposedDuringCompletionIsSkipped()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var first = new RecordingObserver<int>();
        var second = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(first, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));
        var secondSubscription = dispatcher.Subscribe(second, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));
        scheduler.AfterNextSchedule = secondSubscription.Dispose;

        var result = dispatcher.Complete();

        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(first.CompletedCount).IsEqualTo(OneItem);
        await Assert.That(second.CompletedCount).IsEqualTo(None);
    }

    /// <summary>Verifies the default dispatcher scheduler uses the thread pool rather than the publisher stack.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DefaultSchedulerDispatchesThroughThreadPool()
    {
        using var dispatcher = new ObserverNotificationDispatcher<int>();
        var observer = new CompletingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.PublishLatest(FirstValue, OneByte);

        await observer.Notification.Task.WaitAsync(GuardTimeout);
        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(observer.Value).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies explicit initial replay is queued before live values and validates replay size.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeWithInitialValueQueuesReplayBeforeLiveNotification()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();

        using var subscription = dispatcher.Subscribe(observer, new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.Disconnect), true, FirstValue, OneByte);
        var result = dispatcher.PublishEvent(SecondValue, OneByte);
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(observer.Values).Count().IsEqualTo(TwoItems);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.Values[1]).IsEqualTo(SecondValue);
        await Assert.That(() => dispatcher.Subscribe(new RecordingObserver<int>(), new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect), true, FirstValue, NoBytes))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies factory-backed initial replay creates the value only when callbacks drain.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeWithInitialFactoryQueuesReplayBeforeLiveNotification()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        var factoryCalls = 0;

        using var subscription = dispatcher.Subscribe(
            observer,
            new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.Disconnect),
            true,
            () =>
            {
                factoryCalls++;
                return FirstValue;
            },
            OneByte);
        var result = dispatcher.PublishEvent(SecondValue, OneByte);

        await Assert.That(factoryCalls).IsEqualTo(None);
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(factoryCalls).IsEqualTo(OneItem);
        await Assert.That(observer.Values).Count().IsEqualTo(TwoItems);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.Values[1]).IsEqualTo(SecondValue);
        await Assert.That(() => dispatcher.Subscribe(new RecordingObserver<int>(), new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect), true, static () => FirstValue, NoBytes))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies factory-backed latest notifications coalesce through per-callback factories.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestFactoryCoalescesQueuedNotification()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));
        var factoryCalls = 0;

        var first = dispatcher.PublishLatest(static () => FirstValue, OneByte);
        var second = dispatcher.PublishLatest(
            () =>
            {
                factoryCalls++;
                return SecondValue;
            },
            OneByte);

        scheduler.RunAll();

        await Assert.That(first).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(second).IsEqualTo(ObserverNotificationPublishResult.Coalesced);
        await Assert.That(factoryCalls).IsEqualTo(OneItem);
        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(SecondValue);
    }

    /// <summary>Verifies factory-backed latest overflow disconnects when a notification cannot fit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestFactoryDisconnectsWhenNotificationExceedsByteCapacity()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.PublishLatest(static () => FirstValue, TwoBytes);
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Disconnected);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Error).IsTypeOf<ObserverNotificationOverflowException>();
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies factory publication skips a subscription after an overflow terminal has been queued.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestFactoryReturnsStoppedForTerminalQueuedSubscription()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));

        _ = dispatcher.PublishEvent(FirstValue, TwoBytes);
        var result = dispatcher.PublishLatest(static () => SecondValue, OneByte);
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(observer.Error).IsTypeOf<ObserverNotificationOverflowException>();
    }

    /// <summary>Verifies factory-backed publication returns stopped when no subscription can receive it.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestFactoryReturnsStoppedWithoutSubscribers()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);

        var result = dispatcher.PublishLatest(static () => FirstValue, OneByte);

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Stopped);
    }

    /// <summary>Verifies stopped dispatchers reject initial replay overloads and factory publication.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StoppedDispatcherRejectsInitialReplayAndFactoryPublication()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);

        _ = dispatcher.Complete();
        var result = dispatcher.PublishLatest(static () => FirstValue, OneByte);

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(() => dispatcher.Subscribe(new RecordingObserver<int>(), new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect), true, FirstValue, OneByte))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => dispatcher.Subscribe(new RecordingObserver<int>(), new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect), true, static () => FirstValue, OneByte))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies factory-backed scheduler failures detach the subscription.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestFactorySchedulerFailureClearsSubscription()
    {
        var scheduler = new ControlledObserverScheduler { FailNextSchedule = true };
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));

        var result = dispatcher.PublishLatest(static () => FirstValue, OneByte);

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.SchedulerRejected);
        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Verifies factory-backed initial replay scheduler failures detach the subscription.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeWithInitialFactorySchedulerFailureClearsSubscription()
    {
        var scheduler = new ControlledObserverScheduler { FailNextSchedule = true };
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();

        _ = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest), true, static () => FirstValue, OneByte);

        await Assert.That(dispatcher.SubscriptionCount).IsEqualTo(None);
    }

    /// <summary>Provides deterministic execution of scheduled observer work.</summary>
    private sealed class ControlledObserverScheduler : IObserverNotificationScheduler
    {
        /// <summary>Stores scheduled work in FIFO order.</summary>
        private readonly Queue<IWorkItem> _items = new();

        /// <summary>Gets or sets a value indicating whether the next schedule call throws.</summary>
        internal bool FailNextSchedule { get; set; }

        /// <summary>Gets or sets the callback invoked after the next successful schedule.</summary>
        internal Action? AfterNextSchedule { get; set; }

        /// <summary>Gets the number of queued work items.</summary>
        internal int PendingCount => _items.Count;

        /// <inheritdoc />
        public void Schedule(IWorkItem item)
        {
            if (FailNextSchedule)
            {
                FailNextSchedule = false;
                throw new InvalidOperationException("schedule failed");
            }

            _items.Enqueue(item);
            var afterSchedule = AfterNextSchedule;
            AfterNextSchedule = null;
            afterSchedule?.Invoke();
        }

        /// <summary>Runs all currently queued items and reentrant work.</summary>
        internal void RunAll()
        {
            while (_items.Count > 0)
            {
                RunOne();
            }
        }

        /// <summary>Runs one queued item.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RunOne() => _items.Dequeue().Execute();
    }

    /// <summary>Throws if production code writes observer-dispatch faults to global trace listeners.</summary>
    private sealed class ThrowingTraceListener : TraceListener
    {
        /// <summary>Initializes a new instance of the <see cref="ThrowingTraceListener"/> class.</summary>
        private ThrowingTraceListener()
        {
        }

        /// <summary>Gets the number of attempted writes.</summary>
        internal int WriteCount { get; private set; }

        /// <inheritdoc />
        public override void Write(string? message) => WriteCore();

        /// <inheritdoc />
        public override void WriteLine(string? message) => Write(message);

        /// <summary>Installs the listener.</summary>
        /// <returns>The listener to dispose.</returns>
        internal static ThrowingTraceListener Install()
        {
            var listener = new ThrowingTraceListener();
            _ = Trace.Listeners.Add(listener);
            return listener;
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Trace.Listeners.Remove(this);
            }

            base.Dispose(disposing);
        }

        /// <summary>Records a write attempt and fails the test path.</summary>
        /// <exception cref="InvalidOperationException">Always thrown to expose an unsafe trace callback.</exception>
        private void WriteCore()
        {
            WriteCount++;
            throw new InvalidOperationException("trace listener must not run");
        }
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The notification value type.</typeparam>
    private class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Gets recorded values.</summary>
        internal List<T> Values { get; } = [];

        /// <summary>Gets the terminal error.</summary>
        internal Exception? Error { get; private set; }

        /// <summary>Gets the completion count.</summary>
        internal int CompletedCount { get; private set; }

        /// <inheritdoc />
        public virtual void OnCompleted() => CompletedCount++;

        /// <inheritdoc />
        public virtual void OnError(Exception error) => Error = error;

        /// <inheritdoc />
        public virtual void OnNext(T value) => Values.Add(value);
    }

    /// <summary>Publishes from inside a callback to prove serialized drain behavior.</summary>
    /// <param name="dispatcher">The dispatcher used for reentrant publication.</param>
    private sealed class ReentrantObserver(ObserverNotificationDispatcher<int> dispatcher) : RecordingObserver<int>
    {
        /// <summary>Stores the current callback depth.</summary>
        private int _depth;

        /// <summary>Gets the concurrent callback count.</summary>
        internal int ConcurrentCallbacks { get; private set; }

        /// <summary>Gets the maximum callback depth.</summary>
        internal int MaximumDepth { get; private set; }

        /// <inheritdoc />
        public override void OnNext(int value)
        {
            var depth = Interlocked.Increment(ref _depth);
            MaximumDepth = Math.Max(MaximumDepth, depth);
            if (depth > 1)
            {
                ConcurrentCallbacks++;
            }

            try
            {
                base.OnNext(value);
                if (value == FirstValue)
                {
                    _ = dispatcher.PublishLatest(SecondValue, OneByte);
                }
            }
            finally
            {
                _ = Interlocked.Decrement(ref _depth);
            }
        }
    }

    /// <summary>Disposes its subscription during a callback.</summary>
    private sealed class DisposingObserver : RecordingObserver<int>
    {
        /// <summary>Gets or sets the subscription disposed during the first callback.</summary>
        internal IDisposable? Subscription { get; set; }

        /// <inheritdoc />
        public override void OnNext(int value)
        {
            base.OnNext(value);
            Subscription?.Dispose();
        }
    }

    /// <summary>Throws on the first value notification.</summary>
    /// <typeparam name="T">The notification value type.</typeparam>
    private sealed class ThrowingObserver<T> : IObserver<T>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc />
        public void OnNext(T value) => throw new InvalidOperationException("observer failed");
    }

    /// <summary>Completes a task when a value arrives.</summary>
    /// <typeparam name="T">The notification value type.</typeparam>
    private sealed class CompletingObserver<T> : IObserver<T>
    {
        /// <summary>Gets the received notification task.</summary>
        internal TaskCompletionSource Notification { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the received value.</summary>
        internal T? Value { get; private set; }

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc />
        public void OnNext(T value)
        {
            Value = value;
            Notification.SetResult();
        }
    }

    /// <summary>Blocks inside the callback until the test releases it.</summary>
    /// <typeparam name="T">The notification value type.</typeparam>
    private sealed class BlockingObserver<T> : RecordingObserver<T>
    {
        /// <summary>Releases the callback.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the task that signals when the callback has started.</summary>
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc />
        public override void OnNext(T value)
        {
            Entered.SetResult();
            _release.Task.GetAwaiter().GetResult();
            base.OnNext(value);
        }

        /// <summary>Releases the observer callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _release.SetResult();
    }
}
