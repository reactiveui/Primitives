// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ObserverNotificationDispatcher{T}"/>.</summary>
/// <content>Async factory dispatch tests.</content>
public sealed partial class ObserverNotificationDispatcherTests
{
    /// <summary>The polling delay used by asynchronous dispatcher assertions.</summary>
    private const int PollDelayMilliseconds = 10;

    /// <summary>Verifies async latest and sync event factory wrappers deliver values.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FactoryWrappersDeliverLatestAndEventValues()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.CoalesceLatest));

        var latest = dispatcher.PublishLatest(static _ => new ValueTask<int>(FirstValue), OneByte);
        var eventResult = dispatcher.PublishEvent(static () => SecondValue, OneByte);
        scheduler.RunAll();

        await Assert.That(latest).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(eventResult).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(observer.Values).Count().IsEqualTo(TwoItems);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.Values[1]).IsEqualTo(SecondValue);
    }

    /// <summary>Verifies stopped dispatchers reject async factory publication paths.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StoppedDispatcherRejectsAsyncFactoryPublication()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var schedules = new List<Action>();

        _ = dispatcher.Complete();
        var latest = dispatcher.PublishLatest(static _ => new ValueTask<int>(FirstValue), OneByte);
        var deferred = dispatcher.PublishLatestDeferred(static _ => new ValueTask<int>(SecondValue), OneByte, schedules);

        await Assert.That(latest).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(deferred).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(() => dispatcher.Subscribe(
            new RecordingObserver<int>(),
            new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect),
            true,
            static _ => new ValueTask<int>(FirstValue),
            OneByte)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(schedules).IsEmpty();
    }

    /// <summary>Verifies async initial replay uses deferred scheduling and delivers the initial value.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeWithAsyncInitialFactoryDeliversInitialValue()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();

        using var subscription = dispatcher.Subscribe(
            observer,
            new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest),
            true,
            static _ => new ValueTask<int>(FirstValue),
            OneByte);
        scheduler.RunAll();

        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies sync initial replay factories still flow through the public subscription wrapper.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeWithSyncInitialFactoryDeliversInitialValue()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();

        using var subscription = dispatcher.Subscribe(
            observer,
            new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest),
            true,
            static () => FirstValue,
            OneByte);
        scheduler.RunAll();

        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies deferred async latest coalesces queued state before scheduling outside the caller gate.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestDeferredCoalescesAsyncFactories()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.CoalesceLatest));
        var schedules = new List<Action>();

        var first = dispatcher.PublishLatestDeferred(static _ => new ValueTask<int>(FirstValue), OneByte, schedules);
        var second = dispatcher.PublishLatestDeferred(static _ => new ValueTask<int>(SecondValue), OneByte, schedules);
        RunSchedules(schedules);
        scheduler.RunAll();

        await Assert.That(first).IsEqualTo(ObserverNotificationPublishResult.Queued);
        await Assert.That(second).IsEqualTo(ObserverNotificationPublishResult.Coalesced);
        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(SecondValue);
    }

    /// <summary>Verifies deferred async publication skips a subscription after an overflow terminal is queued.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublishLatestDeferredReturnsStoppedForTerminalQueuedSubscription()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));
        var schedules = new List<Action>();

        _ = dispatcher.PublishEvent(FirstValue, TwoBytes);
        var result = dispatcher.PublishLatestDeferred(static _ => new ValueTask<int>(SecondValue), OneByte, schedules);
        RunSchedules(schedules);
        scheduler.RunAll();

        await Assert.That(result).IsEqualTo(ObserverNotificationPublishResult.Stopped);
        await Assert.That(observer.Error).IsTypeOf<ObserverNotificationOverflowException>();
    }

    /// <summary>Verifies in-flight async materialization remains charged against the byte capacity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AsyncMaterializationRetainsBytesUntilDeliveryCompletes()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(TwoItems, OneByte, ObserverNotificationOverflowMode.Disconnect));
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = dispatcher.PublishEvent(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                entered.SetResult();
                await release.Task.ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                return FirstValue;
            },
            OneByte);
        scheduler.RunOne();
        await entered.Task.WaitAsync(GuardTimeout);

        var second = dispatcher.PublishEvent(static _ => new ValueTask<int>(SecondValue), OneByte);
        release.SetResult();
        await WaitUntilAsync(() => observer.Error is not null, GuardTimeout);

        await Assert.That(second).IsEqualTo(ObserverNotificationPublishResult.Disconnected);
        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
        await Assert.That(observer.Error).IsTypeOf<ObserverNotificationOverflowException>();
    }

    /// <summary>Verifies coalescing cannot replace an in-flight value to exceed the notification count bound.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LatestCoalescingCannotReplaceInflightNotification()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        using var subscription = dispatcher.Subscribe(observer, new(OneItem, TwoBytes, ObserverNotificationOverflowMode.CoalesceLatest));
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = dispatcher.PublishLatest(
            async token =>
            {
                token.ThrowIfCancellationRequested();
                entered.SetResult();
                await release.Task.ConfigureAwait(false);
                return FirstValue;
            },
            OneByte);
        scheduler.RunOne();
        await entered.Task.WaitAsync(GuardTimeout);

        var second = dispatcher.PublishLatest(static _ => new ValueTask<int>(SecondValue), OneByte);
        release.SetResult();
        await WaitUntilAsync(() => observer.Error is not null, GuardTimeout);

        await Assert.That(second).IsEqualTo(ObserverNotificationPublishResult.Disconnected);
        await Assert.That(observer.Values).Count().IsEqualTo(OneItem);
        await Assert.That(observer.Values[0]).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies disposing during async materialization cancels the materializer and clears later queued payloads.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringAsyncMaterializationPreventsCallbackAndQueuedMaterialization()
    {
        var scheduler = new ControlledObserverScheduler();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler);
        var observer = new RecordingObserver<int>();
        var subscription = dispatcher.Subscribe(observer, new(TwoItems, TwoBytes, ObserverNotificationOverflowMode.Disconnect));
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondMaterialized = false;
        var materializerToken = CancellationToken.None;

        _ = dispatcher.PublishEvent(
            async token =>
            {
                materializerToken = token;
                entered.SetResult();
                try
                {
                    await release.Task.ConfigureAwait(false);
                    return FirstValue;
                }
                finally
                {
                    completed.SetResult();
                }
            },
            OneByte);
        _ = dispatcher.PublishEvent(
            token =>
            {
                token.ThrowIfCancellationRequested();
                secondMaterialized = true;
                return new ValueTask<int>(SecondValue);
            },
            OneByte);
        scheduler.RunOne();
        await entered.Task.WaitAsync(GuardTimeout);

        subscription.Dispose();
        release.SetResult();
        await completed.Task.WaitAsync(GuardTimeout);
        await Task.Yield();

        await Assert.That(materializerToken.IsCancellationRequested).IsTrue();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(secondMaterialized).IsFalse();
    }

    /// <summary>Verifies cancellation observed by a materializer stops delivery without reporting an observer fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringAsyncMaterializationContainsCancellation()
    {
        var scheduler = new ControlledObserverScheduler();
        var faults = new List<Exception>();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler, faults.Add);
        var observer = new RecordingObserver<int>();
        var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = dispatcher.PublishEvent(
            async token =>
            {
                entered.SetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token).ConfigureAwait(false);
                return FirstValue;
            },
            OneByte);
        scheduler.RunOne();
        await entered.Task.WaitAsync(GuardTimeout);

        subscription.Dispose();
        await WaitUntilAsync(() => dispatcher.SubscriptionCount == None, GuardTimeout);

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(faults).IsEmpty();
    }

    /// <summary>Verifies disposal waits for cancellation callbacks before disposing the materialization token source.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MaterializationCompletingDuringCancellationDoesNotDisposeTokenSourceEarly()
    {
        var scheduler = new ControlledObserverScheduler();
        var faults = new List<Exception>();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler, faults.Add);
        var observer = new RecordingObserver<int>();
        var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource materialized = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim allowCancellationToComplete = new(false);
        var cancellationGate = new MaterializationCancellationGate(release, allowCancellationToComplete);
        var registrations = new List<CancellationTokenRegistration>();

        _ = dispatcher.PublishEvent(
            async token =>
            {
                registrations.Add(token.UnsafeRegister(
                    static state =>
                    {
                        var gate = (MaterializationCancellationGate?)state;
                        ArgumentNullException.ThrowIfNull(gate);
                        gate.ReleaseAndWait();
                    },
                    cancellationGate));
                entered.SetResult();
                await release.Task.ConfigureAwait(false);
                materialized.SetResult();
                return FirstValue;
            },
            OneByte);
        scheduler.RunOne();
        await entered.Task.WaitAsync(GuardTimeout);

        var disposeTask = Task.Run(subscription.Dispose);
        await release.Task.WaitAsync(GuardTimeout);
        await materialized.Task.WaitAsync(GuardTimeout);

        await Assert.That(disposeTask.IsCompleted).IsFalse();
        await Assert.That(faults).IsEmpty();
        await Assert.That(observer.Values).IsEmpty();

        allowCancellationToComplete.Set();
        await disposeTask.WaitAsync(GuardTimeout);
        await WaitUntilAsync(() => dispatcher.SubscriptionCount == None, GuardTimeout);
        for (var index = 0; index < registrations.Count; index++)
        {
            await registrations[index].DisposeAsync().ConfigureAwait(false);
        }

        await Assert.That(faults).IsEmpty();
    }

    /// <summary>Verifies cancellation callback failures are reported outside subscription gates.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeDuringAsyncMaterializationReportsCancellationCallbackFailure()
    {
        var scheduler = new ControlledObserverScheduler();
        var faults = new List<Exception>();
        using var dispatcher = new ObserverNotificationDispatcher<int>(scheduler, faults.Add);
        var observer = new RecordingObserver<int>();
        var subscription = dispatcher.Subscribe(observer, new(OneItem, OneByte, ObserverNotificationOverflowMode.Disconnect));
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = dispatcher.PublishEvent(
            async token =>
            {
                await using var registration = token.Register(static () => throw new InvalidOperationException("cancel failed"));
                entered.SetResult();
                await release.Task.ConfigureAwait(false);
                return FirstValue;
            },
            OneByte);
        scheduler.RunOne();
        await entered.Task.WaitAsync(GuardTimeout);

        subscription.Dispose();
        release.SetResult();
        await WaitUntilAsync(() => faults.Count == OneItem, GuardTimeout);

        await Assert.That(faults[0]).IsTypeOf<AggregateException>();
        await Assert.That(observer.Values).IsEmpty();
    }

    /// <summary>Runs deferred schedule callbacks.</summary>
    /// <param name="schedules">The callbacks to run.</param>
    private static void RunSchedules(List<Action> schedules)
    {
        for (var i = 0; i < schedules.Count; i++)
        {
            schedules[i]();
        }
    }

    /// <summary>Waits for an asynchronous condition to become true.</summary>
    /// <param name="condition">The condition to observe.</param>
    /// <param name="timeout">The maximum wait.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The condition was not met before the timeout.</exception>
    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        using CancellationTokenSource source = new(timeout);
        try
        {
            while (!condition())
            {
                await Task.Delay(PollDelayMilliseconds, source.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException exception) when (source.IsCancellationRequested)
        {
            throw new TimeoutException("The condition was not met before the timeout.", exception);
        }
    }

    /// <summary>Coordinates a cancellation callback that completes materialization before cancellation returns.</summary>
    /// <param name="release">The signal released by cancellation.</param>
    /// <param name="allowCancellationToComplete">The gate allowing cancellation to return.</param>
    private sealed class MaterializationCancellationGate(TaskCompletionSource release, ManualResetEventSlim allowCancellationToComplete)
    {
        /// <summary>Signals materialization and waits until the test allows cancellation to complete.</summary>
        internal void ReleaseAndWait()
        {
            release.SetResult();
            allowCancellationToComplete.Wait();
        }
    }
}
