// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Dispatches bounded, isolated observer notifications for one logical stream.</summary>
/// <typeparam name="T">The notification value type.</typeparam>
/// <remarks>
/// The owning stream lane must serialize publications and terminal signals to establish a common order for every
/// subscription. Each subscription drains independently; an observer callback cannot hold up another subscriber's queue.
/// </remarks>
internal sealed partial class ObserverNotificationDispatcher<T> : IDisposable
{
    /// <summary>The message used when subscribing after terminal dispatcher stop.</summary>
    private const string StoppedMessage = "The observer notification dispatcher has already stopped.";

    /// <summary>Protects subscription membership and lifecycle state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores subscriptions that have not drained, faulted, or been disposed.</summary>
    private readonly List<Subscription> _subscriptions = [];

    /// <summary>Schedules drain work away from the publisher call stack.</summary>
    private readonly IObserverNotificationScheduler _scheduler;

    /// <summary>Receives observer callback failures.</summary>
    private readonly Action<Exception> _reportFault;

    /// <summary>Tracks dispatcher terminal state.</summary>
    private bool _stopped;

    /// <summary>Tracks whether the dispatcher has been disposed.</summary>
    private bool _disposed;

    /// <summary>Tracks whether the diagnostic reporter itself has failed.</summary>
    private int _faultReporterFailed;

    /// <summary>Initializes a new instance of the <see cref="ObserverNotificationDispatcher{T}"/> class.</summary>
    internal ObserverNotificationDispatcher()
        : this(ThreadPoolObserverNotificationScheduler.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ObserverNotificationDispatcher{T}"/> class.</summary>
    /// <param name="scheduler">The scheduler used to drain observer queues.</param>
    /// <param name="reportFault">The optional observer-fault reporter.</param>
    internal ObserverNotificationDispatcher(IObserverNotificationScheduler scheduler, Action<Exception>? reportFault = null)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        _scheduler = scheduler;
        _reportFault = reportFault ?? (static _ => { });
    }

    /// <summary>Gets whether the diagnostic reporter failed while containing an observer callback failure.</summary>
    internal bool HasFaultReporterFailure => Volatile.Read(ref _faultReporterFailed) != 0;

    /// <summary>Gets the number of subscriptions that have not disconnected or been disposed.</summary>
    internal int SubscriptionCount
    {
        get
        {
            lock (_gate)
            {
                return _subscriptions.Count;
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Subscription[] subscriptions;

        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _stopped = true;
            subscriptions = CopySubscriptions();
            _subscriptions.Clear();
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i].Dispose();
        }
    }

    /// <summary>
    /// Completes every subscription after already accepted data notifications. Dispatcher disposal before a drain
    /// claims a notification discards queued notifications; a callback already claimed by a drain may start or return
    /// after disposal.
    /// </summary>
    /// <returns>The publication result.</returns>
    internal ObserverNotificationPublishResult Complete()
    {
        Subscription[] subscriptions;

        lock (_gate)
        {
            if (_stopped)
            {
                return ObserverNotificationPublishResult.Stopped;
            }

            _stopped = true;
            subscriptions = CopySubscriptions();
        }

        var result = ObserverNotificationPublishResult.Stopped;
        var notification = Notification.Completed();
        for (var i = 0; i < subscriptions.Length; i++)
        {
            var subscriptionResult = subscriptions[i].PublishTerminal(notification);
            if (subscriptionResult > result)
            {
                result = subscriptionResult;
            }
        }

        return result;
    }

    /// <summary>Terminates every subscription with an error after already accepted data notifications.</summary>
    /// <param name="error">The terminal error.</param>
    /// <returns>The publication result.</returns>
    internal ObserverNotificationPublishResult Fault(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);
        Subscription[] subscriptions;

        lock (_gate)
        {
            if (_stopped)
            {
                return ObserverNotificationPublishResult.Stopped;
            }

            _stopped = true;
            subscriptions = CopySubscriptions();
        }

        var result = ObserverNotificationPublishResult.Stopped;
        var notification = Notification.Error(error);
        for (var i = 0; i < subscriptions.Length; i++)
        {
            var subscriptionResult = subscriptions[i].PublishTerminal(notification);
            if (subscriptionResult > result)
            {
                result = subscriptionResult;
            }
        }

        return result;
    }

    /// <summary>Publishes a latest-state notification to active subscriptions.</summary>
    /// <param name="value">The state value.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <returns>The aggregate publication result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishLatest(T value, long sizeBytes) =>
        Publish(value, sizeBytes, true);

    /// <summary>Publishes a latest-state notification through a per-observer value factory.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <returns>The aggregate publication result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishLatest(Func<T> valueFactory, long sizeBytes) =>
        Publish(valueFactory, sizeBytes, true);

    /// <summary>Publishes a latest-state notification through an async per-observer value factory.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <returns>The aggregate publication result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishLatest(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes) =>
        Publish(valueFactory, sizeBytes, true);

    /// <summary>Publishes an event notification to active subscriptions.</summary>
    /// <param name="value">The event value.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <returns>The aggregate publication result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishEvent(T value, long sizeBytes) =>
        Publish(value, sizeBytes, false);

    /// <summary>Publishes an event notification through a per-observer value factory.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <returns>The aggregate publication result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishEvent(Func<T> valueFactory, long sizeBytes) =>
        Publish(valueFactory, sizeBytes, false);

    /// <summary>Publishes an event notification through an async per-observer value factory.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <returns>The aggregate publication result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishEvent(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes) =>
        Publish(valueFactory, sizeBytes, false);

    /// <summary>Queues a latest-state notification and returns any drain scheduling work to run later.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <param name="schedules">The scheduling callbacks to run after the caller leaves its own lock.</param>
    /// <returns>The aggregate publication result before deferred scheduling failures are observed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishLatestDeferred(
        Func<CancellationToken, ValueTask<T>> valueFactory,
        long sizeBytes,
        List<Action> schedules) =>
        PublishDeferred(valueFactory, sizeBytes, true, schedules);

    /// <summary>Queues an event notification and returns drain scheduling work to run later.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated retained byte size.</param>
    /// <param name="schedules">The scheduling callbacks to run after the caller leaves its lock.</param>
    /// <returns>The aggregate publication result before deferred scheduling failures are observed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishEventDeferred(
        Func<CancellationToken, ValueTask<T>> valueFactory,
        long sizeBytes,
        List<Action> schedules) =>
        PublishDeferred(valueFactory, sizeBytes, false, schedules);

    /// <summary>Subscribes an observer with a bounded notification queue.</summary>
    /// <param name="observer">The observer receiving serialized callbacks.</param>
    /// <param name="options">The queue options.</param>
    /// <returns>The subscription handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is invalid or the dispatcher has stopped.</exception>
    /// <exception cref="ObjectDisposedException">The dispatcher has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal IDisposable Subscribe(IObserver<T> observer, ObserverNotificationSubscriptionOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        options.Validate();
        var subscription = new Subscription(this, observer, options);

        lock (_gate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
            if (_stopped)
            {
                throw new InvalidOperationException(StoppedMessage);
            }

            _subscriptions.Add(subscription);
        }

        return subscription;
    }

    /// <summary>Subscribes an observer and queues an initial value before later live notifications can reach it.</summary>
    /// <param name="observer">The observer receiving serialized callbacks.</param>
    /// <param name="options">The queue options.</param>
    /// <param name="hasInitial">Whether an initial value should be queued.</param>
    /// <param name="initialValue">The optional initial value.</param>
    /// <param name="initialSizeBytes">The initial value byte size.</param>
    /// <returns>The subscription handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialSizeBytes"/> is not positive when replaying.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is invalid or the dispatcher has stopped.</exception>
    /// <exception cref="ObjectDisposedException">The dispatcher has been disposed.</exception>
    internal IDisposable Subscribe(
        IObserver<T> observer,
        ObserverNotificationSubscriptionOptions options,
        bool hasInitial,
        T initialValue,
        long initialSizeBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        options.Validate();
        if (hasInitial)
        {
            ValidateSize(initialSizeBytes);
        }

        var subscription = new Subscription(this, observer, options);
        var scheduleInitial = false;

        lock (_gate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
            if (_stopped)
            {
                throw new InvalidOperationException(StoppedMessage);
            }

            _subscriptions.Add(subscription);
            if (hasInitial)
            {
                scheduleInitial = subscription.QueueInitial(initialValue, initialSizeBytes);
            }
        }

        if (scheduleInitial)
        {
            subscription.ScheduleInitial();
        }

        return subscription;
    }

    /// <summary>Subscribes an observer and queues an initial value factory before later live notifications can reach it.</summary>
    /// <param name="observer">The observer receiving serialized callbacks.</param>
    /// <param name="options">The queue options.</param>
    /// <param name="hasInitial">Whether an initial value should be queued.</param>
    /// <param name="initialValueFactory">The optional initial value factory.</param>
    /// <param name="initialSizeBytes">The initial value byte size.</param>
    /// <returns>The subscription handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="initialValueFactory"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialSizeBytes"/> is not positive when replaying.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is invalid or the dispatcher has stopped.</exception>
    /// <exception cref="ObjectDisposedException">The dispatcher has been disposed.</exception>
    internal IDisposable Subscribe(
        IObserver<T> observer,
        ObserverNotificationSubscriptionOptions options,
        bool hasInitial,
        Func<T> initialValueFactory,
        long initialSizeBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        options.Validate();
        if (hasInitial)
        {
            ArgumentExceptionHelper.ThrowIfNull(initialValueFactory);
            ValidateSize(initialSizeBytes);
        }

        var subscription = new Subscription(this, observer, options);
        var scheduleInitial = false;

        lock (_gate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
            if (_stopped)
            {
                throw new InvalidOperationException(StoppedMessage);
            }

            _subscriptions.Add(subscription);
            if (hasInitial)
            {
                scheduleInitial = subscription.QueueInitial(initialValueFactory, initialSizeBytes);
            }
        }

        if (scheduleInitial)
        {
            subscription.ScheduleInitial();
        }

        return subscription;
    }

    /// <summary>Subscribes an observer and queues an async initial value factory before later live notifications can reach it.</summary>
    /// <param name="observer">The observer receiving serialized callbacks.</param>
    /// <param name="options">The queue options.</param>
    /// <param name="hasInitial">Whether an initial value should be queued.</param>
    /// <param name="initialValueFactory">The optional initial value factory.</param>
    /// <param name="initialSizeBytes">The initial value byte size.</param>
    /// <returns>The subscription handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="initialValueFactory"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialSizeBytes"/> is not positive when replaying.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is invalid or the dispatcher has stopped.</exception>
    /// <exception cref="ObjectDisposedException">The dispatcher has been disposed.</exception>
    internal IDisposable Subscribe(
        IObserver<T> observer,
        ObserverNotificationSubscriptionOptions options,
        bool hasInitial,
        Func<CancellationToken, ValueTask<T>> initialValueFactory,
        long initialSizeBytes)
    {
        var schedules = new List<Action>(1);
        var subscription = SubscribeDeferred(observer, options, hasInitial, initialValueFactory, initialSizeBytes, schedules);
        RunSchedules(schedules);
        return subscription;
    }

    /// <summary>Subscribes an observer and defers async initial replay scheduling until the caller leaves its own lock.</summary>
    /// <param name="observer">The observer receiving serialized callbacks.</param>
    /// <param name="options">The queue options.</param>
    /// <param name="hasInitial">Whether an initial value should be queued.</param>
    /// <param name="initialValueFactory">The optional initial value factory.</param>
    /// <param name="initialSizeBytes">The initial value byte size.</param>
    /// <param name="schedules">The scheduling callbacks to run after the caller leaves its own lock.</param>
    /// <returns>The subscription handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/>, <paramref name="initialValueFactory"/>, or <paramref name="schedules"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialSizeBytes"/> is not positive when replaying.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is invalid or the dispatcher has stopped.</exception>
    /// <exception cref="ObjectDisposedException">The dispatcher has been disposed.</exception>
    internal IDisposable SubscribeDeferred(
        IObserver<T> observer,
        ObserverNotificationSubscriptionOptions options,
        bool hasInitial,
        Func<CancellationToken, ValueTask<T>> initialValueFactory,
        long initialSizeBytes,
        List<Action> schedules)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentExceptionHelper.ThrowIfNull(schedules);
        options.Validate();
        if (hasInitial)
        {
            ArgumentExceptionHelper.ThrowIfNull(initialValueFactory);
            ValidateSize(initialSizeBytes);
        }

        var subscription = new Subscription(this, observer, options);
        var scheduleInitial = false;

        lock (_gate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
            if (_stopped)
            {
                throw new InvalidOperationException(StoppedMessage);
            }

            _subscriptions.Add(subscription);
            if (hasInitial)
            {
                scheduleInitial = subscription.QueueInitial(initialValueFactory, initialSizeBytes);
            }
        }

        if (scheduleInitial)
        {
            schedules.Add(subscription.ScheduleInitial);
        }

        return subscription;
    }

    /// <summary>Validates the notification size.</summary>
    /// <param name="sizeBytes">The size to validate.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="sizeBytes"/> is not positive.</exception>
    private static void ValidateSize(long sizeBytes)
    {
        if (sizeBytes > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(sizeBytes), "The notification byte size must be positive.");
    }

    /// <summary>Runs deferred scheduling callbacks.</summary>
    /// <param name="schedules">The callbacks.</param>
    private static void RunSchedules(List<Action> schedules)
    {
        for (var i = 0; i < schedules.Count; i++)
        {
            schedules[i]();
        }
    }

    /// <summary>Copies the current subscription list.</summary>
    /// <returns>The active subscriptions.</returns>
    private Subscription[] CopySubscriptions()
    {
        var subscriptions = new Subscription[_subscriptions.Count];
        _subscriptions.CopyTo(subscriptions);
        return subscriptions;
    }

    /// <summary>Forgets a subscription after disposal, overflow, callback failure, or terminal drain.</summary>
    /// <param name="subscription">The subscription to remove.</param>
    private void Forget(Subscription subscription)
    {
        lock (_gate)
        {
            _ = _subscriptions.Remove(subscription);
        }
    }

    /// <summary>Reports an observer fault without letting the reporter affect dispatch state.</summary>
    /// <param name="exception">The exception to report.</param>
    private void ReportFault(Exception exception)
    {
        try
        {
            _reportFault(exception);
        }
        catch (Exception)
        {
            _ = Interlocked.Exchange(ref _faultReporterFailed, 1);
        }
    }

    /// <summary>Publishes a data notification.</summary>
    /// <param name="value">The value to publish.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <param name="coalesceLatest">A value indicating whether overflow may replace queued data with the newest value.</param>
    /// <returns>The aggregate publication result.</returns>
    private ObserverNotificationPublishResult Publish(T value, long sizeBytes, bool coalesceLatest)
    {
        ValidateSize(sizeBytes);
        Subscription[] subscriptions;

        lock (_gate)
        {
            if (_stopped)
            {
                return ObserverNotificationPublishResult.Stopped;
            }

            subscriptions = CopySubscriptions();
        }

        var result = ObserverNotificationPublishResult.Stopped;
        for (var i = 0; i < subscriptions.Length; i++)
        {
            var mode = coalesceLatest && subscriptions[i].Options.OverflowMode == ObserverNotificationOverflowMode.CoalesceLatest
                ? ObserverNotificationOverflowMode.CoalesceLatest
                : ObserverNotificationOverflowMode.Disconnect;
            var subscriptionResult = subscriptions[i].Publish(value, sizeBytes, mode);
            if (subscriptionResult > result)
            {
                result = subscriptionResult;
            }
        }

        return result;
    }

    /// <summary>Publishes a data notification through a per-observer value factory.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <param name="coalesceLatest">A value indicating whether overflow may replace queued data with the newest value.</param>
    /// <returns>The aggregate publication result.</returns>
    private ObserverNotificationPublishResult Publish(Func<T> valueFactory, long sizeBytes, bool coalesceLatest)
    {
        ArgumentExceptionHelper.ThrowIfNull(valueFactory);
        ValidateSize(sizeBytes);
        Subscription[] subscriptions;

        lock (_gate)
        {
            if (_stopped)
            {
                return ObserverNotificationPublishResult.Stopped;
            }

            subscriptions = CopySubscriptions();
        }

        var result = ObserverNotificationPublishResult.Stopped;
        for (var i = 0; i < subscriptions.Length; i++)
        {
            var mode = coalesceLatest && subscriptions[i].Options.OverflowMode == ObserverNotificationOverflowMode.CoalesceLatest
                ? ObserverNotificationOverflowMode.CoalesceLatest
                : ObserverNotificationOverflowMode.Disconnect;
            var subscriptionResult = subscriptions[i].Publish(valueFactory, sizeBytes, mode);
            if (subscriptionResult > result)
            {
                result = subscriptionResult;
            }
        }

        return result;
    }

    /// <summary>Publishes a data notification through an async per-observer value factory.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <param name="coalesceLatest">A value indicating whether overflow may replace queued data with the newest value.</param>
    /// <returns>The aggregate publication result.</returns>
    private ObserverNotificationPublishResult Publish(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes, bool coalesceLatest)
    {
        ArgumentExceptionHelper.ThrowIfNull(valueFactory);
        ValidateSize(sizeBytes);
        Subscription[] subscriptions;

        lock (_gate)
        {
            if (_stopped)
            {
                return ObserverNotificationPublishResult.Stopped;
            }

            subscriptions = CopySubscriptions();
        }

        var result = ObserverNotificationPublishResult.Stopped;
        for (var i = 0; i < subscriptions.Length; i++)
        {
            var mode = coalesceLatest && subscriptions[i].Options.OverflowMode == ObserverNotificationOverflowMode.CoalesceLatest
                ? ObserverNotificationOverflowMode.CoalesceLatest
                : ObserverNotificationOverflowMode.Disconnect;
            var subscriptionResult = subscriptions[i].Publish(valueFactory, sizeBytes, mode);
            if (subscriptionResult > result)
            {
                result = subscriptionResult;
            }
        }

        return result;
    }

    /// <summary>Queues a data notification through a per-observer value factory without scheduling drains inline.</summary>
    /// <param name="valueFactory">The value factory invoked for each observer callback.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <param name="coalesceLatest">A value indicating whether overflow may replace queued data with the newest value.</param>
    /// <param name="schedules">The scheduling callbacks to run after leaving a caller-owned lock.</param>
    /// <returns>The aggregate publication result before deferred scheduling failures are observed.</returns>
    private ObserverNotificationPublishResult PublishDeferred(
        Func<CancellationToken, ValueTask<T>> valueFactory,
        long sizeBytes,
        bool coalesceLatest,
        List<Action> schedules)
    {
        ArgumentExceptionHelper.ThrowIfNull(valueFactory);
        ArgumentExceptionHelper.ThrowIfNull(schedules);
        ValidateSize(sizeBytes);
        Subscription[] subscriptions;

        lock (_gate)
        {
            if (_stopped)
            {
                return ObserverNotificationPublishResult.Stopped;
            }

            subscriptions = CopySubscriptions();
        }

        var result = ObserverNotificationPublishResult.Stopped;
        for (var i = 0; i < subscriptions.Length; i++)
        {
            var mode = coalesceLatest && subscriptions[i].Options.OverflowMode == ObserverNotificationOverflowMode.CoalesceLatest
                ? ObserverNotificationOverflowMode.CoalesceLatest
                : ObserverNotificationOverflowMode.Disconnect;
            var subscriptionResult = subscriptions[i].PublishDeferred(valueFactory, sizeBytes, mode, schedules);
            if (subscriptionResult > result)
            {
                result = subscriptionResult;
            }
        }

        return result;
    }
}
