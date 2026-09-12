// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Dispatches bounded, isolated observer notifications for one logical stream.</summary>
/// <typeparam name="T">The notification value type.</typeparam>
/// <remarks>
/// The owning stream lane must serialize publications and terminal signals to establish a common order for every
/// subscription. Each subscription drains independently; an observer callback cannot hold up another subscriber's queue.
/// </remarks>
internal sealed class ObserverNotificationDispatcher<T> : IDisposable
{
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

    /// <summary>Publishes an event notification to active subscriptions.</summary>
    /// <param name="value">The event value.</param>
    /// <param name="sizeBytes">The estimated byte size.</param>
    /// <returns>The aggregate publication result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ObserverNotificationPublishResult PublishEvent(T value, long sizeBytes) =>
        Publish(value, sizeBytes, false);

    /// <summary>Subscribes an observer with a bounded notification queue.</summary>
    /// <param name="observer">The observer receiving serialized callbacks.</param>
    /// <param name="options">The queue options.</param>
    /// <returns>The subscription handle.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is invalid or the dispatcher has stopped.</exception>
    /// <exception cref="ObjectDisposedException">The dispatcher has been disposed.</exception>
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
                throw new InvalidOperationException("The observer notification dispatcher has already stopped.");
            }

            _subscriptions.Add(subscription);
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

    /// <summary>Stores one queued observer notification.</summary>
    private readonly record struct Notification
    {
        /// <summary>Stores the value for a data notification.</summary>
        [AllowNull]
        private readonly T _value;

        /// <summary>Stores the error for an error notification.</summary>
        private readonly Exception? _error;

        /// <summary>Initializes a new instance of the <see cref="Notification"/> struct.</summary>
        /// <param name="value">The data value.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        private Notification(T value, long sizeBytes)
        {
            _value = value;
            SizeBytes = sizeBytes;
            IsData = true;
        }

        /// <summary>Initializes a new instance of the <see cref="Notification"/> struct.</summary>
        /// <param name="error">The optional terminal error.</param>
        private Notification(Exception? error)
        {
            _value = default;
            _error = error;
            SizeBytes = 0;
            IsData = false;
        }

        /// <summary>Gets the data byte size.</summary>
        internal long SizeBytes { get; }

        /// <summary>Gets whether this notification carries a data value.</summary>
        private bool IsData { get; }

        /// <summary>Creates a data notification.</summary>
        /// <param name="value">The data value.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <returns>The notification.</returns>
        internal static Notification Next(T value, long sizeBytes) => new(value, sizeBytes);

        /// <summary>Creates a completion notification.</summary>
        /// <returns>The notification.</returns>
        internal static Notification Completed() => new(null);

        /// <summary>Creates an error notification.</summary>
        /// <param name="error">The terminal error.</param>
        /// <returns>The notification.</returns>
        internal static Notification Error(Exception error) => new(error);

        /// <summary>Invokes this notification on the supplied observer.</summary>
        /// <param name="observer">The observer to notify.</param>
        /// <returns><see langword="true"/> when the subscription can continue.</returns>
        internal bool Invoke(IObserver<T> observer)
        {
            if (IsData)
            {
                observer.OnNext(_value);
                return true;
            }

            if (_error is null)
            {
                observer.OnCompleted();
                return false;
            }

            observer.OnError(_error);
            return false;
        }
    }

    /// <summary>Drains one subscription queue.</summary>
    /// <param name="subscription">The subscription to drain.</param>
    private sealed class DrainWorkItem(Subscription subscription) : IWorkItem
    {
        /// <summary>Stores the subscription to drain.</summary>
        private readonly Subscription _subscription = subscription;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _subscription.Drain();
    }

    /// <summary>Represents one isolated observer subscription.</summary>
    private sealed class Subscription : IDisposable
    {
        /// <summary>Protects this subscription queue and lifecycle.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores the owning dispatcher.</summary>
        private readonly ObserverNotificationDispatcher<T> _owner;

        /// <summary>Stores the observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Stores queued data notifications.</summary>
        private readonly List<Notification> _queue = [];

        /// <summary>Tracks queued data bytes.</summary>
        private long _bytes;

        /// <summary>Stores a pending terminal notification outside the bounded data queue.</summary>
        private Notification _terminalNotification;

        /// <summary>Tracks whether drain work has been scheduled.</summary>
        private int _scheduled;

        /// <summary>Tracks whether this subscription has reached a terminal state.</summary>
        private bool _terminalQueued;

        /// <summary>Tracks whether this subscription has been disposed.</summary>
        private bool _disposed;

        /// <summary>Initializes a new instance of the <see cref="Subscription"/> class.</summary>
        /// <param name="owner">The owning dispatcher.</param>
        /// <param name="observer">The observer receiving notifications.</param>
        /// <param name="options">The subscription options.</param>
        internal Subscription(
            ObserverNotificationDispatcher<T> owner,
            IObserver<T> observer,
            ObserverNotificationSubscriptionOptions options)
        {
            _owner = owner;
            _observer = observer;
            Options = options;
        }

        /// <summary>Gets the subscription options.</summary>
        internal ObserverNotificationSubscriptionOptions Options { get; }

        /// <inheritdoc />
        public void Dispose()
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                _terminalQueued = true;
                Volatile.Write(ref _scheduled, 0);
                ClearQueuedNotifications();
            }

            _owner.Forget(this);
        }

        /// <summary>Publishes one data notification.</summary>
        /// <param name="value">The value.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <param name="mode">The overflow behavior.</param>
        /// <returns>The publication result.</returns>
        internal ObserverNotificationPublishResult Publish(T value, long sizeBytes, ObserverNotificationOverflowMode mode)
        {
            var result = ObserverNotificationPublishResult.Queued;
            var schedule = false;

            lock (_gate)
            {
                if (!CanAcceptData())
                {
                    return ObserverNotificationPublishResult.Stopped;
                }

                if (CanFit(sizeBytes))
                {
                    EnqueueData(value, sizeBytes);
                }
                else if (mode == ObserverNotificationOverflowMode.CoalesceLatest && CanCoalesce(sizeBytes))
                {
                    Coalesce(value, sizeBytes);
                    result = ObserverNotificationPublishResult.Coalesced;
                }
                else
                {
                    QueueOverflowTerminal(sizeBytes);
                    result = ObserverNotificationPublishResult.Disconnected;
                }

                schedule = TryMarkScheduled();
            }

            if (!schedule)
            {
                return result;
            }

            return ScheduleDrain() ? result : HandleScheduleFailure();
        }

        /// <summary>Publishes a terminal notification.</summary>
        /// <param name="notification">The terminal notification.</param>
        /// <returns>The publication result.</returns>
        internal ObserverNotificationPublishResult PublishTerminal(Notification notification)
        {
            var schedule = false;

            lock (_gate)
            {
                if (_disposed || _terminalQueued)
                {
                    return ObserverNotificationPublishResult.Stopped;
                }

                _terminalQueued = true;
                _terminalNotification = notification;
                schedule = TryMarkScheduled();
            }

            if (!schedule)
            {
                return ObserverNotificationPublishResult.Queued;
            }

            return ScheduleDrain() ? ObserverNotificationPublishResult.Queued : HandleScheduleFailure();
        }

        /// <summary>Drains queued notifications serially.</summary>
        internal void Drain()
        {
            while (TryTakeNotification(out var notification))
            {
                if (Invoke(notification))
                {
                    continue;
                }

                Dispose();
                return;
            }
        }

        /// <summary>Determines whether the subscription accepts data notifications.</summary>
        /// <returns><see langword="true"/> when data can be queued.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CanAcceptData() => !_disposed && !_terminalQueued;

        /// <summary>Determines whether a data notification fits the queue.</summary>
        /// <param name="sizeBytes">The incoming byte size.</param>
        /// <returns><see langword="true"/> when the notification fits.</returns>
        private bool CanFit(long sizeBytes) =>
            _queue.Count < Options.Capacity && sizeBytes <= Options.CapacityBytes - _bytes;

        /// <summary>Determines whether queued state can be replaced by the incoming value.</summary>
        /// <param name="sizeBytes">The incoming byte size.</param>
        /// <returns><see langword="true"/> when coalescing can fit.</returns>
        private bool CanCoalesce(long sizeBytes) => sizeBytes <= Options.CapacityBytes;

        /// <summary>Replaces queued data with the newest state notification.</summary>
        /// <param name="value">The newest value.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        private void Coalesce(T value, long sizeBytes)
        {
            _queue.Clear();
            _bytes = 0;
            EnqueueData(value, sizeBytes);
        }

        /// <summary>Adds one data notification.</summary>
        /// <param name="value">The value.</param>
        /// <param name="sizeBytes">The byte size.</param>
        private void EnqueueData(T value, long sizeBytes)
        {
            _queue.Add(Notification.Next(value, sizeBytes));
            _bytes += sizeBytes;
        }

        /// <summary>Queues a terminal overflow error without exceeding the bounded data queue.</summary>
        /// <param name="sizeBytes">The incoming notification byte size.</param>
        private void QueueOverflowTerminal(long sizeBytes)
        {
            _terminalQueued = true;
            _terminalNotification = Notification.Error(new ObserverNotificationOverflowException(Options.Capacity, Options.CapacityBytes, sizeBytes));
        }

        /// <summary>Clears all pending notifications.</summary>
        private void ClearQueuedNotifications()
        {
            _queue.Clear();
            _bytes = 0;
            _terminalNotification = default;
        }

        /// <summary>Marks the subscription as having scheduled drain work.</summary>
        /// <returns><see langword="true"/> when new work must be scheduled.</returns>
        private bool TryMarkScheduled()
        {
            if (Volatile.Read(ref _scheduled) != 0)
            {
                return false;
            }

            Volatile.Write(ref _scheduled, 1);
            return true;
        }

        /// <summary>Takes the next queued notification.</summary>
        /// <param name="notification">The removed notification.</param>
        /// <returns><see langword="true"/> when a notification was available.</returns>
        private bool TryTakeNotification(out Notification notification)
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    Volatile.Write(ref _scheduled, 0);
                    notification = default;
                    return false;
                }

                if (_queue.Count > 0)
                {
                    notification = _queue[0];
                    _queue.RemoveAt(0);
                    _bytes -= notification.SizeBytes;
                    return true;
                }

                if (_terminalQueued)
                {
                    notification = _terminalNotification;
                    _terminalNotification = default;
                    return true;
                }

                Volatile.Write(ref _scheduled, 0);
                notification = default;
                return false;
            }
        }

        /// <summary>Invokes a notification and contains observer/reporting failures.</summary>
        /// <param name="notification">The notification to invoke.</param>
        /// <returns><see langword="true"/> when the subscription can continue.</returns>
        private bool Invoke(Notification notification)
        {
            try
            {
                return notification.Invoke(_observer);
            }
            catch (Exception exception)
            {
                _owner.ReportFault(exception);
                return false;
            }
        }

        /// <summary>Schedules this subscription for draining.</summary>
        /// <returns><see langword="true"/> when scheduling succeeded.</returns>
        private bool ScheduleDrain()
        {
            try
            {
                _owner._scheduler.Schedule(new DrainWorkItem(this));
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>Recovers subscription state after scheduler rejection.</summary>
        /// <returns>The scheduler rejection publication result.</returns>
        private ObserverNotificationPublishResult HandleScheduleFailure()
        {
            lock (_gate)
            {
                Volatile.Write(ref _scheduled, 0);
                _disposed = true;
                _terminalQueued = true;
                ClearQueuedNotifications();
            }

            _owner.Forget(this);
            return ObserverNotificationPublishResult.SchedulerRejected;
        }
    }
}
