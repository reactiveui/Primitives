// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains per-subscription notification drain mechanics.</summary>
internal sealed partial class ObserverNotificationDispatcher<T>
{
    /// <summary>Stores one queued observer notification.</summary>
    private readonly record struct Notification
    {
        /// <summary>Stores the value for a data notification.</summary>
        [AllowNull]
        private readonly T _value;

        /// <summary>Stores the error for an error notification.</summary>
        private readonly Exception? _error;

        /// <summary>Stores the value factory for a data notification.</summary>
        private readonly Func<T>? _valueFactory;

        /// <summary>Stores the async value factory for a data notification.</summary>
        private readonly Func<CancellationToken, ValueTask<T>>? _asyncValueFactory;

        /// <summary>Initializes a new instance of the <see cref="Notification"/> struct.</summary>
        /// <param name="value">The data value.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        private Notification(T value, long sizeBytes)
        {
            _value = value;
            _error = null;
            _valueFactory = null;
            _asyncValueFactory = null;
            SizeBytes = sizeBytes;
            IsData = true;
        }

        /// <summary>Initializes a new instance of the <see cref="Notification"/> struct.</summary>
        /// <param name="valueFactory">The data value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        private Notification(Func<T> valueFactory, long sizeBytes)
        {
            _value = default;
            _error = null;
            _valueFactory = valueFactory;
            _asyncValueFactory = null;
            SizeBytes = sizeBytes;
            IsData = true;
        }

        /// <summary>Initializes a new instance of the <see cref="Notification"/> struct.</summary>
        /// <param name="valueFactory">The async data value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        private Notification(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes)
        {
            _value = default;
            _error = null;
            _valueFactory = null;
            _asyncValueFactory = valueFactory;
            SizeBytes = sizeBytes;
            IsData = true;
        }

        /// <summary>Initializes a new instance of the <see cref="Notification"/> struct.</summary>
        /// <param name="error">The optional terminal error.</param>
        private Notification(Exception? error)
        {
            _value = default;
            _error = error;
            _valueFactory = null;
            _asyncValueFactory = null;
            SizeBytes = 0;
            IsData = false;
        }

        /// <summary>Gets the data byte size.</summary>
        internal long SizeBytes { get; }

        /// <summary>Gets whether this notification carries a data value.</summary>
        internal bool IsData { get; }

        /// <summary>Creates a data notification.</summary>
        /// <param name="value">The data value.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <returns>The notification.</returns>
        internal static Notification Next(T value, long sizeBytes) => new(value, sizeBytes);

        /// <summary>Creates a factory-backed data notification.</summary>
        /// <param name="valueFactory">The data value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <returns>The notification.</returns>
        internal static Notification Next(Func<T> valueFactory, long sizeBytes) => new(valueFactory, sizeBytes);

        /// <summary>Creates an async factory-backed data notification.</summary>
        /// <param name="valueFactory">The async data value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <returns>The notification.</returns>
        internal static Notification Next(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes) => new(valueFactory, sizeBytes);

        /// <summary>Creates a completion notification.</summary>
        /// <returns>The notification.</returns>
        internal static Notification Completed() => new(null);

        /// <summary>Creates an error notification.</summary>
        /// <param name="error">The terminal error.</param>
        /// <returns>The notification.</returns>
        internal static Notification Error(Exception error) => new(error);

        /// <summary>Creates the notification to deliver after asynchronous data materialization.</summary>
        /// <param name="cancellationToken">The subscription cancellation token.</param>
        /// <returns>The materialized data notification or the unchanged terminal notification.</returns>
        internal async ValueTask<Notification> MaterializeAsync(CancellationToken cancellationToken) =>
            IsData ? Next(await GetValueAsync(cancellationToken).ConfigureAwait(false), SizeBytes) : this;

        /// <summary>Delivers a materialized notification to the supplied observer.</summary>
        /// <param name="observer">The observer to notify.</param>
        /// <returns>Whether the subscription can continue.</returns>
        internal bool Invoke(IObserver<T> observer)
        {
            if (!IsData)
            {
                return InvokeTerminal(observer);
            }

            observer.OnNext(_value);
            return true;
        }

        /// <summary>Materializes this notification's data value.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The materialized data value.</returns>
        private async ValueTask<T> GetValueAsync(CancellationToken cancellationToken)
        {
            if (_asyncValueFactory is not null)
            {
                return await _asyncValueFactory(cancellationToken).ConfigureAwait(false);
            }

            return _valueFactory is null ? _value : _valueFactory();
        }

        /// <summary>Invokes this terminal notification on the supplied observer.</summary>
        /// <param name="observer">The observer to notify.</param>
        /// <returns><see langword="false"/> because terminal notifications stop the subscription.</returns>
        private bool InvokeTerminal(IObserver<T> observer)
        {
            if (_error is null)
            {
                observer.OnCompleted();
            }
            else
            {
                observer.OnError(_error);
            }

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
        public void Execute() => _ = _subscription.DrainAsync();
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

        /// <summary>Cancels data materialization when this subscription is disposed.</summary>
        private readonly CancellationTokenSource _disposeCancellation = new();

        /// <summary>Retains the cancellation token independently of source disposal.</summary>
        private readonly CancellationToken _disposeToken;

        /// <summary>Stores queued data notifications.</summary>
        private readonly List<Notification> _queue = [];

        /// <summary>Tracks queued data bytes.</summary>
        private long _bytes;

        /// <summary>Tracks data notifications being materialized or delivered.</summary>
        private int _inflightCount;

        /// <summary>Tracks bytes retained by in-flight data notifications.</summary>
        private long _inflightBytes;

        /// <summary>Stores a pending terminal notification outside the bounded data queue.</summary>
        private Notification _terminalNotification;

        /// <summary>Tracks whether drain work has been scheduled.</summary>
        private int _scheduled;

        /// <summary>Tracks whether this subscription has reached a terminal state.</summary>
        private bool _terminalQueued;

        /// <summary>Tracks whether this subscription has been disposed.</summary>
        private bool _disposed;

        /// <summary>Tracks whether subscription cancellation has completed.</summary>
        private bool _disposeCancellationCompleted;

        /// <summary>Tracks whether the subscription cancellation source has been disposed.</summary>
        private bool _disposeCancellationDisposed;

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
            _disposeToken = _disposeCancellation.Token;
            Options = options;
        }

        /// <summary>Gets the subscription options.</summary>
        internal ObserverNotificationSubscriptionOptions Options { get; }

        /// <inheritdoc />
        public void Dispose()
        {
            if (!MarkDisposed())
            {
                return;
            }

            CancelDisposeCancellation();
            DisposeCancellationIfIdle();
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

        /// <summary>Publishes one factory-backed data notification.</summary>
        /// <param name="valueFactory">The value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <param name="mode">The overflow behavior.</param>
        /// <returns>The publication result.</returns>
        internal ObserverNotificationPublishResult Publish(Func<T> valueFactory, long sizeBytes, ObserverNotificationOverflowMode mode)
        {
            ArgumentExceptionHelper.ThrowIfNull(valueFactory);
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
                    _queue.Add(Notification.Next(valueFactory, sizeBytes));
                    _bytes += sizeBytes;
                }
                else if (mode == ObserverNotificationOverflowMode.CoalesceLatest && CanCoalesce(sizeBytes))
                {
                    _queue.Clear();
                    _bytes = 0;
                    _queue.Add(Notification.Next(valueFactory, sizeBytes));
                    _bytes += sizeBytes;
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

        /// <summary>Publishes one async factory-backed data notification.</summary>
        /// <param name="valueFactory">The value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <param name="mode">The overflow behavior.</param>
        /// <returns>The publication result.</returns>
        internal ObserverNotificationPublishResult Publish(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes, ObserverNotificationOverflowMode mode)
        {
            var schedules = new List<Action>(1);
            var result = PublishDeferred(valueFactory, sizeBytes, mode, schedules);
            RunSchedules(schedules);
            return result;
        }

        /// <summary>Queues one factory-backed data notification and defers drain scheduling.</summary>
        /// <param name="valueFactory">The value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        /// <param name="mode">The overflow behavior.</param>
        /// <param name="schedules">The scheduling callbacks to run after leaving a caller-owned lock.</param>
        /// <returns>The publication result before deferred scheduling failures are observed.</returns>
        internal ObserverNotificationPublishResult PublishDeferred(
            Func<CancellationToken, ValueTask<T>> valueFactory,
            long sizeBytes,
            ObserverNotificationOverflowMode mode,
            List<Action> schedules)
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
                    EnqueueData(valueFactory, sizeBytes);
                }
                else if (mode == ObserverNotificationOverflowMode.CoalesceLatest && CanCoalesce(sizeBytes))
                {
                    Coalesce(valueFactory, sizeBytes);
                    result = ObserverNotificationPublishResult.Coalesced;
                }
                else
                {
                    QueueOverflowTerminal(sizeBytes);
                    result = ObserverNotificationPublishResult.Disconnected;
                }

                schedule = TryMarkScheduled();
            }

            if (schedule)
            {
                schedules.Add(ScheduleInitial);
            }

            return result;
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

        /// <summary>Queues the initial replay before the subscription is visible to live publishers.</summary>
        /// <param name="value">The replayed value.</param>
        /// <param name="sizeBytes">The estimated byte size.</param>
        /// <returns><see langword="true"/> when drain work must be scheduled.</returns>
        internal bool QueueInitial(T value, long sizeBytes)
        {
            EnqueueData(value, sizeBytes);
            return TryMarkScheduled();
        }

        /// <summary>Queues the factory-backed initial replay before the subscription is visible to live publishers.</summary>
        /// <param name="valueFactory">The replayed value factory.</param>
        /// <param name="sizeBytes">The estimated byte size.</param>
        /// <returns><see langword="true"/> when drain work must be scheduled.</returns>
        internal bool QueueInitial(Func<T> valueFactory, long sizeBytes)
        {
            ArgumentExceptionHelper.ThrowIfNull(valueFactory);
            return QueueInitial(_ => new(valueFactory()), sizeBytes);
        }

        /// <summary>Queues the async factory-backed initial replay before the subscription is visible to live publishers.</summary>
        /// <param name="valueFactory">The replayed value factory.</param>
        /// <param name="sizeBytes">The estimated byte size.</param>
        /// <returns><see langword="true"/> when drain work must be scheduled.</returns>
        internal bool QueueInitial(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes)
        {
            EnqueueData(valueFactory, sizeBytes);
            return TryMarkScheduled();
        }

        /// <summary>Schedules initial replay drain work and detaches the subscription if scheduling fails.</summary>
        internal void ScheduleInitial()
        {
            if (ScheduleDrain())
            {
                return;
            }

            _ = HandleScheduleFailure();
        }

        /// <summary>Drains queued notifications serially.</summary>
        /// <returns>The asynchronous drain operation.</returns>
        internal async Task DrainAsync()
        {
            while (TryTakeNotification(out var notification))
            {
                var keepSubscription = false;
                try
                {
                    keepSubscription = await InvokeAsync(notification).ConfigureAwait(false);
                }
                finally
                {
                    ReleaseInFlight(notification);
                }

                if (keepSubscription)
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
            _queue.Count + _inflightCount < Options.Capacity && sizeBytes <= Options.CapacityBytes - _bytes - _inflightBytes;

        /// <summary>Determines whether queued state can be replaced by the incoming value.</summary>
        /// <param name="sizeBytes">The incoming byte size.</param>
        /// <returns><see langword="true"/> when coalescing can fit.</returns>
        private bool CanCoalesce(long sizeBytes) =>
            _inflightCount < Options.Capacity && sizeBytes <= Options.CapacityBytes - _inflightBytes;

        /// <summary>Determines whether this subscription can invoke the observer after materialization.</summary>
        /// <returns><see langword="true"/> when callbacks can still be delivered.</returns>
        private bool CanNotifyObserver()
        {
            lock (_gate)
            {
                return !_disposed;
            }
        }

        /// <summary>Marks this subscription disposed and clears queued owned notifications.</summary>
        /// <returns><see langword="true"/> when the caller should cancel owned materialization work.</returns>
        private bool MarkDisposed()
        {
            lock (_gate)
            {
                if (_disposed)
                {
                    return false;
                }

                _disposed = true;
                _terminalQueued = true;
                Volatile.Write(ref _scheduled, 0);
                ClearQueuedNotifications();
                return true;
            }
        }

        /// <summary>Cancels the subscription materialization token without letting callbacks escape.</summary>
        private void CancelDisposeCancellation()
        {
            try
            {
                _disposeCancellation.Cancel();
            }
            catch (Exception exception)
            {
                _owner.ReportFault(exception);
            }
            finally
            {
                lock (_gate)
                {
                    _disposeCancellationCompleted = true;
                }
            }
        }

        /// <summary>Disposes the cancellation source once no materializer can still observe its token.</summary>
        private void DisposeCancellationIfIdle()
        {
            var shouldDispose = false;
            lock (_gate)
            {
                if (_disposed && _disposeCancellationCompleted && _inflightCount == 0 && !_disposeCancellationDisposed)
                {
                    _disposeCancellationDisposed = true;
                    shouldDispose = true;
                }
            }

            if (!shouldDispose)
            {
                return;
            }

            _disposeCancellation.Dispose();
        }

        /// <summary>Releases retained in-flight accounting for a data notification.</summary>
        /// <param name="notification">The notification that finished materialization or delivery.</param>
        private void ReleaseInFlight(Notification notification)
        {
            if (!notification.IsData)
            {
                return;
            }

            lock (_gate)
            {
                _inflightCount--;
                _inflightBytes -= notification.SizeBytes;
            }

            DisposeCancellationIfIdle();
        }

        /// <summary>Replaces queued data with the newest state notification.</summary>
        /// <param name="value">The newest value.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        private void Coalesce(T value, long sizeBytes)
        {
            _queue.Clear();
            _bytes = 0;
            EnqueueData(value, sizeBytes);
        }

        /// <summary>Replaces queued data with the newest async factory-backed state notification.</summary>
        /// <param name="valueFactory">The newest value factory.</param>
        /// <param name="sizeBytes">The notification byte size.</param>
        private void Coalesce(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes)
        {
            _queue.Clear();
            _bytes = 0;
            EnqueueData(valueFactory, sizeBytes);
        }

        /// <summary>Adds one data notification.</summary>
        /// <param name="value">The value.</param>
        /// <param name="sizeBytes">The byte size.</param>
        private void EnqueueData(T value, long sizeBytes)
        {
            _queue.Add(Notification.Next(value, sizeBytes));
            _bytes += sizeBytes;
        }

        /// <summary>Adds one async factory-backed data notification.</summary>
        /// <param name="valueFactory">The value factory.</param>
        /// <param name="sizeBytes">The byte size.</param>
        private void EnqueueData(Func<CancellationToken, ValueTask<T>> valueFactory, long sizeBytes)
        {
            _queue.Add(Notification.Next(valueFactory, sizeBytes));
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
                    _inflightCount++;
                    _inflightBytes += notification.SizeBytes;
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
        private async ValueTask<bool> InvokeAsync(Notification notification)
        {
            try
            {
                var materialized = await notification.MaterializeAsync(_disposeToken).ConfigureAwait(false);
                return CanNotifyObserver() && materialized.Invoke(_observer);
            }
            catch (OperationCanceledException) when (_disposeToken.IsCancellationRequested)
            {
                return false;
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
            Dispose();
            return ObserverNotificationPublishResult.SchedulerRejected;
        }
    }
}
