// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>
/// Dedicated signals for the scheduler/time operators, replacing the per-subscription
/// <c>Signal.Create(observer =&gt; ...)</c> closures. The current-thread variants follow the
/// <c>ExpireSignal</c>/<c>ProbeSignal</c> pattern: implement <see cref="IRequireCurrentThread{T}"/>
/// and schedule the subscription onto the current-thread sequencer when required.
/// </summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated signal for <c>Calm</c> (quiet-period debounce).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The quiet period.</param>
    /// <param name="scheduler">The sequencer used to schedule quiet-period timers.</param>
    private sealed class CalmSignal<T>(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler) : IRequireCurrentThread<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The quiet period.</summary>
        private readonly TimeSpan _dueTime = dueTime;

        /// <summary>The sequencer used to schedule quiet-period timers.</summary>
        private readonly ISequencer _scheduler = scheduler;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            CalmCoordinator<T> coordinator = new(_source, _dueTime, _scheduler);
            if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return coordinator.Run(observer);
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (subscription, coordinator, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.coordinator.Run(s.observer));
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }
    }

    /// <summary>Dedicated signal for <c>Shift</c> (delay each notification on a sequencer).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ShiftSignal<T> : IRequireCurrentThread<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The delay applied to each notification.</summary>
        private readonly TimeSpan _dueTime;

        /// <summary>The sequencer used to schedule delayed notifications.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Initializes a new instance of the <see cref="ShiftSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
        internal ShiftSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
        {
            _source = source;
            _dueTime = Sequencer.Normalize(dueTime);
            _scheduler = scheduler;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return RunCore(observer);
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (self: this, subscription, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.self.RunCore(s.observer));
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }

        /// <summary>Subscribes to the source and schedules each notification by the delay.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The disposable that cancels the source subscription and pending timers.</returns>
        private ShiftCoordinator<T> RunCore(IObserver<T> observer)
        {
            ShiftCoordinator<T> coordinator = new(_source, _dueTime, _scheduler, observer);
            return coordinator.Run();
        }
    }

    /// <summary>Coordinates delayed notification delivery with a single serialized timer.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The normalized delay applied to each notification.</param>
    /// <param name="sequencer">The sequencer used to schedule delayed notifications.</param>
    /// <param name="observer">The downstream observer.</param>
    private sealed class ShiftCoordinator<T>(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer, IObserver<T> observer) : IDisposable
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The normalized delay applied to each notification.</summary>
        private readonly TimeSpan _dueTime = dueTime;

        /// <summary>The sequencer used to schedule delayed notifications.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Serializes queue state and downstream callbacks.</summary>
        private readonly Lock _gate = new();

        /// <summary>Active source and timer resources.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>The single active timer slot.</summary>
        private readonly SingleReplaceableDisposable _timer = new();

        /// <summary>Queued delayed notifications in source order.</summary>
        private readonly Queue<DelayedNotification> _queue = [];

        /// <summary>A value indicating whether a timer or drain is active.</summary>
        private bool _timerActive;

        /// <summary>A value indicating whether the source has already signaled terminal notification.</summary>
        private bool _sourceStopped;

        /// <summary>A value indicating whether a terminal notification has been delivered.</summary>
        private bool _done;

        /// <summary>Tracks disposal.</summary>
        private int _disposed;

        /// <summary>The queued notification kind.</summary>
        private enum NotificationKind
        {
            /// <summary>A value notification.</summary>
            Next,

            /// <summary>An error notification.</summary>
            Error,

            /// <summary>A completion notification.</summary>
            Completed
        }

        /// <summary>Gets a value indicating whether the coordinator is disposed.</summary>
        private bool IsDisposed => Volatile.Read(ref _disposed) != 0;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            lock (_gate)
            {
                _timer.Dispose();
                _subscriptions.Dispose();
            }
        }

        /// <summary>Starts the delayed notification coordinator.</summary>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ShiftCoordinator<T> Run()
        {
            _subscriptions.Add(_timer);
            _subscriptions.Add(_source.Subscribe(OnNext, OnError, OnCompleted));
            return this;
        }

        /// <summary>Queues a source value for delayed delivery.</summary>
        /// <param name="value">The source value.</param>
        private void OnNext(T value) => Enqueue(DelayedNotification.Next(value, DueAt()), false);

        /// <summary>Queues a source error for delayed delivery behind earlier values.</summary>
        /// <param name="error">The source error.</param>
        private void OnError(Exception error) => Enqueue(DelayedNotification.Failure(error, DueAt()), true);

        /// <summary>Queues source completion for delayed delivery behind earlier values.</summary>
        private void OnCompleted() => Enqueue(DelayedNotification.Completed(DueAt()), true);

        /// <summary>Computes the due time for the current source notification.</summary>
        /// <returns>The absolute due time.</returns>
        private DateTimeOffset DueAt() => _sequencer.Now + _dueTime;

        /// <summary>Queues a notification and starts the timer when this item owns the drain.</summary>
        /// <param name="notification">The delayed notification.</param>
        /// <param name="isTerminal">A value indicating whether the notification stops the source.</param>
        private void Enqueue(DelayedNotification notification, bool isTerminal)
        {
            TimeSpan delay = default;
            var shouldSchedule = false;
            lock (_gate)
            {
                if (IsDisposed || _done || _sourceStopped)
                {
                    return;
                }

                if (isTerminal)
                {
                    _sourceStopped = true;
                }

                _queue.Enqueue(notification);
                if (!_timerActive)
                {
                    _timerActive = true;
                    delay = DelayUntil(notification.DueAt);
                    shouldSchedule = true;
                }
            }

            if (!shouldSchedule)
            {
                return;
            }

            Schedule(delay);
        }

        /// <summary>Schedules the single active drain timer.</summary>
        /// <param name="delay">The delay before the drain should run.</param>
        private void Schedule(TimeSpan delay) => _timer.Create(_sequencer.Schedule(delay, Tick));

        /// <summary>Drains all due notifications in FIFO order.</summary>
        private void Tick()
        {
            while (true)
            {
                TimeSpan delay;
                var shouldReschedule = false;
                var terminal = false;
                lock (_gate)
                {
                    if (IsDisposed || _done)
                    {
                        _timerActive = false;
                        return;
                    }

                    if (_queue.Count == 0)
                    {
                        _timerActive = false;
                        return;
                    }

                    delay = DelayUntil(_queue.Peek().DueAt);
                    if (delay > TimeSpan.Zero)
                    {
                        shouldReschedule = true;
                    }
                    else
                    {
                        terminal = Deliver(_queue.Dequeue());
                    }
                }

                if (shouldReschedule)
                {
                    Schedule(delay);
                    return;
                }

                if (terminal)
                {
                    Dispose();
                    return;
                }
            }
        }

        /// <summary>Forwards a queued notification while the caller holds the gate.</summary>
        /// <param name="notification">The queued notification.</param>
        /// <returns><see langword="true"/> when a terminal notification was delivered.</returns>
        private bool Deliver(DelayedNotification notification)
        {
            switch (notification.Kind)
            {
                case NotificationKind.Next:
                    {
                        _observer.OnNext(notification.Value!);
                        return false;
                    }

                case NotificationKind.Error:
                    {
                        _done = true;
                        _observer.OnError(notification.Error!);
                        return true;
                    }

                default:
                    {
                        _done = true;
                        _observer.OnCompleted();
                        return true;
                    }
            }
        }

        /// <summary>Computes the remaining delay for an absolute due time.</summary>
        /// <param name="dueAt">The absolute due time.</param>
        /// <returns>The remaining non-negative delay.</returns>
        private TimeSpan DelayUntil(DateTimeOffset dueAt) => Sequencer.Normalize(dueAt - _sequencer.Now);

        /// <summary>A delayed source notification.</summary>
        private sealed class DelayedNotification
        {
            /// <summary>Initializes a new instance of the <see cref="DelayedNotification"/> struct.</summary>
            /// <param name="kind">The notification kind.</param>
            /// <param name="value">The notification value.</param>
            /// <param name="error">The notification error.</param>
            /// <param name="dueAt">The notification due time.</param>
            private DelayedNotification(NotificationKind kind, T? value, Exception? error, DateTimeOffset dueAt)
            {
                Kind = kind;
                Value = value;
                Error = error;
                DueAt = dueAt;
            }

            /// <summary>Gets the notification kind.</summary>
            public NotificationKind Kind { get; }

            /// <summary>Gets the notification value.</summary>
            public T? Value { get; }

            /// <summary>Gets the notification error.</summary>
            public Exception? Error { get; }

            /// <summary>Gets the notification due time.</summary>
            public DateTimeOffset DueAt { get; }

            /// <summary>Creates a value notification.</summary>
            /// <param name="value">The notification value.</param>
            /// <param name="dueAt">The notification due time.</param>
            /// <returns>The delayed notification.</returns>
            public static DelayedNotification Next(T value, DateTimeOffset dueAt) =>
                new(NotificationKind.Next, value, null, dueAt);

            /// <summary>Creates an error notification.</summary>
            /// <param name="error">The notification error.</param>
            /// <param name="dueAt">The notification due time.</param>
            /// <returns>The delayed notification.</returns>
            public static DelayedNotification Failure(Exception error, DateTimeOffset dueAt) =>
                new(NotificationKind.Error, default, error, dueAt);

            /// <summary>Creates a completion notification.</summary>
            /// <param name="dueAt">The notification due time.</param>
            /// <returns>The delayed notification.</returns>
            public static DelayedNotification Completed(DateTimeOffset dueAt) =>
                new(NotificationKind.Completed, default, null, dueAt);
        }
    }

    /// <summary>Dedicated signal for absolute <c>Shift</c> overloads.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The absolute time at which notifications may be forwarded.</param>
    /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
    private sealed class AbsoluteShiftSignal<T>(IObservable<T> source, DateTimeOffset dueTime, ISequencer scheduler) : IRequireCurrentThread<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The absolute time at which notifications may be forwarded.</summary>
        private readonly DateTimeOffset _dueTime = dueTime;

        /// <summary>The sequencer used to schedule delayed notifications.</summary>
        private readonly ISequencer _scheduler = scheduler;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var dueTime = Sequencer.Normalize(_dueTime - _scheduler.Now);
            return _source is RangeSignal range && typeof(T) == typeof(int)
                ? new ShiftedRangeSignal<T>(range, dueTime, _scheduler).Subscribe(observer)
                : new ShiftSignal<T>(_source, dueTime, _scheduler).Subscribe(observer);
        }
    }

    /// <summary>Dedicated signal for <c>SubscribeOn</c> (defer subscription to a sequencer).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">The sequencer the subscription is scheduled onto.</param>
    private sealed class SubscribeOnSignal<T>(IObservable<T> source, ISequencer scheduler) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The sequencer the subscription is scheduled onto.</summary>
        private readonly ISequencer _scheduler = scheduler;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            SingleReplaceableDisposable subscription = new();
            var scheduled = _scheduler.Schedule(
                (self: this, subscription, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.self._source.Subscribe(s.observer));
                    return EmptyDisposable.Instance;
                });
            return new MultipleDisposable(scheduled, subscription);
        }
    }

    /// <summary>Dedicated signal for <c>DelayStart</c> (delay the subscription itself).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay before subscribing to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
    private sealed class DelayStartSignal<T>(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The delay before subscribing to the source.</summary>
        private readonly TimeSpan _dueTime = dueTime;

        /// <summary>The sequencer used to schedule the delayed subscription.</summary>
        private readonly ISequencer _scheduler = scheduler;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            MultipleDisposable pocket = [];
            pocket.Add(_scheduler.Schedule(
                (self: this, pocket, observer),
                Sequencer.Normalize(_dueTime),
                static (_, s) =>
                {
                    s.pocket.Add(s.self._source.Subscribe(s.observer));
                    return EmptyDisposable.Instance;
                }));
            return pocket;
        }
    }

    /// <summary>Dedicated signal for absolute <c>DelayStart</c>/<c>DelaySubscription</c> overloads.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The absolute time at which to subscribe to the source.</param>
    /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
    private sealed class AbsoluteDelayStartSignal<T>(IObservable<T> source, DateTimeOffset dueTime, ISequencer scheduler) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The absolute time at which to subscribe to the source.</summary>
        private readonly DateTimeOffset _dueTime = dueTime;

        /// <summary>The sequencer used to schedule the delayed subscription.</summary>
        private readonly ISequencer _scheduler = scheduler;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var dueTime = Sequencer.Normalize(_dueTime - _scheduler.Now);
            return _source is RangeSignal range && typeof(T) == typeof(int)
                ? new ShiftedRangeSignal<T>(range, dueTime, _scheduler).Subscribe(observer)
                : new DelayStartSignal<T>(_source, dueTime, _scheduler).Subscribe(observer);
        }
    }

    /// <summary>Dedicated signal for absolute <c>Expire</c>/<c>Timeout</c> overloads.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The absolute timeout time.</param>
    /// <param name="scheduler">The sequencer used to schedule the timeout.</param>
    private sealed class AbsoluteExpireSignal<T>(IObservable<T> source, DateTimeOffset dueTime, ISequencer scheduler) : IRequireCurrentThread<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The absolute timeout time.</summary>
        private readonly DateTimeOffset _dueTime = dueTime;

        /// <summary>The sequencer used to schedule the timeout.</summary>
        private readonly ISequencer _scheduler = scheduler;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _scheduler == Sequencer.CurrentThread
            || (_source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread());

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var dueTime = Sequencer.Normalize(_dueTime - _scheduler.Now);
            return new ExpireSignal<T>(_source, dueTime, _scheduler).Subscribe(observer);
        }
    }

    /// <summary>Dedicated signal for <c>Reattempt</c> (retry on error).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="retryCount">The maximum number of retries after the initial subscription.</param>
    private sealed class ReattemptSignal<T>(IObservable<T> source, int retryCount) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The maximum number of retries after the initial subscription.</summary>
        private readonly int _retryCount = retryCount;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new ReattemptCoordinator<T>(_source, _retryCount, observer).Run();
        }
    }

    /// <summary>Coordinates retry-on-error resubscription for <c>Reattempt</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="retryCount">The maximum number of retries.</param>
    /// <param name="observer">The downstream observer.</param>
    private sealed class ReattemptCoordinator<T>(IObservable<T> source, int retryCount, IObserver<T> observer) : IDisposable
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The maximum number of retries.</summary>
        private readonly int _retryCount = retryCount;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Active subscriptions across retries.</summary>
        private readonly MultipleDisposable _pocket = [];

        /// <summary>The number of retries attempted so far.</summary>
        private int _attempts;

        /// <inheritdoc/>
        public void Dispose() => _pocket.Dispose();

        /// <summary>Starts the first subscription attempt.</summary>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ReattemptCoordinator<T> Run()
        {
            SubscribeNext();
            return this;
        }

        /// <summary>Subscribes to the source for the current attempt.</summary>
        private void SubscribeNext() =>
            _pocket.Add(_source.Subscribe(_observer.OnNext, OnError, _observer.OnCompleted));

        /// <summary>Retries the subscription, or forwards the error once retries are exhausted.</summary>
        /// <param name="error">The error raised by the source.</param>
        private void OnError(Exception error)
        {
            var attempt = _attempts;
            _attempts++;
            if (attempt < _retryCount)
            {
                SubscribeNext();
            }
            else
            {
                _observer.OnError(error);
            }
        }
    }
}
