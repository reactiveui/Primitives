// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Conflates an observable stream by delaying updates that occur within a minimum period.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="minimumUpdatePeriod">The minimum period between emissions.</param>
/// <param name="scheduler">The scheduler to run the conflation on.</param>
internal sealed class ConflateObservable<T>(
    IObservable<T> source,
    TimeSpan minimumUpdatePeriod,
    ISequencer scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(scheduler);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ConflateSink sink = new(observer, minimumUpdatePeriod, scheduler);
        sink.AttachSourceSubscription(source.Subscribe(sink));
        return sink;
    }

    /// <summary>
    /// Single observer that combines two previously-distinct concerns into one allocation:
    /// (1) marshals upstream notifications onto the scheduler thread — delegated to the shared
    /// <see cref="ScheduledDrainState{T}"/> FIFO queue and scheduled drain — and (2) applies the conflate
    /// time-window throttle to each <see cref="DrainNotificationKind.Next"/> notification. End-user-observable
    /// semantics are unchanged from the prior two-observer implementation.
    /// </summary>
    internal sealed class ConflateSink : IObserver<T>, IDisposable, IDrainTarget
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _downstream;

        /// <summary>The minimum period between emissions.</summary>
        private readonly TimeSpan _minimumUpdatePeriod;

        /// <summary>The scheduler to run the conflation on.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>The gate protecting the queue, throttle window, and downstream notification.</summary>
        private readonly Lock _gate = new();

        /// <summary>Shared queue / scheduled-drain machinery.</summary>
        private readonly ScheduledDrainState<T> _state;

        /// <summary>The disposable tracking a scheduled deferred emission.</summary>
        private readonly MutableDisposable _updateScheduled = new();

        /// <summary>Wall-clock timestamp of the last emission forwarded downstream.</summary>
        private DateTimeOffset _lastUpdateTime = DateTimeOffset.MinValue;

        /// <summary>Set to <see langword="true"/> when an upstream OnCompleted is queued but a deferred
        /// emission is still pending; the completion fires after that emission lands.</summary>
        private bool _completionRequested;

        /// <summary>Initializes a new instance of the <see cref="ConflateSink"/> class.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="minimumUpdatePeriod">The minimum period between emissions.</param>
        /// <param name="scheduler">The scheduler to run the conflation on.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Correctness",
            "SST2403:Do not let 'this' escape from a constructor",
            Justification =
                "_state is owned solely by this sink and only stores the back-reference, so 'this' never escapes construction.")]
        public ConflateSink(IObserver<T> downstream, TimeSpan minimumUpdatePeriod, ISequencer scheduler)
        {
            _downstream = downstream;
            _minimumUpdatePeriod = minimumUpdatePeriod;
            _scheduler = scheduler;
            _state = new(scheduler, this, _gate);
        }

        /// <summary>Records the upstream subscription so <see cref="Dispose"/> can tear it down.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        public void AttachSourceSubscription(IDisposable subscription) => _state.Attach(subscription);

        /// <inheritdoc/>
        public void OnNext(T value) => _state.EnqueueNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error) => _state.EnqueueError(error);

        /// <inheritdoc/>
        public void OnCompleted() => _state.EnqueueCompleted();

        /// <inheritdoc/>
        public void Dispose()
        {
            IDisposable? subscription;
            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                subscription = _state.BeginDisposeLocked();
                _updateScheduled.Dispose();
            }

            subscription?.Dispose();
        }

        /// <inheritdoc/>
        void IDrainTarget.Drain()
        {
            while (_state.TryDequeue(out var notification))
            {
                switch (notification.Kind)
                {
                    case DrainNotificationKind.Next:
                        {
                            ProcessNext(notification.Value);
                            break;
                        }

                    case DrainNotificationKind.Error:
                        {
                            ForwardError(notification.Error!);
                            return;
                        }

                    default:
                        {
                            // DrainNotificationKind has only three values; the discard arm absorbs
                            // Completed so the compiler sees an exhaustive switch.
                            ForwardCompleted();
                            return;
                        }
                }
            }
        }

        /// <summary>Applies the throttle-window decision to a dequeued value and either emits inline or
        /// schedules a deferred emission. The emission bodies live in covered helpers; only this
        /// race-guarded shell (whose already-done early-out is reachable only when a concurrent dispose
        /// flips the flag between the drain dequeue and this gate acquisition) is excluded.</summary>
        /// <param name="value">The value to forward.</param>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        private void ProcessNext(T value)
        {
            var currentUpdateTime = _scheduler.Now;
            bool scheduleRequired;

            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                scheduleRequired = currentUpdateTime - _lastUpdateTime < _minimumUpdatePeriod;
                if (scheduleRequired && _updateScheduled.Disposable is not null)
                {
                    _updateScheduled.Disposable.Dispose();
                    _updateScheduled.Disposable = null;
                }
            }

            if (scheduleRequired)
            {
                ScheduleDeferredEmission(value);
            }
            else
            {
                EmitInline(value);
            }
        }

        /// <summary>Schedules a deferred emission of <paramref name="value"/> at the end of the throttle window, forwarding a pending completion once it lands.</summary>
        /// <param name="value">The value to emit when the window elapses.</param>
        private void ScheduleDeferredEmission(T value) =>
            _updateScheduled.Disposable = _scheduler.Schedule(
                (Sink: this, Value: value),
                _lastUpdateTime + _minimumUpdatePeriod,
                static (_, state) =>
                {
                    state.Sink.EmitDeferred(state.Value);
                    return EmptyDisposable.Instance;
                });

        /// <summary>Emits a deferred value and forwards a pending completion once the value lands.</summary>
        /// <param name="value">The deferred value.</param>
        private void EmitDeferred(T value)
        {
            _downstream.OnNext(value);

            lock (_gate)
            {
                _lastUpdateTime = _scheduler.Now;
                _updateScheduled.Disposable = null;
                if (_completionRequested)
                {
                    _state.MarkDoneLocked();
                    _downstream.OnCompleted();
                }
            }
        }

        /// <summary>Emits <paramref name="value"/> immediately and records the emission time.</summary>
        /// <param name="value">The value to emit.</param>
        private void EmitInline(T value)
        {
            _downstream.OnNext(value);
            lock (_gate)
            {
                _lastUpdateTime = _scheduler.Now;
            }
        }

        /// <summary>Forwards an error to downstream and terminates the sink.</summary>
        /// <param name="error">The error to forward.</param>
        /// <remarks>The already-terminated early-out is reachable only when a concurrent dispose flips the
        /// flag between the drain dequeue and this gate acquisition; excluded as race-only.</remarks>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        private void ForwardError(Exception error)
        {
            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                _state.MarkDoneLocked();
                _updateScheduled.Dispose();
            }

            _downstream.OnError(error);
        }

        /// <summary>Forwards completion, deferring if a throttled emission is still scheduled.</summary>
        /// <remarks>The already-terminated early-out is reachable only when a concurrent dispose flips the
        /// flag between the drain dequeue and this gate acquisition; excluded as race-only.</remarks>
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
        private void ForwardCompleted()
        {
            lock (_gate)
            {
                if (_state.Done)
                {
                    return;
                }

                if (_updateScheduled.Disposable is not null)
                {
                    _completionRequested = true;
                    return;
                }

                _state.MarkDoneLocked();
            }

            _downstream.OnCompleted();
        }
    }
}
