// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Keeps emissions at least <paramref name="minimumUpdatePeriod"/> apart on <paramref name="scheduler"/>; completion waits for a deferred value, an error discards it.</summary>
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

    /// <summary>Delivers notifications on the scheduler and limits value emissions to the conflate interval.</summary>
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

        /// <summary>The notification queue and scheduled-drain bookkeeping shared with the drain loop.</summary>
        private readonly ScheduledDrainState<T> _state;

        /// <summary>The disposable tracking a scheduled deferred emission.</summary>
        private readonly MutableDisposable _updateScheduled = new();

        /// <summary>Wall-clock timestamp of the last emission forwarded downstream.</summary>
        private DateTimeOffset _lastUpdateTime = DateTimeOffset.MinValue;

        /// <summary>Set when an upstream OnCompleted arrives while a deferred emission is pending, so the completion fires once that emission lands.</summary>
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

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _state.EnqueueNext(value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _state.EnqueueError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
                            ForwardCompleted();
                            return;
                        }
                }
            }
        }

        /// <summary>Records the upstream subscription so <see cref="Dispose"/> can tear it down.</summary>
        /// <param name="subscription">The upstream subscription handle.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AttachSourceSubscription(IDisposable subscription) => _state.Attach(subscription);

        /// <summary>Emits a dequeued value immediately or defers it until the conflate interval ends.</summary>
        /// <param name="value">The value to forward.</param>
        internal void ProcessNext(T value)
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

        /// <summary>Forwards an error to downstream and terminates the sink.</summary>
        /// <param name="error">The error to forward.</param>
        internal void ForwardError(Exception error)
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

        /// <summary>Forwards completion, deferring it when a throttled emission is scheduled.</summary>
        internal void ForwardCompleted()
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

        /// <summary>Schedules a value for the end of the conflate interval.</summary>
        /// <param name="value">The value to emit when the interval elapses.</param>
        private void ScheduleDeferredEmission(T value) =>
            _updateScheduled.Disposable = _scheduler.Schedule(
                (Sink: this, Value: value),
                _lastUpdateTime + _minimumUpdatePeriod,
                static (_, state) =>
                {
                    state.Sink.EmitDeferred(state.Value);
                    return EmptyDisposable.Instance;
                });

        /// <summary>Emits a deferred value before forwarding any pending completion.</summary>
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

        /// <summary>Emits a value immediately and records the emission time.</summary>
        /// <param name="value">The value to emit.</param>
        private void EmitInline(T value)
        {
            _downstream.OnNext(value);
            lock (_gate)
            {
                _lastUpdateTime = _scheduler.Now;
            }
        }
    }
}
