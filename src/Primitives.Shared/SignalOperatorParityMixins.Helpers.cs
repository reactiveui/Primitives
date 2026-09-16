// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Private helper types for parity operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Emits all range values and completion from a scheduled batch.</summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable EmitShiftedRange<T>(IObserver<T> observer, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            observer.OnNext((T)(object)(range.Start + i));
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits all range values and completion from a scheduled batch.</summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="onNext">The next callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <param name="range">The source range.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable EmitShiftedRange<T>(Action<T> onNext, Action onCompleted, RangeSignal range)
    {
        for (var i = 0; i < range.Count; i++)
        {
            onNext((T)(object)(range.Start + i));
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits one value, then the source sequence.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The prepended value.</param>
    private sealed class PrependSignal<T>(IObservable<T> source, T value) : IInlineSignal<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The value emitted before source subscription.</summary>
        private readonly T _value = value;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            observer.OnNext(_value);
            return _source.Subscribe(observer);
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            onNext(_value);
            return _source.Subscribe(onNext, onError, onCompleted);
        }

        /// <summary>Gets the source observable for operator fusion.</summary>
        /// <returns>The source observable.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IObservable<T> GetSource() => _source;

        /// <summary>Gets the prepended value for operator fusion.</summary>
        /// <returns>The prepended value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal T GetValue() => _value;
    }

    /// <summary>Emits the values of an enumerable, then the source sequence.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="values">Values emitted before source subscription.</param>
    private sealed class StartWithEnumerableSignal<T>(IObservable<T> source, IEnumerable<T> values) : IInlineSignal<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>Values emitted before source subscription.</summary>
        private readonly IEnumerable<T> _values = values;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            foreach (var value in _values)
            {
                observer.OnNext(value);
            }

            return _source.Subscribe(observer);
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            foreach (var value in _values)
            {
                onNext(value);
            }

            return _source.Subscribe(onNext, onError, onCompleted);
        }
    }

    /// <summary>Fuses a single prepended value and a single appended value around a source subscription.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="prependValue">The prepended value.</param>
    /// <param name="appendValue">The appended value.</param>
    private sealed class PrependAppendSignal<T>(IObservable<T> source, T prependValue, T appendValue) : IInlineSignal<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The value emitted before source subscription.</summary>
        private readonly T _prependValue = prependValue;

        /// <summary>The value emitted after source completion.</summary>
        private readonly T _appendValue = appendValue;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            observer.OnNext(_prependValue);
            AppendWitness<T> sink = new(observer, _appendValue);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            onNext(_prependValue);
            AppendDelegateWitness<T> sink = new(onNext, onError, onCompleted, _appendValue);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Appends a single value after source completion.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The appended value.</param>
    private sealed class AppendSignal<T>(IObservable<T> source, T value) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The value emitted after source completion.</summary>
        private readonly T _value = value;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            AppendWitness<T> sink = new(observer, _value);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Emits a default value when the source completes without values.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">Value emitted for an empty source.</param>
    private sealed class DefaultIfEmptySignal<T>(IObservable<T> source, T defaultValue) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>Value emitted for an empty source.</summary>
        private readonly T _defaultValue = defaultValue;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            DefaultIfEmptyWitness<T> sink = new(observer, _defaultValue);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Projects a range's values into timestamped moments.</summary>
    /// <typeparam name="T">The range value type.</typeparam>
    /// <param name="range">The range source.</param>
    /// <param name="sequencer">The sequencer used to read timestamps.</param>
    private sealed class TimestampRangeSignal<T>(RangeSignal range, ISequencer sequencer) : IInlineSignal<Moment<T>>
    {
        /// <summary>The range source.</summary>
        private readonly RangeSignal _range = range;

        /// <summary>The sequencer used to read timestamps.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<Moment<T>> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            Emit(observer);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<Moment<T>> onNext, Action<Exception> onError, Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(onNext);

            Emit(onNext);
            onCompleted();
            return EmptyDisposable.Instance;
        }

        /// <summary>Emits timestamped range values.</summary>
        /// <param name="onNext">The next callback.</param>
        private void Emit(Action<Moment<T>> onNext)
        {
            if (_sequencer == Sequencer.Immediate)
            {
                var timestamp = _sequencer.Now;
                for (var i = 0; i < _range.Count; i++)
                {
                    onNext(new((T)(object)(_range.Start + i), timestamp));
                }

                return;
            }

            for (var i = 0; i < _range.Count; i++)
            {
                onNext(new((T)(object)(_range.Start + i), _sequencer.Now));
            }
        }

        /// <summary>Emits timestamped range values straight to an observer.</summary>
        /// <param name="observer">The downstream observer.</param>
        private void Emit(IObserver<Moment<T>> observer)
        {
            if (_sequencer == Sequencer.Immediate)
            {
                var timestamp = _sequencer.Now;
                for (var i = 0; i < _range.Count; i++)
                {
                    observer.OnNext(new((T)(object)(_range.Start + i), timestamp));
                }

                return;
            }

            for (var i = 0; i < _range.Count; i++)
            {
                observer.OnNext(new((T)(object)(_range.Start + i), _sequencer.Now));
            }
        }
    }

    /// <summary>Projects a range's values into interval-tagged values.</summary>
    /// <typeparam name="T">The range value type.</typeparam>
    /// <param name="range">The range source.</param>
    /// <param name="sequencer">The sequencer used to read timestamps.</param>
    private sealed class TimeIntervalRangeSignal<T>(RangeSignal range, ISequencer sequencer) : IInlineSignal<TimeInterval<T>>
    {
        /// <summary>The range source.</summary>
        private readonly RangeSignal _range = range;

        /// <summary>The sequencer used to read timestamps.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TimeInterval<T>> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            Emit(observer);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<TimeInterval<T>> onNext, Action<Exception> onError, Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(onNext);

            Emit(onNext);
            onCompleted();
            return EmptyDisposable.Instance;
        }

        /// <summary>Emits interval-tagged range values.</summary>
        /// <param name="onNext">The next callback.</param>
        private void Emit(Action<TimeInterval<T>> onNext)
        {
            if (_sequencer == Sequencer.Immediate)
            {
                for (var i = 0; i < _range.Count; i++)
                {
                    onNext(new((T)(object)(_range.Start + i), TimeSpan.Zero));
                }

                return;
            }

            var last = _sequencer.Now;
            for (var i = 0; i < _range.Count; i++)
            {
                var now = _sequencer.Now;
                var interval = i == 0 ? TimeSpan.Zero : now - last;
                last = now;
                onNext(new((T)(object)(_range.Start + i), interval));
            }
        }

        /// <summary>Emits interval-tagged range values straight to an observer.</summary>
        /// <param name="observer">The downstream observer.</param>
        private void Emit(IObserver<TimeInterval<T>> observer)
        {
            if (_sequencer == Sequencer.Immediate)
            {
                for (var i = 0; i < _range.Count; i++)
                {
                    observer.OnNext(new((T)(object)(_range.Start + i), TimeSpan.Zero));
                }

                return;
            }

            var last = _sequencer.Now;
            for (var i = 0; i < _range.Count; i++)
            {
                var now = _sequencer.Now;
                var interval = i == 0 ? TimeSpan.Zero : now - last;
                last = now;
                observer.OnNext(new((T)(object)(_range.Start + i), interval));
            }
        }
    }

    /// <summary>Emits a whole range as one batch scheduled after the due time.</summary>
    /// <typeparam name="T">The range value type.</typeparam>
    /// <param name="range">The range source.</param>
    /// <param name="dueTime">The normalized due time.</param>
    /// <param name="sequencer">The sequencer used to schedule the range batch.</param>
    private sealed class ShiftedRangeSignal<T>(RangeSignal range, TimeSpan dueTime, ISequencer sequencer) : IRequireCurrentThread<T>, IInlineSignal<T>
    {
        /// <summary>The range source.</summary>
        private readonly RangeSignal _range = range;

        /// <summary>The normalized due time.</summary>
        private readonly TimeSpan _dueTime = dueTime;

        /// <summary>The sequencer used to schedule the range batch.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _sequencer == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return _sequencer.Schedule(
                (Observer: observer, Range: _range),
                _dueTime,
                static (_, state) => EmitShiftedRange(state.Observer, state.Range));
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            ArgumentExceptionHelper.ThrowIfNull(onNext);

            ArgumentExceptionHelper.ThrowIfNull(onCompleted);

            return _sequencer.Schedule(
                (OnNext: onNext, OnCompleted: onCompleted, Range: _range),
                _dueTime,
                static (_, state) => EmitShiftedRange(state.OnNext, state.OnCompleted, state.Range));
        }
    }

    /// <summary>Coordinates quiet-period emission with one active timer.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <remarks>
    /// The gate only guards the latest value and the flags. Emissions and terminals are queued in order under the gate and
    /// delivered by a <see cref="SerializedDelivery{T}"/> after it is released, so no lock is held while the observer runs.
    /// </remarks>
    private sealed class CalmCoordinator<T> : IDisposable
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The normalized quiet period.</summary>
        private readonly TimeSpan _dueTime;

        /// <summary>The sequencer used to schedule quiet-period timers.</summary>
        private readonly ISequencer _sequencer;

        /// <summary>Guards the latest value and the flags; never held while the observer runs.</summary>
        private readonly Lock _gate = new();

        /// <summary>Active subscription and timer resources.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>The active timer slot.</summary>
        private readonly SingleReplaceableDisposable _timer = new();

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>The downstream observer.</summary>
        private IObserver<T>? _observer;

        /// <summary>The latest source value.</summary>
        private T? _latest;

        /// <summary>A value indicating whether a latest source value is waiting to be emitted.</summary>
        private bool _hasLatest;

        /// <summary>A value indicating whether the timer is active.</summary>
        private bool _timerActive;

        /// <summary>The virtual due time for the current quiet period.</summary>
        private DateTimeOffset _dueAt;

        /// <summary>A value indicating whether a terminal notification has been emitted.</summary>
        private bool _done;

        /// <summary>Initializes a new instance of the <see cref="CalmCoordinator{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The quiet period.</param>
        /// <param name="sequencer">The sequencer used to schedule timers.</param>
        internal CalmCoordinator(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer)
        {
            _source = source;
            _dueTime = Sequencer.Normalize(dueTime);
            _sequencer = sequencer;
            _dueAt = sequencer.Now;
        }

        /// <summary>The action to take when a timer fires.</summary>
        private enum TimerAction
        {
            /// <summary>No value is available.</summary>
            None = 0,

            /// <summary>Emit the captured value.</summary>
            Emit = 1,

            /// <summary>Reschedule for the remaining quiet period.</summary>
            Reschedule = 2,
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _timer.Dispose();
            _subscriptions.Dispose();
        }

        /// <summary>Starts quiet-period coordination.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal CalmCoordinator<T> Run(IObserver<T> observer)
        {
            _observer = observer;
            _subscriptions.Add(_timer);
            _subscriptions.Add(_source.Subscribe(OnNext, OnError, OnCompleted));
            return this;
        }

        /// <summary>Records a source value and schedules a timer when needed.</summary>
        /// <param name="value">The source value.</param>
        private void OnNext(T value)
        {
            var shouldSchedule = false;
            lock (_gate)
            {
                _latest = value;
                _hasLatest = true;
                _dueAt = _sequencer.Now + _dueTime;
                if (!_timerActive)
                {
                    _timerActive = true;
                    shouldSchedule = true;
                }
            }

            if (!shouldSchedule)
            {
                return;
            }

            Schedule(_dueTime);
        }

        /// <summary>Queues a terminal error, delivers it and releases active resources.</summary>
        /// <param name="error">The terminal error.</param>
        private void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                _ = _delivery.PostError(error);
            }

            _delivery.Flush(new PendingDrain(this));
            Dispose();
        }

        /// <summary>Queues any value waiting inside the quiet window followed by completion, delivers them and releases active resources.</summary>
        private void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;

                if (_hasLatest)
                {
                    _hasLatest = false;
                    _ = _delivery.Post(_latest!);
                }

                _ = _delivery.PostCompleted();
            }

            _delivery.Flush(new PendingDrain(this));
            Dispose();
        }

        /// <summary>Schedules the active timer.</summary>
        /// <param name="delay">The timer delay.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Schedule(TimeSpan delay) => TimerSlot.Arm(_timer, _sequencer, delay, Tick);

        /// <summary>Handles a timer tick.</summary>
        private void Tick()
        {
            var action = GetTimerAction(out var delay);
            if (action == TimerAction.Reschedule)
            {
                Schedule(delay);
                return;
            }

            if (action != TimerAction.Emit)
            {
                return;
            }

            _delivery.Flush(new PendingDrain(this));
        }

        /// <summary>Determines what the active timer should do, queuing the value to emit under the gate.</summary>
        /// <param name="delay">The remaining delay when rescheduling is needed.</param>
        /// <returns>The timer action.</returns>
        private TimerAction GetTimerAction(out TimeSpan delay)
        {
            lock (_gate)
            {
                var remaining = _dueAt - _sequencer.Now;
                if (remaining > TimeSpan.Zero)
                {
                    delay = remaining;
                    return TimerAction.Reschedule;
                }

                _timerActive = false;
                delay = default;
                if (_done || !_hasLatest)
                {
                    return TimerAction.None;
                }

                _hasLatest = false;
                _ = _delivery.Post(_latest!);
                return TimerAction.Emit;
            }
        }

        /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The coordinator.</param>
        private readonly record struct PendingDrain(CalmCoordinator<T> Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer!);
        }
    }
}
