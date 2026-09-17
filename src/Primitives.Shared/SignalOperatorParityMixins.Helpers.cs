// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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
}
