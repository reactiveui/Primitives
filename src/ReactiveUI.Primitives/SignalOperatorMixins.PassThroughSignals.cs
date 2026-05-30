// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives;

/// <content>
/// Dedicated signals/sinks for single-source pass-through operators (Tap, IgnoreValues, Spark,
/// Unspark, TimeInterval), replacing the per-subscription <c>Signal.CreateSafe(observer =&gt; ...)</c>
/// closure with a signal that holds the parameters and a sink that forwards.
/// </content>
public static partial class LinqMixins
{
    /// <summary>Dedicated signal for <c>Tap</c> (side-effecting pass-through).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class TapSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The value side-effect.</summary>
        private readonly Action<T> _onNext;

        /// <summary>The error side-effect.</summary>
        private readonly Action<Exception> _onError;

        /// <summary>The completion side-effect.</summary>
        private readonly Action _onCompleted;

        /// <summary>Initializes a new instance of the <see cref="TapSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="onNext">The value side-effect.</param>
        /// <param name="onError">The error side-effect.</param>
        /// <param name="onCompleted">The completion side-effect.</param>
        internal TapSignal(IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            _source = source;
            _onNext = onNext;
            _onError = onError;
            _onCompleted = onCompleted;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new TapObserver<T>(observer, _onNext, _onError, _onCompleted);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Sink that runs side-effects before forwarding each notification.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class TapObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The value side-effect.</summary>
        private readonly Action<T> _onNext;

        /// <summary>The error side-effect.</summary>
        private readonly Action<Exception> _onError;

        /// <summary>The completion side-effect.</summary>
        private readonly Action _onCompleted;

        /// <summary>Initializes a new instance of the <see cref="TapObserver{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="onNext">The value side-effect.</param>
        /// <param name="onError">The error side-effect.</param>
        /// <param name="onCompleted">The completion side-effect.</param>
        internal TapObserver(IObserver<T> observer, Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            _observer = observer;
            _onNext = onNext;
            _onError = onError;
            _onCompleted = onCompleted;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            _onNext(value);
            _observer.OnNext(value);
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            try
            {
                _onError(error);
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            try
            {
                _onCompleted();
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>Dedicated signal for <c>IgnoreValues</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class IgnoreValuesSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>Initializes a new instance of the <see cref="IgnoreValuesSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal IgnoreValuesSignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new IgnoreValuesObserver<T>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Sink that drops values and forwards only terminal notifications.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class IgnoreValuesObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Initializes a new instance of the <see cref="IgnoreValuesObserver{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal IgnoreValuesObserver(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            // Values are intentionally ignored; only terminal notifications are forwarded.
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>Dedicated signal for <c>Spark</c> (materialize).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SparkSignal<T> : IObservable<Spark<T>>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>Initializes a new instance of the <see cref="SparkSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        internal SparkSignal(IObservable<T> source) => _source = source;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<Spark<T>> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new SparkObserver<T>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Sink that materializes notifications into <see cref="Spark{T}"/> values.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SparkObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<Spark<T>> _observer;

        /// <summary>Initializes a new instance of the <see cref="SparkObserver{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal SparkObserver(IObserver<Spark<T>> observer) => _observer = observer;

        /// <inheritdoc/>
        public override void OnNext(T value) => _observer.OnNext(ReactiveUI.Primitives.Core.Spark.CreateOnNext(value));

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            try
            {
                _observer.OnNext(ReactiveUI.Primitives.Core.Spark.CreateOnError<T>(error));
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            try
            {
                _observer.OnNext(ReactiveUI.Primitives.Core.Spark.CreateOnCompleted<T>());
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>Dedicated signal for <c>Unspark</c> (dematerialize).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class UnsparkSignal<T> : IObservable<T>
    {
        /// <summary>The spark source.</summary>
        private readonly IObservable<Spark<T>> _source;

        /// <summary>Initializes a new instance of the <see cref="UnsparkSignal{T}"/> class.</summary>
        /// <param name="source">The spark source.</param>
        internal UnsparkSignal(IObservable<Spark<T>> source) => _source = source;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new UnsparkObserver<T>(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Sink that dematerializes <see cref="Spark{T}"/> values into notifications.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class UnsparkObserver<T> : SingleSourceObserver<Spark<T>>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Initializes a new instance of the <see cref="UnsparkObserver{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal UnsparkObserver(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        public override void OnNext(Spark<T> spark) => spark.Accept(_observer);

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }

    /// <summary>Dedicated signal for the general <c>TimeInterval</c> path.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class TimeIntervalSignal<T> : IObservable<TimeInterval<T>>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The sequencer that supplies timestamps.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Initializes a new instance of the <see cref="TimeIntervalSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="scheduler">The sequencer that supplies timestamps.</param>
        internal TimeIntervalSignal(IObservable<T> source, ISequencer scheduler)
        {
            _source = source;
            _scheduler = scheduler;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TimeInterval<T>> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new TimeIntervalObserver<T>(observer, _scheduler);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Sink that annotates each value with the elapsed interval since the previous value.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class TimeIntervalObserver<T> : SingleSourceObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TimeInterval<T>> _observer;

        /// <summary>The sequencer that supplies timestamps.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>The timestamp of the previous value.</summary>
        private DateTimeOffset _last;

        /// <summary>A value indicating whether the next value is the first.</summary>
        private bool _first = true;

        /// <summary>Initializes a new instance of the <see cref="TimeIntervalObserver{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="scheduler">The sequencer that supplies timestamps.</param>
        internal TimeIntervalObserver(IObserver<TimeInterval<T>> observer, ISequencer scheduler)
        {
            _observer = observer;
            _scheduler = scheduler;
            _last = scheduler.Now;
        }

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            var now = _scheduler.Now;
            var interval = _first ? TimeSpan.Zero : now - _last;
            _first = false;
            _last = now;
            _observer.OnNext(new TimeInterval<T>(value, interval));
        }

        /// <inheritdoc/>
        public override void OnError(Exception error)
        {
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
