// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>
/// Dedicated signals/sinks for single-source pass-through operators (Tap, IgnoreValues, Spark,
/// Unspark, TimeInterval), replacing the per-subscription <c>Signal.CreateSafe(observer =&gt; ...)</c>
/// closure with a signal that holds the parameters and a sink that forwards.
/// </summary>
public static partial class LinqExtensions
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
            ArgumentExceptionHelper.ThrowIfNull(observer);

            TapWitness<T> sink = new(observer, _onNext, _onError, _onCompleted);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
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
            ArgumentExceptionHelper.ThrowIfNull(observer);

            IgnoreValuesWitness<T> sink = new(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
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
            ArgumentExceptionHelper.ThrowIfNull(observer);

            SparkWitness<T> sink = new(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
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
            ArgumentExceptionHelper.ThrowIfNull(observer);

            UnsparkWitness<T> sink = new(observer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
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
            ArgumentExceptionHelper.ThrowIfNull(observer);

            TimeIntervalWitness<T> sink = new(observer, _scheduler);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>Synchronize</c> (gates notifications so downstream sees the serialized grammar).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SynchronizeSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The shared gate, or <see langword="null"/> to give each subscription its own private gate.</summary>
        private readonly Lock? _gate;

        /// <summary>Initializes a new instance of the <see cref="SynchronizeSignal{T}"/> class with a per-subscription gate.</summary>
        /// <param name="source">The source observable.</param>
        internal SynchronizeSignal(IObservable<T> source) => _source = source;

        /// <summary>Initializes a new instance of the <see cref="SynchronizeSignal{T}"/> class sharing the supplied gate.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="gate">The gate shared across subscriptions and other synchronized sequences.</param>
        internal SynchronizeSignal(IObservable<T> source, Lock gate)
        {
            _source = source;
            _gate = gate;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var sink = _gate is null
                ? new(observer)
                : new SynchronizeWitness<T>(observer, _gate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for object-gated <c>Synchronize</c> compatibility.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SynchronizeObjectSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The shared gate.</summary>
        private readonly object _gate;

        /// <summary>Initializes a new instance of the <see cref="SynchronizeObjectSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="gate">The gate shared across subscriptions and other synchronized sequences.</param>
        internal SynchronizeObjectSignal(IObservable<T> source, object gate)
        {
            _source = source;
            _gate = gate;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            SynchronizeObjectWitness sink = new(observer, _gate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Observer that serializes notifications using an object gate.</summary>
        private sealed class SynchronizeObjectWitness : IObserver<T>, IDisposable
        {
            /// <summary>The downstream observer.</summary>
            private readonly IObserver<T> _observer;

            /// <summary>The gate that serializes every forwarded notification.</summary>
            private readonly object _gate;

            /// <summary>The upstream subscription.</summary>
            private IDisposable? _subscription;

            /// <summary>Initializes a new instance of the <see cref="SynchronizeObjectWitness"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            /// <param name="gate">The gate shared with other synchronized observers.</param>
            internal SynchronizeObjectWitness(IObserver<T> observer, object gate)
            {
                _observer = observer;
                _gate = gate;
            }

            /// <inheritdoc/>
            public void OnNext(T value)
            {
                lock (_gate)
                {
                    _observer.OnNext(value);
                }
            }

            /// <inheritdoc/>
            public void OnError(Exception error)
            {
                lock (_gate)
                {
                    _observer.OnError(error);
                }
            }

            /// <inheritdoc/>
            public void OnCompleted()
            {
                lock (_gate)
                {
                    _observer.OnCompleted();
                }
            }

            /// <summary>Assigns the upstream subscription.</summary>
            /// <param name="subscription">The upstream subscription.</param>
            public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

            /// <inheritdoc/>
            public void Dispose() => SinkSubscription.Dispose(ref _subscription);
        }
    }
}
