// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives;

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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new TapWitness<T>(observer, _onNext, _onError, _onCompleted);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new IgnoreValuesWitness<T>(observer);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new SparkWitness<T>(observer);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new UnsparkWitness<T>(observer);
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new TimeIntervalWitness<T>(observer, _scheduler);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }
}
