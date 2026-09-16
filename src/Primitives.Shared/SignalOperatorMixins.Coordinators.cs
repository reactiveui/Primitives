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

/// <summary>Coordinator helpers for multi-source signal operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Range-specialized WithLatest (Latch): emits each left range value paired with the right range's final value.</summary>
    /// <typeparam name="TResult">The result value type.</typeparam>
    /// <param name="left">The left source range.</param>
    /// <param name="right">The right source range.</param>
    /// <param name="selector">The result projection.</param>
    private sealed class RangeWithLatestSignal<TResult>(RangeSignal left, RangeSignal right, Func<int, int, TResult> selector) : IObservable<TResult>
    {
        /// <summary>The left source range.</summary>
        private readonly RangeSignal _left = left;

        /// <summary>The right source range (its final value is the latched value).</summary>
        private readonly RangeSignal _right = right;

        /// <summary>The result projection.</summary>
        private readonly Func<int, int, TResult> _selector = selector;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TResult> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            var rightValue = _right.Start + _right.Count - 1;
            for (var i = 0; i < _left.Count; i++)
            {
                observer.OnNext(_selector(_left.Start + i, rightValue));
            }

            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Dedicated signal for <c>Race</c>, handing each subscription to a coordinator.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class RaceSignal<T> : IObservable<T>
    {
        /// <summary>The candidate sources.</summary>
        private readonly IObservable<IObservable<T>> _sources;

        /// <summary>Initializes a new instance of the <see cref="RaceSignal{T}"/> class.</summary>
        /// <param name="sources">The candidate sources.</param>
        internal RaceSignal(IObservable<IObservable<T>> sources) => _sources = sources;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new RaceCoordinator<T>(observer).Run(_sources);
        }
    }

    /// <summary>Dedicated signal for <c>Blend</c> (concurrent merge of inner sources).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class BlendSignal<T> : IObservable<T>
    {
        /// <summary>The outer sequence of inner sources.</summary>
        private readonly IObservable<IObservable<T>> _sources;

        /// <summary>Initializes a new instance of the <see cref="BlendSignal{T}"/> class.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        internal BlendSignal(IObservable<IObservable<T>> sources) => _sources = sources;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new BlendCoordinator<T>(observer).Run(_sources);
        }
    }

    /// <summary>Coordinates concurrent merging of inner sources for <c>Blend</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <remarks>
    /// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while the downstream observer
    /// runs; values that arrive while another thread is delivering are delivered in arrival order.
    /// </remarks>
    private sealed class BlendCoordinator<T> : IDisposable
    {
        /// <summary>Active subscriptions.</summary>
        private readonly MultipleDisposable _pocket = [];

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Serializes downstream deliveries.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Whether the outer source completed, as 0 or 1.</summary>
        private int _outerCompleted;

        /// <summary>The number of active inner sources.</summary>
        private int _active;

        /// <summary>Initializes a new instance of the <see cref="BlendCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal BlendCoordinator(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _pocket.Dispose();

        /// <summary>Subscribes to the outer source.</summary>
        /// <param name="sources">The outer sequence of inner sources.</param>
        /// <returns>The subscription cleanup.</returns>
        internal BlendCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _pocket.Add(sources.Subscribe(OnSource, OnAnyError, OnOuterCompleted));
            return this;
        }

        /// <summary>Subscribes a new inner source concurrently.</summary>
        /// <param name="source">The inner source.</param>
        private void OnSource(IObservable<T> source)
        {
            if (source is null)
            {
                OnAnyError(new InvalidOperationException("Blend source contained null."));
                return;
            }

            _ = Interlocked.Increment(ref _active);
            _pocket.Add(source.Subscribe(OnInnerNext, OnAnyError, OnInnerCompleted));
        }

        /// <summary>Forwards an inner value, directly when nothing else is delivering.</summary>
        /// <param name="value">The value to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnInnerNext(T value) => _delivery.OnNext(_observer, value, new PendingDrain(this));

        /// <summary>Forwards the first terminal error and suppresses later notifications.</summary>
        /// <param name="error">The error to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void OnAnyError(Exception error) => _delivery.OnError(error, new PendingDrain(this));

        /// <summary>Decrements the active count and attempts completion.</summary>
        private void OnInnerCompleted()
        {
            _ = Interlocked.Decrement(ref _active);
            TryComplete();
        }

        /// <summary>Marks the outer source complete and attempts completion.</summary>
        private void OnOuterCompleted()
        {
            Volatile.Write(ref _outerCompleted, 1);
            TryComplete();
        }

        /// <summary>Delivers completion once the outer and all inners are done.</summary>
        private void TryComplete()
        {
            if (Volatile.Read(ref _outerCompleted) == 0 || Volatile.Read(ref _active) != 0)
            {
                return;
            }

            _delivery.OnCompleted(new PendingDrain(this));
        }

        /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The coordinator.</param>
        private readonly record struct PendingDrain(BlendCoordinator<T> Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
        }
    }

    /// <summary>Coordinates race subscriptions and forwards only the winning source.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class RaceCoordinator<T> : IDisposable
    {
        /// <summary>The shared race-arm bookkeeping.</summary>
        private readonly RaceArms<T> _arms;

        /// <summary>Initializes a new instance of the <see cref="RaceCoordinator{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal RaceCoordinator(IObserver<T> observer) => _arms = new(observer);

        /// <summary>Releases the active subscriptions.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _arms.Dispose();

        /// <summary>Starts observing the candidate source streams.</summary>
        /// <param name="sources">The candidate source streams.</param>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal RaceCoordinator<T> Run(IObservable<IObservable<T>> sources)
        {
            _arms.Add(sources.Subscribe(_arms.OnSource, _arms.OnOuterError, OnOuterCompleted));
            return this;
        }

        /// <summary>Handles completion of the outer sequence.</summary>
        private static void OnOuterCompleted()
        {
            // Only the winning source determines completion.
        }
    }
}
