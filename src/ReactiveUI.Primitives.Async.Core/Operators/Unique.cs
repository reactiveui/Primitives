// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences, enabling operations such as
/// suppressing consecutive duplicate elements.
/// </summary>
/// <remarks>The methods in this class allow developers to filter out consecutive duplicates in observable
/// sequences, either by value or by a specified key. These operations are useful for scenarios where only changes or
/// distinct consecutive values are of interest, such as event streams or state change notifications.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Consecutive-distinctness operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Returns an observable sequence that emits only distinct consecutive elements, suppressing duplicates that
        /// are equal to the previous element.
        /// </summary>
        /// <remarks>Elements are compared using the default equality comparer for the type <typeparamref
        /// name="T"/>. Only consecutive duplicate elements are suppressed; non-consecutive duplicates are not
        /// affected.</remarks>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not equal to their
        /// immediate predecessor.</returns>
        public IObservableAsync<T> Unique()
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);

            return new UniqueSignal<T>(@this, EqualityComparer<T>.Default);
        }

        /// <summary>
        /// Returns an observable sequence that emits elements from the source sequence only when the current element is
        /// not equal to the previous element, as determined by the specified equality comparer.
        /// </summary>
        /// <remarks>Use this method to suppress consecutive duplicate elements in the sequence. Only
        /// elements that differ from their immediate predecessor, according to the provided comparer, are emitted to
        /// observers.</remarks>
        /// <param name="equalityComparer">An equality comparer used to determine whether consecutive elements are considered equal.</param>
        /// <returns>An observable sequence that contains only distinct consecutive elements from the source sequence, as
        /// determined by the specified equality comparer.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="equalityComparer"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> Unique(IEqualityComparer<T> equalityComparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new UniqueSignal<T>(@this, equalityComparer);
        }

        /// <summary>
        /// Returns an observable sequence that emits elements from the source sequence, suppressing consecutive
        /// duplicates as determined by a key selector function.
        /// </summary>
        /// <remarks>The comparison of keys uses the default equality comparer for the type <typeparamref
        /// name="TKey"/>. Only consecutive duplicate elements are suppressed; non-consecutive duplicates are not
        /// affected.</remarks>
        /// <typeparam name="TKey">The type of the key used to determine whether consecutive elements are considered duplicates.</typeparam>
        /// <param name="keySelector">A function that extracts the comparison key from each element in the source sequence.</param>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not consecutive
        /// duplicates according to the specified key.</returns>
        public IObservableAsync<T> UniqueBy<TKey>(Func<T, TKey> keySelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new UniqueBySignal<T, TKey>(@this, keySelector, EqualityComparer<TKey>.Default);
        }

        /// <summary>
        /// Returns an observable sequence that emits elements from the source sequence, suppressing consecutive
        /// duplicates as determined by a key selector and equality comparer.
        /// </summary>
        /// <remarks>The first element in the sequence is always emitted. Subsequent elements are emitted
        /// only if their key, as determined by <paramref name="keySelector"/>, is not equal to the key of the
        /// immediately preceding element, as determined by <paramref name="equalityComparer"/>.</remarks>
        /// <typeparam name="TKey">The type of the key used to determine whether consecutive elements are considered duplicates.</typeparam>
        /// <param name="keySelector">A function that extracts the comparison key from each element in the source sequence.</param>
        /// <param name="equalityComparer">An equality comparer used to compare keys for equality.</param>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not consecutive
        /// duplicates according to the specified key and comparer.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> or <paramref name="equalityComparer"/> is null.</exception>
        public IObservableAsync<T> UniqueBy<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> equalityComparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new UniqueBySignal<T, TKey>(@this, keySelector, equalityComparer);
        }
    }

    /// <summary>
    /// Single-observer-layer <c>DistinctUntilChanged</c>. Replaces the previous \c Create + async-lambda + closure
    /// pattern; per-subscription state lives in observer fields.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="comparer">The equality comparer used to detect duplicates.</param>
    internal sealed class UniqueSignal<T>(IObservableAsync<T> source, IEqualityComparer<T> comparer) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            UniqueWitness sink = new(observer, comparer, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that drops values equal to the most-recently-forwarded one.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="comparer">The equality comparer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class UniqueWitness(
            IObserverAsync<T> downstream,
            IEqualityComparer<T> comparer,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>The previously-forwarded value; valid only when <see cref="_hasPrevious"/> is set.</summary>
            private T? _previous;

            /// <summary>Latches to <see langword="true"/> after the first emission has been forwarded.</summary>
            private bool _hasPrevious;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (_hasPrevious && comparer.Equals(_previous!, value))
                {
                    return default;
                }

                _previous = value;
                _hasPrevious = true;
                return downstream.OnNextAsync(value, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>
    /// Single-observer-layer <c>DistinctUntilChangedBy</c>; key is extracted once per emission and compared
    /// against the most-recently-forwarded key.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key equality comparer.</param>
    internal sealed class UniqueBySignal<T, TKey>(
        IObservableAsync<T> source,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey> comparer) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            UniqueByWitness sink = new(observer, keySelector, comparer, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that compares extracted keys against the most-recently-forwarded one.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <param name="comparer">The key equality comparer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class UniqueByWitness(
            IObserverAsync<T> downstream,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>The previously-forwarded key; valid only when <see cref="_hasPrevious"/> is set.</summary>
            private TKey? _previousKey;

            /// <summary>Latches to <see langword="true"/> after the first emission has been forwarded.</summary>
            private bool _hasPrevious;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                var key = keySelector(value);
                if (_hasPrevious && comparer.Equals(_previousKey!, key))
                {
                    return default;
                }

                _previousKey = key;
                _hasPrevious = true;
                return downstream.OnNextAsync(value, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
