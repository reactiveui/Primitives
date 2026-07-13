// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of static methods for creating and composing asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class offers extension methods that enable functional-style operations, such as
/// filtering for distinct elements, on asynchronous observable sequences. These methods are designed to work with the
/// SignalAsync{T} type, allowing developers to build complex, asynchronous event processing pipelines in a
/// composable manner.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Distinctness operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Returns a sequence that contains only distinct elements from the source sequence, using the default equality
        /// comparer for the element type.
        /// </summary>
        /// <remarks>Elements are considered distinct based on the default equality comparer for type T.
        /// The order of elements is preserved.</remarks>
        /// <returns>An observable sequence that contains distinct elements from the source sequence.</returns>
        public IObservableAsync<T> Distinct()
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);

            return new DistinctSignal<T>(@this, EqualityComparer<T>.Default);
        }

        /// <summary>
        /// Returns an observable sequence that contains only distinct elements from the source sequence, using the
        /// specified equality comparer to determine uniqueness.
        /// </summary>
        /// <remarks>Only the first occurrence of each element, as determined by the specified equality
        /// comparer, is emitted to observers. Subsequent duplicate elements are ignored.</remarks>
        /// <param name="equalityComparer">An equality comparer to compare values for equality. If null, the default equality comparer for the type is
        /// used.</param>
        /// <returns>An observable sequence that emits each distinct element from the source sequence, in the order in which they
        /// are received.</returns>
        public IObservableAsync<T> Distinct(IEqualityComparer<T> equalityComparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new DistinctSignal<T>(@this, equalityComparer);
        }

        /// <summary>
        /// Returns a sequence that contains distinct elements from the source sequence according to a specified key
        /// selector function.
        /// </summary>
        /// <remarks>Elements are considered distinct based on the value returned by the key selector and
        /// the default equality comparer for the key type.</remarks>
        /// <typeparam name="TKey">The type of the key returned by the key selector function.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element. Cannot be null.</param>
        /// <returns>An observable sequence that contains only the first occurrence of each distinct key as determined by the key
        /// selector.</returns>
        public IObservableAsync<T> DistinctBy<TKey>(Func<T, TKey> keySelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new DistinctBySignal<T, TKey>(@this, keySelector, EqualityComparer<TKey>.Default);
        }

        /// <summary>
        /// Returns an observable sequence that contains only distinct elements from the source sequence, comparing
        /// values based on a specified key and equality comparer.
        /// </summary>
        /// <remarks>Elements are considered distinct based on the value returned by the <paramref
        /// name="keySelector"/> function and compared using the provided <paramref name="equalityComparer"/>. Only the
        /// first occurrence of each key is included in the resulting sequence.</remarks>
        /// <typeparam name="TKey">The type of the key used to determine the distinctness of elements.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element. Cannot be null.</param>
        /// <param name="equalityComparer">An equality comparer to compare keys for equality. Cannot be null.</param>
        /// <returns>An observable sequence that contains only the first occurrence of each distinct key as determined by the
        /// specified key selector and equality comparer.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="keySelector"/> or <paramref name="equalityComparer"/> is null.</exception>
        public IObservableAsync<T> DistinctBy<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> equalityComparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new DistinctBySignal<T, TKey>(@this, keySelector, equalityComparer);
        }
    }

    /// <summary>Single-observer-layer <c>Distinct</c> using a per-subscription <see cref="HashSet{T}"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="comparer">The equality comparer used to detect duplicates.</param>
    internal sealed class DistinctSignal<T>(IObservableAsync<T> source, IEqualityComparer<T> comparer)
        : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            DistinctWitness sink = new(observer, comparer, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that tracks seen values in a <see cref="HashSet{T}"/>.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="comparer">The equality comparer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class DistinctWitness(
            IObserverAsync<T> downstream,
            IEqualityComparer<T> comparer,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>Set of previously-forwarded values; <see cref="HashSet{T}.Add"/> returns <see langword="false"/> for duplicates.</summary>
            private readonly HashSet<T> _seen = [with(comparer)];

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                _seen.Add(value) ? downstream.OnNextAsync(value, cancellationToken) : default;

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Single-observer-layer <c>DistinctBy</c> using a per-subscription <see cref="HashSet{TKey}"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="keySelector">The key selector.</param>
    /// <param name="comparer">The key equality comparer.</param>
    internal sealed class DistinctBySignal<T, TKey>(
        IObservableAsync<T> source,
        Func<T, TKey> keySelector,
        IEqualityComparer<TKey> comparer) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            DistinctByWitness sink = new(observer, keySelector, comparer, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that tracks seen keys.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <param name="comparer">The key equality comparer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class DistinctByWitness(
            IObserverAsync<T> downstream,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>Set of previously-seen keys.</summary>
            private readonly HashSet<TKey> _seen = [with(comparer)];

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                _seen.Add(keySelector(value)) ? downstream.OnNextAsync(value, cancellationToken) : default;

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
