// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of static methods for creating and composing asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Distinctness operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns a sequence that contains only distinct elements from the source sequence, using the default equality comparer for the element type.</summary>
        /// <returns>An observable sequence that contains distinct elements from the source sequence.</returns>
        /// <remarks>Only the first occurrence of each element reaches observers, in source order; the set of seen
        /// elements lives for the whole subscription.</remarks>
        public IObservableAsync<T> Distinct()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new DistinctSignal<T>(source, EqualityComparer<T>.Default);
        }

        /// <summary>Returns an observable sequence that contains only distinct elements from the source sequence, using the specified equality comparer to determine uniqueness.</summary>
        /// <param name="equalityComparer">An equality comparer to compare values for equality. If null, the default equality comparer for the type is
        /// used.</param>
        /// <returns>An observable sequence that emits each distinct element from the source sequence, in the order in which they
        /// are received.</returns>
        /// <remarks>Only the first occurrence of each element, as judged by <paramref name="equalityComparer"/>,
        /// reaches observers.</remarks>
        public IObservableAsync<T> Distinct(IEqualityComparer<T> equalityComparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new DistinctSignal<T>(source, equalityComparer);
        }

        /// <summary>Returns a sequence that contains distinct elements from the source sequence according to a specified key selector function.</summary>
        /// <typeparam name="TKey">The type of the key returned by the key selector function.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element. Cannot be null.</param>
        /// <returns>An observable sequence that contains only the first occurrence of each distinct key as determined by the key
        /// selector.</returns>
        /// <remarks>Keys are compared with the default equality comparer for <typeparamref name="TKey"/>.</remarks>
        public IObservableAsync<T> DistinctBy<TKey>(Func<T, TKey> keySelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new DistinctBySignal<T, TKey>(source, keySelector, EqualityComparer<TKey>.Default);
        }

        /// <summary>Returns an observable sequence that contains only distinct elements from the source sequence, comparing values based on a specified key and equality comparer.</summary>
        /// <typeparam name="TKey">The type of the key used to determine the distinctness of elements.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element. Cannot be null.</param>
        /// <param name="equalityComparer">An equality comparer to compare keys for equality. Cannot be null.</param>
        /// <returns>An observable sequence that contains only the first occurrence of each distinct key as determined by the
        /// specified key selector and equality comparer.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> or <paramref name="equalityComparer"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> DistinctBy<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> equalityComparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new DistinctBySignal<T, TKey>(source, keySelector, equalityComparer);
        }
    }

    /// <summary>Single-observer-layer <c>Distinct</c> using a per-subscription <see cref="HashSet{T}"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="comparer">The equality comparer used to detect duplicates.</param>
    internal sealed class DistinctSignal<T>(IObservableAsync<T> source, IEqualityComparer<T> comparer) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            DistinctWitness sink = new(observer, comparer, cancellationToken);

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("DistinctWitness: {_witness}")]
        internal sealed class DistinctWitness(
            IObserverAsync<T> downstream,
            IEqualityComparer<T> comparer,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>The values forwarded so far; <see cref="HashSet{T}.Add"/> returns <see langword="false"/> for a duplicate.</summary>
            private readonly HashSet<T> _seen = [with(comparer)];

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                _seen.Add(value) ? downstream.OnNextAsync(value, cancellationToken) : default;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("DistinctByWitness: {_witness}")]
        internal sealed class DistinctByWitness(
            IObserverAsync<T> downstream,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>The keys seen so far.</summary>
            private readonly HashSet<TKey> _seen = [with(comparer)];

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                _seen.Add(keySelector(value)) ? downstream.OnNextAsync(value, cancellationToken) : default;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
