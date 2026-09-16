// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences, enabling operations such as suppressing consecutive duplicate elements.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Consecutive-distinctness operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns an observable sequence that emits only distinct consecutive elements, suppressing duplicates that are equal to the previous element.</summary>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not equal to their
        /// immediate predecessor.</returns>
        /// <remarks>Uses the default equality comparer.</remarks>
        public IObservableAsync<T> Unique()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new UniqueSignal<T>(source, EqualityComparer<T>.Default);
        }

        /// <summary>
        /// Returns an observable sequence that emits elements from the source sequence only when the current element is
        /// not equal to the previous element, as determined by the specified equality comparer.
        /// </summary>
        /// <param name="equalityComparer">An equality comparer used to determine whether consecutive elements are considered equal.</param>
        /// <returns>An observable sequence that contains only distinct consecutive elements from the source sequence, as
        /// determined by the specified equality comparer.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="equalityComparer"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> Unique(IEqualityComparer<T> equalityComparer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new UniqueSignal<T>(source, equalityComparer);
        }

        /// <summary>Returns an observable sequence that emits elements from the source sequence, suppressing consecutive duplicates as determined by a key selector function.</summary>
        /// <typeparam name="TKey">The type of the key used to determine whether consecutive elements are considered duplicates.</typeparam>
        /// <param name="keySelector">A function that extracts the comparison key from each element in the source sequence.</param>
        /// <returns>An observable sequence that contains only the elements from the source sequence that are not consecutive
        /// duplicates according to the specified key.</returns>
        /// <remarks>Compares keys with the default equality comparer.</remarks>
        public IObservableAsync<T> UniqueBy<TKey>(Func<T, TKey> keySelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new UniqueBySignal<T, TKey>(source, keySelector, EqualityComparer<TKey>.Default);
        }

        /// <summary>Returns an observable sequence that emits elements from the source sequence, suppressing consecutive duplicates as determined by a key selector and equality comparer.</summary>
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
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);
            ArgumentExceptionHelper.ThrowIfNull(equalityComparer);

            return new UniqueBySignal<T, TKey>(source, keySelector, equalityComparer);
        }
    }

    /// <summary>Drops each value that the comparer judges equal to the most-recently-forwarded one.</summary>
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

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("UniqueWitness: {_witness}")]
        internal sealed class UniqueWitness(
            IObserverAsync<T> downstream,
            IEqualityComparer<T> comparer,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>The most-recently-forwarded value; valid only when <see cref="_hasPrevious"/> is set.</summary>
            private T? _previous;

            /// <summary>Latches to <see langword="true"/> after the first emission has been forwarded.</summary>
            private bool _hasPrevious;

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
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Single-observer-layer <c>DistinctUntilChangedBy</c>; key is extracted once per emission and compared against the most-recently-forwarded key.</summary>
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

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("UniqueByWitness: {_witness}")]
        internal sealed class UniqueByWitness(
            IObserverAsync<T> downstream,
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey> comparer,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>The most-recently-forwarded key; valid only when <see cref="_hasPrevious"/> is set.</summary>
            private TKey? _previousKey;

            /// <summary>Latches to <see langword="true"/> after the first emission has been forwarded.</summary>
            private bool _hasPrevious;

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
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
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
