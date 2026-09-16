// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Element-skipping operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns a new observable sequence that skips the specified number of elements from the start of the source sequence.</summary>
        /// <param name="count">The number of elements to skip. Must be greater than or equal to 0.</param>
        /// <returns>An observable sequence that contains the elements of the source sequence after the specified number of
        /// elements have been skipped. If the count is 0, the original sequence is returned.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is less than zero.</exception>
        public IObservableAsync<T> Skip(int count)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

            return count == 0 ? source : new SkipSignal<T>(source, count);
        }
    }

    /// <summary>Drops the first <c>count</c> emissions, then forwards every later value.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="count">The number of leading emissions to drop, always greater than zero.</param>
    internal sealed class SkipSignal<T>(IObservableAsync<T> source, int count) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            SkipWitness sink = new(observer, count, cancellationToken);

            if (observer is IWitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that drops leading emissions then forwards.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="budget">The skip budget, decremented per dropped value.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("SkipWitness: {_witness}")]
        internal sealed class SkipWitness(
            IObserverAsync<T> downstream,
            int budget,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>Remaining skip budget; counts down to zero, at which point every subsequent value is forwarded.</summary>
            private int _remaining = budget;

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
                if (_remaining > 0)
                {
                    _remaining--;
                    return default;
                }

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
