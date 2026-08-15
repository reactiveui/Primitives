// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static extension methods that enable LINQ-style and other
/// operations on asynchronous observables. These methods are intended to facilitate the composition and manipulation of
/// asynchronous data streams in a reactive programming style.</remarks>
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
        /// <exception cref="ArgumentOutOfRangeException">Thrown if count is less than 0.</exception>
        public IObservableAsync<T> Skip(int count)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

            return count == 0 ? source : new SkipSignal<T>(source, count);
        }
    }

    /// <summary>Single-observer-layer <c>Skip(count)</c>. Drops the first <c>count</c> emissions then forwards everything subsequent.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="count">The number of leading emissions to drop (must be &gt; 0; the zero case bypasses this observable entirely).</param>
    internal sealed class SkipSignal<T>(IObservableAsync<T> source, int count) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            SkipWitness sink = new(observer, count, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
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
        internal sealed class SkipWitness(
            IObserverAsync<T> downstream,
            int budget,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>Remaining skip budget; counts down to zero, at which point every subsequent value is forwarded.</summary>
            private int _remaining = budget;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (_remaining > 0)
                {
                    _remaining--;
                    return default;
                }

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
