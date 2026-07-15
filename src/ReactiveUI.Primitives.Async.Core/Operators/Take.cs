// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static extension methods that enable advanced operations on
/// asynchronous observables, such as filtering, transformation, and sequence control. These methods are intended to be
/// used with the SignalAsync{T} type to facilitate reactive programming patterns in asynchronous
/// scenarios.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Element-limiting operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Returns a new observable sequence that emits only the first specified number of elements from the source sequence.</summary>
        /// <remarks>If the source sequence contains fewer elements than <paramref name="count"/>, all
        /// available elements are emitted and the sequence completes. This method does not modify the source sequence;
        /// it returns a new sequence with the specified behavior.</remarks>
        /// <param name="count">The maximum number of elements to emit from the source sequence. Must be greater than or equal to zero.</param>
        /// <returns>An observable sequence that contains at most the first <paramref name="count"/> elements from the source
        /// sequence. If <paramref name="count"/> is zero, the resulting sequence completes immediately without emitting
        /// any elements.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="count"/> is less than zero.</exception>
        public IObservableAsync<T> Take(int count)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

            return count == 0 ? new TakeZeroSignal<T>() : new TakeSignal<T>(@this, count);
        }
    }

    /// <summary><c>Take(0)</c> short-circuit: synthesizes an immediate-completion observable without ever subscribing to the upstream.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    internal sealed class TakeZeroSignal<T> : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }
    }

    /// <summary>Single-observer-layer <c>Take(count)</c>. Forwards values until the budget is exhausted, then signals completion downstream.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="count">The maximum number of values to forward (must be &gt; 0; the zero case uses <see cref="TakeZeroSignal{T}"/>).</param>
    internal sealed class TakeSignal<T>(IObservableAsync<T> source, int count) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            TakeWitness sink = new(observer, count, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that counts emissions and signals completion on the final one.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="budget">The take budget, decremented per emission.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class TakeWitness(
            IObserverAsync<T> downstream,
            int budget,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>Remaining take budget; decremented per forwarded value.</summary>
            private int _remaining = budget;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (_remaining == 0)
                {
                    return default;
                }

                _remaining--;
                return _remaining == 0
                    ? ForwardThenFinishAsync(downstream, value, cancellationToken)
                    : downstream.OnNextAsync(value, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);

            /// <summary>Forwards the final value, then signals downstream completion.</summary>
            /// <param name="target">The downstream observer receiving the final value and the completion.</param>
            /// <param name="value">The final value.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <returns>A task that completes after both the value and the completion are forwarded.</returns>
            private static async ValueTask ForwardThenFinishAsync(
                IObserverAsync<T> target,
                T value,
                CancellationToken cancellationToken)
            {
                await target.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                await target.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            }
        }
    }
}
