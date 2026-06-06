// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for working with asynchronous observable sequences.
/// </summary>
/// <remarks>The SignalAsync class contains static methods that extend the functionality of asynchronous
/// observables, enabling additional operations such as type casting and sequence manipulation. These methods are
/// intended to be used with the SignalAsync{T} type to facilitate reactive programming patterns in asynchronous
/// scenarios.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Projects each element of the observable sequence to the specified result type by performing a runtime cast.
    /// </summary>
    /// <remarks>If an element in the source sequence cannot be cast to <typeparamref
    /// name="TResult"/>, the sequence completes with a failure containing the exception. This method is useful for
    /// working with sequences of objects when the actual element type is known at runtime.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TResult">The type to which the elements of the sequence are cast.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>An observable sequence whose elements are the result of casting each element of the source sequence to
    /// <typeparamref name="TResult"/>.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public extension API — caller specifies TResult explicitly: source.Cast<Derived>().")]
    public static IObservableAsync<TResult> Cast<T, TResult>(this IObservableAsync<T> @this)
    {
        ArgumentExceptionHelper.ThrowIfNull(@this);

        return new CastSignal<T, TResult>(@this);
    }

    /// <summary>Single-observer-layer <c>Cast</c>; failed casts terminate the sequence with failure.</summary>
    /// <typeparam name="T">The upstream element type.</typeparam>
    /// <typeparam name="TResult">The target element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    internal sealed class CastSignal<T, TResult>(IObservableAsync<T> source) : SignalAsync<TResult>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TResult> observer,
            CancellationToken cancellationToken)
        {
            var sink = new CastWitness(observer, cancellationToken);

            if (observer is ObserverAsync<TResult> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that casts each value to <typeparamref name="TResult"/>.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class CastWitness(
            IObserverAsync<TResult> downstream,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                TResult casted;
                try
                {
                    casted = (TResult)(object?)value!;
                }
                catch (Exception e)
                {
                    return downstream.OnCompletedAsync(Result.Failure(e));
                }

                return downstream.OnNextAsync(casted, cancellationToken);
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
