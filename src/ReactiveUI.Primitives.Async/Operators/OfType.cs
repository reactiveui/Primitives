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
/// observables, enabling advanced filtering, transformation, and composition operations. These methods are intended to
/// be used with types implementing asynchronous observable patterns, such as SignalAsync{T}.</remarks>
public static partial class SignalAsync
{
    /// <summary>
    /// Projects each element of the observable sequence to the specified reference type and filters out elements
    /// that are not of that type.
    /// </summary>
    /// <remarks>Elements that are not of type TResult are ignored and not included in the resulting
    /// sequence. This method is useful for working with observable sequences containing heterogeneous types,
    /// allowing subscribers to focus on elements of a specific type.</remarks>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TResult">The reference type to filter and project elements to. Must be a class.</typeparam>
    /// <param name="this">The source observable sequence.</param>
    /// <returns>An observable sequence containing only the elements of type TResult from the original sequence.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public extension API — caller specifies TResult explicitly: source.OfType<Derived>().")]
    public static IObservableAsync<TResult> OfType<T, TResult>(this IObservableAsync<T> @this)
        where TResult : class
    {
        ArgumentExceptionHelper.ThrowIfNull(@this);

        return new OfTypeSignal<T, TResult>(@this);
    }

    /// <summary>Single-observer-layer <c>OfType</c>; non-matching elements are silently dropped.</summary>
    /// <typeparam name="T">The upstream element type.</typeparam>
    /// <typeparam name="TResult">The target reference type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    internal sealed class OfTypeSignal<T, TResult>(IObservableAsync<T> source) : SignalAsync<TResult>
        where TResult : class
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TResult> observer,
            CancellationToken cancellationToken)
        {
            var sink = new OfTypeObserver(observer, cancellationToken);

            if (observer is ObserverAsync<TResult> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.SetSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that forwards values matching <typeparamref name="TResult"/>.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class OfTypeObserver(
            IObserverAsync<TResult> downstream,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                value is TResult matched ? downstream.OnNextAsync(matched, cancellationToken) : default;

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
