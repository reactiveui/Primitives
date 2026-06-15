// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static methods that extend the functionality of asynchronous
/// observables, enabling advanced filtering, transformation, and composition operations. These methods are intended to
/// be used with types implementing asynchronous observable patterns, such as SignalAsync{T}.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Type-filtering operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Projects each element of the observable sequence to the specified reference type and filters out elements
        /// that are not of that type.
        /// </summary>
        /// <remarks>Elements that are not of type TResult are ignored and not included in the resulting
        /// sequence. This method is useful for working with observable sequences containing heterogeneous types,
        /// allowing subscribers to focus on elements of a specific type.</remarks>
        /// <typeparam name="TResult">The reference type to filter and project elements to. Must be a class.</typeparam>
        /// <returns>An observable sequence containing only the elements of type TResult from the original sequence.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S4018:Generic methods should provide type parameters",
            Justification = "Public extension API — caller specifies TResult explicitly: source.OfType<Derived>().")]
        public IObservableAsync<TResult> OfType<TResult>()
            where TResult : class
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);

            return new OfTypeSignal<T, TResult>(@this);
        }
    }

    /// <summary>Type-filtering operators for an untyped observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservableAsync<object?> source)
    {
        /// <summary>Keeps values assignable to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result element type to keep.</typeparam>
        /// <returns>An observable sequence of values assignable to <typeparamref name="TResult"/>.</returns>
        [SuppressMessage(
            "Minor Code Smell",
            "S4018:All type parameters should be used in the parameter list to enable type inference",
            Justification = "Deliberate lack of type inference.")]
        public IObservableAsync<TResult> KeepType<TResult>()
            where TResult : class
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new OfTypeSignal<object?, TResult>(source);
        }
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
            OfTypeWitness sink = new(observer, cancellationToken);

            if (observer is WitnessAsync<TResult> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that forwards values matching <typeparamref name="TResult"/>.</summary>
        /// <param name="downstream">The downstream witness.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class OfTypeWitness(
            IObserverAsync<TResult> downstream,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
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
