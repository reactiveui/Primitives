// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Type-casting operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Projects each element of the observable sequence to the specified result type by performing a runtime cast.</summary>
        /// <typeparam name="TResult">The type to which the elements of the sequence are cast.</typeparam>
        /// <returns>An observable sequence whose elements are the result of casting each element of the source sequence to
        /// <typeparamref name="TResult"/>.</returns>
        /// <remarks>A failed cast does not throw at the call site: the sequence completes with a failure carrying the
        /// cast exception.</remarks>
        [SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "The caller chooses TResult; no parameter carries it.")]
        public IObservableAsync<TResult> Cast<TResult>()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CastSignal<T, TResult>(source);
        }
    }

    /// <summary>Type-casting operators for an untyped observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    extension(IObservableAsync<object?> source)
    {
        /// <summary>Casts each value to <typeparamref name="TResult"/>.</summary>
        /// <typeparam name="TResult">The result element type to cast to.</typeparam>
        /// <returns>An observable sequence of values cast to <typeparamref name="TResult"/>.</returns>
        [SuppressMessage(
            "Design",
            "SST2307:Generic method type parameters should be inferable from the parameters",
            Justification = "The caller chooses TResult; no parameter carries it.")]
        public IObservableAsync<TResult> CastTo<TResult>()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new CastSignal<object?, TResult>(source);
        }
    }

    /// <summary>Single-observer-layer <c>Cast</c>; failed casts terminate the sequence with failure.</summary>
    /// <typeparam name="T">The upstream element type.</typeparam>
    /// <typeparam name="TResult">The target element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    internal sealed class CastSignal<T, TResult>(IObservableAsync<T> source) : IObservableAsync<TResult>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
            IObserverAsync<TResult> observer,
            CancellationToken cancellationToken)
        {
            CastWitness sink = new(observer, cancellationToken);

            if (observer is IWitnessAsync<TResult> downstreamBase)
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
        [DebuggerDisplay("CastWitness: {_witness}")]
        internal sealed class CastWitness(
            IObserverAsync<TResult> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
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
