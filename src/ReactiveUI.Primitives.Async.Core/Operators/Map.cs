// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and transforming asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Projection (Map/Select) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Projects each element of the observable sequence into a new form using the specified asynchronous selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function and produced by the resulting observable sequence.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a value of type <typeparamref
        /// name="TDest"/> asynchronously. The function receives the source element and a cancellation token.</param>
        /// <returns>An observable sequence of type <typeparamref name="TDest"/> containing the results of applying the selector
        /// function to each element of the source sequence.</returns>
        /// <remarks>The selector runs for each element as it is observed; a thrown exception or a faulted task is
        /// propagated to the observer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Map<TDest>(
            Func<T, CancellationToken, ValueTask<TDest>> selector) =>
            new MapAsyncSignal<T, TDest>(source, selector);

        /// <summary>Projects each element of the observable sequence into a new form using the specified selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a new value. Cannot be null.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the selector function on each element of
        /// the source sequence.</returns>
        /// <remarks>The selector runs for each element as it is observed; a thrown exception is propagated to the
        /// observer.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Map<TDest>(
            Func<T, TDest> selector) =>
            new MapSyncSignal<T, TDest>(source, selector);

        /// <summary>Projects each value using caller-supplied state.</summary>
        /// <typeparam name="TState">The caller-supplied state type.</typeparam>
        /// <typeparam name="TDest">The result element type.</typeparam>
        /// <param name="state">The caller-supplied state passed to the selector.</param>
        /// <param name="selector">The projection applied to each value and the state.</param>
        /// <returns>An observable sequence of projected values.</returns>
        public IObservableAsync<TDest> MapWith<TState, TDest>(
            TState state,
            Func<TState, T, TDest> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new MapSyncSignal<T, TDest>(source, value => selector(state, value));
        }

        /// <summary>Projects each element of the observable sequence into a new form using the specified asynchronous selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function and produced by the resulting observable sequence.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a value.</param>
        /// <returns>An observable sequence containing the results of applying the selector function to each source element.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Select<TDest>(
            Func<T, CancellationToken, ValueTask<TDest>> selector) =>
            new MapAsyncSignal<T, TDest>(source, selector);

        /// <summary>Projects each element of the observable sequence into a new form using the specified selector function.</summary>
        /// <typeparam name="TDest">The type of the value returned by the selector function.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a new value.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the selector function.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<TDest> Select<TDest>(
            Func<T, TDest> selector) =>
            new MapSyncSignal<T, TDest>(source, selector);
    }

    /// <summary>Applies an asynchronous selector to each source value, allocating one observer per subscription.</summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <typeparam name="TDest">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The asynchronous selector.</param>
    internal sealed class MapAsyncSignal<T, TDest>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<TDest>> selector) : IObservableAsync<TDest>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<TDest>.SubscribeAsync(
            IObserverAsync<TDest> observer,
            CancellationToken cancellationToken)
        {
            MapAsyncWitness sink = new(observer, selector, cancellationToken);

            if (observer is WitnessAsync<TDest> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that applies the async selector and forwards each result.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="selector">The async selector.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class MapAsyncWitness(
            IObserverAsync<TDest> downstream,
            Func<T, CancellationToken, ValueTask<TDest>> selector,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                var mapped = await selector(value, cancellationToken).ConfigureAwait(false);
                await downstream.OnNextAsync(mapped, cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Applies a synchronous selector to each source value, forwarding without an await state machine.</summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <typeparam name="TDest">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The synchronous selector.</param>
    internal sealed class MapSyncSignal<T, TDest>(
        IObservableAsync<T> source,
        Func<T, TDest> selector) : IObservableAsync<TDest>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<TDest>.SubscribeAsync(
            IObserverAsync<TDest> observer,
            CancellationToken cancellationToken)
        {
            MapSyncWitness sink = new(observer, selector, cancellationToken);

            if (observer is WitnessAsync<TDest> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that applies the sync selector and forwards each result.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="selector">The sync selector.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class MapSyncWitness(
            IObserverAsync<TDest> downstream,
            Func<T, TDest> selector,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                downstream.OnNextAsync(selector(value), cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
