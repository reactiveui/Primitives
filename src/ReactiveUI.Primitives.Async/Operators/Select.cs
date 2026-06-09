// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and transforming asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable functional-style operations, such as projection, on asynchronous
/// observables. These extensions facilitate composing and manipulating streams of data in an asynchronous context,
/// similar to LINQ operations for synchronous observables.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Projection (Select) operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Projects each element of the observable sequence into a new form using the specified asynchronous selector function.</summary>
        /// <remarks>The selector function is invoked for each element as it is observed. If the selector
        /// function throws an exception or returns a faulted task, the error is propagated to the observer. The
        /// operation supports cancellation via the provided cancellation token.</remarks>
        /// <typeparam name="TDest">The type of the value returned by the selector function and produced by the resulting observable sequence.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a value of type <typeparamref
        /// name="TDest"/> asynchronously. The function receives the source element and a cancellation token.</param>
        /// <returns>An observable sequence of type <typeparamref name="TDest"/> containing the results of applying the selector
        /// function to each element of the source sequence.</returns>
        public IObservableAsync<TDest> Select<TDest>(
            Func<T, CancellationToken, ValueTask<TDest>> selector) =>
            new SelectAsyncSignal<T, TDest>(@this, selector);

        /// <summary>Projects each element of the observable sequence into a new form using the specified selector function.</summary>
        /// <remarks>The selector function is applied to each element as it is observed. If the selector
        /// throws an exception, the error is propagated to the observer. This method does not modify the source
        /// sequence; it produces a new sequence with transformed elements.</remarks>
        /// <typeparam name="TDest">The type of the value returned by the selector function.</typeparam>
        /// <param name="selector">A function that transforms each element of the source sequence into a new value. Cannot be null.</param>
        /// <returns>An observable sequence whose elements are the result of invoking the selector function on each element of
        /// the source sequence.</returns>
        public IObservableAsync<TDest> Select<TDest>(
            Func<T, TDest> selector) =>
            new SelectSyncSignal<T, TDest>(@this, selector);
    }

    /// <summary>
    /// Async-selector variant of <see cref="Select{T,TDest}(IObservableAsync{T}, Func{T,CancellationToken,ValueTask{TDest}})"/>.
    /// Allocates one observable wrapper and one sealed observer per subscription — no per-emission closure or
    /// state-machine box from the previous <c>Create&lt;TDest&gt;((observer, token) =&gt; ...)</c> pattern.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <typeparam name="TDest">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The asynchronous selector.</param>
    internal sealed class SelectAsyncSignal<T, TDest>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<TDest>> selector) : SignalAsync<TDest>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TDest> observer,
            CancellationToken cancellationToken)
        {
            var sink = new SelectAsyncWitness(observer, selector, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so the downstream's hot path
            // recognises this token without allocating a per-emission linked CTS.
            if (observer is ObserverAsync<TDest> downstreamBase)
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
        internal sealed class SelectAsyncWitness(
            IObserverAsync<TDest> downstream,
            Func<T, CancellationToken, ValueTask<TDest>> selector,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
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

    /// <summary>
    /// Synchronous-selector variant of <see cref="Select{T,TDest}(IObservableAsync{T}, Func{T,TDest})"/>. Same
    /// allocation profile as <see cref="SelectAsyncSignal{T,TDest}"/> but the per-emission <c>OnNextAsyncCore</c>
    /// is sync-completed so no state-machine box is allocated when the downstream completes synchronously.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <typeparam name="TDest">The projected element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The synchronous selector.</param>
    internal sealed class SelectSyncSignal<T, TDest>(
        IObservableAsync<T> source,
        Func<T, TDest> selector) : SignalAsync<TDest>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TDest> observer,
            CancellationToken cancellationToken)
        {
            var sink = new SelectSyncWitness(observer, selector, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so the downstream's hot path
            // recognises this token without allocating a per-emission linked CTS.
            if (observer is ObserverAsync<TDest> downstreamBase)
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
        internal sealed class SelectSyncWitness(
            IObserverAsync<TDest> downstream,
            Func<T, TDest> selector,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
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
