// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and manipulating asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class offers LINQ-style operators for working with asynchronous observables,
/// enabling developers to compose, filter, and transform event streams in an asynchronous context. These methods are
/// designed to integrate with the SignalAsync{T} type, supporting both synchronous and asynchronous predicate
/// functions for filtering sequences.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Filtering operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Creates a new observable sequence that contains only the elements from the source sequence that satisfy the
        /// specified asynchronous predicate.
        /// </summary>
        /// <remarks>The predicate is invoked asynchronously for each element as it is observed. If the
        /// predicate throws an exception or the ValueTask is faulted, the resulting sequence will propagate the error
        /// to its observers. The cancellation token provided to the predicate can be used to observe cancellation
        /// requests during predicate evaluation.</remarks>
        /// <param name="predicate">A function that evaluates each element and its associated cancellation token, returning a ValueTask that
        /// resolves to <see langword="true"/> to include the element in the resulting sequence; otherwise, <see
        /// langword="false"/>.</param>
        /// <returns>An observable sequence that emits only those elements for which the predicate returns <see
        /// langword="true"/>.</returns>
        public IObservableAsync<T> Where(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            new WhereAsyncSignal<T>(@this, predicate);

        /// <summary>
        /// Creates a new observable sequence that contains only the elements from the current sequence that satisfy the
        /// specified predicate.
        /// </summary>
        /// <remarks>The resulting observable emits only those elements for which the <paramref
        /// name="predicate"/> returns <see langword="true"/>. The order and timing of element emission are preserved
        /// from the original sequence.</remarks>
        /// <param name="predicate">A function to test each element for a condition. The element is included in the resulting sequence if the
        /// function returns <see langword="true"/>.</param>
        /// <returns>An observable sequence that contains elements from the current sequence that satisfy the specified
        /// predicate.</returns>
        public IObservableAsync<T> Where(Func<T, bool> predicate) =>
            new WhereSyncSignal<T>(@this, predicate);
    }

    /// <summary>
    /// Async-predicate variant of <see cref="Where{T}(IObservableAsync{T}, Func{T,CancellationToken,ValueTask{bool}})"/>.
    /// Allocates one observable wrapper and one sealed observer per subscription — no per-emission closure or
    /// state-machine box from the previous <c>Create&lt;T&gt;((observer, token) =&gt; ...)</c> pattern.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The asynchronous predicate.</param>
    internal sealed class WhereAsyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new WhereAsyncWitness(observer, predicate, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so the downstream's hot path
            // recognises this token without allocating a per-emission linked CTS.
            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that applies the async predicate and forwards passing elements.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The async predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class WhereAsyncWitness(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask<bool>> predicate,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (await predicate(value, cancellationToken).ConfigureAwait(false))
                {
                    await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                }
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
    /// Synchronous-predicate variant of <see cref="Where{T}(IObservableAsync{T}, Func{T,bool})"/>. Same allocation
    /// profile as <see cref="WhereAsyncSignal{T}"/> but the per-emission <c>OnNextAsyncCore</c> is sync-completed
    /// when the predicate rejects, avoiding any state-machine box on rejection.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The synchronous predicate.</param>
    internal sealed class WhereSyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, bool> predicate) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new WhereSyncWitness(observer, predicate, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so the downstream's hot path
            // recognises this token without allocating a per-emission linked CTS.
            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that applies the sync predicate and forwards passing elements.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The sync predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class WhereSyncWitness(
            IObserverAsync<T> downstream,
            Func<T, bool> predicate,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (!predicate(value))
                {
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
