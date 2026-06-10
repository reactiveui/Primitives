// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for asynchronous observable sequences, enabling functional operations such as scanning
/// and accumulation over streamed data.
/// </summary>
/// <remarks>The methods in this class allow developers to perform stateful transformations and aggregations on
/// asynchronous observables. These operations are useful for scenarios where intermediate results or running totals are
/// needed as items are received. All methods are designed to work with asynchronous patterns and support cancellation
/// via tokens.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Scan (running accumulation) operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Applies an accumulator function over the observable sequence and returns each intermediate result
        /// using the specified asynchronous accumulator.
        /// </summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An asynchronous accumulator function to be invoked on each element. Receives the current accumulator value,
        /// the current element, and a cancellation token.</param>
        /// <returns>An observable sequence containing the accumulated values produced after each element is processed.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="accumulator"/> is null.</exception>
        public IObservableAsync<TAcc> Scan<TAcc>(
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new ScanAsyncSignal<T, TAcc>(@this, seed, accumulator);
        }

        /// <summary>Applies an accumulator function over the observable sequence and returns each intermediate result.</summary>
        /// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
        /// <param name="seed">The initial accumulator value.</param>
        /// <param name="accumulator">An accumulator function to be invoked on each element. Receives the current accumulator value and the
        /// current element.</param>
        /// <returns>An observable sequence containing the accumulated values produced after each element is processed.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="accumulator"/> is null.</exception>
        public IObservableAsync<TAcc> Scan<TAcc>(TAcc seed, Func<TAcc, T, TAcc> accumulator)
        {
            ArgumentExceptionHelper.ThrowIfNull(accumulator);

            return new ScanSyncSignal<T, TAcc>(@this, seed, accumulator);
        }
    }

    /// <summary>
    /// Async-accumulator variant of <see cref="Scan{T,TAcc}(IObservableAsync{T},TAcc,Func{TAcc,T,CancellationToken,ValueTask{TAcc}})"/>.
    /// Allocates one observable wrapper and one sealed observer per subscription — no per-emission closure or
    /// state-machine box from the previous <c>Create&lt;TAcc&gt;((observer, token) =&gt; ...)</c> pattern.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <typeparam name="TAcc">The accumulator type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The asynchronous accumulator.</param>
    internal sealed class ScanAsyncSignal<T, TAcc>(
        IObservableAsync<T> source,
        TAcc seed,
        Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator) : SignalAsync<TAcc>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TAcc> observer,
            CancellationToken cancellationToken)
        {
            var sink = new ScanAsyncWitness(observer, seed, accumulator, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so the downstream's hot path
            // recognises this token without allocating a per-emission linked CTS.
            if (observer is ObserverAsync<TAcc> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that maintains the running accumulator and forwards each result.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="seed">The initial accumulator value used to prime <see cref="_acc"/>.</param>
        /// <param name="accumulator">The asynchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class ScanAsyncWitness(
            IObserverAsync<TAcc> downstream,
            TAcc seed,
            Func<TAcc, T, CancellationToken, ValueTask<TAcc>> accumulator,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>The running accumulator state. Mutated only inside <see cref="OnNextAsyncCore"/>, which the
            /// base observer serializes via its reentrancy gate, so no additional locking is required.</summary>
            private TAcc _acc = seed;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                var pending = accumulator(_acc, value, cancellationToken);
                if (pending.IsCompletedSuccessfully)
                {
                    _acc = pending.Result;
                    return downstream.OnNextAsync(_acc, cancellationToken);
                }

                return AwaitAndForwardAsync(pending, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);

            /// <summary>Slow path for asynchronously-completing accumulators; awaits and then forwards.</summary>
            /// <param name="pending">The pending accumulator <see cref="ValueTask{TResult}"/>.</param>
            /// <param name="cancellationToken">The cancellation token to pass downstream.</param>
            /// <returns>A task that completes after the accumulator resolves and the downstream emission completes.</returns>
            private async ValueTask AwaitAndForwardAsync(ValueTask<TAcc> pending, CancellationToken cancellationToken)
            {
                _acc = await pending.ConfigureAwait(false);
                await downstream.OnNextAsync(_acc, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Synchronous-accumulator variant of <see cref="Scan{T,TAcc}(IObservableAsync{T},TAcc,Func{TAcc,T,TAcc})"/>. Same
    /// allocation profile as <see cref="ScanAsyncSignal{T,TAcc}"/> but the per-emission <c>OnNextAsyncCore</c> is
    /// sync-completed when the downstream completes synchronously.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <typeparam name="TAcc">The accumulator type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The synchronous accumulator.</param>
    internal sealed class ScanSyncSignal<T, TAcc>(
        IObservableAsync<T> source,
        TAcc seed,
        Func<TAcc, T, TAcc> accumulator) : SignalAsync<TAcc>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TAcc> observer,
            CancellationToken cancellationToken)
        {
            var sink = new ScanSyncWitness(observer, seed, accumulator, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so the downstream's hot path
            // recognises this token without allocating a per-emission linked CTS.
            if (observer is ObserverAsync<TAcc> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that maintains the running accumulator and forwards each result.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="seed">The initial accumulator value used to prime <see cref="_acc"/>.</param>
        /// <param name="accumulator">The synchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class ScanSyncWitness(
            IObserverAsync<TAcc> downstream,
            TAcc seed,
            Func<TAcc, T, TAcc> accumulator,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>The running accumulator state. Mutated only inside <see cref="OnNextAsyncCore"/>, which the
            /// base observer serializes via its reentrancy gate, so no additional locking is required.</summary>
            private TAcc _acc = seed;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                _acc = accumulator(_acc, value);
                return downstream.OnNextAsync(_acc, cancellationToken);
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
