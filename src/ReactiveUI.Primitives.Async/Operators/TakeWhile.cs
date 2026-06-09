// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides TakeWhile extension methods for asynchronous observable sequences.</summary>
/// <remarks>TakeWhile emits elements from the source sequence as long as a predicate is satisfied,
/// then completes the sequence when the predicate returns false.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>TakeWhile operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>Returns elements from the observable sequence as long as the specified asynchronous condition is true, then completes.</summary>
        /// <param name="predicate">An asynchronous function to test each element for a condition. Receives the element
        /// and a cancellation token.</param>
        /// <returns>An observable sequence that contains elements from the source sequence that satisfy the
        /// condition, completing as soon as the predicate returns false.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public IObservableAsync<T> TakeWhile(Func<T, CancellationToken, ValueTask<bool>> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new TakeWhileAsyncSignal<T>(@this, predicate);
        }

        /// <summary>Returns elements from the observable sequence as long as the specified condition is true, then completes.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that contains elements from the source sequence that satisfy the
        /// condition, completing as soon as the predicate returns false.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public IObservableAsync<T> TakeWhile(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new TakeWhileSyncSignal<T>(@this, predicate);
        }
    }

    /// <summary>
    /// Synchronous-predicate <c>TakeWhile</c> as a single-observer-layer observable. Forwards while
    /// the predicate holds; on the first miss signals downstream completion and stops.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The take-while predicate.</param>
    internal sealed class TakeWhileSyncSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new TakeWhileSyncWitness(observer, predicate, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that forwards while the predicate holds.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The take-while predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class TakeWhileSyncWitness(
            IObserverAsync<T> downstream,
            Func<T, bool> predicate,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>Latches to <see langword="true"/> once the predicate has returned <see langword="false"/>.</summary>
            private bool _terminated;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (_terminated)
                {
                    return default;
                }

                if (predicate(value))
                {
                    return downstream.OnNextAsync(value, cancellationToken);
                }

                _terminated = true;
                return downstream.OnCompletedAsync(Result.Success);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Async-predicate <c>TakeWhile</c> as a single-observer-layer observable with a sync-completion fast path.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The async take-while predicate.</param>
    internal sealed class TakeWhileAsyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new TakeWhileAsyncWitness(observer, predicate, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer with an async predicate sync-completion fast path.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The async take-while predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class TakeWhileAsyncWitness(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask<bool>> predicate,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>Latches to <see langword="true"/> once the predicate has returned <see langword="false"/>.</summary>
            private bool _terminated;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (_terminated)
                {
                    return default;
                }

                var pending = predicate(value, cancellationToken);
                if (pending.IsCompletedSuccessfully)
                {
                    if (pending.Result)
                    {
                        return downstream.OnNextAsync(value, cancellationToken);
                    }

                    _terminated = true;
                    return downstream.OnCompletedAsync(Result.Success);
                }

                return EvaluateAndForwardAsync(pending, value, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);

            /// <summary>Slow path when the async predicate does not complete synchronously.</summary>
            /// <param name="pending">The pending predicate evaluation.</param>
            /// <param name="value">The candidate value.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <returns>A task that completes after the predicate resolves and the downstream emission / completion runs.</returns>
            private async ValueTask EvaluateAndForwardAsync(ValueTask<bool> pending, T value, CancellationToken cancellationToken)
            {
                if (await pending.ConfigureAwait(false))
                {
                    await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                    return;
                }

                _terminated = true;
                await downstream.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            }
        }
    }
}
