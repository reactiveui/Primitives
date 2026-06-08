// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides SkipWhile extension methods for asynchronous observable sequences.</summary>
/// <remarks>SkipWhile bypasses elements in the source sequence as long as a predicate is satisfied,
/// then emits all remaining elements.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>SkipWhile operators for an observable source sequence.</summary>
    /// <param name="this">The source observable sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Bypasses elements in the observable sequence as long as the specified asynchronous condition is true,
        /// then emits all remaining elements.
        /// </summary>
        /// <param name="predicate">An asynchronous function to test each element for a condition. Receives the element
        /// and a cancellation token.</param>
        /// <returns>An observable sequence that skips elements while the predicate returns true and emits
        /// all subsequent elements.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public IObservableAsync<T> SkipWhile(Func<T, CancellationToken, ValueTask<bool>> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new SkipWhileAsyncSignal<T>(@this, predicate);
        }

        /// <summary>
        /// Bypasses elements in the observable sequence as long as the specified condition is true,
        /// then emits all remaining elements.
        /// </summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that skips elements while the predicate returns true and emits
        /// all subsequent elements.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="predicate"/> is null.</exception>
        public IObservableAsync<T> SkipWhile(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new SkipWhileSyncSignal<T>(@this, predicate);
        }
    }

    /// <summary>
    /// Synchronous-predicate <c>SkipWhile</c> as a single-observer-layer observable; once the
    /// predicate returns <see langword="false"/> the gate latches and every subsequent emission
    /// forwards without a predicate call.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The skip-while predicate.</param>
    internal sealed class SkipWhileSyncSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new SkipWhileSyncObserver(observer, predicate, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer maintaining the latched-gate state.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The skip-while predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class SkipWhileSyncObserver(
            IObserverAsync<T> downstream,
            Func<T, bool> predicate,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>Latches to <see langword="false"/> once the predicate fails — every subsequent value forwards.</summary>
            private bool _skipping = true;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (_skipping)
                {
                    if (predicate(value))
                    {
                        return default;
                    }

                    _skipping = false;
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

    /// <summary>
    /// Async-predicate <c>SkipWhile</c> as a single-observer-layer observable with a sync-completion
    /// fast path for the post-latch case.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The async skip-while predicate.</param>
    internal sealed class SkipWhileAsyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new SkipWhileAsyncObserver(observer, predicate, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer with latched gate; once gated, async predicate is no longer invoked.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The async skip-while predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class SkipWhileAsyncObserver(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask<bool>> predicate,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>Latches to <see langword="false"/> after the predicate first returns <see langword="false"/>.</summary>
            private bool _skipping = true;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (!_skipping)
                {
                    return downstream.OnNextAsync(value, cancellationToken);
                }

                var pending = predicate(value, cancellationToken);
                if (pending.IsCompletedSuccessfully)
                {
                    if (pending.Result)
                    {
                        return default;
                    }

                    _skipping = false;
                    return downstream.OnNextAsync(value, cancellationToken);
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
            /// <returns>A task that completes after the predicate resolves and (if not skipped) the downstream forward completes.</returns>
            private async ValueTask EvaluateAndForwardAsync(ValueTask<bool> pending, T value, CancellationToken cancellationToken)
            {
                if (await pending.ConfigureAwait(false))
                {
                    return;
                }

                _skipping = false;
                await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
