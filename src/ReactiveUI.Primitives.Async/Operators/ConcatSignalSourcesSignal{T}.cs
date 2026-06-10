// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Async observable that concatenates inner observable sequences emitted by an outer observable,
/// subscribing to each inner sequence only after the previous one completes.
/// </summary>
/// <typeparam name="T">The type of elements produced by the inner observable sequences.</typeparam>
/// <param name="source">The outer observable sequence that emits inner observable sequences to concatenate.</param>
internal sealed class ConcatSignalSourcesSignal<T>(IObservableAsync<IObservableAsync<T>> source) : SignalAsync<T>
{
    /// <summary>Subscribes the specified observer by creating a <see cref="ConcatCoordinator"/> that manages sequential subscription to inner observables.</summary>
    /// <param name="observer">The observer to receive elements from the concatenated sequences.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down the subscription when disposed.</returns>
    protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        var subscription = new ConcatCoordinator(observer);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeAsync(source, cancellationToken));
    }

    /// <summary>
    /// Manages the lifetime of the outer subscription and buffers inner observables,
    /// subscribing to each one sequentially as the previous completes.
    /// </summary>
    internal sealed class ConcatCoordinator : IAsyncDisposable
    {
        /// <summary>Concurrent queue that buffers inner observables waiting to be subscribed to.</summary>
        private readonly ConcurrentQueue<IObservableAsync<T>> _buffer = new();

        /// <summary>Cancellation token source used to signal disposal of the subscription.</summary>
        private readonly CancellationTokenSource _disposeCts = new();

        /// <summary>Cached cancellation token from the dispose cancellation token source.</summary>
        private readonly CancellationToken _disposedCancellationToken;

        /// <summary>Disposable that holds the single outer subscription.</summary>
        private readonly SingleAssignmentDisposableAsync _outerDisposable = new();

        /// <summary>Serial disposable that holds the currently active inner subscription, disposing the previous one when replaced.</summary>
        private readonly SingleReplaceableDisposableAsync _innerSubscription = new();

        /// <summary>The downstream observer to forward elements to.</summary>
        private readonly IObserverAsync<T> _observer;

        /// <summary>Async gate that serializes observer callbacks to ensure thread-safe emission.</summary>
        private readonly AsyncSerialGate _observerOnSomethingGate = new();

        /// <summary>Indicates whether the outer observable sequence has completed.</summary>
        private bool _outerCompleted;

        /// <summary>Flag indicating whether this subscription has been disposed (1 = disposed, 0 = active).</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="ConcatCoordinator"/> class.</summary>
        /// <param name="observer">The downstream observer to forward elements to.</param>
        public ConcatCoordinator(IObserverAsync<T> observer)
        {
            _observer = observer;
            _disposedCancellationToken = _disposeCts.Token;
        }

        /// <summary>Subscribes to the outer observable sequence.</summary>
        /// <param name="source">The outer observable that emits inner observable sequences.</param>
        /// <param name="subscriptionToken">A token to cancel the subscription.</param>
        /// <returns>A task representing the asynchronous subscribe operation.</returns>
        public async ValueTask SubscribeAsync(
            IObservableAsync<IObservableAsync<T>> source,
            CancellationToken subscriptionToken)
        {
            var outerSubscription = await source.SubscribeAsync(new ConcatOuterWitness(this), subscriptionToken).ConfigureAwait(false);
            await _outerDisposable.SetDisposableAsync(outerSubscription).ConfigureAwait(false);
        }

        /// <summary>
        /// Handles a new inner observable from the outer sequence by buffering it and subscribing
        /// if no inner sequence is currently active.
        /// </summary>
        /// <param name="inner">The inner observable to enqueue.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask AcceptOuterValueAsync(IObservableAsync<T> inner)
        {
            var shouldSubscribe = false;
            lock (_buffer)
            {
                _buffer.Enqueue(inner);
                if (_buffer.Count == 1)
                {
                    shouldSubscribe = true;
                }
            }

            if (!shouldSubscribe)
            {
                return default;
            }

            return SubscribeCurrentInnerAsync(inner);
        }

        /// <summary>
        /// Handles the outer sequence completing, propagating completion downstream when the buffer is empty
        /// or when the outer fails.
        /// </summary>
        /// <param name="result">The completion result from the outer sequence.</param>
        /// <returns>A task representing the asynchronous completion operation.</returns>
        public ValueTask AcceptOuterCompletionAsync(Result result)
        {
            var shouldComplete = false;
            Result? completeResult = null;
            lock (_buffer)
            {
                _outerCompleted = true;
                if (result.IsFailure || _buffer.IsEmpty)
                {
                    shouldComplete = true;
                    completeResult = result;
                }
            }

            return shouldComplete ? FinishAsync(completeResult) : default;
        }

        /// <summary>
        /// Handles the current inner sequence completing, subscribing to the next buffered inner
        /// sequence or completing the subscription if the outer has also completed.
        /// </summary>
        /// <param name="result">The completion result from the inner sequence.</param>
        /// <returns>A task representing the asynchronous completion operation.</returns>
        public ValueTask AcceptInnerCompletionAsync(Result result)
        {
            if (result.IsFailure)
            {
                return FinishAsync(result);
            }

            IObservableAsync<T>? nextInner;
            bool outerCompleted;
            lock (_buffer)
            {
                _buffer.TryDequeue(out _);
                _buffer.TryPeek(out nextInner);
                outerCompleted = _outerCompleted;
            }

            if (nextInner is null)
            {
                return outerCompleted ? FinishAsync(Result.Success) : default;
            }

            return SubscribeCurrentInnerAsync(nextInner);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => FinishAsync(null);

        /// <summary>Handles a second call to <see cref="FinishAsync"/> when already disposed, routing any failure exception to the unhandled exception handler.</summary>
        /// <param name="result">The completion result from the second call.</param>
        internal static void HandleAlreadyDisposed(Result? result)
        {
            if (result?.Exception is not { } exception)
            {
                return;
            }

            UnhandledExceptionHandler.ReportUnhandledException(exception);
        }

        /// <summary>Subscribes to the specified inner observable, setting it as the current active inner subscription.</summary>
        /// <param name="currentInner">The inner observable to subscribe to.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async ValueTask SubscribeCurrentInnerAsync(IObservableAsync<T> currentInner)
        {
            try
            {
                var innerSubscription =
                    await currentInner.SubscribeAsync(new ConcatInnerWitness(this), _disposedCancellationToken).ConfigureAwait(false);
                await _innerSubscription.SetDisposableAsync(innerSubscription).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await FinishAsync(Result.Failure(e)).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Disposes the inner and outer subscriptions and optionally forwards a completion result to
        /// the downstream observer. This method is idempotent.
        /// </summary>
        /// <param name="result">The completion result to forward, or <see langword="null"/> if disposing without signaling completion.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async ValueTask FinishAsync(Result? result)
        {
            if (DisposalHelper.TrySetDisposed(ref _disposed))
            {
                HandleAlreadyDisposed(result);
                return;
            }

            await _disposeCts.CancelAsync().ConfigureAwait(false);
            await _innerSubscription.DisposeAsync().ConfigureAwait(false);
            await _outerDisposable.DisposeAsync().ConfigureAwait(false);
            if (result is not null)
            {
                await _observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            }

            _disposeCts.Dispose();
            _observerOnSomethingGate.Dispose();
        }

        /// <summary>A witness for the outer observable sequence that delegates to the parent <see cref="ConcatCoordinator"/>.</summary>
        /// <param name="subscription">The parent concat subscription.</param>
        internal sealed class ConcatOuterWitness(ConcatCoordinator subscription) : WitnessAsync<IObservableAsync<T>>
        {
            /// <summary>Forwards a new inner observable to the parent subscription for buffering and sequential subscription.</summary>
            /// <param name="value">The new inner observable.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnNextAsyncCore(IObservableAsync<T> value, CancellationToken cancellationToken)
                => subscription.AcceptOuterValueAsync(value);

            /// <summary>Forwards a non-fatal error from the outer sequence to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override async ValueTask OnErrorResumeAsyncCore(
                Exception error,
                CancellationToken cancellationToken)
            {
                // The outer subscription is rooted in _disposedCancellationToken, so its disposal
                // already cascades into this observer's cancellation. Forwarding the dispose token
                // directly preserves the cancellation semantics that a linked CTS would have
                // provided, without the per-emission Linked2CancellationTokenSource alloc.
                _ = cancellationToken;
                var token = subscription._disposedCancellationToken;
                using (await subscription._observerOnSomethingGate.EnterAsync(token).ConfigureAwait(false))
                {
                    await subscription._observer.OnErrorResumeAsync(error, token).ConfigureAwait(false);
                }
            }

            /// <summary>Handles the outer sequence completing.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnCompletedAsyncCore(Result result)
                => subscription.AcceptOuterCompletionAsync(result);
        }

        /// <summary>A witness for the currently active inner observable sequence that delegates to the parent <see cref="ConcatCoordinator"/>.</summary>
        /// <param name="subscription">The parent concat subscription.</param>
        internal sealed class ConcatInnerWitness(ConcatCoordinator subscription) : WitnessAsync<T>
        {
            /// <summary>Forwards an element from the inner sequence to the downstream observer.</summary>
            /// <param name="value">The element to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                var token = subscription._disposedCancellationToken;
                using (await subscription._observerOnSomethingGate.EnterAsync(token).ConfigureAwait(false))
                {
                    await subscription._observer.OnNextAsync(value, token).ConfigureAwait(false);
                }
            }

            /// <summary>Forwards a non-fatal error from the inner sequence to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override async ValueTask OnErrorResumeAsyncCore(
                Exception error,
                CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                var token = subscription._disposedCancellationToken;
                using (await subscription._observerOnSomethingGate.EnterAsync(token).ConfigureAwait(false))
                {
                    await subscription._observer.OnErrorResumeAsync(error, token).ConfigureAwait(false);
                }
            }

            /// <summary>Handles the inner sequence completing, triggering subscription to the next buffered sequence.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnCompletedAsyncCore(Result result)
                => subscription.AcceptInnerCompletionAsync(result);
        }
    }
}
