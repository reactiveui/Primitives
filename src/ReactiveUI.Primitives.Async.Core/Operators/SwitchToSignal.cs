// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Async observable that switches to the most recently emitted inner observable sequence,
/// unsubscribing from the previous inner sequence each time a new one arrives.
/// </summary>
/// <typeparam name="T">The type of elements produced by the inner observable sequences.</typeparam>
/// <param name="source">The outer observable sequence that emits inner observable sequences.</param>
public sealed class SwitchToSignal<T>(IObservableAsync<IObservableAsync<T>> source) : SignalAsync<T>
{
    /// <summary>Subscribes the specified observer by creating a <see cref="SwitchToCoordinator"/> that manages the outer and inner observable lifetimes.</summary>
    /// <param name="observer">The observer to receive elements from the most recent inner sequence.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down the subscription when disposed.</returns>
    protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        SwitchToCoordinator subscription = new(observer);
        subscription.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeAsync(source, cancellationToken));
    }

    /// <summary>
    /// Manages the lifetime of the outer subscription and the currently active inner subscription,
    /// switching to new inner sequences as they arrive.
    /// </summary>
    internal sealed class SwitchToCoordinator : IAsyncDisposable
    {
        /// <summary>The downstream observer to forward elements to.</summary>
        private readonly IObserverAsync<T> _observer;

        /// <summary>Disposable that holds the single outer subscription.</summary>
        private readonly SingleAssignmentDisposableAsync _outerDisposable = new();

        /// <summary>Cancellation token source used to signal disposal of the subscription.</summary>
        private readonly CancellationTokenSource _disposeCts = new();

        /// <summary>Cached cancellation token from the dispose cancellation token source.</summary>
        private readonly CancellationToken _disposeCancellationToken;

        /// <summary>Lock that protects mutable state from concurrent access.</summary>
        private readonly Lock _gate = new();

        /// <summary>Async gate that serializes observer callbacks to ensure thread-safe emission.</summary>
        private readonly AsyncSerialGate _observerOnSomethingGate = new();

        /// <summary>Registration that propagates the original subscribe-token cancellation into <see cref="_disposeCts"/>.</summary>
        private CancellationTokenRegistration _externalLinkRegistration;

        /// <summary>The currently active inner subscription, or <see langword="null"/> if none is active.</summary>
        private IAsyncDisposable? _currentInnerSubscription;

        /// <summary>Indicates whether the outer observable sequence has completed.</summary>
        private bool _outerCompleted;

        /// <summary>Indicates whether this subscription has been disposed.</summary>
        private bool _disposed;

        /// <summary>Initializes a new instance of the <see cref="SwitchToCoordinator"/> class.</summary>
        /// <param name="observer">The downstream observer to forward elements to.</param>
        public SwitchToCoordinator(IObserverAsync<T> observer)
        {
            _observer = observer;
            _disposeCancellationToken = _disposeCts.Token;
        }

        /// <summary>Subscribes to the outer observable sequence.</summary>
        /// <param name="source">The outer observable that emits inner observable sequences.</param>
        /// <param name="subscriptionToken">A token to cancel the subscription.</param>
        /// <returns>A task representing the asynchronous subscribe operation.</returns>
        public async ValueTask SubscribeAsync(
            IObservableAsync<IObservableAsync<T>> source,
            CancellationToken subscriptionToken)
        {
            var outerSubscription = await source.SubscribeAsync(new SwitchToOuterWitness(this), subscriptionToken).ConfigureAwait(false);
            await _outerDisposable.SetDisposableAsync(outerSubscription).ConfigureAwait(false);
        }

        /// <summary>
        /// Handles a new inner observable from the outer sequence by disposing the previous inner subscription
        /// and subscribing to the new one.
        /// </summary>
        /// <param name="inner">The new inner observable to switch to.</param>
        /// <returns>A task representing the asynchronous switch operation.</returns>
        public ValueTask AcceptOuterValueAsync(IObservableAsync<T> inner)
        {
            IAsyncDisposable? previousSubscription;
            lock (_gate)
            {
                previousSubscription = _currentInnerSubscription;
                _currentInnerSubscription = null;
            }

            return SubscribeReplacementInnerAsync(inner, previousSubscription);
        }

        /// <summary>
        /// Handles the outer sequence completing, propagating completion downstream when no inner
        /// sequence is active or when the outer fails.
        /// </summary>
        /// <param name="result">The completion result from the outer sequence.</param>
        /// <returns>A task representing the asynchronous completion operation.</returns>
        public ValueTask AcceptOuterCompletionAsync(Result result)
        {
            if (result.IsFailure)
            {
                return FinishAsync(result);
            }

            bool shouldComplete;
            lock (_gate)
            {
                _outerCompleted = true;
                shouldComplete = _currentInnerSubscription is null;
            }

            return shouldComplete ? FinishAsync(Result.Success) : default;
        }

        /// <summary>
        /// Handles the current inner sequence completing, propagating completion downstream
        /// if the outer has also completed, or waiting for the next inner sequence otherwise.
        /// </summary>
        /// <param name="result">The completion result from the inner sequence.</param>
        /// <returns>A task representing the asynchronous completion operation.</returns>
        public ValueTask AcceptInnerCompletionAsync(Result result)
        {
            Result? actualResult = null;
            lock (_gate)
            {
                _currentInnerSubscription = null;
                if (result.IsFailure)
                {
                    actualResult = result;
                }
                else if (_outerCompleted)
                {
                    actualResult = Result.Success;
                }
            }

            return actualResult is not null ? FinishAsync(actualResult) : default;
        }

        /// <summary>Forwards an element from the current inner sequence to the downstream observer.</summary>
        /// <param name="value">The element to forward.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous forward operation.</returns>
        public async ValueTask AcceptInnerValueAsync(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            using (await _observerOnSomethingGate.EnterAsync(_disposeCancellationToken).ConfigureAwait(false))
            {
                await _observer.OnNextAsync(value, _disposeCancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Forwards a non-fatal error from the current inner sequence to the downstream observer.</summary>
        /// <param name="error">The error to forward.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous error forwarding operation.</returns>
        public async ValueTask AcceptInnerErrorAsync(Exception error, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            using (await _observerOnSomethingGate.EnterAsync(_disposeCancellationToken).ConfigureAwait(false))
            {
                await _observer.OnErrorResumeAsync(error, _disposeCancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => FinishAsync(null);

        /// <summary>
        /// Links the original subscribe-time cancellation token into this subscription's dispose chain so
        /// later per-emission methods can rely on <see cref="_disposeCancellationToken"/> instead of
        /// allocating a per-emission linked CTS.
        /// </summary>
        /// <param name="external">The subscribe-time token.</param>
        internal void LinkExternalCancellation(CancellationToken external)
        {
            if (!external.CanBeCanceled || external == _disposeCancellationToken)
            {
                return;
            }

            if (external.IsCancellationRequested)
            {
                _disposeCts.Cancel();
                return;
            }

            _externalLinkRegistration = external.UnsafeRegister(
                static state => ((CancellationTokenSource)state!).Cancel(),
                _disposeCts);
        }

        /// <summary>Disposes the previous inner subscription (if any) and subscribes to the new inner observable.</summary>
        /// <param name="inner">The new inner observable to subscribe to.</param>
        /// <param name="previousSubscription">The previous inner subscription to dispose, or <see langword="null"/> if none.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async ValueTask SubscribeReplacementInnerAsync(
            IObservableAsync<T> inner,
            IAsyncDisposable? previousSubscription)
        {
            try
            {
                if (previousSubscription is not null)
                {
                    try
                    {
                        await previousSubscription.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        await FinishAsync(Result.Failure(e)).ConfigureAwait(false);
                        return;
                    }
                }

                SwitchToInnerWitness innerObserver = new(this);
                var innerSubscription = await inner.SubscribeAsync(innerObserver, _disposeCancellationToken).ConfigureAwait(false);
                var shouldDispose = false;
                lock (_gate)
                {
                    if (!_disposed)
                    {
                        _currentInnerSubscription = innerSubscription;
                    }
                    else
                    {
                        shouldDispose = true;
                    }
                }

                if (shouldDispose)
                {
                    await innerSubscription.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                await FinishAsync(Result.Failure(e)).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Disposes the current inner subscription, the outer subscription, and optionally forwards a
        /// completion result to the downstream observer. This method is idempotent.
        /// </summary>
        /// <param name="result">The completion result to forward, or <see langword="null"/> if disposing without signaling completion.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async ValueTask FinishAsync(Result? result)
        {
            IAsyncDisposable? toDispose;
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                toDispose = _currentInnerSubscription;
                _currentInnerSubscription = null;
            }

            await _disposeCts.CancelAsync().ConfigureAwait(false);
            if (toDispose is not null)
            {
                await toDispose.DisposeAsync().ConfigureAwait(false);
            }

            await _outerDisposable.DisposeAsync().ConfigureAwait(false);

            if (result is not null)
            {
                await _observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            }

#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
            await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
            _externalLinkRegistration.Dispose();
#endif
            _disposeCts.Dispose();
            _observerOnSomethingGate.Dispose();
        }

        /// <summary>Witness for the outer observable sequence that delegates to the parent <see cref="SwitchToCoordinator"/>.</summary>
        /// <param name="subscription">The parent switch subscription.</param>
        internal sealed class SwitchToOuterWitness(SwitchToCoordinator subscription) : WitnessAsync<IObservableAsync<T>>
        {
            /// <summary>Forwards a new inner observable to the parent subscription for switching.</summary>
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
                _ = cancellationToken;
                using (await subscription._observerOnSomethingGate.EnterAsync(subscription._disposeCancellationToken).ConfigureAwait(false))
                {
                    await subscription._observer.OnErrorResumeAsync(error, subscription._disposeCancellationToken).ConfigureAwait(false);
                }
            }

            /// <summary>Handles the outer sequence completing.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnCompletedAsyncCore(Result result)
                => subscription.AcceptOuterCompletionAsync(result);
        }

        /// <summary>Witness for the currently active inner observable sequence that delegates to the parent <see cref="SwitchToCoordinator"/>.</summary>
        /// <param name="subscription">The parent switch subscription.</param>
        internal sealed class SwitchToInnerWitness(SwitchToCoordinator subscription) : WitnessAsync<T>
        {
            /// <summary>Forwards an element from the inner sequence to the downstream witness.</summary>
            /// <param name="value">The element to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
                => subscription.AcceptInnerValueAsync(value, cancellationToken);

            /// <summary>Forwards a non-fatal error from the inner sequence to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
                => subscription.AcceptInnerErrorAsync(error, cancellationToken);

            /// <summary>Handles the inner sequence completing.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnCompletedAsyncCore(Result result)
                => subscription.AcceptInnerCompletionAsync(result);
        }
    }
}
