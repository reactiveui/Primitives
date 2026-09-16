// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Async observable that switches to the most recently emitted inner observable sequence, unsubscribing from the previous inner sequence each time a new one arrives.</summary>
/// <typeparam name="T">The type of elements produced by the inner observable sequences.</typeparam>
/// <param name="source">The outer observable sequence that emits inner observable sequences.</param>
public sealed class SwitchToSignal<T>(IObservableAsync<IObservableAsync<T>> source) : IObservableAsync<T>
{
    /// <summary>Subscribes the specified observer by creating a <see cref="SwitchToCoordinator"/> that manages the outer and inner observable lifetimes.</summary>
    /// <param name="observer">The observer to receive elements from the most recent inner sequence.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down the subscription when disposed.</returns>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        SwitchToCoordinator subscription = new(observer);
        subscription.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeAsync(source, cancellationToken));
    }

    /// <summary>Manages the lifetime of the outer subscription and the currently active inner subscription, switching to new inner sequences as they arrive.</summary>
    internal sealed class SwitchToCoordinator : IAsyncDisposable
    {
        /// <summary>The downstream observer to forward elements to.</summary>
        private readonly IObserverAsync<T> _observer;

        /// <summary>Disposable that holds the single outer subscription.</summary>
        private readonly SingleAssignmentDisposableAsync _outerDisposable = new();

        /// <summary>The cancellation token source that signals disposal of the subscription.</summary>
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

        /// <summary>The generation of the most recent inner sequence; completions and subscriptions from older generations are stale.</summary>
        private long _innerGeneration;

        /// <summary>Indicates whether the most recent inner sequence has not yet completed.</summary>
        private bool _innerActive;

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

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => FinishAsync(null);

        /// <summary>Subscribes to the outer observable sequence.</summary>
        /// <param name="source">The outer observable that emits inner observable sequences.</param>
        /// <param name="subscriptionToken">A token to cancel the subscription.</param>
        /// <returns>A task representing the asynchronous subscribe operation.</returns>
        internal async ValueTask SubscribeAsync(
            IObservableAsync<IObservableAsync<T>> source,
            CancellationToken subscriptionToken)
        {
            var outerSubscription = await source.SubscribeAsync(new SwitchToOuterWitness(this), subscriptionToken)
                .ConfigureAwait(false);
            await _outerDisposable.SetDisposableAsync(outerSubscription).ConfigureAwait(false);
        }

        /// <summary>Handles a new inner observable from the outer sequence by disposing the previous inner subscription and subscribing to the new one.</summary>
        /// <param name="inner">The new inner observable to switch to.</param>
        /// <returns>A task representing the asynchronous switch operation.</returns>
        internal ValueTask AcceptOuterValueAsync(IObservableAsync<T> inner)
        {
            IAsyncDisposable? previousSubscription;
            long generation;
            lock (_gate)
            {
                previousSubscription = _currentInnerSubscription;
                _currentInnerSubscription = null;
                generation = ++_innerGeneration;
                _innerActive = true;
            }

            return SubscribeReplacementInnerAsync(inner, previousSubscription, generation);
        }

        /// <summary>Handles the outer sequence completing, propagating completion downstream when no inner sequence is active or when the outer fails.</summary>
        /// <param name="result">The completion result from the outer sequence.</param>
        /// <returns>A task representing the asynchronous completion operation.</returns>
        internal ValueTask AcceptOuterCompletionAsync(Result result)
        {
            if (result.IsFailure)
            {
                return FinishAsync(result);
            }

            bool shouldComplete;
            lock (_gate)
            {
                _outerCompleted = true;
                shouldComplete = !_innerActive;
            }

            return shouldComplete ? FinishAsync(Result.Success) : default;
        }

        /// <summary>Handles the current inner sequence completing, propagating completion downstream if the outer has also completed, or waiting for the next inner sequence otherwise.</summary>
        /// <param name="generation">The generation of the inner sequence that completed.</param>
        /// <param name="result">The completion result from the inner sequence.</param>
        /// <returns>A task representing the asynchronous completion operation.</returns>
        internal ValueTask AcceptInnerCompletionAsync(long generation, Result result)
        {
            Result? actualResult = null;
            lock (_gate)
            {
                if (generation == _innerGeneration)
                {
                    _currentInnerSubscription = null;
                    _innerActive = false;
                }

                if (result.IsFailure)
                {
                    actualResult = result;
                }
                else if (_outerCompleted && !_innerActive)
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
        internal async ValueTask AcceptInnerValueAsync(T value, CancellationToken cancellationToken)
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
        internal async ValueTask AcceptInnerErrorAsync(Exception error, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            using (await _observerOnSomethingGate.EnterAsync(_disposeCancellationToken).ConfigureAwait(false))
            {
                await _observer.OnErrorResumeAsync(error, _disposeCancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Routes cancellation of the subscribe-time token into <see cref="_disposeCts"/>, so per-emission code needs no linked source.</summary>
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
        /// <param name="generation">The generation assigned to <paramref name="inner"/>.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async ValueTask SubscribeReplacementInnerAsync(
            IObservableAsync<T> inner,
            IAsyncDisposable? previousSubscription,
            long generation)
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

                SwitchToInnerWitness innerObserver = new(this, generation);
                var innerSubscription = await inner.SubscribeAsync(innerObserver, _disposeCancellationToken)
                    .ConfigureAwait(false);
                bool shouldDispose;
                lock (_gate)
                {
                    shouldDispose = _disposed || generation != _innerGeneration || !_innerActive;
                    if (!shouldDispose)
                    {
                        _currentInnerSubscription = innerSubscription;
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

    /// <summary>Disposes the current inner and outer subscriptions once, optionally forwarding completion.</summary>
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
        [DebuggerDisplay("SwitchToOuterWitness: {_witness}")]
        internal sealed class SwitchToOuterWitness(SwitchToCoordinator subscription) : IWitnessAsync<IObservableAsync<T>>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness;

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(IObservableAsync<T> value, CancellationToken cancellationToken) =>
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

            /// <summary>Forwards a new inner observable to the parent subscription for switching.</summary>
            /// <param name="value">The new inner observable.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<IObservableAsync<T>>.OnNextAsyncCore(IObservableAsync<T> value, CancellationToken cancellationToken) =>
                subscription.AcceptOuterValueAsync(value);

            /// <summary>Forwards a non-fatal error from the outer sequence to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            async ValueTask IWitnessAsync<IObservableAsync<T>>.OnErrorResumeAsyncCore(
                Exception error,
                CancellationToken cancellationToken)
            {
                _ = cancellationToken;
                using (await subscription._observerOnSomethingGate.EnterAsync(subscription._disposeCancellationToken)
                           .ConfigureAwait(false))
                {
                    await subscription._observer.OnErrorResumeAsync(error, subscription._disposeCancellationToken)
                        .ConfigureAwait(false);
                }
            }

            /// <summary>Handles the outer sequence completing.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<IObservableAsync<T>>.OnCompletedAsyncCore(Result result) =>
                subscription.AcceptOuterCompletionAsync(result);
        }

        /// <summary>Witness for the currently active inner observable sequence that delegates to the parent <see cref="SwitchToCoordinator"/>.</summary>
        /// <param name="subscription">The parent switch subscription.</param>
        /// <param name="generation">The generation of the inner sequence this witness observes.</param>
        [DebuggerDisplay("SwitchToInnerWitness: {_witness}")]
        internal sealed class SwitchToInnerWitness(SwitchToCoordinator subscription, long generation) : IWitnessAsync<T>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness;

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

            /// <summary>Forwards an element from the inner sequence to the downstream witness.</summary>
            /// <param name="value">The element to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                subscription.AcceptInnerValueAsync(value, cancellationToken);

            /// <summary>Forwards a non-fatal error from the inner sequence to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                subscription.AcceptInnerErrorAsync(error, cancellationToken);

            /// <summary>Handles the inner sequence completing.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
                subscription.AcceptInnerCompletionAsync(generation, result);
        }
    }
}
