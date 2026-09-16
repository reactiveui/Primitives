// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Represents an asynchronous observable sequence that concatenates multiple asynchronous observables, emitting their elements in order as each completes.</summary>
/// <typeparam name="T">The type of elements produced by the concatenated observable sequences.</typeparam>
/// <param name="signals">A collection of asynchronous signals to be concatenated. Each signal is subscribed to sequentially; the next
/// begins only after the previous completes.</param>
/// <remarks>An error from any signal terminates the concatenation and is propagated to the observer.</remarks>
[System.Diagnostics.DebuggerDisplay("ChainEnumerableSignal: Signals = {_signals}")]
public sealed class ChainEnumerableSignal<T>(IEnumerable<IObservableAsync<T>> signals) : IObservableAsync<T>
{
    /// <summary>The enumerable collection of signal sequences to concatenate.</summary>
    private readonly IEnumerable<IObservableAsync<T>> _signals = signals;

    /// <summary>Subscribes the observer to a <see cref="ChainSequenceCoordinator"/> that walks the signals one at a time.</summary>
    /// <param name="observer">The observer to receive elements from the concatenated sequences.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down the subscription when disposed.</returns>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        ChainSequenceCoordinator subscription = new(this, observer);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            subscription.SubscribeNextSignalAsync);
    }

    /// <summary>Manages sequential iteration through the enumerable of observables, subscribing to each inner observable only after the previous one completes.</summary>
    internal sealed class ChainSequenceCoordinator : IAsyncDisposable
    {
        /// <summary>Enumerator that iterates through the collection of observable sequences to concatenate.</summary>
        private readonly IEnumerator<IObservableAsync<T>> _enumerator;

        /// <summary>Serial disposable that holds the currently active inner subscription, disposing the previous one when replaced.</summary>
        private readonly SingleReplaceableDisposableAsync _innerDisposable = new();

        /// <summary>Cancellation token source used to signal disposal of the subscription.</summary>
        private readonly CancellationTokenSource _cts = new();

        /// <summary>Token signalled when this subscription is disposed.</summary>
        private readonly CancellationToken _disposedCancellationToken;

        /// <summary>The downstream observer to forward elements to.</summary>
        private readonly IObserverAsync<T> _observer;

        /// <summary>Flag indicating whether this subscription has been disposed (1 = disposed, 0 = active).</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="ChainSequenceCoordinator"/> class.</summary>
        /// <param name="parent">The parent observable that provides the enumerable of observables.</param>
        /// <param name="observer">The downstream observer to forward elements to.</param>
        public ChainSequenceCoordinator(ChainEnumerableSignal<T> parent, IObserverAsync<T> observer)
        {
            _observer = observer;
            _enumerator = parent._signals.GetEnumerator();
            _disposedCancellationToken = _cts.Token;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => FinishAsync(null);

        /// <summary>Routes the failure carried by a redundant <see cref="FinishAsync"/> call to the unhandled exception handler.</summary>
        /// <param name="result">The completion result from the redundant call.</param>
        internal static void HandleAlreadyDisposed(Result? result)
        {
            if (result?.Exception is not { } exception)
            {
                return;
            }

            UnhandledExceptionHandler.ReportUnhandledException(exception);
        }

        /// <summary>Advances to and subscribes to the next observable in the enumerable, or completes if no more observables are available.</summary>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async ValueTask SubscribeNextSignalAsync()
        {
            try
            {
                if (_enumerator.MoveNext())
                {
                    var subscription = await _enumerator.Current.SubscribeAsync(
                        RelayInnerValueAsync,
                        RelayInnerErrorAsync,
                        result => result.IsFailure ? FinishAsync(result) : SubscribeNextSignalAsync(),
                        _disposedCancellationToken).ConfigureAwait(false);

                    await _innerDisposable.SetDisposableAsync(subscription).ConfigureAwait(false);
                }
                else
                {
                    await FinishAsync(Result.Success).ConfigureAwait(false);
                }
            }
            catch (Exception e)
            {
                await FinishAsync(Result.Failure(e)).ConfigureAwait(false);
            }
        }

        /// <summary>Forwards a non-fatal error from the current inner sequence to the downstream observer.</summary>
        /// <param name="exception">The error to forward.</param>
        /// <param name="cancellationToken">A token to cancel the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal ValueTask RelayInnerErrorAsync(Exception exception, CancellationToken cancellationToken)
        {
            // Notifications use subscription cancellation instead of the supplied token.
            _ = cancellationToken;
            return _observer.OnErrorResumeAsync(exception, _disposedCancellationToken);
        }

        /// <summary>Forwards an element from the current inner sequence to the downstream observer.</summary>
        /// <param name="value">The element to forward.</param>
        /// <param name="cancellationToken">A token to cancel the operation. Ignored - see <see cref="RelayInnerErrorAsync"/>.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal ValueTask RelayInnerValueAsync(T value, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            return _observer.OnNextAsync(value, _disposedCancellationToken);
        }

    /// <summary>Disposes the inner subscription and enumerator once, optionally forwarding completion.</summary>
        /// <param name="result">The completion result to forward, or <see langword="null"/> if disposing without signaling completion.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        internal async ValueTask FinishAsync(Result? result)
        {
            if (DisposalHelper.TrySetDisposed(ref _disposed))
            {
                HandleAlreadyDisposed(result);
                return;
            }

            await _cts.CancelAsync().ConfigureAwait(false);
            await _innerDisposable.DisposeAsync().ConfigureAwait(false);
            if (result is not null)
            {
                await _observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            }

            _enumerator.Dispose();
            _cts.Dispose();
        }
    }
}
