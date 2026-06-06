// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Provides a base implementation for an asynchronous Signal that replays the latest value to new subscribers and
/// supports asynchronous notification of observers.
/// </summary>
/// <remarks>This abstract class is intended to be inherited by types that implement custom replay and
/// notification logic for asynchronous observers. When a new observer subscribes, it immediately receives the latest
/// value if one is available. The Signal supports asynchronous notification of values, errors, and completion, and
/// ensures thread-safe access for concurrent operations.</remarks>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">An optional initial value to be emitted to new subscribers before any other values are published.</param>
public abstract class BaseReplayLatestSignalAsync<T>(Optional<T> startValue) : SignalAsync<T>, ISignalAsync<T>
{
    /// <summary>
    /// The asynchronous gate used to synchronize access to the Signal's mutable state.
    /// </summary>
    private readonly AsyncSerialGate _gate = new();

    /// <summary>
    /// The cancellation token source that is cancelled when this instance is disposed.
    /// </summary>
    private readonly CancellationTokenSource _disposedCts = new();

    /// <summary>
    /// The most recently published value, replayed to new subscribers upon subscription.
    /// </summary>
    private Optional<T> _lastValue = startValue;

    /// <summary>
    /// The immutable list of currently subscribed observers.
    /// </summary>
    private ImmutableArray<IObserverAsync<T>> _observers = [];

    /// <summary>
    /// The completion result, or <see langword="null"/> if the Signal has not yet completed.
    /// </summary>
    private Result? _result;

    /// <summary>
    /// A value indicating whether this instance has been disposed.
    /// </summary>
    private bool _isDisposed;

    /// <summary>
    /// Gets an observable sequence that represents the asynchronous values of the Signal.
    /// </summary>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>
    /// Gets the cancellation token that is cancelled when this instance is disposed.
    /// </summary>
    private CancellationToken DisposedCancellationToken => _disposedCts.Token;

    /// <summary>
    /// Asynchronously notifies all subscribed observers with the specified value.
    /// </summary>
    /// <remarks>If the sequence has already completed, this method does not notify observers.</remarks>
    /// <param name="value">The value to send to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public async ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
    {
        CancellationTokenSource? linkedCts;
        var token = GetOperationCancellationToken(cancellationToken, out linkedCts);
        using var _ = linkedCts;

        ImmutableArray<IObserverAsync<T>> observers;
        using (await _gate.EnterAsync(token).ConfigureAwait(false))
        {
            if (_result is not null)
            {
                return;
            }

            _lastValue = new(value);
            observers = _observers;
        }

        // Pass CancellationToken.None into the broadcast loop so downstream observers'
        // TryEnter takes the None fast path; otherwise the Signal's own dispose token would
        // appear foreign to each observer and force a Linked2CancellationTokenSource per
        // emission, as discovered profiling Publish(initialValue) / ReplayLatestPublish.
        // Signal disposal still terminates emissions because we set _result before locking
        // and observers stop being added once the Signal has completed; in-flight
        // forwarding does not need the dispose token threaded through.
        await OnNextAsyncCore(observers, value, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Notifies all observers of an error and resumes asynchronous processing as appropriate.
    /// </summary>
    /// <remarks>If the result has already been set, this method returns immediately without notifying
    /// observers. Observers are notified asynchronously.</remarks>
    /// <param name="error">The exception that occurred and will be sent to observers. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public async ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
    {
        var token = GetOperationCancellationToken(cancellationToken, out var linkedCts);
        using var _ = linkedCts;

        ImmutableArray<IObserverAsync<T>> observers;
        using (await _gate.EnterAsync(token).ConfigureAwait(false))
        {
            if (_result is not null)
            {
                return;
            }

            observers = _observers;
        }

        await OnErrorResumeAsyncCore(observers, error, token).ConfigureAwait(false);
    }

    /// <summary>
    /// Notifies all registered observers that the asynchronous operation has completed and provides the final result.
    /// </summary>
    /// <remarks>Subsequent calls after the first completion will have no effect. This method is thread-safe
    /// and ensures that observers are notified only once.</remarks>
    /// <param name="result">The result to deliver to observers upon completion. Cannot be null.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation. The task completes when all observers have
    /// been notified.</returns>
    public async ValueTask OnCompletedAsync(Result result)
    {
        ImmutableArray<IObserverAsync<T>> observers;
        using (await _gate.EnterAsync(DisposedCancellationToken).ConfigureAwait(false))
        {
            if (_result is not null)
            {
                return;
            }

            _result = result;
            observers = _observers;
            _observers = [];
        }

        await OnCompletedAsyncCore(observers, result).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously releases the unmanaged resources used by the object.
    /// </summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
        {
            return;
        }

        _isDisposed = true;
        await _disposedCts.CancelAsync().ConfigureAwait(false);
        _gate.Dispose();
        _disposedCts.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Asynchronously notifies the specified observers with the provided value.
    /// </summary>
    /// <param name="observers">A read-only list of observers to be notified. Cannot be null.</param>
    /// <param name="value">The value to send to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected abstract ValueTask OnNextAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken);

    /// <summary>
    /// Handles error recovery for the specified observers by resuming asynchronous processing after an error occurs.
    /// </summary>
    /// <remarks>Override this method to implement custom error recovery logic for asynchronous observers. The
    /// method is called when an error occurs and provides an opportunity to resume or redirect processing for the
    /// affected observers.</remarks>
    /// <param name="observers">A read-only list of observers to notify or resume after the error. Cannot be null.</param>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask that represents the asynchronous error recovery operation.</returns>
    protected abstract ValueTask OnErrorResumeAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken);

    /// <summary>
    /// Invoked to asynchronously notify all observers of the completion event with the specified result.
    /// </summary>
    /// <remarks>Implementations should ensure that all observers are notified according to the completion
    /// semantics of the operation. Exceptions thrown during notification may affect the completion of the returned
    /// task.</remarks>
    /// <param name="observers">A read-only list of observers to be notified of the completion event. Cannot be null.</param>
    /// <param name="result">The result to provide to each observer upon completion.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected abstract ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result);

    /// <summary>
    /// Subscribes the specified asynchronous observer to receive notifications from the observable sequence.
    /// </summary>
    /// <remarks>If the sequence has already completed, the observer will immediately receive the completion
    /// notification and will not be added to the list of active observers. If a last value is available, it is pushed
    /// to the observer upon subscription.</remarks>
    /// <param name="observer">The asynchronous observer that will receive notifications. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A disposable object that can be used to unsubscribe the observer from the sequence. If the sequence has already
    /// completed, returns an empty disposable.</returns>
    protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var token = GetOperationCancellationToken(cancellationToken, out var linkedCts);
        try
        {
            token.ThrowIfCancellationRequested();

            Result? result;
            using (await _gate.EnterAsync(token).ConfigureAwait(false))
            {
                result = _result;
                if (result is null)
                {
                    _observers = _observers.Add(observer);
                    if (_lastValue.TryGetValue(out var lastValue))
                    {
                        await observer.OnNextAsync(lastValue, token).ConfigureAwait(false);
                    }
                }
            }

            if (result is null)
            {
                return new WitnessLease(this, observer);
            }

            await observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }
        finally
        {
            linkedCts?.Dispose();
        }
    }

    /// <summary>
    /// Gets the cancellation token used for a gate-protected operation, creating a linked source only when the caller
    /// supplied an independent cancellable token.
    /// </summary>
    /// <param name="cancellationToken">The caller-supplied cancellation token.</param>
    /// <param name="linkedCts">The linked source created for the operation, or <see langword="null"/> on the fast path.</param>
    /// <returns>The token to use while entering the gate and invoking immediate subscription callbacks.</returns>
    private CancellationToken GetOperationCancellationToken(
        CancellationToken cancellationToken,
        out CancellationTokenSource? linkedCts)
    {
        if (!cancellationToken.CanBeCanceled || cancellationToken == DisposedCancellationToken)
        {
            linkedCts = null;
            return DisposedCancellationToken;
        }

        linkedCts = CancellationTokenSource.CreateLinkedTokenSource(DisposedCancellationToken, cancellationToken);
        return linkedCts.Token;
    }

    /// <summary>
    /// Removes an observer from the replay signal under the serialization gate.
    /// </summary>
    /// <param name="observer">The observer to remove.</param>
    /// <returns>A task that represents the asynchronous removal operation.</returns>
    private async ValueTask RemoveObserverAsync(IObserverAsync<T> observer)
    {
        if (_isDisposed)
        {
            return;
        }

        try
        {
            using (await _gate.EnterAsync(DisposedCancellationToken).ConfigureAwait(false))
            {
                _observers = _observers.Remove(observer);
            }
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (ObjectDisposedException)
        {
            return;
        }
    }

    /// <summary>
    /// Subscription handle that removes an witness from a replay signal when disposed.
    /// </summary>
    /// <param name="signal">The signal that owns the witness list.</param>
    /// <param name="observer">The witness to remove when the lease is disposed.</param>
    private sealed class WitnessLease(BaseReplayLatestSignalAsync<T> signal, IObserverAsync<T> observer)
        : IAsyncDisposable
    {
        /// <summary>
        /// Indicates whether the lease has already removed its observer.
        /// </summary>
        private int _disposed;

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            return Interlocked.Exchange(ref _disposed, 1) != 0
                ? default
                : signal.RemoveObserverAsync(observer);
        }
    }
}
