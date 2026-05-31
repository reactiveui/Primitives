// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Provides a base class for asynchronous Signals that support both publishing values to observers and receiving
/// values asynchronously.
/// </summary>
/// <remarks>This class enables the implementation of asynchronous Signals that can broadcast values, errors, and
/// completion notifications to multiple observers. It manages observer registration, notification, and completion in a
/// thread-safe manner. Derived classes should override the core notification methods to customize how observers are
/// notified asynchronously. The Signal supports asynchronous subscription and notification patterns, making it
/// suitable for reactive and event-driven programming scenarios.</remarks>
/// <typeparam name="T">The type of elements processed by the Signal and observed by subscribers.</typeparam>
public abstract class BaseSignalAsync<T> : SignalAsync<T>, ISignalAsync<T>
{
    /// <summary>
    /// The lock object used to synchronize access to the Signal's mutable state.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// The immutable list of currently subscribed observers.
    /// </summary>
    private ImmutableArray<IObserverAsync<T>> _observers = [];

    /// <summary>
    /// The completion result, or <see langword="null"/> if the Signal has not yet completed.
    /// </summary>
    private Result? _result;

    /// <summary>
    /// Gets an observable sequence that represents the asynchronous values published by the Signal.
    /// </summary>
    /// <remarks>Subscribers receive notifications for each value published after they subscribe. The sequence
    /// may complete or error according to the Signal's state.</remarks>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>
    /// Asynchronously notifies all subscribed observers of a new value.
    /// </summary>
    /// <remarks>If the sequence has already completed, this method does not notify observers.</remarks>
    /// <param name="value">The value to send to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken)
    {
        ImmutableArray<IObserverAsync<T>> observers;

        lock (_gate)
        {
            if (_result is not null)
            {
                return default;
            }

            observers = _observers;
        }

        return OnNextAsyncCore(observers, value, cancellationToken);
    }

    /// <summary>
    /// Notifies all observers of an error and allows asynchronous error handling to resume observation.
    /// </summary>
    /// <remarks>If the sequence has already completed or an error has previously been signaled, this method
    /// has no effect.</remarks>
    /// <param name="error">The exception that occurred and will be sent to observers. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous error notification operation.</param>
    /// <returns>A ValueTask that represents the asynchronous operation of notifying observers of the error.</returns>
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
    {
        ImmutableArray<IObserverAsync<T>> observers;

        lock (_gate)
        {
            if (_result is not null)
            {
                return default;
            }

            observers = _observers;
        }

        return OnErrorResumeAsyncCore(observers, error, cancellationToken);
    }

    /// <summary>
    /// Notifies all registered observers that the asynchronous operation has completed and provides the final result.
    /// </summary>
    /// <remarks>If the operation has already completed, this method returns immediately without notifying
    /// observers again. This method is thread-safe and ensures that observers are notified only once.</remarks>
    /// <param name="result">The result to deliver to observers upon completion. Cannot be null.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation. The task completes when all observers have
    /// been notified.</returns>
    public ValueTask OnCompletedAsync(Result result)
    {
        ImmutableArray<IObserverAsync<T>> observers;
        lock (_gate)
        {
            if (_result is not null)
            {
                return default;
            }

            _result = result;
            observers = _observers;
            _observers = [];
        }

        return OnCompletedAsyncCore(observers, result);
    }

    /// <summary>
    /// Asynchronously releases the unmanaged resources used by the object.
    /// </summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return default;
    }

    /// <summary>
    /// Subscribes the specified asynchronous observer to receive notifications from the observable sequence.
    /// </summary>
    /// <remarks>If the observable sequence has already completed, the observer receives the completion
    /// notification immediately and the returned disposable is a no-op. Otherwise, the observer is added to the list of
    /// active observers and will receive future notifications until unsubscribed or the sequence completes.</remarks>
    /// <param name="observer">The asynchronous observer that will receive notifications from the observable sequence.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A task that represents the asynchronous subscription operation. The result contains a disposable object that can
    /// be used to unsubscribe the observer.</returns>
    protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Result? result;

        lock (_gate)
        {
            result = _result;
            if (result is null)
            {
                _observers = _observers.Add(observer);
            }
        }

        if (result is not null)
        {
            await observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }

        return DisposableAsync.Create(
            (signal: this, observer),
            static state =>
            {
                lock (state.signal._gate)
                {
                    state.signal._observers = state.signal._observers.Remove(state.observer);
                }

                return default;
            });
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
    /// <remarks>Override this method to implement custom error recovery strategies for observers. The method
    /// is called when an error occurs during asynchronous processing, allowing derived classes to determine how to
    /// resume or notify observers. If the operation is canceled via the provided cancellation token, the returned task
    /// should reflect the cancellation.</remarks>
    /// <param name="observers">A read-only list of observers that are to be notified or resumed following the error. Cannot be null.</param>
    /// <param name="error">The exception that triggered the error handling logic. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous error recovery operation.</param>
    /// <returns>A ValueTask that represents the asynchronous error recovery operation.</returns>
    protected abstract ValueTask OnErrorResumeAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken);

    /// <summary>
    /// Invoked to asynchronously notify all observers of the completion event with the specified result.
    /// </summary>
    /// <remarks>Implementations should ensure that all observers are notified, and handle any exceptions that
    /// may occur during notification according to the desired error-handling policy.</remarks>
    /// <param name="observers">A read-only list of observers to be notified. Cannot be null.</param>
    /// <param name="result">The result to provide to each observer upon completion.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected abstract ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result);
}
