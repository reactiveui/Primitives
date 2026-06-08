// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Provides a base class for implementing asynchronous, stateless Signals that broadcast notifications to multiple
/// observers. Supports asynchronous notification, error handling, and completion signaling for observers subscribing to
/// the Signal.
/// </summary>
/// <remarks>This abstract class enables the creation of custom asynchronous Signals that do not maintain
/// internal state between notifications. It manages observer subscriptions and provides extensibility points for
/// handling value notifications, error recovery, and completion events. Implementations should override the core
/// notification methods to define specific broadcasting or error-handling behaviors. Thread safety and observer
/// management are handled by the base class, allowing derived classes to focus on notification logic.</remarks>
/// <typeparam name="T">The type of the elements processed and broadcast by the Signal.</typeparam>
public abstract class BaseStatelessSignalAsync<T> : SignalAsync<T>, ISignalAsync<T>
{
    /// <summary>The immutable array of currently subscribed observers.</summary>
    private ImmutableArray<IObserverAsync<T>> _observers = [];

    /// <summary>Gets an observable sequence that represents the current and future values of the Signal.</summary>
    /// <remarks>Subscribers to this property receive all values published by the Signal, including those
    /// emitted after subscription. The returned sequence may emit values asynchronously, depending on the
    /// implementation.</remarks>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>Asynchronously notifies all subscribed observers of a new value.</summary>
    /// <param name="value">The value to send to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        OnNextAsyncCore(_observers, value, cancellationToken);

    /// <summary>
    /// Handles an error by resuming the asynchronous operation, allowing observers to continue receiving notifications
    /// despite the specified exception.
    /// </summary>
    /// <param name="error">The exception that caused the error condition. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the resume operation.</param>
    /// <returns>A ValueTask that represents the asynchronous resume operation.</returns>
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        OnErrorResumeAsyncCore(_observers, error, cancellationToken);

    /// <summary>Notifies all registered observers that the operation has completed and provides the final result asynchronously.</summary>
    /// <param name="result">The result to deliver to observers upon completion. Cannot be null.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    public ValueTask OnCompletedAsync(Result result) => OnCompletedAsyncCore(_observers, result);

    /// <summary>Asynchronously releases resources used by the current instance.</summary>
    /// <remarks>After calling this method, the instance should not be used. This method is intended to be
    /// called when the object is no longer needed, to ensure that all resources are properly released.</remarks>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync()
    {
        ImmutableInterlocked.Update(ref _observers, static observers => observers.Clear());
        GC.SuppressFinalize(this);
        return default;
    }

    /// <summary>Subscribes the specified asynchronous observer to receive notifications from the observable sequence.</summary>
    /// <remarks>Disposing the returned <see cref="IAsyncDisposable"/> will remove the observer from the
    /// subscription list. The subscription is established immediately upon calling this method.</remarks>
    /// <param name="observer">The asynchronous observer that will receive notifications. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the subscription operation.</param>
    /// <returns>A value task that represents the asynchronous subscription operation. The result is an <see
    /// cref="IAsyncDisposable"/> that can be disposed to unsubscribe the observer.</returns>
    protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var disposable = DisposableAsync.Create(
            (signal: this, observer),
            static state =>
            {
                ImmutableInterlocked.Update(
                    ref state.signal._observers,
                    static (observers, observer) => observers.Remove(observer),
                    state.observer);
                return default;
            });

        ImmutableInterlocked.Update(ref _observers, static (observers, observer) => observers.Add(observer), observer);
        return new(disposable);
    }

    /// <summary>Asynchronously notifies the specified observers with the provided value.</summary>
    /// <param name="observers">A read-only list of observers to be notified. Cannot be null.</param>
    /// <param name="value">The value to send to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected abstract ValueTask OnNextAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken);

    /// <summary>Handles error recovery for the specified observers by resuming asynchronous processing after an error occurs.</summary>
    /// <remarks>Implementations should ensure that observers are notified or resumed appropriately based on
    /// the error. This method is intended to be called when an error occurs during asynchronous observation, allowing
    /// custom error handling strategies.</remarks>
    /// <param name="observers">A read-only list of observers that should resume processing after the error. Cannot be null.</param>
    /// <param name="error">The exception that triggered the error recovery logic. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask that represents the asynchronous error recovery operation.</returns>
    protected abstract ValueTask OnErrorResumeAsyncCore(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken);

    /// <summary>Invoked to asynchronously notify all observers of the completion event with the specified result.</summary>
    /// <remarks>Implementations should ensure that all observers are notified, and handle any exceptions
    /// according to the desired notification semantics.</remarks>
    /// <param name="observers">The collection of observers to be notified. Cannot be null.</param>
    /// <param name="result">The result to provide to each observer upon completion.</param>
    /// <returns>A ValueTask that represents the asynchronous notification operation.</returns>
    protected abstract ValueTask OnCompletedAsyncCore(ImmutableArray<IObserverAsync<T>> observers, Result result);
}
