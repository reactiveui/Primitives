// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Provides an asynchronous Signal that forwards notifications to observers concurrently.</summary>
/// <remarks>Observers are notified in parallel for each event. This class is suitable for scenarios where high
/// throughput and concurrent notification of multiple observers are required. Thread safety is ensured for observer
/// notification operations. Cancellation tokens can be used to cancel ongoing notification tasks.</remarks>
/// <typeparam name="T">The type of value observed and forwarded to observers.</typeparam>
public sealed class ConcurrentSignalAsync<T> : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly SignalAsyncState<T> _state = new();

    /// <inheritdoc/>
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.OnNextAsync(_state, SignalBroadcastKind.Concurrent, value, cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.OnErrorResumeAsync(_state, SignalBroadcastKind.Concurrent, error, cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnCompletedAsync(Result result) =>
        SignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Concurrent, result);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => SignalAsyncStateHelper.DisposeAsync();

    /// <inheritdoc/>
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
