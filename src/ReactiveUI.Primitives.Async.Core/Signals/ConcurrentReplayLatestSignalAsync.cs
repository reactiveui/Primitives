// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents an asynchronous Signal that replays only the latest value to new observers and supports concurrent
/// notification of observers.
/// </summary>
/// <remarks>This Signal notifies all observers concurrently, which can improve throughput in scenarios with
/// multiple observers. The order in which observers receive notifications is not guaranteed. This type is thread-safe
/// and suitable for use in asynchronous and concurrent environments.</remarks>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">An optional initial value to be emitted to observers upon subscription if no other value has been published.</param>
public sealed class ConcurrentReplayLatestSignalAsync<T>(Optional<T> startValue)
    : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly ReplayLatestSignalAsyncState<T> _state = new(startValue);

    /// <inheritdoc/>
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        ReplayLatestSignalAsyncStateHelper.OnNextAsync(_state, SignalBroadcastKind.Concurrent, value, cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        ReplayLatestSignalAsyncStateHelper.OnErrorResumeAsync(_state, SignalBroadcastKind.Concurrent, error, cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnCompletedAsync(Result result) =>
        ReplayLatestSignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Concurrent, result);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => ReplayLatestSignalAsyncStateHelper.DisposeAsync(_state);

    /// <inheritdoc/>
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        ReplayLatestSignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
