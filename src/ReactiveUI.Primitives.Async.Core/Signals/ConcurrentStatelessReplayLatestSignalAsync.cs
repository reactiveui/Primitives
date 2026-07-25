// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents an asynchronous Signal that replays the latest value to new observers and forwards notifications to all
/// observers concurrently without maintaining internal state.
/// </summary>
/// <remarks>This Signal is designed for concurrent scenarios where notifications to observers should be
/// delivered in parallel. It does not buffer or store a sequence of values, but only replays the most recent value (if
/// any) to new subscribers. Thread safety is ensured for concurrent observer notifications. If a notification operation
/// is canceled, not all observers may receive the notification.</remarks>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">An optional initial value to be replayed to new observers. If not specified, no value is replayed until the first
/// value is published.</param>
public sealed class ConcurrentStatelessReplayLatestSignalAsync<T>(Optional<T> startValue) : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly StatelessReplayLatestSignalAsyncState<T> _state = new(startValue);

    /// <inheritdoc/>
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnNextAsync(
            _state,
            SignalBroadcastKind.Concurrent,
            value,
            cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnErrorResumeAsync(
            _state,
            SignalBroadcastKind.Concurrent,
            error,
            cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnCompletedAsync(Result result) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Concurrent, result);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => StatelessReplayLatestSignalAsyncStateHelper.DisposeAsync(_state);

    /// <inheritdoc/>
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        StatelessReplayLatestSignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
