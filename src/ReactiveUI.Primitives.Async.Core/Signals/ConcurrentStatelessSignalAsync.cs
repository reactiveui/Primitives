// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents a stateless asynchronous Signal that forwards notifications to observers concurrently.</summary>
/// <remarks>This Signal distributes notifications to all subscribed observers in parallel, allowing for improved
/// throughput in scenarios where observer processing can occur independently. Use this type when observer notification
/// order is not important and concurrent delivery is desired. Thread safety is ensured for concurrent observer
/// notifications.</remarks>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
public sealed class ConcurrentStatelessSignalAsync<T> : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly StatelessSignalAsyncState<T> _state = new();

    /// <inheritdoc/>
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        StatelessSignalAsyncStateHelper.OnNextAsync(_state, SignalBroadcastKind.Concurrent, value, cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        StatelessSignalAsyncStateHelper.OnErrorResumeAsync(
            _state,
            SignalBroadcastKind.Concurrent,
            error,
            cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnCompletedAsync(Result result) =>
        StatelessSignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Concurrent, result);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => StatelessSignalAsyncStateHelper.DisposeAsync(_state);

    /// <inheritdoc/>
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        StatelessSignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
