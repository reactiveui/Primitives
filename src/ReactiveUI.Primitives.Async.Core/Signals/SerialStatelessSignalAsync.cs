// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Represents a stateless asynchronous Signal that notifies observers of events in a serial, sequential manner.</summary>
/// <remarks>Observers are notified one at a time in the order they are registered. Each observer receives the
/// event only after the previous observer has completed processing. This class is suitable for scenarios where event
/// delivery order and sequential processing are required. Thread safety and ordering are managed internally.</remarks>
/// <typeparam name="T">The type of the elements processed and observed by the Signal.</typeparam>
public sealed class SerialStatelessSignalAsync<T> : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly StatelessSignalAsyncState<T> _state = new();

    /// <inheritdoc/>
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        StatelessSignalAsyncStateHelper.OnNextAsync(_state, SignalBroadcastKind.Serial, value, cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        StatelessSignalAsyncStateHelper.OnErrorResumeAsync(
            _state,
            SignalBroadcastKind.Serial,
            error,
            cancellationToken);

    /// <inheritdoc/>
    public ValueTask OnCompletedAsync(Result result) =>
        StatelessSignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Serial, result);

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => StatelessSignalAsyncStateHelper.DisposeAsync(_state);

    /// <inheritdoc/>
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        StatelessSignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
