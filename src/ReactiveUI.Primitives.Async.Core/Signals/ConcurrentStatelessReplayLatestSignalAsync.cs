// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>An asynchronous Signal that replays its latest value to each new subscriber, notifies observers concurrently, and resets once the last observer leaves.</summary>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">The value replayed until something is published, and restored when the last observer
/// leaves.</param>
/// <remarks>Cancellation can stop some observers from receiving a notification the others do receive.</remarks>
[System.Diagnostics.DebuggerDisplay("ConcurrentStatelessReplayLatestSignalAsync: Value = {_state.Value}, IsDisposed = {_state.IsDisposed}")]
public sealed class ConcurrentStatelessReplayLatestSignalAsync<T>(Optional<T> startValue) : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly StatelessReplayLatestSignalAsyncState<T> _state = new(startValue);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnNextAsync(
            _state,
            SignalBroadcastKind.Concurrent,
            value,
            cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnErrorResumeAsync(
            _state,
            SignalBroadcastKind.Concurrent,
            error,
            cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Concurrent, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => StatelessReplayLatestSignalAsyncStateHelper.DisposeAsync(_state);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        StatelessReplayLatestSignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
