// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents an asynchronous Signal that replays only the latest value to new subscribers and ensures that
/// notifications are delivered to observers in a serial, thread-safe manner.
/// </summary>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">An optional initial value to be emitted to new subscribers before any other values are published.</param>
/// <remarks>This Signal is designed for scenarios where only the most recent value is relevant to subscribers.
/// When a new observer subscribes, it immediately receives the latest value (if any) and then all subsequent
/// notifications. All observer notifications are performed asynchronously and in a serial order, ensuring thread
/// safety. This type is suitable for use cases where replaying only the latest value is desired, such as event streams
/// or state broadcasts.</remarks>
[System.Diagnostics.DebuggerDisplay("SerialReplayLatestSignalAsync: LastValue = {_state.LastValue}, IsDisposed = {_state.IsDisposed}")]
public sealed class SerialReplayLatestSignalAsync<T>(Optional<T> startValue) : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly ReplayLatestSignalAsyncState<T> _state = new(startValue);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        ReplayLatestSignalAsyncStateHelper.OnNextAsync(_state, SignalBroadcastKind.Serial, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        ReplayLatestSignalAsyncStateHelper.OnErrorResumeAsync(
            _state,
            SignalBroadcastKind.Serial,
            error,
            cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) =>
        ReplayLatestSignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Serial, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => ReplayLatestSignalAsyncStateHelper.DisposeAsync(_state);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        ReplayLatestSignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
