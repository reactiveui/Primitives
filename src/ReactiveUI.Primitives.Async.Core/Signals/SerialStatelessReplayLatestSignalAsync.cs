// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents a serial, stateless asynchronous Signal that replays only the last value to new observers and supports
/// asynchronous notification delivery.
/// </summary>
/// <typeparam name="T">The type of the elements processed by the Signal.</typeparam>
/// <param name="startValue">An optional initial value to be replayed to new observers before any values are published. If not specified, no
/// value is replayed until the first value is received.</param>
/// <remarks>This Signal delivers notifications to observers one at a time in the order they are received. It
/// does not maintain any state beyond the most recent value, and only the last value (if any) is replayed to new
/// subscribers. All observer notifications are dispatched asynchronously and serially, ensuring that each observer
/// receives notifications in the correct order.</remarks>
[System.Diagnostics.DebuggerDisplay("State = {_state}")]
public sealed class SerialStatelessReplayLatestSignalAsync<T>(Optional<T> startValue) : ISignalAsync<T>
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
            SignalBroadcastKind.SerialMulti,
            value,
            cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnErrorResumeAsync(
            _state,
            SignalBroadcastKind.SerialMulti,
            error,
            cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) =>
        StatelessReplayLatestSignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.SerialMulti, result);

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
