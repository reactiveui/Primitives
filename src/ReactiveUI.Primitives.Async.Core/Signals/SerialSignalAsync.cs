// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Represents an asynchronous Signal that notifies observers in a serial manner, ensuring each observer is notified
/// one at a time.
/// </summary>
/// <typeparam name="T">The type of the elements processed and observed by the Signal.</typeparam>
/// <remarks>SerialSignalAsync{T} is designed for scenarios where observers must be notified sequentially rather
/// than concurrently. This can be useful when observer operations are not thread-safe or when order of notification is
/// important. Notifications to observers are performed asynchronously and in sequence.</remarks>
[System.Diagnostics.DebuggerDisplay("Observers = {_state.Observers.Length}, Result = {_state.Result}")]
public sealed class SerialSignalAsync<T> : ISignalAsync<T>
{
    /// <inheritdoc/>
    IObservableAsync<T> ISignalAsync<T>.Values => this;

    /// <summary>The mutable signal state.</summary>
    private readonly SignalAsyncState<T> _state = new();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(
        T value,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.OnNextAsync(_state, SignalBroadcastKind.Serial, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(
        Exception error,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.OnErrorResumeAsync(_state, SignalBroadcastKind.Serial, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) =>
        SignalAsyncStateHelper.OnCompletedAsync(_state, SignalBroadcastKind.Serial, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => SignalAsyncStateHelper.DisposeAsync();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<IAsyncDisposable> SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        SignalAsyncStateHelper.SubscribeAsync(_state, observer, cancellationToken);
}
