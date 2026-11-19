// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription witness that drops upstream emissions while a prior action is pending.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="asyncAction">The async side-effect invoked for accepted values.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
[System.Diagnostics.DebuggerDisplay("DropIfBusyWitness<T>: {_witness}")]
public sealed class DropIfBusyWitness<T>(
    IObserverAsync<T> downstream,
    Func<T, CancellationToken, ValueTask> asyncAction,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>0 when idle, 1 while an emission is being processed.</summary>
    private int _isBusy;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness = new(subscribeToken);

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _isBusy, 1, 0) != 0)
        {
            return default;
        }

        ValueTask actionTask;
        try
        {
            actionTask = asyncAction(value, cancellationToken);
        }
        catch
        {
            Volatile.Write(ref _isBusy, 0);
            throw;
        }

        if (actionTask.IsCompletedSuccessfully)
        {
            var forward = downstream.OnNextAsync(value, cancellationToken);
            if (forward.IsCompletedSuccessfully)
            {
                Volatile.Write(ref _isBusy, 0);
                return default;
            }

            return AwaitForwardAsync(forward);
        }

        return AwaitFullAsync(actionTask, value, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        downstream.OnCompletedAsync(result);

    /// <summary>Slow path when the downstream forwarding is asynchronous but the inner action completed sync.</summary>
    /// <param name="forward">The pending downstream forward.</param>
    /// <returns>A task that completes after the forward resolves and the busy flag is reset.</returns>
    private async ValueTask AwaitForwardAsync(ValueTask forward)
    {
        try
        {
            await forward.ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _isBusy, 0);
        }
    }

    /// <summary>Slow path when the inner action does not complete synchronously.</summary>
    /// <param name="actionTask">The pending inner action.</param>
    /// <param name="value">The value being processed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes after both the action and downstream forwarding resolve.</returns>
    private async ValueTask AwaitFullAsync(ValueTask actionTask, T value, CancellationToken cancellationToken)
    {
        try
        {
            await actionTask.ConfigureAwait(false);
            await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _isBusy, 0);
        }
    }
}
