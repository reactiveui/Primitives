// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription observer that fuses the bypass-condition + Switch-debounce pipeline.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="debounce">The debounce window.</param>
/// <param name="condition">The bypass-the-delay condition.</param>
/// <param name="timeProvider">The time provider used for the debounce timer.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("DebounceUntilWitness<T>: {_witness}")]
public sealed class DebounceUntilWitness<T>(
    IObserverAsync<T> downstream,
    TimeSpan debounce,
    Func<T, bool> condition,
    TimeProvider timeProvider,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>Synchronization gate protecting the id counter.</summary>
    private readonly Lock _gate = new();

    /// <summary>Monotonically increasing identifier stamped on each pending delay; a newer value supersedes the old id.</summary>
    private long _id;

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
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _id++;
        }

        return WitnessAsync.DisposeStateAsync(this);
    }

    /// <summary>Reports whether the stamped id is the current one, so no newer value has superseded this delay.</summary>
    /// <param name="id">The id stamped when this delay was started.</param>
    /// <returns><see langword="true"/> when the caller should forward the value; <see langword="false"/> when a
    /// newer upstream value superseded it.</returns>
    internal bool IsCurrentEmission(long id)
    {
        lock (_gate)
        {
            return _id == id;
        }
    }

    /// <summary>Starts a delay for the value, superseding any pending one.</summary>
    /// <param name="value">The pending value.</param>
    /// <param name="cancellationToken">Cancellation for the delay.</param>
    /// <returns>The delay and any downstream notification.</returns>
    internal Task StartDelayAsync(T value, CancellationToken cancellationToken)
    {
        long currentId;
        lock (_gate)
        {
            currentId = ++_id;
        }

        return DelayAndEmitAsync(value, currentId, cancellationToken);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        if (condition(value))
        {
            // Bypass cancels pending delayed delivery and emits immediately.
            lock (_gate)
            {
                _id++;
            }

            return downstream.OnNextAsync(value, cancellationToken);
        }

        _ = StartDelayAsync(value, cancellationToken);
        return default;
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _id++;
        }

        return downstream.OnErrorResumeAsync(error, cancellationToken);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        lock (_gate)
        {
            _id++;
        }

        return downstream.OnCompletedAsync(result);
    }

    /// <summary>Waits the debounce window, then forwards the value when <see cref="IsCurrentEmission"/> confirms it.</summary>
    /// <param name="value">The candidate value.</param>
    /// <param name="id">The id stamped when this delay was started.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous wait-and-maybe-forward operation.</returns>
    private async Task DelayAndEmitAsync(T value, long id, CancellationToken cancellationToken)
    {
        try
        {
            await SignalAsyncExtensions.DelayAsync(debounce, timeProvider, cancellationToken).ConfigureAwait(false);

            if (!IsCurrentEmission(id))
            {
                return;
            }

            await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }
}
