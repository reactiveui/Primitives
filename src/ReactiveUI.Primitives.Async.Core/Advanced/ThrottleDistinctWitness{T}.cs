// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-subscription witness fusing upstream-distinct + debounce + downstream-distinct.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="downstream">The downstream observer.</param>
/// <param name="dueTime">The debounce window.</param>
/// <param name="timeProvider">The time provider used for the debounce timer.</param>
/// <param name="subscribeToken">The subscribe-time cancellation token.</param>
[System.Diagnostics.DebuggerDisplay("ThrottleDistinctWitness<T>: {_witness}")]
public sealed class ThrottleDistinctWitness<T>(
    IObserverAsync<T> downstream,
    TimeSpan dueTime,
    TimeProvider timeProvider,
    CancellationToken subscribeToken) : IWitnessAsync<T>
{
    /// <summary>Equality comparer used for both distinct layers.</summary>
    private static readonly EqualityComparer<T> Comparer = EqualityComparer<T>.Default;

    /// <summary>Synchronization gate protecting throttle/distinct state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Most-recent upstream value (for upstream DistinctUntilChanged).</summary>
    private T _lastUpstream = default!;

    /// <summary>Most-recently-forwarded value (for downstream DistinctUntilChanged).</summary>
    private T _lastEmitted = default!;

    /// <summary>Set to <see langword="true"/> after the first upstream emission has been seen.</summary>
    private bool _hasUpstream;

    /// <summary>Set to <see langword="true"/> after the first value has been forwarded downstream.</summary>
    private bool _hasEmitted;

    /// <summary>Monotonically increasing identifier stamped on each pending delay; a mismatch marks it superseded.</summary>
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

    /// <summary>Claims the emission when the stamped id is current and the value differs from the last forwarded one.</summary>
    /// <param name="value">The candidate value.</param>
    /// <param name="id">The id stamped when this delay was started.</param>
    /// <returns><see langword="true"/> when the caller should forward the value; <see langword="false"/> when it
    /// was superseded or duplicates the last forwarded value.</returns>
    internal bool TryClaimEmission(T value, long id)
    {
        lock (_gate)
        {
            if (_id != id)
            {
                return false;
            }

            if (_hasEmitted && Comparer.Equals(value, _lastEmitted))
            {
                return false;
            }

            _lastEmitted = value;
            _hasEmitted = true;
            return true;
        }
    }

    /// <summary>Starts a delay for a distinct upstream value and supersedes the previous pending value.</summary>
    /// <param name="value">The upstream value.</param>
    /// <param name="cancellationToken">Cancellation for the delay.</param>
    /// <returns>The pending emission, or a completed task for a duplicate value.</returns>
    internal Task StartDelayAsync(T value, CancellationToken cancellationToken)
    {
        long currentId;
        lock (_gate)
        {
            if (_hasUpstream && Comparer.Equals(value, _lastUpstream))
            {
                return Task.CompletedTask;
            }

            _lastUpstream = value;
            _hasUpstream = true;
            currentId = ++_id;
        }

        return FireAfterDelayAsync(value, currentId, cancellationToken);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
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

    /// <summary>Waits the debounce window, then forwards the value when <see cref="TryClaimEmission"/> approves it.</summary>
    /// <param name="value">The candidate value.</param>
    /// <param name="id">The id stamped when this delay was started.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous wait-and-maybe-forward operation.</returns>
    private async Task FireAfterDelayAsync(T value, long id, CancellationToken cancellationToken)
    {
        try
        {
            await SignalAsyncExtensions.DelayAsync(dueTime, timeProvider, cancellationToken).ConfigureAwait(false);

            if (!TryClaimEmission(value, id))
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
