// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Delays each value and forwards it only if no newer value supersedes it.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="observer">The downstream observer to forward debounced elements to.</param>
/// <param name="dueTime">The quiet period that must elapse before an element is forwarded.</param>
/// <param name="timeProvider">The time provider used for scheduling the debounce timer.</param>
[System.Diagnostics.DebuggerDisplay("ThrottleWitness<T>: {_witness}")]
public sealed class ThrottleWitness<T>(IObserverAsync<T> observer, TimeSpan dueTime, TimeProvider timeProvider) : IWitnessAsync<T>
{
    /// <summary>The synchronization gate protecting shared throttle state.</summary>
    private readonly Lock _gate = new();

    /// <summary>A monotonically increasing identifier used to detect whether a newer element has superseded the current timer.</summary>
    private long _id;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

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

    /// <summary>Invalidates pending values before releasing the observer.</summary>
    /// <returns>A completed task.</returns>
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _id++;
        }

        return WitnessAsync.DisposeStateAsync(this);
    }

    /// <summary>Starts a debounce delay with a fresh identifier.</summary>
    /// <param name="value">The value to forward if it remains current.</param>
    /// <param name="cancellationToken">Cancellation for the delay.</param>
    /// <returns>The delay and notification operation.</returns>
    internal Task StartDelayAsync(T value, CancellationToken cancellationToken)
    {
        long currentId;
        lock (_gate)
        {
            currentId = ++_id;
        }

        return FireAfterDelayAsync(value, currentId, cancellationToken);
    }

    /// <summary>Waits for the debounce delay and then forwards the value if it has not been superseded.</summary>
    /// <param name="value">The value to forward after the delay.</param>
    /// <param name="id">The identifier of this timer; if superseded by a newer id, the value is discarded.</param>
    /// <param name="cancellationToken">A token to cancel the delay.</param>
    /// <returns>A task representing the asynchronous delay and forwarding operation.</returns>
    internal async Task FireAfterDelayAsync(T value, long id, CancellationToken cancellationToken)
    {
        try
        {
            await SignalAsyncExtensions.DelayAsync(dueTime, timeProvider, cancellationToken).ConfigureAwait(false);

            lock (_gate)
            {
                if (_id != id)
                {
                    return;
                }
            }

            await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Delay or downstream delivery cancelled on disposal.
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Schedules the value's delay and discards completion if a newer value supersedes it.</summary>
    /// <param name="value">The element to potentially forward after the debounce period.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A completed task; the actual forwarding happens asynchronously after the delay.</returns>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        _ = StartDelayAsync(value, cancellationToken.CanBeCanceled ? cancellationToken : this.InternalDisposedToken);
        return default;
    }

    /// <summary>Marks any in-flight delay as superseded and forwards the error to the downstream observer.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _id++;
        }

        return observer.OnErrorResumeAsync(error, cancellationToken);
    }

    /// <summary>Marks any in-flight delay as superseded and forwards completion to the downstream observer.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        lock (_gate)
        {
            _id++;
        }

        return observer.OnCompletedAsync(result);
    }
}
