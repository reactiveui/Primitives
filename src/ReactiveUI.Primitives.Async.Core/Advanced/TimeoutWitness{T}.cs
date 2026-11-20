// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer that resets a timer on each received element and signals a <see cref="TimeoutException"/> if no element arrives within the configured interval.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="observer">The downstream observer to forward elements to.</param>
/// <param name="dueTime">The maximum allowed inter-element interval.</param>
/// <param name="timeProvider">The time provider used to schedule the deadline.</param>
[System.Diagnostics.DebuggerDisplay("TimeoutWitness<T>: {_witness}")]
public sealed class TimeoutWitness<T>(IObserverAsync<T> observer, TimeSpan dueTime, TimeProvider timeProvider) : IWitnessAsync<T>
{
    /// <summary>Synchronization gate protecting timer state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes downstream notifications between upstream deliveries and the timer's timeout completion.</summary>
    private readonly ReactiveUI.Primitives.Async.Internal.AsyncFlowGate _downstreamGate = new();

    /// <summary>
    /// The one timer for this subscription, rearmed via <see cref="ITimer.Change(TimeSpan, TimeSpan)"/> on
    /// every emission so that tracking the deadline costs no per-emission allocation.
    /// </summary>
    private ITimer? _timer;

    /// <summary>Set once the observer has been terminated; suppresses any later timeout signal.</summary>
    private bool _completed;

    /// <summary>Non-zero once a terminal notification (source completion or timeout) has been claimed for downstream delivery.</summary>
    private int _terminated;

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

    /// <summary>Disposes the deadline timer during teardown.</summary>
    /// <returns>A completed task.</returns>
    public async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _completed = true;
        }

        if (_timer is not null)
        {
            await _timer.DisposeAsync().ConfigureAwait(false);
            _timer = null;
        }

        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
    }

    /// <summary>Allocates the timer and schedules the first deadline tick.</summary>
    /// <param name="cancellationToken">Unused; the timer carries its own deadline.</param>
    internal void StartTimer(CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        if (IsCompleted())
        {
            return;
        }

        try
        {
            var timer = timeProvider.CreateTimer(
                static state => ((TimeoutWitness<T>)state!).OnTimerFired(),
                this,
                dueTime,
                System.Threading.Timeout.InfiniteTimeSpan);

            if (!TryStoreTimer(timer))
            {
                timer.Dispose();
            }
        }
        catch (Exception e)
        {
            // If timeout setup fails, values continue without timeout enforcement.
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Rearms the deadline unless disposal has removed the timer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RearmTimer() =>
        _timer?.Change(dueTime, System.Threading.Timeout.InfiniteTimeSpan);

    /// <summary>Stops the deadline unless disposal has removed the timer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void StopTimer() =>
        _timer?.Change(System.Threading.Timeout.InfiniteTimeSpan, System.Threading.Timeout.InfiniteTimeSpan);

    /// <summary>Rearms the deadline timer and forwards the element to the downstream observer.</summary>
    /// <param name="value">The element to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        if (IsTerminated())
        {
            return default;
        }

        RearmTimer();
        return _downstreamGate.RunAsync(
            (Self: this, Value: value, Token: cancellationToken),
            static s => s.Self.IsTerminated() ? default : s.Self.ForwardNextAsync(s.Value, s.Token),
            cancellationToken);
    }

    /// <summary>Forwards the error to the downstream observer, leaving the deadline running.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        // A resumed error is not an element and does not end the sequence, so the deadline keeps running.
        return _downstreamGate.RunAsync(
            (Self: this, Error: error, Token: cancellationToken),
            static s => s.Self.IsTerminated() ? default : s.Self.ForwardErrorResumeAsync(s.Error, s.Token),
            cancellationToken);
    }

    /// <summary>Stops the deadline timer and forwards completion to the downstream observer.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        lock (_gate)
        {
            _completed = true;
        }

        StopTimer();
        return TryClaimTerminal()
            ? _downstreamGate.RunAsync(
                (Self: this, Result: result),
                static s => s.Self.ForwardCompletedAsync(s.Result),
                CancellationToken.None)
            : default;
    }

    /// <summary>Delivers the timeout completion after any in-flight delivery finishes, then tears down upstream.</summary>
    /// <param name="self">The timeout witness whose upstream subscription should be released.</param>
    /// <returns>A task representing the asynchronous notification.</returns>
    private static async Task FireTimeoutAsync(TimeoutWitness<T> self)
    {
        try
        {
            await self._downstreamGate.RunAsync(
                self,
                static s => s.ForwardCompletedAsync(Result.Failure(new TimeoutException())),
                CancellationToken.None).ConfigureAwait(false);
            await self._witness.DisposeSourceSubscriptionAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Gets whether a terminal notification has been claimed.</summary>
    /// <returns><see langword="true"/> once completion or timeout has been claimed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsTerminated() => Volatile.Read(ref _terminated) != 0;

    /// <summary>Claims the single terminal notification.</summary>
    /// <returns><see langword="true"/> for the first caller only.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryClaimTerminal() => Interlocked.Exchange(ref _terminated, 1) == 0;

    /// <summary>Forwards an element to the downstream observer.</summary>
    /// <param name="value">The element.</param>
    /// <param name="cancellationToken">A token that cancels the delivery.</param>
    /// <returns>The downstream delivery.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask ForwardNextAsync(T value, CancellationToken cancellationToken) =>
        observer.OnNextAsync(value, cancellationToken);

    /// <summary>Forwards a resumed error to the downstream observer.</summary>
    /// <param name="error">The error.</param>
    /// <param name="cancellationToken">A token that cancels the delivery.</param>
    /// <returns>The downstream delivery.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask ForwardErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        observer.OnErrorResumeAsync(error, cancellationToken);

    /// <summary>Forwards completion to the downstream observer.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>The downstream delivery.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask ForwardCompletedAsync(Result result) => observer.OnCompletedAsync(result);

    /// <summary>Checks whether the witness has already terminated or been disposed.</summary>
    /// <returns><see langword="true"/> when terminated; otherwise, <see langword="false"/>.</returns>
    private bool IsCompleted()
    {
        lock (_gate)
        {
            return _completed;
        }
    }

    /// <summary>Stores the created timer unless the witness terminated while the timer was being constructed.</summary>
    /// <param name="timer">The newly created timer.</param>
    /// <returns><see langword="true"/> when stored; <see langword="false"/> when the caller must dispose <paramref name="timer"/> immediately.</returns>
    private bool TryStoreTimer(ITimer timer)
    {
        lock (_gate)
        {
            if (_completed)
            {
                return false;
            }

            _timer = timer;
            return true;
        }
    }

    /// <summary>Timer callback: completes the downstream observer with a <see cref="TimeoutException"/> unless it has terminated.</summary>
    private void OnTimerFired()
    {
        lock (_gate)
        {
            if (_completed)
            {
                return;
            }

            _completed = true;
        }

        // Claiming here, before waiting for the gate, stops values that arrive meanwhile from following the timeout.
        if (TryClaimTerminal())
        {
            _ = FireTimeoutAsync(this);
        }
    }
}
