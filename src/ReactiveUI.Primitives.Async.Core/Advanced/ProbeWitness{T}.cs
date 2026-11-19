// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Holds the latest element and forwards it when the sampling timer elapses.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="period">The sampling period.</param>
/// <param name="timeProvider">The time provider used for the sampling timer.</param>
[System.Diagnostics.DebuggerDisplay("ProbeWitness<T>: {_witness}")]
public sealed class ProbeWitness<T>(IObserverAsync<T> observer, TimeSpan period, TimeProvider timeProvider) : IWitnessAsync<T>
{
    /// <summary>Guards the held element and the timer flag.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes downstream observer notifications between the timer tick and upstream completion/error.</summary>
    private readonly ReactiveUI.Primitives.Async.Internal.AsyncFlowGate _downstreamGate = new();

    /// <summary>The most recent element; valid only while <see cref="_hasLatest"/> is set.</summary>
    private T? _latest;

    /// <summary>Whether an element has arrived since the last tick.</summary>
    private bool _hasLatest;

    /// <summary>Whether a sampling timer is already running.</summary>
    private bool _timerActive;

    /// <summary>Non-zero once completion has been delivered under <see cref="_downstreamGate"/>.</summary>
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

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _hasLatest = false;
            _latest = default;
            _timerActive = false;
        }

        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        _ = HoldAsync(value, cancellationToken.CanBeCanceled ? cancellationToken : this.InternalDisposedToken);
        return default;
    }

    /// <summary>Holds the element, starting a sampling timer when none is running.</summary>
    /// <param name="value">The element to hold until the next tick.</param>
    /// <param name="cancellationToken">Cancellation for the sampling timer.</param>
    /// <returns>The timer operation, or a completed task when a timer is already running.</returns>
    internal Task HoldAsync(T value, CancellationToken cancellationToken)
    {
        bool startTimer;
        lock (_gate)
        {
            _latest = value;
            _hasLatest = true;
            startTimer = !_timerActive;
            _timerActive = true;
        }

        return startTimer ? TickAfterPeriodAsync(cancellationToken) : Task.CompletedTask;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _downstreamGate.RunAsync(
            (Self: this, Error: error, Token: cancellationToken),
            static s => s.Self.IsDeliverable()
                ? s.Self.ForwardErrorResumeAsync(s.Error, s.Token)
                : default,
            cancellationToken);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        T? pending;
        bool hasPending;
        lock (_gate)
        {
            hasPending = _hasLatest;
            pending = _latest;
            _latest = default;
            _hasLatest = false;
            _timerActive = false;
        }

        return _downstreamGate.RunAsync(
            (Self: this, Pending: pending, HasPending: hasPending, Result: result),
            static s => s.Self.DeliverTerminalAsync(s.Pending, s.HasPending, s.Result),
            CancellationToken.None);
    }

    /// <summary>Waits one period and forwards whichever element is held when it elapses.</summary>
    /// <param name="cancellationToken">A token that cancels the wait.</param>
    /// <returns>The wait and forwarding operation.</returns>
    internal async Task TickAfterPeriodAsync(CancellationToken cancellationToken)
    {
        // The tick runs on its own schedule. When it was started from inside a downstream delivery, it must still wait for
        // the gate instead of counting as that delivery re-entering.
        _downstreamGate.DetachCurrentFlow();
        try
        {
            await SignalAsyncExtensions.DelayAsync(period, timeProvider, cancellationToken).ConfigureAwait(false);

            T? pending;
            bool hasPending;
            lock (_gate)
            {
                _timerActive = false;
                hasPending = _hasLatest;
                pending = _latest;
                _latest = default;
                _hasLatest = false;
            }

            if (hasPending)
            {
                await _downstreamGate.RunAsync(
                    (Self: this, Pending: pending!, Token: cancellationToken),
                    static s => s.Self.IsDeliverable()
                        ? s.Self.ForwardNextAsync(s.Pending, s.Token)
                        : default,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Cancelled by disposal or caller token.
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Delivers the held element, then completion, once, while holding the downstream gate.</summary>
    /// <param name="pending">The element held when the source completed.</param>
    /// <param name="hasPending">Whether <paramref name="pending"/> holds an element.</param>
    /// <param name="result">The completion result.</param>
    /// <returns>The downstream delivery.</returns>
    internal async ValueTask DeliverTerminalAsync(T? pending, bool hasPending, Result result)
    {
        if (Interlocked.Exchange(ref _terminated, 1) != 0)
        {
            return;
        }

        if (hasPending)
        {
            await observer.OnNextAsync(pending!, CancellationToken.None).ConfigureAwait(false);
        }

        await observer.OnCompletedAsync(result).ConfigureAwait(false);
    }

    /// <summary>Gets whether the downstream observer can still receive values and resumed errors.</summary>
    /// <returns><see langword="true"/> before completion is delivered and before disposal.</returns>
    private bool IsDeliverable() => Volatile.Read(ref _terminated) == 0 && !_witness.HasDisposed;

    /// <summary>Forwards a sampled element to the downstream observer.</summary>
    /// <param name="value">The sampled element.</param>
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
}
