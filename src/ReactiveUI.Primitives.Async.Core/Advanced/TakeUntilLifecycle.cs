// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Shared subscription lifecycle for the TakeUntil family of operators (CancellationToken / Task /
/// raw-signal / async-observable / async-predicate variants). Each per-trigger Subscription
/// composes one instance of this class (has-a, not is-a) and forwards forward-on-next / -error /
/// -completed plus external-cancellation linking into it, so the previously-duplicated
/// infrastructure (gate, dispose CTS, external-link registration, gated observer fan-out) lives
/// in one place.
/// </summary>
/// <typeparam name="T">The downstream element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Observer = {_observer}, DisposeRequested = {DisposeToken.IsCancellationRequested}")]
public sealed class TakeUntilLifecycle<T> : IAsyncDisposable
{
    /// <summary>Cancellation source for subscription disposal; cancelled exactly once.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>Serializes downstream notifications so OnNext / OnError / OnCompleted never overlap.</summary>
    private readonly AsyncSerialGate _gate = new();

    /// <summary>The downstream observer that receives values, errors, and the terminal completion.</summary>
    private readonly IObserverAsync<T> _observer;

    /// <summary>Registration that propagates the subscribe-time cancellation token into <see cref="_cts"/>.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>Initializes a new instance of the <see cref="TakeUntilLifecycle{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public TakeUntilLifecycle(IObserverAsync<T> observer)
    {
        _observer = observer;
        DisposeToken = _cts.Token;
    }

    /// <summary>Gets the cached cancellation token that fires on subscription disposal.</summary>
    public CancellationToken DisposeToken { get; }

    /// <summary>
    /// Links the original subscribe-time cancellation token into this subscription's dispose chain so
    /// per-emission methods can use <see cref="DisposeToken"/> directly instead of allocating a
    /// per-emission linked CTS.
    /// </summary>
    /// <param name="external">The subscribe-time token.</param>
    public void LinkExternalCancellation(CancellationToken external)
    {
        if (!external.CanBeCanceled || external == DisposeToken)
        {
            return;
        }

        if (external.IsCancellationRequested)
        {
            _cts.Cancel();
            return;
        }

        _externalLinkRegistration = external.UnsafeRegister(
            static state => ((CancellationTokenSource)state!).Cancel(),
            _cts);
    }

    /// <summary>Forwards a value to the downstream observer under the serialization gate.</summary>
    /// <param name="value">The value to forward.</param>
    /// <returns>A ValueTask representing the asynchronous forward.</returns>
    public async ValueTask RelayNextAsync(T value)
    {
        using (await _gate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            await _observer.OnNextAsync(value, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards a non-terminal error to the downstream observer under the serialization gate.</summary>
    /// <param name="error">The error to forward.</param>
    /// <returns>A ValueTask representing the asynchronous forward.</returns>
    public async ValueTask RelayErrorAsync(Exception error)
    {
        using (await _gate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            await _observer.OnErrorResumeAsync(error, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards the completion signal to the downstream observer under the serialization gate.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A ValueTask representing the asynchronous forward.</returns>
    public async ValueTask RelayCompletionAsync(Result result)
    {
        using (await _gate.EnterAsync(CancellationToken.None).ConfigureAwait(false))
        {
            await _observer.OnCompletedAsync(result).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Tears down the shared subscription state: cancels the dispose CTS, releases the external-link
    /// registration, disposes the gate. The owning Subscription is responsible for disposing its
    /// per-operator handles (source subscription, token callback, etc.) BEFORE or AFTER calling this
    /// — the order is operator-specific. The release of unmanaged-style primitives is wrapped in
    /// try/finally so a misbehaving downstream can't leak the gate's SemaphoreSlim or the CTS wait
    /// handles.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous teardown.</returns>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _cts.CancelAsync().ConfigureAwait(false);
        }
        finally
        {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
            await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
            _externalLinkRegistration.Dispose();
#endif
            _cts.Dispose();
            _gate.Dispose();
        }
    }
}
