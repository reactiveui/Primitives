// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Subscription lifecycle for the TakeUntil family of operators (CancellationToken, Task,
/// raw-signal, async-observable and async-predicate triggers): owns the serialization gate, the
/// dispose cancellation source, the external-link registration and the gated observer fan-out.
/// </summary>
/// <typeparam name="T">The downstream element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TakeUntilLifecycle: Observer = {_observer}, DisposeRequested = {DisposeToken.IsCancellationRequested}")]
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
    /// Links the subscribe-time cancellation token into this subscription's dispose chain, so
    /// <see cref="DisposeToken"/> alone covers both and no per-emission linked source is needed.
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
    /// Cancels the dispose token, releases the external-link registration and disposes the gate. The
    /// owning subscription disposes its own per-operator handles (source subscription, token
    /// callback) separately, in whichever order that operator requires.
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
