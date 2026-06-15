// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Shared subscription lifecycle for the arity-specific <c>CombineLatestN</c> operators (2..16) and
/// the enumerable variant. Each per-arity <c>SyncLatestCoordinator</c> composes one instance of
/// this class (has-a, not is-a) and forwards lifecycle / error / gating work into it, so the
/// previously-duplicated infrastructure (gate, dispose CTS, external-link registration, observer
/// fan-out, completion-bitmask handling) lives in one place.
/// </summary>
/// <typeparam name="TResult">The downstream element type.</typeparam>
public sealed class SyncLatestLifecycle<TResult> : IAsyncDisposable
{
    /// <summary>Serializes downstream notifications so OnNext / OnError / OnCompleted never overlap.</summary>
    private readonly AsyncSerialGate _gate = new();

    /// <summary>Cancellation source for subscription disposal; cancelled exactly once.</summary>
    private readonly CancellationTokenSource _disposeCts = new();

    /// <summary>Lock protecting <see cref="_doneFlags"/> updates.</summary>
    private readonly Lock _completionLock = new();

    /// <summary>The downstream observer that receives the projected results.</summary>
    private readonly IObserverAsync<TResult> _observer;

    /// <summary>Bitmask value with every source-completion bit set; the sequence completes when <see cref="_doneFlags"/> equals this value.</summary>
    private readonly int _allDoneMask;

    /// <summary>Bitmask of completed sources. Bit N is set when source N completes (no failure).</summary>
    private int _doneFlags;

    /// <summary>Set once disposal has begun, via <see cref="DisposalHelper"/>.</summary>
    private int _disposed;

    /// <summary>Registration that propagates the original subscribe-time cancellation into <see cref="_disposeCts"/>.</summary>
    private CancellationTokenRegistration _externalLinkRegistration;

    /// <summary>Initializes a new instance of the <see cref="SyncLatestLifecycle{TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sourceCount">The number of upstream sources (e.g. 2 for arity-2).</param>
    public SyncLatestLifecycle(IObserverAsync<TResult> observer, int sourceCount)
    {
        _observer = observer;
        Subscriptions = new IAsyncDisposable?[sourceCount];
        _allDoneMask = (1 << sourceCount) - 1;
        DisposeToken = _disposeCts.Token;
    }

    /// <summary>Gets the cached cancellation token that fires on subscription disposal.</summary>
    public CancellationToken DisposeToken { get; }

    /// <summary>Gets the per-source subscription slots (indexed 0..N-1).</summary>
    public IAsyncDisposable?[] Subscriptions { get; }

    /// <summary>Gets a value indicating whether disposal has been signalled.</summary>
    public bool HasDisposed => DisposalHelper.HasDisposed(_disposed);

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
            _disposeCts.Cancel();
            return;
        }

        _externalLinkRegistration = external.UnsafeRegister(
            static state => ((CancellationTokenSource)state!).Cancel(),
            _disposeCts);
    }

    /// <summary>Forwards an upstream error to the downstream observer under the gate, respecting disposal.</summary>
    /// <param name="error">The error to forward.</param>
    /// <returns>A ValueTask representing the asynchronous forward.</returns>
    public async ValueTask OnErrorResumeAsync(Exception error)
    {
        using (await _gate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            if (HasDisposed)
            {
                return;
            }

            await _observer.OnErrorResumeAsync(error, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>Emits a projected value to the downstream observer under the gate, respecting disposal.</summary>
    /// <param name="value">The projected value.</param>
    /// <returns>A ValueTask representing the asynchronous emit.</returns>
    public async ValueTask EmitDownstreamAsync(TResult value)
    {
        using (await _gate.EnterAsync(DisposeToken).ConfigureAwait(false))
        {
            if (HasDisposed)
            {
                return;
            }

            await _observer.OnNextAsync(value, DisposeToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Records completion of a single source. If the source failed, completes the combined sequence
    /// with the failure; otherwise sets the matching <paramref name="doneBit"/> and, once every
    /// source bit is set, completes the combined sequence successfully.
    /// </summary>
    /// <param name="result">The completion result from the upstream source.</param>
    /// <param name="doneBit">The bitmask bit owned by the completing source (<c>1 &lt;&lt; index</c>).</param>
    /// <returns>A ValueTask representing the asynchronous handler.</returns>
    public ValueTask OnSourceCompletedAsync(Result result, int doneBit)
    {
        if (result.IsFailure)
        {
            return FinishAsync(result);
        }

        int updated;
        lock (_completionLock)
        {
            _doneFlags |= doneBit;
            updated = _doneFlags;
        }

        return updated == _allDoneMask ? _observer.OnCompletedAsync(result) : default;
    }

    /// <summary>Disposes the lifecycle without signalling a terminal notification.</summary>
    /// <returns>A ValueTask representing the asynchronous teardown.</returns>
    public ValueTask DisposeAsync() => FinishAsync(null);

    /// <summary>Completes the combined sequence and disposes every source subscription.</summary>
    /// <param name="result">The completion result, or <see langword="null"/> when disposing without signaling.</param>
    /// <returns>A ValueTask representing the asynchronous teardown.</returns>
    public async ValueTask FinishAsync(Result? result)
    {
        if (DisposalHelper.TrySetDisposed(ref _disposed))
        {
            return;
        }

        try
        {
            await _disposeCts.CancelAsync().ConfigureAwait(false);

            for (var i = 0; i < Subscriptions.Length; i++)
            {
                var d = Subscriptions[i];
                if (d is not null)
                {
                    await d.DisposeAsync().ConfigureAwait(false);
                }
            }

            if (result is not null)
            {
                await _observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            }
        }
        finally
        {
            // Always release the unmanaged-style primitives even if upstream DisposeAsync or
            // OnCompletedAsync throws — otherwise a misbehaving downstream leaks the gate's
            // SemaphoreSlim and the dispose CTS's wait handles.
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
            await _externalLinkRegistration.DisposeAsync().ConfigureAwait(false);
#else
            _externalLinkRegistration.Dispose();
#endif
            _disposeCts.Dispose();
            _gate.Dispose();
        }
    }
}
