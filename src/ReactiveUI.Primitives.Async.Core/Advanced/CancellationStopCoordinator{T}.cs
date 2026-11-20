// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Manages the subscription lifetime and completes the sequence when the cancellation token is canceled.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("CancellationStopCoordinator<T>: Parent = {_parent}")]
public sealed class CancellationStopCoordinator<T> : IAsyncDisposable
{
    /// <summary>The parent observable that owns this subscription.</summary>
    private readonly CancellationStopSignal<T> _parent;

    /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
    private readonly TakeUntilLifecycle<T> _lifecycle;

    /// <summary>The inner subscription handle.</summary>
    private IAsyncDisposable? _subscription;

    /// <summary>The registration handle for the external cancellation token callback.</summary>
    private CancellationTokenRegistration? _tokenRegistration;

    /// <summary>Initializes a new instance of the <see cref="CancellationStopCoordinator{T}"/> class.</summary>
    /// <param name="parent">The parent observable that owns this subscription.</param>
    /// <param name="observer">The downstream observer to forward items to.</param>
    public CancellationStopCoordinator(CancellationStopSignal<T> parent, IObserverAsync<T> observer)
    {
        _parent = parent;
        _lifecycle = new(observer);
    }

    /// <summary>Asynchronously releases resources used by this subscription.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_tokenRegistration is { } reg)
        {
            _tokenRegistration = null;
#if NET8_0_OR_GREATER
            await reg.DisposeAsync().ConfigureAwait(false);
#else
            reg.Dispose();
#endif
        }

        await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        await _lifecycle.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Subscribes to the source observable and registers the cancellation token callback.</summary>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
    {
        if (_parent.CancellationToken.IsCancellationRequested)
        {
            await CompleteFromCancellationAsync().ConfigureAwait(false);
            return;
        }

        _tokenRegistration = _parent.CancellationToken.Register(CompleteFromCancellation);
        var sub = await _parent.Source
            .SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
        await DisposableAsyncSlot.AssignAsync(ref _subscription, sub).ConfigureAwait(false);
        if (_lifecycle.IsCompleted || _parent.CancellationToken.IsCancellationRequested)
        {
            await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
    /// <param name="external">The subscribe-time token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LinkExternalCancellation(CancellationToken external) =>
        _lifecycle.LinkExternalCancellation(external);

    /// <summary>Callback invoked when the external cancellation token is canceled; forwards completion to the observer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void CompleteFromCancellation() => FireAndForgetHelper.Run(CompleteAfterYieldAsync);

    /// <summary>Forwards successful completion through the notification gate and tears down upstream.</summary>
    /// <returns>The completion notification.</returns>
    internal async ValueTask CompleteFromCancellationAsync()
    {
        await _lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
        await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
    }

    /// <summary>Defers completion until after the cancellation callback returns.</summary>
    /// <returns>The deferred completion notification.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private async ValueTask CompleteAfterYieldAsync()
    {
        await Task.Yield();
        await CompleteFromCancellationAsync().ConfigureAwait(false);
    }
}
