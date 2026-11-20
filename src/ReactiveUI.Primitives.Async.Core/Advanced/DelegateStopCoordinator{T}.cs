// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Manages the subscription lifetime and completes when the raw stop signal fires.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("DelegateStopCoordinator<T>: Parent = {_parent}")]
public sealed class DelegateStopCoordinator<T> : IAsyncDisposable
{
    /// <summary>The parent observable that owns this subscription.</summary>
    private readonly DelegateStopSignal<T> _parent;

    /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
    private readonly TakeUntilLifecycle<T> _lifecycle;

    /// <summary>The inner subscription handle.</summary>
    private IAsyncDisposable? _subscription;

    /// <summary>The handle the stop delegate returned, disposed once the signal has been dealt with.</summary>
    private IAsyncDisposable? _stopRegistration;

    /// <summary>Set once the stop signal has fired, so a second notification is ignored.</summary>
    private int _stopSignalled;

    /// <summary>Set once <see cref="_stopRegistration"/> has been disposed, so it is disposed once.</summary>
    private int _stopRegistrationDisposed;

    /// <summary>The registration that completes this sequence when the stop token fires.</summary>
    private CancellationTokenRegistration _stopTokenRegistration;

    /// <summary>Initializes a new instance of the <see cref="DelegateStopCoordinator{T}"/> class.</summary>
    /// <param name="parent">The parent observable that owns this subscription.</param>
    /// <param name="observer">The downstream observer to forward items to.</param>
    public DelegateStopCoordinator(DelegateStopSignal<T> parent, IObserverAsync<T> observer)
    {
        _parent = parent;
        _lifecycle = new(observer);
    }

    /// <summary>Asynchronously releases resources used by this subscription.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
#if NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1_OR_GREATER
        await _stopTokenRegistration.DisposeAsync().ConfigureAwait(false);
#else
        _stopTokenRegistration.Dispose();
#endif

        await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        await DisposeStopRegistrationAsync().ConfigureAwait(false);
        await _lifecycle.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Subscribes to the source observable and begins waiting for the stop signal.</summary>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
    {
        _stopTokenRegistration = _lifecycle.CompleteWhenCancelled(
            () => DisposableAsyncSlot.DisposeAsync(ref _subscription),
            _parent.StopToken);
        AwaitStopThenComplete();
        var sub = await _parent.Source
            .SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
        await DisposableAsyncSlot.AssignAsync(ref _subscription, sub).ConfigureAwait(false);
        if (_lifecycle.IsCompleted || Volatile.Read(ref _stopSignalled) != 0 || _parent.StopToken.IsCancellationRequested)
        {
            await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
    /// <param name="external">The subscribe-time token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LinkExternalCancellation(CancellationToken external) =>
        _lifecycle.LinkExternalCancellation(external);

    /// <summary>Hands the stop callback to the delegate and releases a registration that arrives after the callback has fired.</summary>
    internal void AwaitStopThenComplete()
    {
        Volatile.Write(ref _stopRegistration, _parent.StopSignal(Stop));

        // Registrations returned after termination are disposed immediately.
        if (Volatile.Read(ref _stopSignalled) != 1)
        {
            return;
        }

        FireAndForgetHelper.Run(DisposeStopRegistrationAsync);
    }

    /// <summary>Ends the sequence the first time the stop delegate notifies.</summary>
    /// <param name="result">The result the stop delegate reported.</param>
    private void Stop(Result result)
    {
        if (Interlocked.Exchange(ref _stopSignalled, 1) != 0)
        {
            return;
        }

        FireAndForgetHelper.Run(() => CompleteAsync(result));
    }

    /// <summary>Releases the stop registration, then forwards the outcome to the downstream observer.</summary>
    /// <param name="result">The result the stop delegate reported.</param>
    /// <returns>A task representing the asynchronous completion.</returns>
    private async ValueTask CompleteAsync(Result result)
    {
        await DisposeStopRegistrationAsync().ConfigureAwait(false);

        if (!result.IsFailure)
        {
            await _lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
            await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
            return;
        }

        var error = result.Exception;
        if (_parent.Options.SourceFailsWhenOtherFails)
        {
            await _lifecycle.RelayCompletionAsync(Result.Failure(error)).ConfigureAwait(false);
            await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
            return;
        }

        await _lifecycle.RelayErrorAsync(error).ConfigureAwait(false);
    }

    /// <summary>Disposes the handle the stop delegate returned, exactly once.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    private async ValueTask DisposeStopRegistrationAsync()
    {
        if (Volatile.Read(ref _stopRegistration) is not { } registration
            || Interlocked.Exchange(ref _stopRegistrationDisposed, 1) != 0)
        {
            return;
        }

        try
        {
            await registration.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception disposeError)
        {
            // Secondary disposal failures reach the global handler.
            UnhandledExceptionHandler.ReportUnhandledException(disposeError);
        }
    }
}
