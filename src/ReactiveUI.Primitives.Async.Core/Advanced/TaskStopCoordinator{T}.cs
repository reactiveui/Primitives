// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Manages the subscription lifetime and completes the sequence when the task finishes.</summary>
/// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("TaskStopCoordinator<T>: Parent = {_parent}")]
public sealed class TaskStopCoordinator<T> : IAsyncDisposable
{
    /// <summary>The parent observable that owns this subscription.</summary>
    private readonly TaskStopSignal<T> _parent;

    /// <summary>Shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
    private readonly TakeUntilLifecycle<T> _lifecycle;

    /// <summary>The inner subscription handle.</summary>
    private IAsyncDisposable? _subscription;

    /// <summary>The registration that completes this sequence when the stop token fires.</summary>
    private CancellationTokenRegistration _stopTokenRegistration;

    /// <summary>Initializes a new instance of the <see cref="TaskStopCoordinator{T}"/> class.</summary>
    /// <param name="parent">The parent observable that owns this subscription.</param>
    /// <param name="observer">The downstream observer to forward items to.</param>
    public TaskStopCoordinator(TaskStopSignal<T> parent, IObserverAsync<T> observer)
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
        await _lifecycle.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Subscribes to the source observable and begins waiting for the task to complete.</summary>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
    {
        _stopTokenRegistration = _lifecycle.CompleteWhenCancelled(
            () => DisposableAsyncSlot.DisposeAsync(ref _subscription),
            _parent.StopToken);
        AwaitStopThenComplete(_parent.Task);
        var sub = await _parent.Source
            .SubscribeAsync(new TakeUntilSourceWitness<T>(_lifecycle), cancellationToken).ConfigureAwait(false);
        await DisposableAsyncSlot.AssignAsync(ref _subscription, sub).ConfigureAwait(false);
        if (_lifecycle.IsCompleted || _parent.Task.IsCompleted || _parent.StopToken.IsCancellationRequested)
        {
            await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards the original subscribe-time token into the shared lifecycle's dispose chain.</summary>
    /// <param name="external">The subscribe-time token.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LinkExternalCancellation(CancellationToken external) =>
        _lifecycle.LinkExternalCancellation(external);

    /// <summary>Waits for the task to complete, then forwards completion or error to the downstream observer.</summary>
    /// <param name="task">The task to await.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void AwaitStopThenComplete(Task task) => FireAndForgetHelper.Run(async () =>
    {
        try
        {
            await task.WaitAsync(System.Threading.Timeout.InfiniteTimeSpan, _lifecycle.DisposeToken)
                .ConfigureAwait(false);
            await _lifecycle.RelayCompletionAsync(Result.Success).ConfigureAwait(false);
            await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_lifecycle.DisposeToken.IsCancellationRequested)
        {
            // Subscription disposed before task completed.
        }
        catch (Exception e)
        {
            if (_parent.Options.SourceFailsWhenOtherFails)
            {
                await _lifecycle.RelayCompletionAsync(Result.Failure(e)).ConfigureAwait(false);
                await DisposableAsyncSlot.DisposeAsync(ref _subscription).ConfigureAwait(false);
            }
            else
            {
                await _lifecycle.RelayErrorAsync(e).ConfigureAwait(false);
            }
        }
    });
}
