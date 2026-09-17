// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Waits for a task and forwards the signal notification for one subscription.</summary>
[System.Diagnostics.DebuggerDisplay("ToAsyncSignalSubscription: SourceTask = {SourceTask}, Completed = {SourceTask.IsCompleted}")]
public sealed class ToAsyncSignalSubscription : IAsyncDisposable, ITaskSignalJob<RxVoid>
{
    /// <summary>The observer receiving the job's notifications.</summary>
    private readonly IObserverAsync<RxVoid> _observer;

    /// <summary>Runs the job and joins it on disposal.</summary>
    private readonly TaskSignalState _task = new();

    /// <summary>Initializes a new instance of the <see cref="ToAsyncSignalSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving the signal notification.</param>
    /// <param name="task">The task observed by this subscription.</param>
    public ToAsyncSignalSubscription(IObserverAsync<RxVoid> observer, Task task)
    {
        _observer = observer;
        SourceTask = task;
    }

    /// <summary>Gets the task observed by this subscription.</summary>
    private Task SourceTask { get; }

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _task.Start(this, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _task.DisposeAsync();

    /// <inheritdoc/>
    async ValueTask ITaskSignalJob<RxVoid>.ExecuteAsync(
        IObserverAsync<RxVoid> observer,
        CancellationToken cancellationToken)
    {
        await SourceTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
