// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that awaits a task and emits its result.</summary>
/// <typeparam name="T">The task result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TaskResultSubscription: Task = {Task}")]
public sealed class TaskResultSubscription<T> : IAsyncDisposable, ITaskSignalJob<T>
{
    /// <summary>The observer receiving the job's notifications.</summary>
    private readonly IObserverAsync<T> _observer;

    /// <summary>Runs the job and joins it on disposal.</summary>
    private readonly TaskSignalState _task = new();

    /// <summary>Initializes a new instance of the <see cref="TaskResultSubscription{T}"/> class.</summary>
    /// <param name="observer">The observer receiving the task result.</param>
    /// <param name="task">The task to observe.</param>
    public TaskResultSubscription(IObserverAsync<T> observer, Task<T> task)
    {
        _observer = observer;
        Task = task;
    }

    /// <summary>Gets the task to observe.</summary>
    private Task<T> Task { get; }

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _task.Start(this, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _task.DisposeAsync();

    /// <inheritdoc/>
    async ValueTask ITaskSignalJob<T>.ExecuteAsync(IObserverAsync<T> observer, CancellationToken cancellationToken)
    {
        var result = await Task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(result, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
