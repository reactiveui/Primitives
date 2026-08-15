// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that awaits a task and emits its result.</summary>
/// <typeparam name="T">The task result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Task = {Task}")]
public sealed class TaskResultSubscription<T> : TaskSignalSubscription<T>
{
    /// <summary>Initializes a new instance of the <see cref="TaskResultSubscription{T}"/> class.</summary>
    /// <param name="observer">The observer receiving the task result.</param>
    /// <param name="task">The task to observe.</param>
    public TaskResultSubscription(IObserverAsync<T> observer, Task<T> task)
        : base(observer) =>
        Task = task;

    /// <summary>Gets the task to observe.</summary>
    private Task<T> Task { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken)
    {
        var result = await Task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(result, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
