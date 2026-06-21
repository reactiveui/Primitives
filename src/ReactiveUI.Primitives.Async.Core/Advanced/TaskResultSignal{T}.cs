// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that emits the result of a task for each subscription.</summary>
/// <typeparam name="T">The task result type.</typeparam>
public sealed class TaskResultSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="TaskResultSignal{T}"/> class.</summary>
    /// <param name="task">The task to observe.</param>
    public TaskResultSignal(Task<T> task) => Task = task;

    /// <summary>Gets the task to observe.</summary>
    private Task<T> Task { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        TaskResultSubscription<T> subscription = new(observer, Task);
        subscription.Start();
        return new(subscription);
    }
}
