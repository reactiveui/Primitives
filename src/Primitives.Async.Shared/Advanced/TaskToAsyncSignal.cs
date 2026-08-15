// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Observes a task and emits <see cref="RxVoid.Default"/> when it completes successfully.</summary>
[System.Diagnostics.DebuggerDisplay("SourceTask = {SourceTask}")]
public sealed class TaskToAsyncSignal : IObservableAsync<RxVoid>
{
    /// <summary>Initializes a new instance of the <see cref="TaskToAsyncSignal"/> class.</summary>
    /// <param name="task">The task observed by each subscription.</param>
    public TaskToAsyncSignal(Task task) => SourceTask = task;

    /// <summary>Gets the task observed by each subscription.</summary>
    private Task SourceTask { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<RxVoid>.SubscribeAsync(
        IObserverAsync<RxVoid> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        ToAsyncSignalSubscription subscription = new(observer, SourceTask);
        subscription.Start();
        return new(subscription);
    }
}
