// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Waits for a task and forwards the signal notification for one subscription.</summary>
public sealed class ToAsyncSignalSubscription : TaskSignalSubscription<RxVoid>
{
    /// <summary>Initializes a new instance of the <see cref="ToAsyncSignalSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving the signal notification.</param>
    /// <param name="task">The task observed by this subscription.</param>
    public ToAsyncSignalSubscription(IObserverAsync<RxVoid> observer, Task task)
        : base(observer) =>
        SourceTask = task;

    /// <summary>Gets the task observed by this subscription.</summary>
    private Task SourceTask { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(IObserverAsync<RxVoid> observer, CancellationToken cancellationToken)
    {
        await SourceTask.WaitAsync(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
