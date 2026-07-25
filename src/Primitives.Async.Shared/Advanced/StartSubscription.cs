// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Runs an action and forwards the signal notification for one subscription.</summary>
public sealed class StartSubscription : TaskSignalSubscription<RxVoid>
{
    /// <summary>Initializes a new instance of the <see cref="StartSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving the signal notification.</param>
    /// <param name="action">The action invoked for this subscription.</param>
    /// <param name="taskScheduler">The scheduler used to start the action, or null to run during subscription start.</param>
    public StartSubscription(
        IObserverAsync<RxVoid> observer,
        Action action,
        TaskScheduler? taskScheduler)
        : base(observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        Action = action;
        TaskScheduler = taskScheduler;
    }

    /// <summary>Gets the action invoked for this subscription.</summary>
    private Action Action { get; }

    /// <summary>Gets the scheduler used to start the action.</summary>
    private TaskScheduler? TaskScheduler { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(
        IObserverAsync<RxVoid> observer,
        CancellationToken cancellationToken)
    {
        var taskScheduler = TaskScheduler;
        if (taskScheduler is null)
        {
            await ExecuteActionAsync(observer, cancellationToken).ConfigureAwait(false);
            return;
        }

        await Task.Factory.StartNew(
                static s =>
                {
                    var (self, observer, cancellationToken) =
                        ((StartSubscription, IObserverAsync<RxVoid>, CancellationToken))s!;
                    return self.ExecuteActionAsync(observer, cancellationToken).AsTask();
                },
                (this, observer, cancellationToken),
                cancellationToken,
                TaskCreationOptions.DenyChildAttach,
                taskScheduler)
            .Unwrap()
            .ConfigureAwait(false);
    }

    /// <summary>Runs the action and forwards the completion signal.</summary>
    /// <param name="observer">The observer receiving the signal notification.</param>
    /// <param name="cancellationToken">The cancellation token for notification calls.</param>
    /// <returns>A task that completes after the action and signal notification complete.</returns>
    private async ValueTask ExecuteActionAsync(IObserverAsync<RxVoid> observer, CancellationToken cancellationToken)
    {
        Action();
        await observer.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
