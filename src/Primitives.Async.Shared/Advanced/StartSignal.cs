// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Runs an action for each subscription and emits <see cref="RxVoid.Default"/> after it completes.</summary>
[System.Diagnostics.DebuggerDisplay("Action = {Action}, TaskScheduler = {TaskScheduler}")]
public sealed class StartSignal : IObservableAsync<RxVoid>
{
    /// <summary>Initializes a new instance of the <see cref="StartSignal"/> class.</summary>
    /// <param name="action">The action invoked for each subscription.</param>
    /// <param name="taskScheduler">The scheduler used to start the action, or null to run during subscription start.</param>
    public StartSignal(Action action, TaskScheduler? taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        Action = action;
        TaskScheduler = taskScheduler;
    }

    /// <summary>Gets the action invoked for each subscription.</summary>
    private Action Action { get; }

    /// <summary>Gets the scheduler used to start the action.</summary>
    private TaskScheduler? TaskScheduler { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<RxVoid>.SubscribeAsync(
        IObserverAsync<RxVoid> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        StartSubscription subscription = new(observer, Action, TaskScheduler);
        subscription.Start();
        return new(subscription);
    }
}
