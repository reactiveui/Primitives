// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that runs a supplied asynchronous job for each subscription.</summary>
/// <typeparam name="T">The element type emitted by the job.</typeparam>
public sealed class BackgroundJobSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="BackgroundJobSignal{T}"/> class.</summary>
    /// <param name="job">The job to execute for each subscription.</param>
    /// <param name="startSynchronously">A value indicating whether the job starts synchronously on subscribe.</param>
    /// <param name="taskScheduler">The scheduler used to start the job asynchronously.</param>
    public BackgroundJobSignal(
        Func<IObserverAsync<T>, CancellationToken, ValueTask> job,
        bool startSynchronously,
        TaskScheduler? taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(job);

        Job = job;
        StartSynchronously = startSynchronously;
        TaskScheduler = taskScheduler;
    }

    /// <summary>Gets the job to execute for each subscription.</summary>
    private Func<IObserverAsync<T>, CancellationToken, ValueTask> Job { get; }

    /// <summary>Gets a value indicating whether the job starts synchronously on subscribe.</summary>
    private bool StartSynchronously { get; }

    /// <summary>Gets the scheduler used to start the job asynchronously.</summary>
    private TaskScheduler? TaskScheduler { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        if (StartSynchronously)
        {
            return new(TaskSignalSubscription.StartNew(Job, observer));
        }

        var taskScheduler = TaskScheduler;
        return taskScheduler is null
            ? new(TaskSignalSubscription.StartNew(ExecuteAfterYieldAsync, observer))
            : new(TaskSignalSubscription.StartNew(
                (obs, ct) => ExecuteOnSchedulerAsync(obs, taskScheduler, ct),
                observer));
    }

    /// <summary>Starts the job after yielding to the scheduler.</summary>
    /// <param name="observer">The observer receiving job notifications.</param>
    /// <param name="cancellationToken">The cancellation token for the job.</param>
    /// <returns>A task representing the job.</returns>
    private async ValueTask ExecuteAfterYieldAsync(IObserverAsync<T> observer, CancellationToken cancellationToken)
    {
        await Task.Yield();
        await Job(observer, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Starts the job through the configured task scheduler.</summary>
    /// <param name="observer">The observer receiving job notifications.</param>
    /// <param name="taskScheduler">The scheduler used to run the job.</param>
    /// <param name="cancellationToken">The cancellation token for the job.</param>
    /// <returns>A task representing the job.</returns>
    private async ValueTask ExecuteOnSchedulerAsync(
        IObserverAsync<T> observer,
        TaskScheduler taskScheduler,
        CancellationToken cancellationToken) =>
        await Task.Factory.StartNew(
                () => Job(observer, cancellationToken).AsTask(),
                cancellationToken,
                TaskCreationOptions.DenyChildAttach,
                taskScheduler)
            .Unwrap().ConfigureAwait(false);
}
