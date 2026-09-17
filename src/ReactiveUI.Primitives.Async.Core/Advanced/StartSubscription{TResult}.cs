// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that invokes a synchronous function and emits its result.</summary>
/// <typeparam name="TResult">The result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("StartSubscription: Function = {Function}, TaskScheduler = {TaskScheduler}")]
public sealed class StartSubscription<TResult> : IAsyncDisposable, ITaskSignalJob<TResult>
{
    /// <summary>The observer receiving the job's notifications.</summary>
    private readonly IObserverAsync<TResult> _observer;

    /// <summary>Runs the job and joins it on disposal.</summary>
    private readonly TaskSignalState _task = new();

    /// <summary>Initializes a new instance of the <see cref="StartSubscription{TResult}"/> class.</summary>
    /// <param name="observer">The observer receiving the produced value.</param>
    /// <param name="function">The function to invoke.</param>
    /// <param name="taskScheduler">The optional scheduler that invokes the function; <see langword="null"/> invokes it inline.</param>
    public StartSubscription(
        IObserverAsync<TResult> observer,
        Func<TResult> function,
        TaskScheduler? taskScheduler)
    {
        _observer = observer;
        ArgumentExceptionHelper.ThrowIfNull(function);

        Function = function;
        TaskScheduler = taskScheduler;
    }

    /// <summary>Gets the function to invoke.</summary>
    private Func<TResult> Function { get; }

    /// <summary>Gets the optional scheduler that invokes the function.</summary>
    private TaskScheduler? TaskScheduler { get; }

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _task.Start(this, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _task.DisposeAsync();

    /// <inheritdoc/>
    async ValueTask ITaskSignalJob<TResult>.ExecuteAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        var taskScheduler = TaskScheduler;
        if (taskScheduler is null)
        {
            await ExecuteFunctionAsync(observer, cancellationToken).ConfigureAwait(false);
            return;
        }

        await ExecuteOnSchedulerAsync(observer, taskScheduler, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Starts the function through the supplied scheduler.</summary>
    /// <param name="observer">The observer receiving the produced value.</param>
    /// <param name="taskScheduler">The scheduler that starts the function.</param>
    /// <param name="cancellationToken">Cancellation for the scheduled task and notifications.</param>
    /// <returns>The scheduled function and notification operation.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private Task ExecuteOnSchedulerAsync(
        IObserverAsync<TResult> observer,
        TaskScheduler taskScheduler,
        CancellationToken cancellationToken) =>
        Task.Factory.StartNew(
                static s =>
                {
                    var (self, observer, cancellationToken) =
                        ((StartSubscription<TResult>, IObserverAsync<TResult>, CancellationToken))s!;
                    return self.ExecuteFunctionAsync(observer, cancellationToken).AsTask();
                },
                (this, observer, cancellationToken),
                cancellationToken,
                TaskCreationOptions.DenyChildAttach,
                taskScheduler)
            .Unwrap();

    /// <summary>Invokes the function and forwards its result to the observer.</summary>
    /// <param name="observer">The observer receiving the produced value.</param>
    /// <param name="cancellationToken">The cancellation token for observer notifications.</param>
    /// <returns>A task representing the asynchronous notification work.</returns>
    private async ValueTask ExecuteFunctionAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        await observer.OnNextAsync(Function(), cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
