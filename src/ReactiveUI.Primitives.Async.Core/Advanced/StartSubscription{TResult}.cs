// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that invokes a synchronous function and emits its result.</summary>
/// <typeparam name="TResult">The result type.</typeparam>
public sealed class StartSubscription<TResult> : TaskSignalSubscription<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="StartSubscription{TResult}"/> class.</summary>
    /// <param name="observer">The observer receiving the produced value.</param>
    /// <param name="function">The function to invoke.</param>
    /// <param name="taskScheduler">The optional scheduler used to invoke the function.</param>
    public StartSubscription(
        IObserverAsync<TResult> observer,
        Func<TResult> function,
        TaskScheduler? taskScheduler)
        : base(observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(function);

        Function = function;
        TaskScheduler = taskScheduler;
    }

    /// <summary>Gets the function to invoke.</summary>
    private Func<TResult> Function { get; }

    /// <summary>Gets the optional scheduler used to invoke the function.</summary>
    private TaskScheduler? TaskScheduler { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        var taskScheduler = TaskScheduler;
        if (taskScheduler is null)
        {
            await ExecuteFunctionAsync(observer, cancellationToken).ConfigureAwait(false);
            return;
        }

        await Task.Factory.StartNew(
                static s =>
                {
                    var (self, observer, cancellationToken) =
                        ((StartSubscription<TResult>, IObserverAsync<TResult>, CancellationToken))(
                            s ?? throw new InvalidOperationException("The start state is missing."));
                    return self.ExecuteFunctionAsync(observer, cancellationToken).AsTask();
                },
                (this, observer, cancellationToken),
                cancellationToken,
                TaskCreationOptions.DenyChildAttach,
                taskScheduler)
            .Unwrap()
            .ConfigureAwait(false);
    }

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
