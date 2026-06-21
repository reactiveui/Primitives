// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that invokes a synchronous function and emits its result for each subscription.</summary>
/// <typeparam name="TResult">The result type.</typeparam>
public sealed class StartSignal<TResult> : IObservableAsync<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="StartSignal{TResult}"/> class.</summary>
    /// <param name="function">The function to invoke.</param>
    /// <param name="taskScheduler">The optional scheduler used to invoke the function.</param>
    public StartSignal(Func<TResult> function, TaskScheduler? taskScheduler)
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
    ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        StartSubscription<TResult> subscription = new(observer, Function, TaskScheduler);
        subscription.Start();
        return new(subscription);
    }
}
