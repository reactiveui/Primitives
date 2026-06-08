// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides static methods for creating and manipulating asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class offers factory methods and utilities for working with asynchronous
/// observables, enabling reactive programming patterns with support for asynchronous event streams. Members of this
/// class are thread-safe and designed for use in concurrent environments.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that executes the supplied action and emits <see cref="RxVoid.Default"/>.</summary>
    /// <param name="action">The action to execute.</param>
    /// <returns>An observable sequence that completes after the action has run.</returns>
    public static IObservableAsync<RxVoid> Start(Action action) => Start(action, null);

    /// <summary>Creates an observable sequence that executes the supplied action and emits <see cref="RxVoid.Default"/>.</summary>
    /// <param name="action">The action to execute.</param>
    /// <param name="taskScheduler">An optional scheduler used to start the action.</param>
    /// <returns>An observable sequence that completes after the action has run.</returns>
    public static IObservableAsync<RxVoid> Start(Action action, TaskScheduler? taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        return taskScheduler is null
            ? FromAsync(_ =>
            {
                action();
                return default;
            })
            : CreateAsBackgroundJob<RxVoid>(
                async (observer, cancellationToken) =>
                {
                    action();
                    await observer.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
                    await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                },
                taskScheduler);
    }

    /// <summary>Creates an observable sequence that executes the supplied function and emits its result.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function to execute.</param>
    /// <returns>An observable sequence that emits the function result and then completes.</returns>
    public static IObservableAsync<TResult> Start<TResult>(Func<TResult> function) => Start(function, null);

    /// <summary>Creates an observable sequence that executes the supplied function and emits its result.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function to execute.</param>
    /// <param name="taskScheduler">An optional scheduler used to start the function.</param>
    /// <returns>An observable sequence that emits the function result and then completes.</returns>
    public static IObservableAsync<TResult> Start<TResult>(Func<TResult> function, TaskScheduler? taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(function);

        return taskScheduler is null
            ? FromAsync(_ => new ValueTask<TResult>(function()))
            : CreateAsBackgroundJob<TResult>(
                async (observer, cancellationToken) =>
                {
                    await observer.OnNextAsync(function(), cancellationToken).ConfigureAwait(false);
                    await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                },
                taskScheduler);
    }
}
