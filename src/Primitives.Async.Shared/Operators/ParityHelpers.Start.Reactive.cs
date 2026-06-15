// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides shim-typed factory methods that execute actions and emit <see cref="RxVoid"/>.</summary>
public static partial class SignalAsyncReactiveExtensions
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
            : SignalAsync.CreateAsBackgroundJob<RxVoid>(
                async (observer, cancellationToken) =>
                {
                    action();
                    await observer.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
                    await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                },
                taskScheduler);
    }
}
