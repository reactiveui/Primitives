// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Operator that runs an action and completes immediately.</summary>
/// <param name="action">The action to run.</param>
/// <param name="scheduler">An optional scheduler; <c>null</c> runs the action synchronously inline.</param>
internal sealed class StartActionObservable(Action action, ISequencer? scheduler) : IObservable<RxVoid>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(action);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (scheduler is null)
        {
            Run(observer, action);
            return EmptyDisposable.Instance;
        }

        var capturedObserver = observer;
        var capturedAction = action;
        return scheduler.Schedule((capturedObserver, capturedAction), static (_, state) =>
        {
            Run(state.capturedObserver, state.capturedAction);
            return EmptyDisposable.Instance;
        });
    }

    /// <summary>Runs the action, then signals the observer with <see cref="RxVoid.Default"/> and completion.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="action">The action to invoke.</param>
    private static void Run(IObserver<RxVoid> observer, Action action)
    {
        try
        {
            action();
        }
        catch (Exception error)
        {
            observer.OnError(error);
            return;
        }

        observer.OnNext(RxVoid.Default);
        observer.OnCompleted();
    }
}
