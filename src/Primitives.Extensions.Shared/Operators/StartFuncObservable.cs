// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Runs the function once — inline when no scheduler is supplied, otherwise on the scheduler —
/// emits the result, then completes.
/// </summary>
/// <typeparam name="TResult">Result type.</typeparam>
/// <param name="function">Function to run.</param>
/// <param name="scheduler">Optional scheduler; null runs inline.</param>
internal sealed class StartFuncObservable<TResult>(
    Func<TResult> function,
    ISequencer? scheduler) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(function);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (scheduler is null)
        {
            Run(observer, function);
            return EmptyDisposable.Instance;
        }

        var capturedObserver = observer;
        var capturedFunction = function;
        return scheduler.Schedule((capturedObserver, capturedFunction), static (_, state) =>
        {
            Run(state.capturedObserver, state.capturedFunction);
            return EmptyDisposable.Instance;
        });
    }

    /// <summary>Runs the function, then signals the observer with the result and completion.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="function">The function to invoke.</param>
    private static void Run(IObserver<TResult> observer, Func<TResult> function)
    {
        TResult result;
        try
        {
            result = function();
        }
        catch (Exception error)
        {
            observer.OnError(error);
            return;
        }

        observer.OnNext(result);
        observer.OnCompleted();
    }
}
