// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Resource-bound factory observable that runs an optional
/// <see cref="Action{T}"/> against a captured <see cref="IDisposable"/>
/// resource, emits <see cref="RxVoid.Default"/>, completes, and finally disposes
/// the resource. Replaces the legacy
/// <c>Observable.Using(() =&gt; obj, id =&gt; Observable.Start(...))</c> pattern.
/// </summary>
/// <typeparam name="T">The disposable resource type.</typeparam>
/// <param name="resource">The resource to use during the operation and dispose at the end.</param>
/// <param name="action">An optional action invoked against the resource.</param>
/// <param name="scheduler">An optional scheduler; <c>null</c> runs the action synchronously inline.</param>
internal sealed class UsingActionObservable<T>(
    T resource,
    Action<T>? action,
    ISequencer? scheduler) : IObservable<RxVoid>
    where T : IDisposable
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (scheduler is null)
        {
            Run(observer, resource, action);
            return EmptyDisposable.Instance;
        }

        var capturedObserver = observer;
        var capturedResource = resource;
        var capturedAction = action;
        return scheduler.Schedule(
            (capturedObserver, capturedResource, capturedAction),
            static (_, state) =>
            {
                Run(state.capturedObserver, state.capturedResource, state.capturedAction);
                return EmptyDisposable.Instance;
            });
    }

    /// <summary>Invokes the optional action against the resource, signals <see cref="RxVoid.Default"/> and completion, then disposes the resource.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="resource">The resource to use and dispose.</param>
    /// <param name="action">The optional action.</param>
    private static void Run(IObserver<RxVoid> observer, T resource, Action<T>? action)
    {
        try
        {
            action?.Invoke(resource);
        }
        catch (Exception error)
        {
            try
            {
                resource?.Dispose();
            }
            catch
            {
                // Swallow secondary dispose failure; the primary exception is what callers care about.
            }

            observer.OnError(error);
            return;
        }

        observer.OnNext(RxVoid.Default);
        observer.OnCompleted();
        resource?.Dispose();
    }
}
