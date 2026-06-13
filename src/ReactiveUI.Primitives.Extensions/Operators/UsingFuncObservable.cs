// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Resource-bound factory observable that runs a <see cref="Func{T, TResult}"/>
/// against a captured <see cref="IDisposable"/> resource, emits the result,
/// completes, and finally disposes the resource. Replaces the legacy
/// <c>Observable.Using(() =&gt; obj, id =&gt; Observable.Start(() =&gt; func(id)))</c> pattern.
/// </summary>
/// <typeparam name="T">The disposable resource type.</typeparam>
/// <typeparam name="TResult">The result type emitted to the downstream observer.</typeparam>
/// <param name="resource">The resource to use during the operation and dispose at the end.</param>
/// <param name="function">The function invoked against the resource to produce the emitted value.</param>
/// <param name="scheduler">An optional scheduler; <c>null</c> runs the function synchronously inline.</param>
internal sealed class UsingFuncObservable<T, TResult>(
    T resource,
    Func<T, TResult> function,
    ISequencer? scheduler) : IObservable<TResult>
    where T : IDisposable
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(function);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (scheduler is null)
        {
            Run(observer, resource, function);
            return EmptyDisposable.Instance;
        }

        var capturedObserver = observer;
        var capturedResource = resource;
        var capturedFunction = function;
        return scheduler.Schedule(
            (capturedObserver, capturedResource, capturedFunction),
            static (_, state) =>
            {
                Run(state.capturedObserver, state.capturedResource, state.capturedFunction);
                return EmptyDisposable.Instance;
            });
    }

    /// <summary>
    /// Invokes the function against the resource, signals the produced value
    /// followed by completion, then disposes the resource.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="resource">The resource to use and dispose.</param>
    /// <param name="function">The function invoked against the resource.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST1429:Handle, rethrow, or narrow this catch; an empty catch of the base exception hides failures",
        Justification =
            "A secondary dispose failure during error handling is intentionally swallowed so the primary exception sent to the observer wins; this assembly has no unhandled-exception sink.")]
    private static void Run(IObserver<TResult> observer, T resource, Func<T, TResult> function)
    {
        TResult result;
        try
        {
            result = function(resource);
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

        observer.OnNext(result);
        observer.OnCompleted();
        resource?.Dispose();
    }
}
