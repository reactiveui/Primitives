// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Factory operator that emits each element of an <see cref="IEnumerable{T}"/>
/// to a single subscriber, then completes. The emission can optionally be
/// marshalled through a scheduler. Replaces the
/// <c>Observable.Create&lt;T&gt;(o =&gt; scheduler.ScheduleSafe(() =&gt; o.FastForEach(source)))</c>
/// pattern with a dedicated <see cref="IObservable{T}"/> implementation.
/// </summary>
/// <typeparam name="T">The element type emitted to the downstream observer.</typeparam>
/// <param name="source">The source enumerable whose elements are pumped on subscription.</param>
/// <param name="scheduler">An optional scheduler used to dispatch the pump.</param>
internal sealed class FromArrayObservable<T>(
    IEnumerable<T> source,
    ISequencer? scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (scheduler is null)
        {
            Pump(observer, source);
            return EmptyDisposable.Instance;
        }

        var capturedObserver = observer;
        var capturedSource = source;
        return scheduler.Schedule(
            (capturedObserver, capturedSource),
            static (_, state) =>
            {
                Pump(state.capturedObserver, state.capturedSource);
                return EmptyDisposable.Instance;
            });
    }

    /// <summary>Pumps each element into the observer and completes, forwarding enumeration errors.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="elements">The elements to pump.</param>
    private static void Pump(IObserver<T> observer, IEnumerable<T> elements)
    {
        try
        {
            observer.FastForEach(elements);
        }
        catch (Exception error)
        {
            observer.OnError(error);
            return;
        }

        observer.OnCompleted();
    }
}
