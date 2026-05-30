// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Fused <c>Where(predicate).Select(selector)</c> operator. Replaces the two-operator
/// Rx chain with a single observable + observer pair, saving the intermediate
/// <c>Select</c> operator allocation (and its <see cref="IObserver{T}"/>) per
/// subscription on hot paths.
/// </summary>
/// <typeparam name="TIn">The source element type.</typeparam>
/// <typeparam name="TOut">The projected element type after applying the selector.</typeparam>
/// <param name="source">The source observable to filter and project.</param>
/// <param name="predicate">Predicate applied to each source element; only elements returning <see langword="true"/> are forwarded through <paramref name="selector"/>.</param>
/// <param name="selector">Projection applied to elements that pass <paramref name="predicate"/>.</param>
internal sealed class WhereSelectObservable<TIn, TOut>(
    IObservable<TIn> source,
    Func<TIn, bool> predicate,
    Func<TIn, TOut> selector) : IObservable<TOut>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TOut> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(predicate);
        InvalidOperationExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new WhereSelectObserver(observer, predicate, selector));
    }

    /// <summary>
    /// Forwarding observer that applies the predicate and selector inline on each
    /// <see cref="OnNext"/>. Any exception thrown by either delegate is routed to
    /// <see cref="IObserver{T}.OnError"/> on the downstream observer.
    /// </summary>
    /// <param name="downstream">The downstream observer receiving projected values.</param>
    /// <param name="predicate">Filter delegate.</param>
    /// <param name="selector">Projection delegate.</param>
    private sealed class WhereSelectObserver(
        IObserver<TOut> downstream,
        Func<TIn, bool> predicate,
        Func<TIn, TOut> selector) : IObserver<TIn>
    {
        /// <inheritdoc/>
        public void OnNext(TIn value)
        {
            TOut projected;
            try
            {
                if (!predicate(value))
                {
                    return;
                }

                projected = selector(value);
            }
            catch (Exception ex)
            {
                downstream.OnError(ex);
                return;
            }

            downstream.OnNext(projected);
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
