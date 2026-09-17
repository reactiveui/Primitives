// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits each source element the predicate accepts, projected by the selector.</summary>
/// <typeparam name="TIn">The source element type.</typeparam>
/// <typeparam name="TOut">The projected element type after applying the selector.</typeparam>
/// <param name="source">The source observable to filter and project.</param>
/// <param name="predicate">Predicate applied to each source element; only elements returning <see langword="true"/> are forwarded through <paramref name="selector"/>.</param>
/// <param name="selector">Projection applied to elements that pass <paramref name="predicate"/>.</param>
/// <remarks>An exception from either delegate terminates the sequence.</remarks>
public sealed class WhereSelectObservable<TIn, TOut>(
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
        return source.Subscribe(new WhereSelectWitness(observer, predicate, selector));
    }

    /// <summary>Forwarding observer that applies the predicate then the selector, routing an exception from either to the downstream error channel.</summary>
    /// <param name="downstream">The downstream observer receiving projected values.</param>
    /// <param name="predicate">Filter delegate.</param>
    /// <param name="selector">Projection delegate.</param>
    private sealed class WhereSelectWitness(
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
