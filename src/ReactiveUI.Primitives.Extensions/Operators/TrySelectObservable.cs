// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Fused <c>.Select(selector).Where(x =&gt; x is not null).Select(x =&gt; x!)</c> operator
/// that applies a transform and emits only non-null results. Replaces the 3-operator
/// null-filtering chain with a single operator + observer allocation.
/// </summary>
/// <typeparam name="TIn">The source element type.</typeparam>
/// <typeparam name="TOut">The projected element type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="selector">Projection that may return <see langword="null"/> for elements that should be skipped.</param>
internal sealed class TrySelectObservable<TIn, TOut>(
    IObservable<TIn> source,
    Func<TIn, TOut?> selector) : IObservable<TOut>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TOut> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new TrySelectWitness(observer, selector));
    }

    /// <summary>Observer that applies the selector and only forwards non-null results. Exceptions from the selector are routed to <see cref="IObserver{T}.OnError"/>.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="selector">The projection delegate.</param>
    private sealed class TrySelectWitness(
        IObserver<TOut> downstream,
        Func<TIn, TOut?> selector) : IObserver<TIn>
    {
        /// <inheritdoc/>
        public void OnNext(TIn value)
        {
            TOut? projected;
            try
            {
                projected = selector(value);
            }
            catch (Exception ex)
            {
                downstream.OnError(ex);
                return;
            }

            if (projected is null)
            {
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
