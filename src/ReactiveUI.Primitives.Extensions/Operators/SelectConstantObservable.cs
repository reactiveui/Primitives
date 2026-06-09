// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Projection operator that ignores every source element and emits a stored constant
/// instead. Replaces the common <c>.Select(_ =&gt; value)</c> pattern, avoiding the
/// per-subscription closure allocation that the lambda <c>_ =&gt; value</c> would
/// capture.
/// </summary>
/// <typeparam name="TSource">The source element type (ignored).</typeparam>
/// <typeparam name="TResult">The result element type emitted to the downstream observer.</typeparam>
/// <param name="source">The source observable whose values are ignored.</param>
/// <param name="constant">The constant value emitted for each source element.</param>
internal sealed class SelectConstantObservable<TSource, TResult>(
    IObservable<TSource> source,
    TResult constant) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new SelectConstantWitness(observer, constant));
    }

    /// <summary>
    /// Forwarding observer that replaces every <see cref="OnNext"/> value with
    /// the stored constant. Error and completion signals pass through unchanged.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="constant">The constant value to emit.</param>
    private sealed class SelectConstantWitness(IObserver<TResult> downstream, TResult constant) : IObserver<TSource>
    {
        /// <inheritdoc/>
        public void OnNext(TSource value) => downstream.OnNext(constant);

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
