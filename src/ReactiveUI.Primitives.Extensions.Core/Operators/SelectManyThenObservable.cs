// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Projects each source value through two successive observable selectors and forwards the second-stage values.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TMid">The intermediate element type produced by the first projection.</typeparam>
/// <typeparam name="TResult">The final element type produced by the second projection.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="first">First projection: source element to intermediate observable.</param>
/// <param name="second">Second projection: intermediate element to result observable.</param>
/// <remarks>A selector failure terminates the sequence. Completion arrives once, after the source and every sequence
/// either projection opened have all finished.</remarks>
public sealed class SelectManyThenObservable<TSource, TMid, TResult>(
    IObservable<TSource> source,
    Func<TSource, IObservable<TMid>> first,
    Func<TMid, IObservable<TResult>> second) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(first);
        InvalidOperationExceptionHelper.ThrowIfNull(second);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new SelectManyThenCoordinator<TSource, TMid, TResult>(observer, first, second).Run(source);
    }
}
