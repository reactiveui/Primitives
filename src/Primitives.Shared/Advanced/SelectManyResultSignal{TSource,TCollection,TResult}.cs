// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Concrete signal for concurrent observable <c>SelectMany</c> with a result selector.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TCollection">The inner value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SelectManyResultSignal: Source = {Source}, CollectionSelector = {CollectionSelector}")]
public sealed class SelectManyResultSignal<TSource, TCollection, TResult> : IObservable<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="SelectManyResultSignal{TSource, TCollection, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="collectionSelector">The selector that creates an inner observable for each source value.</param>
    /// <param name="resultSelector">The selector that combines outer and inner values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="collectionSelector"/> or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
    public SelectManyResultSignal(
        IObservable<TSource> source,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        CollectionSelector = collectionSelector ?? throw new ArgumentNullException(nameof(collectionSelector));
        ResultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<TSource> Source { get; }

    /// <summary>Gets the selector that creates inner observables.</summary>
    private Func<TSource, IObservable<TCollection>> CollectionSelector { get; }

    /// <summary>Gets the selector that combines outer and inner values.</summary>
    private Func<TSource, TCollection, TResult> ResultSelector { get; }

    /// <summary>Creates a concurrent <c>SelectMany</c> result sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="collectionSelector">The selector that creates inner observables.</param>
    /// <param name="resultSelector">The selector that combines outer and inner values.</param>
    /// <returns>The merged observable sequence.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(
        IObservable<TSource> source,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(collectionSelector);
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);

        return source switch
        {
            ImmutableEmptySignal<TSource> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<TSource> => ImmutableNeverSignal<TResult>.Instance,
            _ => new SelectManyResultSignal<TSource, TCollection, TResult>(source, collectionSelector, resultSelector),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new SelectManyResultCoordinator<TSource, TCollection, TResult>(
            observer,
            CollectionSelector,
            ResultSelector).Run(Source);
    }
}
