// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Chaining FlatMap signal with an outer/inner result selector.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TCollection">The inner value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FlatMapResultSignal: Source = {_source}")]
public sealed class FlatMapResultSignal<TSource, TCollection, TResult> : IObservable<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>Projects source values to inner observables.</summary>
    private readonly Func<TSource, IObservable<TCollection>> _collectionSelector;

    /// <summary>Projects outer and inner values to result values.</summary>
    private readonly Func<TSource, TCollection, TResult> _resultSelector;

    /// <summary>Initializes a new instance of the <see cref="FlatMapResultSignal{TSource, TCollection, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="collectionSelector">Projects source values to inner observables.</param>
    /// <param name="resultSelector">Projects outer and inner values to result values.</param>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public FlatMapResultSignal(
        IObservable<TSource> source,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(collectionSelector);
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
        _collectionSelector = collectionSelector;
        _resultSelector = resultSelector;
    }

    /// <summary>Creates a chaining flat-map result sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="collectionSelector">Projects source values to inner observables.</param>
    /// <param name="resultSelector">Projects outer and inner values to result values.</param>
    /// <returns>The projected observable sequence.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(
        IObservable<TSource> source,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(collectionSelector);
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source switch
        {
            ImmutableEmptySignal<TSource> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<TSource> => ImmutableNeverSignal<TResult>.Instance,
            _ => new FlatMapResultSignal<TSource, TCollection, TResult>(source, collectionSelector, resultSelector),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var collectionSelector = _collectionSelector;
        var resultSelector = _resultSelector;

        return new FlatMapCoordinator<TSource, TResult>(
            _source,
            value => new FlatMapResultInnerSignal<TSource, TCollection, TResult>(
                value,
                collectionSelector(value),
                resultSelector),
            observer).Run();
    }
}
