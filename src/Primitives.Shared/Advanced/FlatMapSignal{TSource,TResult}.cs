// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>FlatMap signal that subscribes the projected inner observables in source order.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FlatMapSignal: Source = {_source}")]
public sealed class FlatMapSignal<TSource, TResult> : IObservable<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>Projects source values to inner observables.</summary>
    private readonly Func<TSource, IObservable<TResult>> _selector;

    /// <summary>Initializes a new instance of the <see cref="FlatMapSignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">Projects source values to inner observables.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public FlatMapSignal(IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        _source = source;
        _selector = selector;
    }

    /// <summary>Creates a chaining flat-map sequence for <c>FlatMap</c> with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">Projects source values to inner observables.</param>
    /// <returns>The concatenated observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return source switch
        {
            ImmutableEmptySignal<TSource> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<TSource> => ImmutableNeverSignal<TResult>.Instance,
            _ => new FlatMapSignal<TSource, TResult>(source, selector),
        };
    }

    /// <summary>Creates a chaining flat-map sequence for <c>Bind</c> with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">Projects source values to inner observables.</param>
    /// <returns>The concatenated observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="selector"/> or <paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> CreateBind(IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(selector);
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source switch
        {
            ImmutableEmptySignal<TSource> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<TSource> => ImmutableNeverSignal<TResult>.Instance,
            _ => new FlatMapSignal<TSource, TResult>(source, selector),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new FlatMapCoordinator<TSource, TResult>(_source, _selector, observer).Run();
    }
}
