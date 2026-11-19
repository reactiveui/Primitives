// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Projects each source value together with its zero-based position in the sequence.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("MapIndexedSignal: Source = {_source}")]
public sealed class MapIndexedSignal<TSource, TResult> : IRequireCurrentThread<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The indexed selector.</summary>
    private readonly Func<TSource, int, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="MapIndexedSignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The indexed selector.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public MapIndexedSignal(IObservable<TSource> source, Func<TSource, int, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        _source = source;
        _selector = selector;
    }

    /// <summary>Creates an indexed projection sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The indexed selector.</param>
    /// <returns>The projected observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(IObservable<TSource> source, Func<TSource, int, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return source switch
        {
            ImmutableEmptySignal<TSource> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<TSource> => ImmutableNeverSignal<TResult>.Instance,
            _ => new MapIndexedSignal<TSource, TResult>(source, selector),
        };
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        CurrentThreadRequirement.IsRequired(_source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new MapIndexedWitness<TSource, TResult>(observer, _selector));
    }
}
