// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that emits the running accumulation of the source values.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FoldSignal: Source = {_source}")]
public sealed class FoldSignal<TSource, TAccumulate> : IObservable<TAccumulate>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The initial accumulated value.</summary>
    private readonly TAccumulate _seed;

    /// <summary>The accumulator function.</summary>
    private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator;

    /// <summary>Initializes a new instance of the <see cref="FoldSignal{TSource, TAccumulate}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="seed">The initial accumulated value.</param>
    /// <param name="accumulator">The accumulator function.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="accumulator"/> is <see langword="null"/>.</exception>
    public FoldSignal(
        IObservable<TSource> source,
        TAccumulate seed,
        Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(accumulator);

        _source = source;
        _seed = seed;
        _accumulator = accumulator;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TAccumulate> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        FoldWitness<TSource, TAccumulate> sink = new(observer, _seed, _accumulator);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
