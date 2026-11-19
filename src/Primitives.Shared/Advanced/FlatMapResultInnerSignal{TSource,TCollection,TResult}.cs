// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Maps inner values with a captured outer value.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TCollection">The inner value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FlatMapResultInnerSignal: SourceValue = {_sourceValue}")]
public sealed class FlatMapResultInnerSignal<TSource, TCollection, TResult> : IObservable<TResult>
{
    /// <summary>Captured outer value.</summary>
    private readonly TSource _sourceValue;

    /// <summary>Inner observable.</summary>
    private readonly IObservable<TCollection> _source;

    /// <summary>Projects outer and inner values to result values.</summary>
    private readonly Func<TSource, TCollection, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="FlatMapResultInnerSignal{TSource, TCollection, TResult}"/> class.</summary>
    /// <param name="sourceValue">Captured outer value.</param>
    /// <param name="source">Inner observable.</param>
    /// <param name="selector">Projects outer and inner values to result values.</param>
    /// <exception cref="InvalidOperationException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public FlatMapResultInnerSignal(
        TSource sourceValue,
        IObservable<TCollection> source,
        Func<TSource, TCollection, TResult> selector)
    {
        _sourceValue = sourceValue;
        _source = source ?? throw new InvalidOperationException("The FlatMap collection selector returned null.");
        _selector = selector;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new FlatMapResultWitness<TSource, TCollection, TResult>(_sourceValue, _selector, observer));
    }
}
