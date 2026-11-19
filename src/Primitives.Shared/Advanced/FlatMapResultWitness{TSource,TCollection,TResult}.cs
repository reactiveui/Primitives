// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Maps inner source values.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TCollection">The inner value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <param name="sourceValue">Captured outer value.</param>
/// <param name="selector">Projects outer and inner values to result values.</param>
/// <param name="observer">The downstream observer.</param>
[System.Diagnostics.DebuggerDisplay("FlatMapResultWitness: SourceValue = {_sourceValue}")]
public sealed class FlatMapResultWitness<TSource, TCollection, TResult>(
    TSource sourceValue,
    Func<TSource, TCollection, TResult> selector,
    IObserver<TResult> observer) : IObserver<TCollection>
{
    /// <summary>Captured outer value.</summary>
    private readonly TSource _sourceValue = sourceValue;

    /// <summary>Projects outer and inner values to result values.</summary>
    private readonly Func<TSource, TCollection, TResult> _selector = selector;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _observer.OnCompleted();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _observer.OnError(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(TCollection value) => _observer.OnNext(_selector(_sourceValue, value));
}
