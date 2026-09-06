// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that projects each value to an enumerable sequence and emits the enumerable values.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="selector">The enumerable projection.</param>
[System.Diagnostics.DebuggerDisplay("SelectManyEnumerableSignal: Source = {_source}")]
public sealed class SelectManyEnumerableSignal<TSource, TResult>(IObservable<TSource> source, Func<TSource, IEnumerable<TResult>> selector) : IObservable<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The enumerable projection.</summary>
    private readonly Func<TSource, IEnumerable<TResult>> _selector = selector;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SelectManyEnumerableWitness<TSource, TResult> sink = new(observer, _selector);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
