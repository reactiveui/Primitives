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
public sealed class SelectManyEnumerableSignal<TSource, TResult> : IObservable<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The enumerable projection.</summary>
    private readonly Func<TSource, IEnumerable<TResult>> _selector;

    /// <summary>Initializes a new instance of the <see cref="SelectManyEnumerableSignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The enumerable projection.</param>
    public SelectManyEnumerableSignal(IObservable<TSource> source, Func<TSource, IEnumerable<TResult>> selector)
    {
        _source = source;
        _selector = selector;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SelectManyEnumerableObserver<TSource, TResult> sink = new(observer, _selector);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
