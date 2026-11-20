// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core.Internal;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Signal that projects each source value through a selector before forwarding it.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="selector">The projection applied to each source value.</param>
[System.Diagnostics.DebuggerDisplay("MapSignal: Source = {_source}, Selector = {_selector}")]
public sealed class MapSignal<TSource, TResult>(IObservable<TSource> source, Func<TSource, TResult> selector) : IRequireCurrentThread<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The projection applied to each source value.</summary>
    private readonly Func<TSource, TResult> _selector = selector;

    /// <summary>Preserves the source's current-thread subscription requirement.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <summary>Subscribes an observer to the projected source values.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (DirectSequenceEmission.TrySubscribeProjected(
            _source,
            _selector,
            static (item, sel) => System.Runtime.CompilerServices.Unsafe.As<Func<int, TResult>>(sel)(item),
            static (item, sel) => sel(item),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        CoreProjectedObserverWitness<TSource, Func<TSource, TResult>, TResult> sink = new(
            observer,
            _selector,
            static (item, sel) => sel(item));
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
