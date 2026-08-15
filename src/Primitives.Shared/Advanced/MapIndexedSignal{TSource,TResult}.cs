// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Indexed map signal.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="selector">The indexed selector.</param>
[System.Diagnostics.DebuggerDisplay("Source = {_source}")]
public sealed class MapIndexedSignal<TSource, TResult>(IObservable<TSource> source, Func<TSource, int, TResult> selector) : IRequireCurrentThread<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The indexed selector.</summary>
    private readonly Func<TSource, int, TResult> _selector = selector;

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
