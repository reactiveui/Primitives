// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Single-stage fusable projection signal returned by <c>Map</c> and <c>Select</c>.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FusedMapSignal: Source = {_source}")]
public sealed class FusedMapSignal<TSource, TResult> : IRequireCurrentThread<TResult>, IInlineSignal<TResult>, IMapFusable<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The projection function.</summary>
    private readonly Func<TSource, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="FusedMapSignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The projection function.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public FusedMapSignal(IObservable<TSource> source, Func<TSource, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        _source = source;
        _selector = selector;
    }

    /// <summary>Applies <c>Map</c>/<c>Select</c> reductions and fusions.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="selector">The projection function.</param>
    /// <returns>The reduced or fused observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(IObservable<TSource> source, Func<TSource, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return source switch
        {
            ImmutableEmptySignal<TSource> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<TSource> => ImmutableNeverSignal<TResult>.Instance,
            IMapFusable<TSource> fusable => fusable.FuseMap(selector),
            FusedKeepSignal<TSource> keep => new KeepMapSignal<TSource, TResult>(keep.Source, keep.Predicate, selector),
            _ => new FusedMapSignal<TSource, TResult>(source, selector),
        };
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IObservable<TNext> IMapFusable<TResult>.FuseMap<TNext>(Func<TResult, TNext> nextSelector) =>
        new MapMapSignal<TSource, TResult, TNext>(_source, _selector, nextSelector);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IObservable<TResult> IMapFusable<TResult>.FuseKeep(Func<TResult, bool> predicate) =>
        new MapKeepSignal<TSource, TResult>(_source, _selector, predicate);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IObservable<TNonNull> IMapFusable<TResult>.FuseKeepNotNull<TNonNull>()
        where TNonNull : class =>
        new MapKeepNotNullSignal<TSource, TNonNull>(_source, Unsafe.As<Func<TSource, TNonNull?>>(_selector));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IObservable<TTarget> IMapFusable<TResult>.FuseKeepType<TTarget>() =>
        new MapKeepTypeSignal<TSource, TTarget>(_source, Unsafe.As<Func<TSource, object?>>(_selector));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IObservable<TTarget> IMapFusable<TResult>.FuseCast<TTarget>() =>
        new MapCastSignal<TSource, TTarget>(_source, Unsafe.As<Func<TSource, object?>>(_selector));

    /// <inheritdoc/>
    IObservable<TInner> IMapFusable<TResult>.FuseSwitch<TInner>()
    {
        var typedSelector = Unsafe.As<Func<TSource, IObservable<TInner>>>(_selector);
        return _source is ISwitchSelectFusable<TSource> notNullSource
            ? notNullSource.FuseSwitchSelect(typedSelector)
            : new SwitchMapSignal<TSource, TInner>(_source, typedSelector);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (DirectSequenceEmission.TrySubscribeProjected(
            _source,
            _selector,
            static (item, sel) => Unsafe.As<Func<int, TResult>>(sel)(item),
            static (item, sel) => sel(item),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        FusedMapWitness<TSource, TResult> sink = new(observer, _selector);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        var callbacks = DirectSequenceEmission.ValidateCallbacks(onNext, onError, onCompleted);
        return DirectSequenceEmission.TrySubscribeProjectedCallbacks(
            _source,
            _selector,
            static (item, sel) => Unsafe.As<Func<int, TResult>>(sel)(item),
            static (item, sel) => sel(item),
            callbacks)
            ? EmptyDisposable.Instance
            : Subscribe(new EmptyWitness<TResult>(onNext, onError, onCompleted));
    }
}
