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

/// <summary>Single-stage fusable filter signal returned by <c>Keep</c> and <c>Where</c>.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FusedKeepSignal: Source = {_source}")]
public sealed class FusedKeepSignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The filter predicate.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="FusedKeepSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The filter predicate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public FusedKeepSignal(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _source = source;
        _predicate = predicate;
    }

    /// <summary>Gets the source sequence.</summary>
    internal IObservable<T> Source => _source;

    /// <summary>Gets the filter predicate.</summary>
    internal Func<T, bool> Predicate => _predicate;

    /// <summary>Applies <c>Keep</c>/<c>Where</c> reductions and fusions.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">The filter predicate.</param>
    /// <returns>The reduced or fused observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        return source switch
        {
            ImmutableEmptySignal<T> or ImmutableNeverSignal<T> => source,
            FusedKeepSignal<T> keep => new KeepKeepSignal<T>(keep.Source, keep.Predicate, predicate),
            IMapFusable<T> map => map.FuseKeep(predicate),
            _ => new FusedKeepSignal<T>(source, predicate),
        };
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (DirectSequenceEmission.TrySubscribeFiltered(
            _source,
            _predicate,
            static (item, pred) => Unsafe.As<Func<int, bool>>(pred)(item),
            static (item, pred) => pred(item),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        FusedKeepWitness<T> sink = new(observer, _predicate);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        var callbacks = DirectSequenceEmission.ValidateCallbacks(onNext, onError, onCompleted);
        return DirectSequenceEmission.TrySubscribeRangeFilteredCallbacks(
            _source,
            _predicate,
            static (item, pred) => Unsafe.As<Func<int, bool>>(pred)(item),
            callbacks)
            ? EmptyDisposable.Instance
            : Subscribe(new EmptyWitness<T>(onNext, onError, onCompleted));
    }
}
