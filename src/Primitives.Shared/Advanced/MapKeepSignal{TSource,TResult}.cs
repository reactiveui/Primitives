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

/// <summary>Fused <c>Map(selector).Keep(predicate)</c> signal.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The projected and filtered element type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="selector">The projection function.</param>
/// <param name="predicate">The filter predicate applied to projected values.</param>
[System.Diagnostics.DebuggerDisplay("MapKeepSignal: Source = {_source}")]
public sealed class MapKeepSignal<TSource, TResult>(
    IObservable<TSource> source,
    Func<TSource, TResult> selector,
    Func<TResult, bool> predicate) : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The projection function.</summary>
    private readonly Func<TSource, TResult> _selector = selector;

    /// <summary>The filter predicate applied to projected values.</summary>
    private readonly Func<TResult, bool> _predicate = predicate;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StagePair<Func<TSource, TResult>, Func<TResult, bool>> stages = new(_selector, _predicate);
        if (DirectSequenceEmission.TrySubscribeFilteredProjection(
            _source,
            stages,
            static (item, s) => Evaluate(Unsafe.As<Func<int, TResult>>(s.First)(item), s.Second),
            static (item, s) => Evaluate(s.First(item), s.Second),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        MapKeepWitness<TSource, TResult> sink = new(observer, _selector, _predicate);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        var callbacks = DirectSequenceEmission.ValidateCallbacks(onNext, onError, onCompleted);
        StagePair<Func<TSource, TResult>, Func<TResult, bool>> stages = new(_selector, _predicate);
        return DirectSequenceEmission.TrySubscribeRangeFilteredProjectionCallbacks(
            _source,
            stages,
            static (item, s) => Evaluate(Unsafe.As<Func<int, TResult>>(s.First)(item), s.Second),
            callbacks)
            ? EmptyDisposable.Instance
            : Subscribe(new EmptyWitness<TResult>(onNext, onError, onCompleted));
    }

    /// <summary>Pairs <paramref name="projected"/> with whether <paramref name="predicate"/> accepted it.</summary>
    /// <param name="projected">The projected value.</param>
    /// <param name="predicate">The acceptance predicate.</param>
    /// <returns>The projected outcome.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ProjectedOutcome<TResult> Evaluate(TResult projected, Func<TResult, bool> predicate) =>
        new(predicate(projected), projected);
}
