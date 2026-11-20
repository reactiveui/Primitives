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

/// <summary>Fused <c>Keep(predicate).Map(selector)</c> signal.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="predicate">The filter predicate.</param>
/// <param name="selector">The projection function.</param>
[System.Diagnostics.DebuggerDisplay("KeepMapSignal: Source = {_source}")]
public sealed class KeepMapSignal<TSource, TResult>(
    IObservable<TSource> source,
    Func<TSource, bool> predicate,
    Func<TSource, TResult> selector) : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The filter predicate.</summary>
    private readonly Func<TSource, bool> _predicate = predicate;

    /// <summary>The projection function.</summary>
    private readonly Func<TSource, TResult> _selector = selector;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StagePair<Func<TSource, bool>, Func<TSource, TResult>> stages = new(_predicate, _selector);
        if (DirectSequenceEmission.TrySubscribeFilteredProjection(
            _source,
            stages,
            static (item, s) => Unsafe.As<Func<int, bool>>(s.First)(item)
                ? new(true, Unsafe.As<Func<int, TResult>>(s.Second)(item))
                : default,
            static (item, s) => s.First(item) ? new(true, s.Second(item)) : default,
            observer))
        {
            return EmptyDisposable.Instance;
        }

        KeepMapWitness<TSource, TResult> sink = new(observer, _predicate, _selector);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        _ = DirectSequenceEmission.ValidateCallbacks(onNext, onError, onCompleted);
        return Subscribe(new EmptyWitness<TResult>(onNext, onError, onCompleted));
    }
}
