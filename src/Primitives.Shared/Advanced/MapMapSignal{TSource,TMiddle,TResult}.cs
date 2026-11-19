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

/// <summary>Fused two-stage <c>Map(first).Map(second)</c> signal.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TMiddle">The intermediate element type.</typeparam>
/// <typeparam name="TResult">The final projected element type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="first">The first projection.</param>
/// <param name="second">The second projection.</param>
[System.Diagnostics.DebuggerDisplay("MapMapSignal: Source = {_source}")]
public sealed class MapMapSignal<TSource, TMiddle, TResult>(
    IObservable<TSource> source,
    Func<TSource, TMiddle> first,
    Func<TMiddle, TResult> second) : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The first projection.</summary>
    private readonly Func<TSource, TMiddle> _first = first;

    /// <summary>The second projection.</summary>
    private readonly Func<TMiddle, TResult> _second = second;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StagePair<Func<TSource, TMiddle>, Func<TMiddle, TResult>> stages = new(_first, _second);
        if (DirectSequenceEmission.TrySubscribeProjected(
            _source,
            stages,
            static (item, s) => s.Second(Unsafe.As<Func<int, TMiddle>>(s.First)(item)),
            static (item, s) => s.Second(s.First(item)),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        MapMapWitness<TSource, TMiddle, TResult> sink = new(observer, _first, _second);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        var callbacks = DirectSequenceEmission.ValidateCallbacks(onNext, onError, onCompleted);
        StagePair<Func<TSource, TMiddle>, Func<TMiddle, TResult>> stages = new(_first, _second);
        return DirectSequenceEmission.TrySubscribeProjectedCallbacks(
            _source,
            stages,
            static (item, s) => s.Second(Unsafe.As<Func<int, TMiddle>>(s.First)(item)),
            null,
            callbacks)
            ? EmptyDisposable.Instance
            : Subscribe(new EmptyWitness<TResult>(onNext, onError, onCompleted));
    }
}
