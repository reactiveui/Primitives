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

/// <summary>Fused two-stage <c>Keep(first).Keep(second)</c> signal.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="first">The first predicate.</param>
/// <param name="second">The second predicate.</param>
[System.Diagnostics.DebuggerDisplay("KeepKeepSignal: Source = {_source}")]
public sealed class KeepKeepSignal<T>(IObservable<T> source, Func<T, bool> first, Func<T, bool> second) : IRequireCurrentThread<T>, IInlineSignal<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The first predicate.</summary>
    private readonly Func<T, bool> _first = first;

    /// <summary>The second predicate.</summary>
    private readonly Func<T, bool> _second = second;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StagePair<Func<T, bool>, Func<T, bool>> predicates = new(_first, _second);
        if (DirectSequenceEmission.TrySubscribeFiltered(
            _source,
            predicates,
            static (item, p) => Unsafe.As<Func<int, bool>>(p.First)(item) && Unsafe.As<Func<int, bool>>(p.Second)(item),
            static (item, p) => p.First(item) && p.Second(item),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        KeepKeepWitness<T> sink = new(observer, _first, _second);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        _ = DirectSequenceEmission.ValidateCallbacks(onNext, onError, onCompleted);
        return Subscribe(new EmptyWitness<T>(onNext, onError, onCompleted));
    }
}
