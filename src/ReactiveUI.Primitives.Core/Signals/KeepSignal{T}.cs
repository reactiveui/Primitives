// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core.Internal;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Signal that forwards only the source values the predicate accepts.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="predicate">The predicate applied to each source value.</param>
[System.Diagnostics.DebuggerDisplay("KeepSignal: Source = {_source}, Predicate = {_predicate}")]
public sealed class KeepSignal<T>(IObservable<T> source, Func<T, bool> predicate) : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The predicate applied to each source value.</summary>
    private readonly Func<T, bool> _predicate = predicate;

    /// <summary>Preserves the source's current-thread subscription requirement.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <summary>Subscribes an observer to source values accepted by the predicate.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (DirectSequenceEmission.TrySubscribeFiltered(
            _source,
            _predicate,
            static (item, pred) => System.Runtime.CompilerServices.Unsafe.As<Func<int, bool>>(pred)(item),
            static (item, pred) => pred(item),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        CoreFilteredObserverWitness<T, Func<T, bool>> sink = new(
            observer,
            _predicate,
            static (item, pred) => pred(item));
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
