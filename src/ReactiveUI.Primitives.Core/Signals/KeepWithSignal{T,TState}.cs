// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core.Internal;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Filters source values using a caller-supplied state value, without allocating a per-value closure: the state is stored on the sink and passed to the predicate for each element.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TState">The state type passed to the predicate.</typeparam>
[System.Diagnostics.DebuggerDisplay("KeepWithSignal: Source = {_source}, State = {_state}")]
public sealed class KeepWithSignal<T, TState> : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The state passed to the predicate.</summary>
    private readonly TState _state;

    /// <summary>The predicate applied to each source value and the state.</summary>
    private readonly Func<TState, T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="KeepWithSignal{T, TState}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the predicate.</param>
    /// <param name="predicate">The predicate applied to each source value and the state.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public KeepWithSignal(IObservable<T> source, TState state, Func<TState, T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _source = source;
        _state = state;
        _predicate = predicate;
    }

    /// <summary>Creates a stateful filter sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the predicate.</param>
    /// <param name="predicate">The predicate applied to each source value and the state.</param>
    /// <returns>The filtered observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, TState state, Func<TState, T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T>
            ? source
            : new KeepWithSignal<T, TState>(source, state, predicate);
    }

    /// <summary>Determines whether the sink must subscribe on the current thread.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <summary>Subscribes the observer to the filtered sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StateCallbackPair<TState, Func<TState, T, bool>> pair = new(_state, _predicate);
        if (DirectSequenceEmission.TrySubscribeFiltered(
            _source,
            pair,
            static (item, ctx) => System.Runtime.CompilerServices.Unsafe.As<Func<TState, int, bool>>(ctx.Callback)(ctx.State, item),
            static (item, ctx) => ctx.Callback(ctx.State, item),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        CoreFilteredObserverWitness<T, StateCallbackPair<TState, Func<TState, T, bool>>> sink = new(
            observer,
            pair,
            static (item, ctx) => ctx.Callback(ctx.State, item));
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
