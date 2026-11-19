// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core.Internal;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Projects each source value using the supplied state and selector.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TState">The state type passed to the selector.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("MapWithSignal: Source = {_source}, State = {_state}")]
public sealed class MapWithSignal<TSource, TState, TResult> : IRequireCurrentThread<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The state passed to the selector.</summary>
    private readonly TState _state;

    /// <summary>The transform applied to each source value and the state.</summary>
    private readonly Func<TState, TSource, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="MapWithSignal{TSource, TState, TResult}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the selector.</param>
    /// <param name="selector">The transform applied to each source value and the state.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public MapWithSignal(
        IObservable<TSource> source,
        TState state,
        Func<TState, TSource, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        _source = source;
        _state = state;
        _selector = selector;
    }

    /// <summary>Creates a stateful projection sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the selector.</param>
    /// <param name="selector">The transform applied to each source value and the state.</param>
    /// <returns>The projected observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public static IObservable<TResult> Create(
        IObservable<TSource> source,
        TState state,
        Func<TState, TSource, TResult> selector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(selector);

        return source switch
        {
            ImmutableEmptySignal<TSource> => ImmutableEmptySignal<TResult>.Instance,
            ImmutableNeverSignal<TSource> => ImmutableNeverSignal<TResult>.Instance,
            _ => new MapWithSignal<TSource, TState, TResult>(source, state, selector),
        };
    }

    /// <summary>Determines whether the sink must subscribe on the current thread.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <summary>Subscribes the observer to the projected sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StateCallbackPair<TState, Func<TState, TSource, TResult>> pair = new(_state, _selector);
        if (DirectSequenceEmission.TrySubscribeProjected(
            _source,
            pair,
            static (item, ctx) => System.Runtime.CompilerServices.Unsafe.As<Func<TState, int, TResult>>(ctx.Callback)(ctx.State, item),
            static (item, ctx) => ctx.Callback(ctx.State, item),
            observer))
        {
            return EmptyDisposable.Instance;
        }

        CoreProjectedObserverWitness<TSource, StateCallbackPair<TState, Func<TState, TSource, TResult>>, TResult> sink = new(
            observer,
            pair,
            static (item, ctx) => ctx.Callback(ctx.State, item));
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
