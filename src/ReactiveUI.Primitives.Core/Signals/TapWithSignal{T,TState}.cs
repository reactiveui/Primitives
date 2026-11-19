// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core.Internal;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Invokes the supplied action with each source value and state, then forwards the value unchanged.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TState">The state type passed to the action.</typeparam>
[System.Diagnostics.DebuggerDisplay("TapWithSignal: Source = {_source}, State = {_state}")]
public sealed class TapWithSignal<T, TState> : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The state passed to the action.</summary>
    private readonly TState _state;

    /// <summary>The action invoked for each value and the state.</summary>
    private readonly Action<TState, T> _onNext;

    /// <summary>Initializes a new instance of the <see cref="TapWithSignal{T, TState}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the action.</param>
    /// <param name="onNext">The action invoked for each value and the state.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    public TapWithSignal(IObservable<T> source, TState state, Action<TState, T> onNext)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        _source = source;
        _state = state;
        _onNext = onNext;
    }

    /// <summary>Creates a stateful side-effect sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the action.</param>
    /// <param name="onNext">The action invoked for each value and the state.</param>
    /// <returns>The tapped observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, TState state, Action<TState, T> onNext)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T>
            ? source
            : new TapWithSignal<T, TState>(source, state, onNext);
    }

    /// <summary>Determines whether the sink must subscribe on the current thread.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <summary>Subscribes the observer to the tapped sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StateCallbackPair<TState, Action<TState, T>> pair = new(_state, _onNext);
        if (DirectSequenceEmission.TrySubscribeTapped(_source, pair, observer))
        {
            return EmptyDisposable.Instance;
        }

        CoreProjectedObserverWitness<T, StateCallbackPair<TState, Action<TState, T>>, T> sink = new(
            observer,
            pair,
            static (item, ctx) =>
            {
                ctx.Callback(ctx.State, item);
                return item;
            });
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
