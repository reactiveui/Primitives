// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that walks a state machine synchronously, emitting one projected value per iteration.</summary>
/// <typeparam name="TState">The state type.</typeparam>
/// <typeparam name="TResult">The emitted value type.</typeparam>
/// <param name="initialState">The state the walk starts from.</param>
/// <param name="condition">Evaluated before each iteration; emission stops when it returns <see langword="false"/>.</param>
/// <param name="iterate">Produces the next state from the current one.</param>
/// <param name="resultSelector">Projects the current state to the emitted value.</param>
[System.Diagnostics.DebuggerDisplay("UnfoldSignal: InitialState = {_initialState}, Condition = {_condition}")]
public sealed class UnfoldSignal<TState, TResult>(
    TState initialState,
    Func<TState, bool> condition,
    Func<TState, TState> iterate,
    Func<TState, TResult> resultSelector) : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
{
    /// <summary>The state the walk starts from.</summary>
    private readonly TState _initialState = initialState;

    /// <summary>The continuation condition, evaluated before each iteration.</summary>
    private readonly Func<TState, bool> _condition = condition;

    /// <summary>The transition from the current state to the next.</summary>
    private readonly Func<TState, TState> _iterate = iterate;

    /// <summary>The projection from state to emitted value.</summary>
    private readonly Func<TState, TResult> _resultSelector = resultSelector;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var state = _initialState;
        while (_condition(state))
        {
            observer.OnNext(_resultSelector(state));
            state = _iterate(state);
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        var state = _initialState;
        while (_condition(state))
        {
            onNext(_resultSelector(state));
            state = _iterate(state);
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
