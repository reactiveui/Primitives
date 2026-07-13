// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Finite state expansion signal.</summary>
/// <typeparam name="TState">State type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
/// <param name="initialState">Initial state.</param>
/// <param name="condition">Loop condition.</param>
/// <param name="iterate">State iterator.</param>
/// <param name="resultSelector">Result selector.</param>
public sealed class UnfoldSignal<TState, TResult>(
    TState initialState,
    Func<TState, bool> condition,
    Func<TState, TState> iterate,
    Func<TState, TResult> resultSelector) : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
{
    /// <summary>Initial state.</summary>
    private readonly TState _initialState = initialState;

    /// <summary>Loop condition.</summary>
    private readonly Func<TState, bool> _condition = condition;

    /// <summary>State iterator.</summary>
    private readonly Func<TState, TState> _iterate = iterate;

    /// <summary>Result selector.</summary>
    private readonly Func<TState, TResult> _resultSelector = resultSelector;

    /// <inheritdoc/>
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
