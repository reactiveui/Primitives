// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives;

/// <summary>Observer that forwards notifications to stateful delegates.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <typeparam name="TState">The state type.</typeparam>
public sealed class StatefulWitness<T, TState> : IObserver<T>
{
    /// <summary>Callback state.</summary>
    private readonly TState _state;

    /// <summary>Next notification callback.</summary>
    private readonly Action<T, TState> _onNext;

    /// <summary>Error notification callback.</summary>
    private readonly Action<Exception, TState>? _onError;

    /// <summary>Completion notification callback.</summary>
    private readonly Action<Result, TState>? _onCompleted;

    /// <summary>Initializes a new instance of the <see cref="StatefulWitness{T, TState}"/> class.</summary>
    /// <param name="state">The callback state.</param>
    /// <param name="onNext">Next notification callback.</param>
    /// <param name="onError">Error notification callback.</param>
    /// <param name="onCompleted">Completion notification callback.</param>
    public StatefulWitness(
        TState state,
        Action<T, TState> onNext,
        Action<Exception, TState>? onError,
        Action<Result, TState>? onCompleted)
    {
        _state = state;
        _onNext = onNext ?? throw new ArgumentNullException(nameof(onNext));
        _onError = onError;
        _onCompleted = onCompleted;
    }

    /// <inheritdoc/>
    public void OnCompleted() => _onCompleted?.Invoke(Result.Success, _state);

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (_onError is not null)
        {
            _onError(error, _state);
            return;
        }

        ExceptionDispatchInfo.Capture(error).Throw();
    }

    /// <inheritdoc/>
    public void OnNext(T value) => _onNext(value, _state);
}
