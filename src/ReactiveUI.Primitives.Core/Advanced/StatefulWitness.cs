// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer that forwards notifications to stateful delegates.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <typeparam name="TState">The state type.</typeparam>
[System.Diagnostics.DebuggerDisplay("State = {_state}, OnError = {_onError}")]
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
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _onCompleted?.Invoke(Result.Success, _state);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => (_onError ?? Rethrow)(error, _state);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => _onNext(value, _state);

    /// <summary>Rethrows the supplied exception without losing its stack information.</summary>
    /// <param name="error">The exception to rethrow.</param>
    /// <param name="state">Unused callback state.</param>
    /// <remarks>Excluded from coverage: the unreachable sequence point after <see cref="ExceptionDispatchInfo"/> rethrow cannot be credited by cobertura.</remarks>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void Rethrow(Exception error, TState state)
    {
        _ = state;
        ExceptionDispatchInfo.Capture(error).Throw();
    }
}
