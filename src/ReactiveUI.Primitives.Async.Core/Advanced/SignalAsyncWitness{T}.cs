// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An asynchronous observer that forwards all notifications to the wrapped signal.</summary>
/// <typeparam name="T">The type of elements processed by the observer.</typeparam>
[System.Diagnostics.DebuggerDisplay("SignalAsyncWitness: {_witness}")]
public sealed class SignalAsyncWitness<T> : IWitnessAsync<T>
{
    /// <summary>The wrapped signal.</summary>
    private readonly ISignalAsync<T> _signal;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <summary>Initializes a new instance of the <see cref="SignalAsyncWitness{T}"/> class.</summary>
    /// <param name="signal">The signal to forward notifications to.</param>
    public SignalAsyncWitness(ISignalAsync<T> signal)
    {
        ArgumentExceptionHelper.ThrowIfNull(signal);
        _signal = signal;
    }

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

    /// <summary>Forwards the value with an uncancelable token after the observer's disposal check.</summary>
    /// <param name="value">The value to be processed by the observer.</param>
    /// <param name="cancellationToken">The token captured by the base observer's TryEnter scope. Ignored on the forward.</param>
    /// <returns>A ValueTask that represents the asynchronous operation.</returns>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return _signal.OnNextAsync(value, CancellationToken.None);
    }

    /// <summary>Forwards the error to the wrapped signal.</summary>
    /// <param name="error">The exception that caused the error condition. Cannot be null.</param>
    /// <param name="cancellationToken">The token captured by the base observer's TryEnter scope. Ignored on the forward.</param>
    /// <returns>A ValueTask that represents the asynchronous error handling operation.</returns>
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return _signal.OnErrorResumeAsync(error, CancellationToken.None);
    }

    /// <summary>Forwards the terminal result to the wrapped signal.</summary>
    /// <param name="result">The result of the completed operation, containing any relevant outcome information.</param>
    /// <returns>A task that completes when the signal has handled the result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => _signal.OnCompletedAsync(result);
}
