// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observer that logs resumable errors and terminal failures before forwarding them downstream.</summary>
/// <typeparam name="T">The element type.</typeparam>
[DebuggerDisplay("LogErrorsWitness: Downstream = {Downstream}, Logger = {Logger}")]
public sealed class LogErrorsWitness<T> : IWitnessAsync<T>
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <summary>Initializes a new instance of the <see cref="LogErrorsWitness{T}"/> class.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="logger">The error logger.</param>
    public LogErrorsWitness(IObserverAsync<T> downstream, Action<Exception> logger)
    {
        ArgumentExceptionHelper.ThrowIfNull(downstream);
        ArgumentExceptionHelper.ThrowIfNull(logger);

        Downstream = downstream;
        Logger = logger;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserverAsync<T> Downstream { get; }

    /// <summary>Gets the error logger.</summary>
    private Action<Exception> Logger { get; }

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

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        Downstream.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        Logger(error);
        return Downstream.OnErrorResumeAsync(error, cancellationToken);
    }

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        if (result.Exception is { } failure)
        {
            Logger(failure);
        }

        return Downstream.OnCompletedAsync(result);
    }
}
