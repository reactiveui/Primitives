// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Routes notifications to callbacks and unhandled failures to the exception handler.</summary>
/// <typeparam name="T">The type of the elements received by the witness.</typeparam>
/// <param name="onNextAsync">The asynchronous function invoked for each element.</param>
/// <param name="onErrorResumeAsync">An optional asynchronous function invoked when a resumable error occurs.</param>
/// <param name="onCompletedAsync">An optional asynchronous function invoked when the sequence completes.</param>
[DebuggerDisplay("CallbackWitnessAsync: {_witness}")]
public sealed class CallbackWitnessAsync<T>(
    Func<T, CancellationToken, ValueTask> onNextAsync,
    Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync = null,
    Func<Result, ValueTask>? onCompletedAsync = null) : IWitnessAsync<T>
{
    /// <summary>The asynchronous function invoked when a resumable error occurs.</summary>
    private readonly Func<Exception, CancellationToken, ValueTask> _onErrorResumeAsync =
        onErrorResumeAsync ?? ReportUnhandledError;

    /// <summary>The asynchronous function invoked when the sequence completes.</summary>
    private readonly Func<Result, ValueTask> _onCompletedAsync =
        onCompletedAsync ?? ReportUnhandledCompletion;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

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
        onNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _onErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _onCompletedAsync(result);

    /// <summary>Reports an unhandled resumable error when no error callback was supplied.</summary>
    /// <param name="error">The unhandled exception.</param>
    /// <param name="cancellationToken">The cancellation token supplied by the source.</param>
    /// <returns>A completed value task.</returns>
    private static ValueTask ReportUnhandledError(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        UnhandledExceptionHandler.ReportUnhandledException(error);
        return default;
    }

    /// <summary>Reports a terminal failure when no completion callback was supplied.</summary>
    /// <param name="result">The terminal result.</param>
    /// <returns>A completed value task.</returns>
    private static ValueTask ReportUnhandledCompletion(Result result)
    {
        var exception = result.Exception;
        if (exception is null)
        {
            return default;
        }

        UnhandledExceptionHandler.ReportUnhandledException(exception);
        return default;
    }
}
