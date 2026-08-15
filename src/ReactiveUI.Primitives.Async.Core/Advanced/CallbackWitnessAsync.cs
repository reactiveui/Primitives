// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An witness that routes notifications through user-supplied asynchronous callbacks.</summary>
/// <typeparam name="T">The type of the elements received by the witness.</typeparam>
/// <param name="onNextAsync">The asynchronous function invoked for each element.</param>
/// <param name="onErrorResumeAsync">An optional asynchronous function invoked when a resumable error occurs.</param>
/// <param name="onCompletedAsync">An optional asynchronous function invoked when the sequence completes.</param>
[System.Diagnostics.DebuggerDisplay("HasDisposed = {HasDisposed}")]
public sealed class CallbackWitnessAsync<T>(
    Func<T, CancellationToken, ValueTask> onNextAsync,
    Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync = null,
    Func<Result, ValueTask>? onCompletedAsync = null) : WitnessAsync<T>
{
    /// <summary>The asynchronous function invoked when a resumable error occurs.</summary>
    private readonly Func<Exception, CancellationToken, ValueTask> _onErrorResumeAsync =
        onErrorResumeAsync ?? ReportUnhandledError;

    /// <summary>The asynchronous function invoked when the sequence completes.</summary>
    private readonly Func<Result, ValueTask> _onCompletedAsync =
        onCompletedAsync ?? ReportUnhandledCompletion;

    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        onNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _onErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
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
