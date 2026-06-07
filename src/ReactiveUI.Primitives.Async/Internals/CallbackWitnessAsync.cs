// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// An witness that routes notifications through user-supplied asynchronous callbacks.
/// </summary>
/// <typeparam name="T">The type of the elements received by the witness.</typeparam>
/// <param name="onNextAsync">The asynchronous function invoked for each element.</param>
/// <param name="onErrorResumeAsync">An optional asynchronous function invoked when a resumable error occurs.</param>
/// <param name="onCompletedAsync">An optional asynchronous function invoked when the sequence completes.</param>
internal sealed class CallbackWitnessAsync<T>(
    Func<T, CancellationToken, ValueTask> onNextAsync,
    Func<Exception, CancellationToken, ValueTask>? onErrorResumeAsync = null,
    Func<Result, ValueTask>? onCompletedAsync = null) : ObserverAsync<T>
{
    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        onNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        if (onErrorResumeAsync is null)
        {
            UnhandledExceptionHandler.ReportUnhandledException(error);
            return default;
        }

        return onErrorResumeAsync.Invoke(error, cancellationToken);
    }

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result)
    {
        if (onCompletedAsync is null)
        {
            var exception = result.Exception;
            if (exception is not null)
            {
                UnhandledExceptionHandler.ReportUnhandledException(exception);
            }

            return default;
        }

        return onCompletedAsync.Invoke(result);
    }
}
