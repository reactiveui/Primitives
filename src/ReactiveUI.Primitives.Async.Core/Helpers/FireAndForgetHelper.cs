// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Helpers;

/// <summary>
/// Runs asynchronous work from a synchronous callback — a cancellation-token registration, a signal handler — where
/// there is no caller to observe a task, reporting failures to <see cref="UnhandledExceptionHandler"/>.
/// </summary>
[ExcludeFromCodeCoverage]
public static class FireAndForgetHelper
{
    /// <summary>Executes an async action as fire-and-forget, swallowing all exceptions.</summary>
    /// <param name="action">The async action to execute.</param>
    [SuppressMessage(
        "Concurrency",
        "SST1905:Do not use async void",
        Justification = "There is no caller to hand a Task back to, so failures go to the global handler instead.")]
    [SuppressMessage(
        "ReSharper",
        "AsyncVoidMethod",
        Justification = "This is a fire-and-forget helper.")]
    public static async void Run(Func<ValueTask> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // Fire-and-forget has no caller to propagate to; surface the failure to the global handler.
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }
}
