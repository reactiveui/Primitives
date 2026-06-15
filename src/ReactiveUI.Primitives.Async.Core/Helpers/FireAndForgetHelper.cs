// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Helpers;

/// <summary>
/// Provides a helper for executing async actions as fire-and-forget with exception swallowing.
/// Used for async void callbacks (e.g. cancellation token registrations, signal handlers)
/// where exceptions cannot propagate to a caller.
/// </summary>
[ExcludeFromCodeCoverage]
public static class FireAndForgetHelper
{
    /// <summary>Executes an async action as fire-and-forget, swallowing all exceptions.</summary>
    /// <param name="action">The async action to execute.</param>
    [SuppressMessage(
        "Major Bug",
        "S3168:\"async\" methods should not return \"void\"",
        Justification = "This is a fire-and-forget helper.")]
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
