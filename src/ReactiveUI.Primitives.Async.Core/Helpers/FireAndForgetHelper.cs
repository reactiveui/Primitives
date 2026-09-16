// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Helpers;

/// <summary>
/// Runs asynchronous work from a synchronous callback - a cancellation-token registration, a signal handler - where
/// there is no caller to observe a task, reporting failures to <see cref="UnhandledExceptionHandler"/>.
/// </summary>
public static class FireAndForgetHelper
{
    /// <summary>Starts <paramref name="action"/> with no task to await, routing any failure to <see cref="UnhandledExceptionHandler"/>.</summary>
    /// <param name="action">The asynchronous action to start.</param>
    [SuppressMessage(
        "Concurrency",
        "SST1905:Do not use async void",
        Justification = "There is no caller to hand a Task back to, so failures go to the global handler instead.")]
    [SuppressMessage(
        "ReSharper",
        "AsyncVoidMethod",
        Justification = "There is no caller to hand a Task back to, so failures go to the global handler instead.")]
    [ExcludeFromCodeCoverage]
    public static async void Run(Func<ValueTask> action) =>
        await RunAsync(action).ConfigureAwait(false);

    /// <summary>Runs the action and reports failures to the unhandled exception handler.</summary>
    /// <param name="action">The action to execute.</param>
    /// <returns>A task that completes once the action and any failure reporting have run.</returns>
    internal static async ValueTask RunAsync(Func<ValueTask> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            // Detached-operation failures reach the global handler.
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }
}
