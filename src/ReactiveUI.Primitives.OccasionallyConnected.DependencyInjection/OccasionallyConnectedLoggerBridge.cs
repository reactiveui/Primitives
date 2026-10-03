// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Logging;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Observes startup failures and emits bounded dependency-injection logs.</summary>
internal static class OccasionallyConnectedLoggerBridge
{
    /// <summary>The startup failure event identifier.</summary>
    private static readonly EventId StartupFailureEvent = new(1001, "OC.Startup");

    /// <summary>The bounded startup failure log callback.</summary>
    private static readonly Action<ILogger, Exception?> LogStartupFailure = LoggerMessage.Define(
        LogLevel.Error,
        StartupFailureEvent,
        "OC.Startup failure status Faulted");

    /// <summary>Observes the context startup task.</summary>
    /// <param name="context">The context to observe.</param>
    /// <param name="logger">The optional logger.</param>
    internal static void ObserveStartup(OccasionallyConnectedContext context, ILogger? logger)
    {
        if (logger is null)
        {
            _ = ObserveWithoutLoggingAsync(context.StartupTask);
            return;
        }

        _ = ObserveWithLoggingAsync(context.StartupTask, logger);
    }

    /// <summary>Observes startup failure without logging.</summary>
    /// <param name="startupTask">The startup task.</param>
    /// <returns>The observation task.</returns>
    private static async Task ObserveWithoutLoggingAsync(Task startupTask)
    {
        try
        {
            await startupTask.ConfigureAwait(false);
        }
        catch
        {
            _ = startupTask.Exception;
        }
    }

    /// <summary>Observes startup failure and isolates logger failures.</summary>
    /// <param name="startupTask">The startup task.</param>
    /// <param name="logger">The logger.</param>
    /// <returns>The observation task.</returns>
    private static async Task ObserveWithLoggingAsync(Task startupTask, ILogger logger)
    {
        try
        {
            await startupTask.ConfigureAwait(false);
        }
        catch
        {
            _ = startupTask.Exception;
            try
            {
                LogStartupFailure(logger, null);
            }
            catch (Exception exception)
            {
                ObserveLoggerFailure(exception);
            }
        }
    }

    /// <summary>Observes an isolated logger failure without rethrowing into the discarded observer task.</summary>
    /// <param name="exception">The logger failure.</param>
    private static void ObserveLoggerFailure(Exception exception) => _ = exception.Message;
}
