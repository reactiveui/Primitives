// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a mechanism for registering a global handler to process unhandled exceptions that occur during application execution.</summary>
/// <remarks>The default handler writes to System.Diagnostics.Trace; cancellation exceptions are ignored.</remarks>
public static class UnhandledExceptionHandler
{
    /// <summary>The currently registered handler action invoked when an unhandled exception occurs.</summary>
    private static Action<Exception> _unhandledException = TraceUnhandledException;

    /// <summary>Gets the handler that <see cref="ReportUnhandledException"/> routes to.</summary>
    internal static Action<Exception> CurrentHandler => _unhandledException;

    /// <summary>Registers a handler to be invoked when an unhandled exception occurs.</summary>
    /// <param name="unhandledExceptionHandler">The action that receives each unhandled exception. Cannot be null.</param>
    /// <remarks>Replaces the process-wide handler; exceptions thrown by the handler are ignored.</remarks>
    public static void Register(Action<Exception> unhandledExceptionHandler) =>
        _unhandledException = unhandledExceptionHandler;

    /// <summary>Invokes the application's unhandled exception handler for exceptions that are not operation cancellations.</summary>
    /// <param name="e">The exception to be processed by the unhandled exception handler. Cannot be null.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST1429:Handle, rethrow, or narrow this catch; an empty catch of the base exception hides failures",
        Justification = "This is the last-resort sink, so rethrowing a handler's own failure would have nowhere to go but back here.")]
    internal static void ReportUnhandledException(Exception e)
    {
        if (e is OperationCanceledException)
        {
            return;
        }

        try
        {
            _unhandledException(e);
        }
        catch
        {
            // Exceptions from the registered handler are ignored.
        }
    }

    /// <summary>The default handler: writes the exception to <see cref="System.Diagnostics.Trace"/> as an error.</summary>
    /// <param name="exception">The exception that was not handled. Cannot be null.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void TraceUnhandledException(Exception exception) =>
        System.Diagnostics.Trace.TraceError("UnhandleException: {0}", exception);
}
