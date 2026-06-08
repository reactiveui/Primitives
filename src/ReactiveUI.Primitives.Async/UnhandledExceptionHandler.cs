// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a mechanism for registering a global handler to process unhandled exceptions that occur during
/// application execution.
/// </summary>
/// <remarks>Use this class to specify a custom action to be invoked when an unhandled exception is
/// encountered. By default, unhandled exceptions are written to the console. Registering a handler allows
/// applications to perform custom logging, cleanup, or user notification in response to unexpected errors. This
/// class does not handle exceptions of type OperationCanceledException.</remarks>
public static class UnhandledExceptionHandler
{
    /// <summary>The currently registered handler action invoked when an unhandled exception occurs.</summary>
    private static Action<Exception> _unhandledException = TraceUnhandledException;

    /// <summary>Gets the currently registered handler. Used for save/restore in tests.</summary>
    internal static Action<Exception> CurrentHandler => _unhandledException;

    /// <summary>Registers a handler to be invoked when an unhandled exception occurs.</summary>
    /// <remarks>Registering a new handler replaces any previously registered handler. The handler
    /// will be called for each unhandled exception that occurs after registration.</remarks>
    /// <param name="unhandledExceptionHandler">An action to execute when an unhandled exception is encountered. The exception instance is passed as a
    /// parameter to the handler. Cannot be null.</param>
    public static void Register(Action<Exception> unhandledExceptionHandler) =>
        _unhandledException = unhandledExceptionHandler;

    /// <summary>Invokes the application's unhandled exception handler for exceptions that are not operation cancellations.</summary>
    /// <remarks>OperationCanceledException instances are ignored and not passed to the
    /// handler.</remarks>
    /// <param name="e">The exception to be processed by the unhandled exception handler. Cannot be null.</param>
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
            // Ignored
        }
    }

    /// <summary>Handles unhandled exceptions by writing an error message to the console output.</summary>
    /// <remarks>This method is intended to be used as a default handler for unhandled exceptions in
    /// an application. It writes the exception details to the standard console output for diagnostic
    /// purposes.</remarks>
    /// <param name="exception">The exception that was not handled. Cannot be null.</param>
    internal static void TraceUnhandledException(Exception exception) =>
        System.Diagnostics.Trace.TraceError("UnhandleException: {0}", exception);
}
