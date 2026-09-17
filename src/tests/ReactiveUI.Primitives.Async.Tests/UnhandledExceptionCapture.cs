// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Captures unhandled async exceptions while restoring the previous process-wide handler on disposal.</summary>
internal sealed class UnhandledExceptionCapture : IDisposable
{
    /// <summary>Synchronizes access to the captured exception list.</summary>
    private readonly Lock _gate = new();

    /// <summary>Exceptions captured through the temporary unhandled exception handler.</summary>
    private readonly List<Exception> _exceptions = [];

    /// <summary>The handler that was active before this capture was installed.</summary>
    private readonly Action<Exception> _previousHandler;

    /// <summary>Signals the next captured exception.</summary>
    private TaskCompletionSource _arrival = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Set to one once the prior handler has been restored.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="UnhandledExceptionCapture"/> class.</summary>
    public UnhandledExceptionCapture()
    {
        _previousHandler = UnhandledExceptionHandler.CurrentHandler;
        UnhandledExceptionHandler.Register(Capture);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Restore();

    /// <summary>Waits for an exception with the expected message.</summary>
    /// <param name="message">The expected exception message.</param>
    /// <returns>The matching exception.</returns>
    internal Task<Exception?> WaitForAsync(string message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return WaitForAsync(ex => ex.Message == message);
    }

    /// <summary>Waits for a captured exception that satisfies the supplied predicate.</summary>
    /// <param name="predicate">The predicate used to find the expected exception.</param>
    /// <returns>The matching exception.</returns>
    internal async Task<Exception?> WaitForAsync(Func<Exception, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        while (true)
        {
            Task arrival;
            lock (_gate)
            {
                var match = _exceptions.FirstOrDefault(predicate);
                if (match is not null)
                {
                    return match;
                }

                arrival = _arrival.Task;
            }

            await arrival;
        }
    }

    /// <summary>Stores an exception routed through the temporary handler.</summary>
    /// <param name="exception">The routed exception.</param>
    private void Capture(Exception exception)
    {
        lock (_gate)
        {
            _exceptions.Add(exception);
            var arrival = _arrival;
            _arrival = new(TaskCreationOptions.RunContinuationsAsynchronously);
            arrival.SetResult();
        }
    }

    /// <summary>Restores the previously registered unhandled exception handler.</summary>
    private void Restore()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        UnhandledExceptionHandler.Register(_previousHandler);
    }
}
