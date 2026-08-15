// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Captures unhandled async exceptions while restoring the previous process-wide handler on disposal.</summary>
internal sealed class UnhandledExceptionCapture : IDisposable
{
    /// <summary>The polling interval used while waiting for delayed fire-and-forget callbacks.</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);

    /// <summary>Synchronizes access to the captured exception list.</summary>
    private readonly Lock _gate = new();

    /// <summary>Exceptions captured through the temporary unhandled exception handler.</summary>
    private readonly List<Exception> _exceptions = [];

    /// <summary>The handler that was active before this capture was installed.</summary>
    private readonly Action<Exception> _previousHandler;

    /// <summary>Tracks whether the capture has already restored the previous handler.</summary>
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
    /// <param name="timeout">Maximum time to wait.</param>
    /// <returns>The matched exception, or <see langword="null"/> if no match is observed.</returns>
    internal Task<Exception?> WaitForAsync(string message, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(message);
        return WaitForAsync(ex => ex.Message == message, timeout);
    }

    /// <summary>Waits for a captured exception that satisfies the supplied predicate.</summary>
    /// <param name="predicate">The predicate used to find the expected exception.</param>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <returns>The matched exception, or <see langword="null"/> if no match is observed.</returns>
    internal async Task<Exception?> WaitForAsync(Func<Exception, bool> predicate, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);

        var deadline = TimeProvider.System.GetUtcNow().Add(timeout);
        var match = Find(predicate);

        if (match is not null)
        {
            return match;
        }

        using PeriodicTimer poll = new(PollInterval);

        while (TimeProvider.System.GetUtcNow() < deadline
               && await poll.WaitForNextTickAsync(CancellationToken.None))
        {
            match = Find(predicate);

            if (match is not null)
            {
                return match;
            }
        }

        return Find(predicate);
    }

    /// <summary>Stores an exception routed through the temporary handler.</summary>
    /// <param name="exception">The routed exception.</param>
    private void Capture(Exception exception)
    {
        lock (_gate)
        {
            _exceptions.Add(exception);
        }
    }

    /// <summary>Finds the first captured exception that matches the predicate.</summary>
    /// <param name="predicate">The predicate used to find the expected exception.</param>
    /// <returns>The matched exception, or <see langword="null"/> if none match.</returns>
    private Exception? Find(Func<Exception, bool> predicate)
    {
        lock (_gate)
        {
            return _exceptions.FirstOrDefault(predicate);
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
