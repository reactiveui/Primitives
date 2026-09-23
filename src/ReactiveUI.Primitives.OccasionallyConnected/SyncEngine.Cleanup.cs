// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Cleanup helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Creates a completion source whose continuations cannot execute inside the engine lock.</summary>
    /// <returns>The new completion source.</returns>
    private static TaskCompletionSource<bool> CreateCompletion() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Releases waiters successfully.</summary>
    /// <param name="waiters">The waiters.</param>
    private static void ReleaseWaiters(CapacityWaiter[] waiters)
    {
        for (var i = 0; i < waiters.Length; i++)
        {
            waiters[i].Release();
        }
    }

    /// <summary>Releases waiters with an error.</summary>
    /// <param name="waiters">The waiters.</param>
    /// <param name="exception">The error.</param>
    private static void ReleaseWaiters(CapacityWaiter[] waiters, Exception exception)
    {
        for (var i = 0; i < waiters.Length; i++)
        {
            waiters[i].Release(exception);
        }
    }

    /// <summary>Runs one asynchronous cleanup stage and preserves the first failure.</summary>
    /// <param name="failure">The current first failure.</param>
    /// <param name="cleanup">The cleanup stage.</param>
    /// <returns>The first failure, if any.</returns>
    private static async ValueTask<Exception?> CaptureCleanupFailureAsync(Exception? failure, Func<ValueTask> cleanup)
    {
        try
        {
            await cleanup().ConfigureAwait(false);
            return failure;
        }
        catch (Exception exception)
        {
            return failure ?? exception;
        }
    }

    /// <summary>Throws the captured failure when one was preserved.</summary>
    /// <param name="failure">The captured failure.</param>
    private static void ThrowCaptured(Exception? failure)
    {
        if (failure is null)
        {
            return;
        }

        ExceptionDispatchInfo.Capture(failure).Throw();
    }

    /// <summary>Runs lifecycle disposal and preserves the first failure.</summary>
    /// <param name="failure">The current first failure.</param>
    /// <returns>The first failure, if any.</returns>
    private async ValueTask<Exception?> CaptureLifecycleDisposeFailureAsync(Exception? failure)
    {
        try
        {
            await _lifecycle.DisposeAsync().ConfigureAwait(false);
            return failure;
        }
        catch (Exception exception)
        {
            return failure ?? exception;
        }
    }
}
