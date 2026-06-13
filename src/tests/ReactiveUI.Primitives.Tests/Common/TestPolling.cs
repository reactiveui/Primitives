// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Shared polling helpers for asynchronous coverage branches.</summary>
internal static class TestPolling
{
    /// <summary>The delay in milliseconds between condition polls.</summary>
    private const int PollDelayMilliseconds = 10;

    /// <summary>Polls a condition until it succeeds or the timeout elapses.</summary>
    /// <param name="condition">The condition to evaluate on each poll.</param>
    /// <param name="timeout">The maximum time to wait for the condition.</param>
    /// <returns>A task that completes when the condition is satisfied.</returns>
    public static async Task SpinUntil(Func<bool> condition, TimeSpan timeout)
    {
        var attempts = (int)(timeout.TotalMilliseconds / PollDelayMilliseconds);
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(PollDelayMilliseconds).ConfigureAwait(false);
        }

        throw new TimeoutException("Timed out waiting for asynchronous coverage branch.");
    }
}
