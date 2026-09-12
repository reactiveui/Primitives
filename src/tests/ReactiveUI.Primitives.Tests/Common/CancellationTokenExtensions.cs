// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Awaitable adapters over a <see cref="CancellationToken"/>.</summary>
internal static class CancellationTokenExtensions
{
    /// <summary>
    /// Returns a task that stays pending until the token is cancelled and then throws
    /// <see cref="OperationCanceledException"/>. It models work that only ever ends by cancellation, so a test can
    /// hold a factory open for as long as it needs to without handing a deadline to a clock.
    /// </summary>
    /// <param name="token">The token whose cancellation ends the wait.</param>
    /// <returns>A task that transitions to cancelled when <paramref name="token"/> is cancelled.</returns>
    internal static async Task WhenCanceled(this CancellationToken token)
    {
        // No RunContinuationsAsynchronously: the awaiting body resumes inside Cancel, so the cancellation path
        // has run by the time the caller's Cancel call returns.
        TaskCompletionSource completion = new();
        using var registration = token.Register(
            static state => ((TaskCompletionSource)state!).TrySetCanceled(),
            completion);
        await completion.Task.ConfigureAwait(false);
    }
}
