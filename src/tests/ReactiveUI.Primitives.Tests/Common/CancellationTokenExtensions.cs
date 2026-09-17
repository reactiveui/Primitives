// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Awaitable adapters over a <see cref="CancellationToken"/>.</summary>
internal static class CancellationTokenExtensions
{
    /// <summary>Provides cancellation completion for a token.</summary>
    /// <param name="token">The token to observe.</param>
    extension(CancellationToken token)
    {
        /// <summary>Completes with cancellation when the token is canceled.</summary>
        /// <returns>The cancellation task.</returns>
        internal async Task WhenCanceled()
        {
            TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            await using var registration = token.UnsafeRegister(
                static state => _ = ((TaskCompletionSource)state!).TrySetCanceled(),
                completion);
            await completion.Task.ConfigureAwait(false);
        }
    }
}
