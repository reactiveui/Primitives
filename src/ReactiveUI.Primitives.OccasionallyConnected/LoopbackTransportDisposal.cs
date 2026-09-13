// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Completes loopback disposal work after admission gates have been released.</summary>
internal static class LoopbackTransportDisposal
{
    /// <summary>Disposes the captured session and completes the supplied signal.</summary>
    /// <param name="session">The session captured before leaving the adapter gate.</param>
    /// <param name="completion">The disposal completion signal.</param>
    /// <returns>The disposal task.</returns>
    internal static async Task DisposeSessionAsync(IAsyncDisposable? session, TaskCompletionSource<object?> completion)
    {
        try
        {
            if (session is not null)
            {
                await session.DisposeAsync().ConfigureAwait(false);
            }

            _ = completion.TrySetResult(null);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
        }
    }
}
