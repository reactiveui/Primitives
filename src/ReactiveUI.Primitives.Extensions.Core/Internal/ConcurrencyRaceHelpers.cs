// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>Claims one-time transitions and tolerates cancellation of disposed token sources.</summary>
internal static class ConcurrencyRaceHelpers
{
    /// <summary>
    /// Atomically transitions <paramref name="state"/> from <paramref name="openSentinel"/>
    /// to <paramref name="claimedSentinel"/>. Returns <see langword="true"/> if this caller
    /// won the race; <see langword="false"/> if another caller had already claimed the state.
    /// </summary>
    /// <param name="state">The reference to the state field.</param>
    /// <param name="openSentinel">The sentinel value the state must currently hold.</param>
    /// <param name="claimedSentinel">The sentinel value the state transitions to on success.</param>
    /// <returns>
    /// <see langword="true"/> if the claim succeeded; <see langword="false"/> if another caller
    /// already claimed the state.
    /// </returns>
    internal static bool TryClaim(ref int state, int openSentinel, int claimedSentinel) =>
        Interlocked.CompareExchange(ref state, claimedSentinel, openSentinel) == openSentinel;

    /// <summary>Cancels the source asynchronously, tolerating concurrent disposal.</summary>
    /// <param name="cts">The cancellation token source to cancel.</param>
    /// <returns>
    /// <see langword="true"/> if the cancellation completed; <see langword="false"/> if the
    /// source was already disposed.
    /// </returns>
    internal static async ValueTask<bool> TryCancelAsync(CancellationTokenSource cts)
    {
        try
        {
            await cts.CancelAsync().ConfigureAwait(false);
            return true;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }
}
