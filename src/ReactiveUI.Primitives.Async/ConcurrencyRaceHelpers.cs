// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Internal;

/// <summary>
/// Pure helpers for the two recurring race-claim primitives in the async layer:
/// the "first caller wins" <see cref="Interlocked.CompareExchange(ref int, int, int)"/>
/// transition used by <c>PooledDelaySource</c>, and the "tolerate already-disposed CTS"
/// <c>CancellationTokenSource.CancelAsync</c> wrapper used by <c>ObserverAsync</c>'s
/// dispose path. Both are pure functions over their inputs and are directly unit-tested
/// against this class.
/// </summary>
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
    public static bool TryClaim(ref int state, int openSentinel, int claimedSentinel) =>
        Interlocked.CompareExchange(ref state, claimedSentinel, openSentinel) == openSentinel;

    /// <summary>
    /// Calls <c>CancellationTokenSource.CancelAsync</c> on <paramref name="cts"/>,
    /// tolerating the <see cref="ObjectDisposedException"/> that another concurrent dispose
    /// may have already raced ahead with. Returns <see langword="true"/> if the cancellation
    /// went through; <see langword="false"/> if another caller had already cancelled-and-
    /// disposed the source.
    /// </summary>
    /// <param name="cts">The cancellation token source to cancel.</param>
    /// <returns>
    /// <see langword="true"/> if the cancellation completed; <see langword="false"/> if the
    /// source was already disposed.
    /// </returns>
    public static async ValueTask<bool> TryCancelAsync(CancellationTokenSource cts)
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
