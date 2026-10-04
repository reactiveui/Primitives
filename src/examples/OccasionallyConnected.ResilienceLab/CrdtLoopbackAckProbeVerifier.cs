// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Verifies the public behavior used to prove same-subscription ACK resume.</summary>
internal static class CrdtLoopbackAckProbeVerifier
{
    /// <summary>Gets whether an ACK probe page contains one expected operation group.</summary>
    /// <param name="page">The received page.</param>
    /// <param name="value">The expected G-counter value.</param>
    /// <param name="expectedEventCount">The expected event and completion count.</param>
    /// <returns>Whether the page matched the expected probe shape.</returns>
    internal static bool IsExpectedAckProbePage(
        CrdtLoopbackReceivedStream page,
        int value,
        int expectedEventCount) =>
        page.EventCount == expectedEventCount
        && page.CompletedOperationCount == expectedEventCount
        && GetCounterValue(page.States[^1]) == value;

    /// <summary>Gets whether a resumed page starts at the acknowledged cursor without replaying old events.</summary>
    /// <param name="first">The acknowledged page.</param>
    /// <param name="second">The resumed page.</param>
    /// <param name="value">The expected resumed G-counter value.</param>
    /// <param name="expectedEventCount">The expected event and completion count.</param>
    /// <returns>Whether the resume proof passed.</returns>
    internal static bool IsExpectedResumePage(
        CrdtLoopbackReceivedStream first,
        CrdtLoopbackReceivedStream second,
        int value,
        int expectedEventCount) =>
        string.Equals(second.PreviousCursor, first.Cursor, StringComparison.Ordinal)
        && IsExpectedAckProbePage(second, value, expectedEventCount)
        && !ContainsAnyEventId(second.EventIds, first.EventIds);

    /// <summary>Gets whether a same-subscription stale initial read is rejected after ACK.</summary>
    /// <param name="receive">The stale receive attempt.</param>
    /// <param name="diagnosticFragment">The expected diagnostic fragment.</param>
    /// <returns>Whether the public subscription rejected the rewind.</returns>
    internal static async ValueTask<bool> IsStaleInitialReadRejectedAsync(
        Func<ValueTask<CrdtLoopbackReceivedStream>> receive,
        string diagnosticFragment)
    {
        try
        {
            _ = await receive().ConfigureAwait(false);
            return false;
        }
        catch (InvalidOperationException exception)
            when (exception.Message.Contains(diagnosticFragment, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }
    }

    /// <summary>Gets whether one event id set contains any id from another set.</summary>
    /// <param name="candidates">The event ids to inspect.</param>
    /// <param name="previous">The previously received event ids.</param>
    /// <returns>Whether any event id was replayed.</returns>
    internal static bool ContainsAnyEventId(IReadOnlyList<Guid> candidates, IReadOnlyList<Guid> previous)
    {
        for (var candidateIndex = 0; candidateIndex < candidates.Count; candidateIndex++)
        {
            for (var previousIndex = 0; previousIndex < previous.Count; previousIndex++)
            {
                if (candidates[candidateIndex] == previous[previousIndex])
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>Gets a counter value from a received authoritative state.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <returns>The counter value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetCounterValue(CrdtState state) =>
        checked((int)state.Value.Counter);
}
