// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Evaluates whether independently received authoritative histories commute to the expected CRDT value.</summary>
internal static class CrdtLoopbackHistoryEvaluator
{
    /// <summary>Evaluates both received histories for a counter CRDT.</summary>
    /// <param name="clientAStream">The client A received stream.</param>
    /// <param name="clientBStream">The client B received stream.</param>
    /// <param name="kind">The CRDT kind.</param>
    /// <param name="expected">The expected counter value.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The expected counter value when all projected histories match; otherwise the first divergent actual value.</returns>
    internal static int EvaluateCounter(
        CrdtLoopbackReceivedStream clientAStream,
        CrdtLoopbackReceivedStream clientBStream,
        CrdtKind kind,
        int expected,
        CrdtBounds bounds)
    {
        var clientAForward = GetCounterValue(MergeHistory(clientAStream.States, kind, reverse: false, bounds));
        var clientAReverse = GetCounterValue(MergeHistory(clientAStream.States, kind, reverse: true, bounds));
        var clientBForward = GetCounterValue(MergeHistory(clientBStream.States, kind, reverse: false, bounds));
        var clientBReverse = GetCounterValue(MergeHistory(clientBStream.States, kind, reverse: true, bounds));
        var comparison = new CrdtLoopbackHistoryComparison<int>(
            expected,
            clientAForward,
            clientAReverse,
            clientBForward,
            clientBReverse);
        return comparison.GetActual(EqualityComparer<int>.Default);
    }

    /// <summary>Evaluates both received histories for a string-projected CRDT.</summary>
    /// <param name="clientAStream">The client A received stream.</param>
    /// <param name="clientBStream">The client B received stream.</param>
    /// <param name="kind">The CRDT kind.</param>
    /// <param name="selector">The state value selector.</param>
    /// <param name="expected">The expected string value.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The expected string value when both histories commute; otherwise a failing diagnostic.</returns>
    internal static string EvaluateString(
        CrdtLoopbackReceivedStream clientAStream,
        CrdtLoopbackReceivedStream clientBStream,
        CrdtKind kind,
        Func<CrdtState, string> selector,
        string expected,
        CrdtBounds bounds)
    {
        var clientAForward = selector(MergeHistory(clientAStream.States, kind, reverse: false, bounds));
        var clientAReverse = selector(MergeHistory(clientAStream.States, kind, reverse: true, bounds));
        var clientBForward = selector(MergeHistory(clientBStream.States, kind, reverse: false, bounds));
        var clientBReverse = selector(MergeHistory(clientBStream.States, kind, reverse: true, bounds));
        var comparison = new CrdtLoopbackHistoryComparison<string>(
            expected,
            clientAForward,
            clientAReverse,
            clientBForward,
            clientBReverse);
        return comparison.GetActual(StringComparer.Ordinal);
    }

    /// <summary>Merges a received history in one selected order.</summary>
    /// <param name="states">The received authoritative states.</param>
    /// <param name="kind">The CRDT kind.</param>
    /// <param name="reverse">Whether to apply states in reverse order.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The merged CRDT state.</returns>
    private static CrdtState MergeHistory(
        IReadOnlyList<CrdtState> states,
        CrdtKind kind,
        bool reverse,
        CrdtBounds bounds)
    {
        var state = CrdtFunctions.Empty(kind);
        if (reverse)
        {
            for (var index = states.Count - 1; index >= 0; index--)
            {
                state = CrdtFunctions.Merge(state, states[index], bounds);
            }

            return state;
        }

        for (var index = 0; index < states.Count; index++)
        {
            state = CrdtFunctions.Merge(state, states[index], bounds);
        }

        return state;
    }

    /// <summary>Gets a counter value from a received authoritative state.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <returns>The counter value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetCounterValue(CrdtState state) =>
        checked((int)state.Value.Counter);
}
