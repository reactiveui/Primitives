// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Evaluates whether independently received CRDT values share one authoritative frontier.</summary>
internal static class CrdtLoopbackConvergenceEvaluator
{
    /// <summary>The diagnostic prefix expected when converged values do not share a frontier.</summary>
    internal const string FrontierMismatchDiagnostic = "frontier-mismatch";

    /// <summary>Evaluates an integer convergence actual value using the current scenario behavior.</summary>
    /// <param name="clientA">The client A received value and cursor.</param>
    /// <param name="clientB">The client B received value and cursor.</param>
    /// <param name="expected">The expected value.</param>
    /// <returns>The actual value to publish in a case result.</returns>
    internal static int EvaluateCounter(
        CrdtLoopbackConvergenceParticipant<int> clientA,
        CrdtLoopbackConvergenceParticipant<int> clientB,
        int expected) =>
        clientA.Value == expected
            && clientB.Value == expected
            && CursorsMatch(clientA, clientB)
            ? expected
            : int.MinValue;

    /// <summary>Evaluates a string convergence actual value.</summary>
    /// <param name="clientA">The client A received value and cursor.</param>
    /// <param name="clientB">The client B received value and cursor.</param>
    /// <param name="expected">The expected value.</param>
    /// <returns>The actual value to publish in a case result.</returns>
    internal static string EvaluateString(
        CrdtLoopbackConvergenceParticipant<string> clientA,
        CrdtLoopbackConvergenceParticipant<string> clientB,
        string expected)
    {
        var clientAMatches = string.Equals(clientA.Value, expected, StringComparison.Ordinal);
        var clientBMatches = string.Equals(clientB.Value, expected, StringComparison.Ordinal);
        if (clientAMatches && clientBMatches && CursorsMatch(clientA, clientB))
        {
            return expected;
        }

        return clientAMatches && clientBMatches
            ? FrontierMismatchDiagnostic
            : GetMismatchedStringValue(clientA, clientB, clientAMatches);
    }

    /// <summary>Gets the shared-frontier actual value for two participants.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="clientA">The client A received value and cursor.</param>
    /// <param name="clientB">The client B received value and cursor.</param>
    /// <param name="same">The matching-frontier value.</param>
    /// <param name="different">The divergent-frontier value.</param>
    /// <returns>The frontier comparison output.</returns>
    internal static string EvaluateFrontier<T>(
        CrdtLoopbackConvergenceParticipant<T> clientA,
        CrdtLoopbackConvergenceParticipant<T> clientB,
        string same,
        string different) =>
        CursorsMatch(clientA, clientB) ? same : different;

    /// <summary>Gets whether two participants share the same authoritative cursor.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="clientA">The client A received value and cursor.</param>
    /// <param name="clientB">The client B received value and cursor.</param>
    /// <returns>Whether cursors match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CursorsMatch<T>(
        CrdtLoopbackConvergenceParticipant<T> clientA,
        CrdtLoopbackConvergenceParticipant<T> clientB) =>
        string.Equals(clientA.Cursor, clientB.Cursor, StringComparison.Ordinal);

    /// <summary>Gets the non-matching string value to publish in a failed case result.</summary>
    /// <param name="clientA">The client A received value and cursor.</param>
    /// <param name="clientB">The client B received value and cursor.</param>
    /// <param name="clientAMatches">Whether client A matched the expected value.</param>
    /// <returns>The string value that did not match the expectation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetMismatchedStringValue(
        CrdtLoopbackConvergenceParticipant<string> clientA,
        CrdtLoopbackConvergenceParticipant<string> clientB,
        bool clientAMatches) =>
        clientAMatches ? clientB.Value : clientA.Value;
}
