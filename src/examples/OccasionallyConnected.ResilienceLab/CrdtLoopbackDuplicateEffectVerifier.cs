// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Verifies that replaying a duplicate operation did not append another effect group.</summary>
internal static class CrdtLoopbackDuplicateEffectVerifier
{
    /// <summary>Gets the duplicate effect delta result.</summary>
    /// <param name="beforeDuplicate">The stream page before replaying the duplicate.</param>
    /// <param name="afterDuplicate">The stream page after replaying the duplicate.</param>
    /// <param name="expectedDelta">The expected event and completion delta.</param>
    /// <returns>The expected delta when the duplicate produced no additional effect; otherwise a failing sentinel.</returns>
    internal static int GetEffectDelta(
        CrdtLoopbackReceivedStream beforeDuplicate,
        CrdtLoopbackReceivedStream afterDuplicate,
        int expectedDelta)
    {
        var eventDelta = afterDuplicate.EventCount - beforeDuplicate.EventCount;
        var completionDelta = afterDuplicate.CompletedOperationCount - beforeDuplicate.CompletedOperationCount;
        return eventDelta == expectedDelta
            && completionDelta == expectedDelta
            && FrontiersMatch(beforeDuplicate, afterDuplicate)
            ? expectedDelta
            : int.MinValue;
    }

    /// <summary>Gets whether two received streams share a cursor frontier.</summary>
    /// <param name="clientAStream">The client A received stream.</param>
    /// <param name="clientBStream">The client B received stream.</param>
    /// <returns>Whether both frontier cursors match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool FrontiersMatch(CrdtLoopbackReceivedStream clientAStream, CrdtLoopbackReceivedStream clientBStream) =>
        string.Equals(clientAStream.Cursor, clientBStream.Cursor, StringComparison.Ordinal);
}
