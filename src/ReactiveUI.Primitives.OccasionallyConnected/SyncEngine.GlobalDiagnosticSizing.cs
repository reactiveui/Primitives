// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Charges retained global engine diagnostic values.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>The minimum retained byte charge for an engine diagnostic value.</summary>
    private const long MinimumDiagnosticNotificationBytes = 256;

    /// <summary>The nominal object overhead charged for diagnostic values and text.</summary>
    private const long DiagnosticObjectOverheadBytes = 32;

    /// <summary>A fault retains its envelope and a sanitized exception.</summary>
    private const int DiagnosticFaultObjectCount = 2;

    /// <summary>Charges a global synchronization state retained by an observer.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The estimated retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetGlobalSyncStateSize(SyncState state) =>
        Math.Max(MinimumDiagnosticNotificationBytes, DiagnosticObjectOverheadBytes + GetDiagnosticTextSize(state.ReasonCode));

    /// <summary>Charges an operation state retained by an observer.</summary>
    /// <param name="status">The status.</param>
    /// <returns>The estimated retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetGlobalOperationStateSize(SyncOperationStatus status) =>
        Math.Max(
            MinimumDiagnosticNotificationBytes,
            DiagnosticObjectOverheadBytes + GetDiagnosticTextSize(status.StreamId.Value) + GetDiagnosticTextSize(status.ReasonCode));

    /// <summary>Charges a sanitized fault retained by an observer.</summary>
    /// <param name="fault">The fault.</param>
    /// <returns>The estimated retained bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetGlobalFaultSize(OccasionallyConnectedFault fault) =>
        Math.Max(
            MinimumDiagnosticNotificationBytes,
            (DiagnosticObjectOverheadBytes * DiagnosticFaultObjectCount)
            + GetDiagnosticTextSize(fault.Code)
            + GetDiagnosticTextSize(fault.Message)
            + GetDiagnosticTextSize(fault.StreamId?.Value)
            + GetDiagnosticTextSize(fault.Exception?.Message));

    /// <summary>Charges optional UTF-16 text retained in a diagnostic notification.</summary>
    /// <param name="value">The text.</param>
    /// <returns>The estimated retained bytes.</returns>
    private static long GetDiagnosticTextSize(string? value) =>
        value is null ? 0 : DiagnosticObjectOverheadBytes + ((long)value.Length * sizeof(char));
}
