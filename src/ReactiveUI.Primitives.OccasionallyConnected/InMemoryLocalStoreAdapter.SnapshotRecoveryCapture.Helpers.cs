// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains snapshot recovery capture helpers.</summary>
internal sealed partial class InMemoryLocalStoreAdapter
{
    /// <summary>Validates capture request shape.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The request is malformed.</exception>
    private static void ValidateSnapshotRecoveryCaptureRequest(LocalSnapshotRecoveryCaptureRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        InMemoryLocalStoreAdapterValidation.ValidateRecoveryInput(request.StreamId, request.SubscriptionId);
        ArgumentExceptionHelper.ThrowIfNull(request.Limits);
        request.Limits.Validate();
    }

    /// <summary>Counts capture header and snapshot logical bytes before operation admission.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="serverCursor">The server cursor.</param>
    /// <param name="snapshot">The snapshot.</param>
    /// <param name="limits">The capture limits.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private static long GetSnapshotRecoveryCaptureHeaderLogicalBytes(
        StreamId streamId,
        string? serverCursor,
        LocalSnapshot? snapshot,
        SnapshotRecoveryLimits limits)
    {
        long bytes = GetRequiredSnapshotRecoveryUtf8Bytes(streamId.Value)
            + GuidEncodedBytes
            + GetOptionalSnapshotRecoveryUtf8Bytes(serverCursor)
            + Int64EncodedBytes
            + Int32EncodedBytes
            + Int32EncodedBytes;
        if (snapshot is not null)
        {
            bytes = checked(bytes + GetSnapshotRecoverySnapshotLogicalBytes(snapshot, limits));
        }

        ThrowIfSnapshotRecoveryCapacityExceeded(bytes, limits.MaximumLogicalBytes, nameof(SnapshotRecoveryLimits.MaximumLogicalBytes));
        return bytes;
    }

    /// <summary>Counts logical bytes for one snapshot.</summary>
    /// <param name="snapshot">The snapshot.</param>
    /// <param name="limits">The capture limits.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private static long GetSnapshotRecoverySnapshotLogicalBytes(LocalSnapshot snapshot, SnapshotRecoveryLimits limits) =>
        GetRequiredSnapshotRecoveryUtf8Bytes(snapshot.StreamId.Value)
        + Int32EncodedBytes
        + GetOptionalSnapshotRecoveryUtf8Bytes(snapshot.ServerCursor)
        + GetSnapshotRecoveryPayloadLogicalBytes(snapshot.State, limits)
        + Int64EncodedBytes
        + DateTimeOffsetEncodedBytes
        + (snapshot.AuthoritativeState is null ? 0 : GetSnapshotRecoveryPayloadLogicalBytes(snapshot.AuthoritativeState, limits));

    /// <summary>Counts logical bytes for one operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="limits">The capture limits.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private static long GetSnapshotRecoveryOperationLogicalBytes(SyncOperation operation, SnapshotRecoveryLimits limits) =>
        GuidEncodedBytes
        + GetRequiredSnapshotRecoveryUtf8Bytes(operation.StreamId.Value)
        + Int64EncodedBytes
        + DateTimeOffsetEncodedBytes
        + Int32EncodedBytes
        + GetSnapshotRecoveryPayloadLogicalBytes(operation.Payload, limits)
        + GetOptionalSnapshotRecoveryUtf8Bytes(operation.BaseVersion)
        + GetSnapshotRecoveryMetadataLogicalBytes(operation.Metadata, limits)
        + Int32EncodedBytes
        + Int32EncodedBytes
        + Int32EncodedBytes
        + Int32EncodedBytes;

    /// <summary>Counts logical bytes for one payload after checking payload capacity.</summary>
    /// <param name="payload">The payload.</param>
    /// <param name="limits">The capture limits.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private static long GetSnapshotRecoveryPayloadLogicalBytes(PayloadEnvelope payload, SnapshotRecoveryLimits limits)
    {
        ThrowIfSnapshotRecoveryCapacityExceeded(
            payload.PayloadLength,
            limits.MaximumPayloadBytes,
            nameof(SnapshotRecoveryLimits.MaximumPayloadBytes));
        return Int32EncodedBytes
            + Int64EncodedBytes
            + GetRequiredSnapshotRecoveryUtf8Bytes(payload.ContractId)
            + GetRequiredSnapshotRecoveryUtf8Bytes(payload.ContentType)
            + GetRequiredSnapshotRecoveryUtf8Bytes(payload.PayloadHash)
            + payload.PayloadLength;
    }

    /// <summary>Counts logical bytes for metadata after checking metadata capacity.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <param name="limits">The capture limits.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The capture exceeds a configured limit.</exception>
    private static long GetSnapshotRecoveryMetadataLogicalBytes(IReadOnlyDictionary<string, string> metadata, SnapshotRecoveryLimits limits)
    {
        ThrowIfSnapshotRecoveryCapacityExceeded(
            metadata.Count,
            limits.MaximumMetadataEntries,
            nameof(SnapshotRecoveryLimits.MaximumMetadataEntries));
        var bytes = (long)Int32EncodedBytes;
        foreach (var pair in metadata)
        {
            bytes = checked(bytes + GetRequiredSnapshotRecoveryUtf8Bytes(pair.Key));
            bytes = checked(bytes + GetRequiredSnapshotRecoveryUtf8Bytes(pair.Value));
        }

        ThrowIfSnapshotRecoveryCapacityExceeded(
            bytes,
            limits.MaximumMetadataBytes,
            nameof(SnapshotRecoveryLimits.MaximumMetadataBytes));
        return bytes;
    }

    /// <summary>Gets UTF-8 byte count for an optional string.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The UTF-8 byte count.</returns>
    private static int GetOptionalSnapshotRecoveryUtf8Bytes(string? value) =>
        value is null ? 0 : GetRequiredSnapshotRecoveryUtf8Bytes(value);

    /// <summary>Gets UTF-8 byte count for a required string.</summary>
    /// <param name="value">The string.</param>
    /// <returns>The UTF-8 byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetRequiredSnapshotRecoveryUtf8Bytes(string value) => Encoding.UTF8.GetByteCount(value);

    /// <summary>Throws when a capacity is exceeded.</summary>
    /// <param name="observed">The observed value.</param>
    /// <param name="maximum">The configured maximum.</param>
    /// <param name="limitName">The limit name.</param>
    /// <exception cref="SnapshotRecoveryCapacityExceededException">The observed value exceeds the configured maximum.</exception>
    private static void ThrowIfSnapshotRecoveryCapacityExceeded(long observed, long maximum, string limitName)
    {
        if (observed <= maximum)
        {
            return;
        }

        throw new SnapshotRecoveryCapacityExceededException(limitName, maximum, observed);
    }
}
