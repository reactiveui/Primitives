// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Computes logical encoded retention sizes for the server commit journal.</summary>
internal static class ServerCommitJournalSizer
{
    /// <summary>The logical accounting bytes for a retained stream record shell.</summary>
    private const int StreamRecordBytes = 24;

    /// <summary>The logical accounting bytes for a retained terminal entry shell.</summary>
    private const int LedgerRecordBytes = 64;

    /// <summary>The logical accounting bytes for a retained event sidecar row.</summary>
    private const int EventRecordBytes = 40;

    /// <summary>The logical accounting bytes for fixed write-stamp fields.</summary>
    private const int WriteStampRecordBytes = 24;

    /// <summary>The logical accounting bytes for an operation result shell.</summary>
    private const long OperationResultBytes = 16L;

    /// <summary>Computes the retained state byte delta for a commit.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The retained logical byte delta.</returns>
    internal static long GetStateDelta(ServerCommitStreamRecord stream, ServerCommitValidationResult commit)
    {
        if (commit.NewState is null)
        {
            return GetAppendOnlyStampDelta(stream, commit);
        }

        var stateDelta = commit.StateBytes - stream.StateBytes;
        var stampDelta = GetWriteStampBytes(commit.NewWriteStamp) - GetWriteStampBytes(stream.LastWriteStamp);
        return AddLogicalBytes(stateDelta, stampDelta);
    }

    /// <summary>Computes retained stream key bytes.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <returns>The logical byte count.</returns>
    internal static long GetStreamKeyBytes(ServerStreamKey streamKey) =>
        StreamRecordBytes
        + ServerCommitJournalGuard.GetTextBytes(streamKey.TenantId)
        + ServerCommitJournalGuard.GetTextBytes(streamKey.StreamId.Value);

    /// <summary>Computes retained state bytes.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The logical byte count.</returns>
    internal static long GetStateBytes(ServerState state) =>
        ServerCommitJournalGuard.GetTextBytes(state.Version) + GetPayloadBytes(state.State);

    /// <summary>Computes retained terminal entry bytes.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns>The logical byte count.</returns>
    internal static long GetEntryBytes(ServerLedgerEntry entry)
    {
        var bytes = LedgerRecordBytes
            + ServerCommitJournalGuard.GetTextBytes(entry.OperationKey.ClientId)
            + ServerCommitFingerprint.Length
            + GetResultBytes(entry.Result);
        for (var index = 0; index < entry.Conflicts.Count; index++)
        {
            bytes = AddLogicalBytes(bytes, GetConflictBytes(entry.Conflicts[index]));
        }

        for (var index = 0; index < entry.Events.Count; index++)
        {
            bytes = AddLogicalBytes(bytes, GetEventBytes(entry.Events[index]));
        }

        return bytes;
    }

    /// <summary>Computes retained payload envelope bytes.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetPayloadBytes(PayloadEnvelope payload) =>
        GetPayloadBytes(
            ServerCommitJournalGuard.GetTextBytes(payload.ContractId),
            ServerCommitJournalGuard.GetTextBytes(payload.ContentType),
            ServerCommitJournalGuard.GetTextBytes(payload.PayloadHash),
            payload.PayloadLength);

    /// <summary>Computes retained payload envelope bytes from validated component sizes.</summary>
    /// <param name="contractBytes">The contract identifier bytes.</param>
    /// <param name="contentTypeBytes">The content type bytes.</param>
    /// <param name="payloadHashBytes">The payload hash bytes.</param>
    /// <param name="payloadLength">The payload bytes.</param>
    /// <returns>The logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetPayloadBytes(int contractBytes, int contentTypeBytes, int payloadHashBytes, int payloadLength) =>
        (long)contractBytes + contentTypeBytes + payloadHashBytes + payloadLength;

    /// <summary>Adds logical byte counts with overflow protection.</summary>
    /// <param name="current">The current count.</param>
    /// <param name="delta">The delta.</param>
    /// <returns>The new count.</returns>
    /// <exception cref="InvalidOperationException">The logical byte count overflowed.</exception>
    internal static long AddLogicalBytes(long current, long delta)
    {
        try
        {
            return checked(current + delta);
        }
        catch (OverflowException exception)
        {
            throw new InvalidOperationException("Server journal logical byte accounting overflowed.", exception);
        }
    }

    /// <summary>Computes the append-only write-stamp byte delta.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The logical byte delta.</returns>
    private static long GetAppendOnlyStampDelta(ServerCommitStreamRecord stream, ServerCommitValidationResult commit) =>
        commit.NewWriteStamp.HasValue ? GetWriteStampBytes(commit.NewWriteStamp) - GetWriteStampBytes(stream.LastWriteStamp) : 0;

    /// <summary>Computes retained write-stamp bytes.</summary>
    /// <param name="writeStamp">The optional write stamp.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetWriteStampBytes(ServerWriteStamp? writeStamp) =>
        writeStamp.HasValue ? WriteStampRecordBytes + ServerCommitJournalGuard.GetTextBytes(writeStamp.Value.ClientId) : 0;

    /// <summary>Computes retained operation result bytes.</summary>
    /// <param name="result">The operation result.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetResultBytes(OperationSyncResult result)
    {
        var bytes = OperationResultBytes;
        if (result.ReasonCode is not null)
        {
            bytes = AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(result.ReasonCode));
        }

        if (result.ServerVersion is not null)
        {
            bytes = AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(result.ServerVersion));
        }

        return bytes;
    }

    /// <summary>Computes retained conflict bytes.</summary>
    /// <param name="conflict">The conflict.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetConflictBytes(ResolvedConflict conflict)
    {
        var bytes = OperationResultBytes + ServerCommitJournalGuard.GetTextBytes(conflict.ResolutionCode);
        if (conflict.ResolvedPayload is not null)
        {
            bytes = AddLogicalBytes(bytes, GetPayloadBytes(conflict.ResolvedPayload));
        }

        return bytes;
    }

    /// <summary>Computes retained event bytes.</summary>
    /// <param name="remoteEvent">The event.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetEventBytes(RemoteEvent remoteEvent)
    {
        var bytes = EventRecordBytes
            + ServerCommitJournalGuard.GetTextBytes(remoteEvent.ServerCursor)
            + GetPayloadBytes(remoteEvent.Payload);
        foreach (var metadata in remoteEvent.Metadata)
        {
            bytes = AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(metadata.Key));
            bytes = AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(metadata.Value));
        }

        return bytes;
    }
}
