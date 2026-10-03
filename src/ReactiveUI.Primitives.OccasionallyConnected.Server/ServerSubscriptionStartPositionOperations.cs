// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Resolves durable subscription start positions against retained receive history.</summary>
internal static class ServerSubscriptionStartPositionOperations
{
    /// <summary>Captures the registration-time anchor for a start position.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="startPosition">The requested start position.</param>
    /// <param name="stream">The retained stream state.</param>
    /// <returns>The captured anchor.</returns>
    internal static ServerSubscriptionInitialAnchor CaptureInitialAnchor(
        ServerStreamKey streamKey,
        StartPosition startPosition,
        ServerCommitStreamRecord? stream)
    {
        if (startPosition.Kind == StartPositionKind.Latest)
        {
            return CaptureLatestAnchor(streamKey, stream);
        }

        return TryResolveAnchor(streamKey, startPosition, stream, out var anchor) == ServerSubscriptionAnchorResolution.Resolved
            ? anchor
            : new(null, 0, false);
    }

    /// <summary>Resolves a deferred initial anchor during first receive-page selection.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="startPosition">The requested start position.</param>
    /// <param name="stream">The retained stream state.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <returns>The resolution outcome.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The position kind is invalid.</exception>
    /// <exception cref="ArgumentException">The cursor does not identify a complete group.</exception>
    internal static ServerSubscriptionAnchorResolution TryResolveAnchor(
        ServerStreamKey streamKey,
        StartPosition startPosition,
        ServerCommitStreamRecord? stream,
        out ServerSubscriptionInitialAnchor anchor)
    {
        anchor = default;
        if (startPosition.Kind == StartPositionKind.Latest)
        {
            return ResolveLatestAnchor(streamKey, stream, out anchor);
        }

        if (startPosition.Kind == StartPositionKind.FromSequence)
        {
            return ResolveSequenceAnchor(streamKey, startPosition.Sequence.GetValueOrDefault(), stream, out anchor);
        }

        return startPosition.Kind == StartPositionKind.FromTimestamp
            ? ResolveTimestampAnchor(streamKey, startPosition.Timestamp.GetValueOrDefault(), stream, out anchor)
            : ResolveCursorAnchor(streamKey, startPosition.Cursor.AsSpan().ToString(), stream, out anchor);
    }

    /// <summary>Returns the effective cursor for a first read from a resolved initial anchor.</summary>
    /// <param name="record">The subscription record.</param>
    /// <returns>The cursor to pass into receive-page selection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string? GetInitialReadCursor(ServerSubscriptionRecord record) =>
        record.InitialAnchorGroupSequence == 0 ? null : record.InitialAnchorCursor;

    /// <summary>Returns a copy of the result with the caller's original previous cursor.</summary>
    /// <param name="result">The receive page result.</param>
    /// <param name="previousCursor">The caller-supplied previous cursor.</param>
    /// <returns>The adjusted result.</returns>
    internal static ServerReceivePageResult WithClientPreviousCursor(ServerReceivePageResult result, string? previousCursor)
    {
        if (result.Batch is null || string.Equals(result.Batch.PreviousCursor, previousCursor, StringComparison.Ordinal))
        {
            return result;
        }

        var batch = new RemoteEventBatch(
            result.Batch.BatchId,
            result.Batch.StreamId,
            previousCursor,
            result.Batch.NextCursor,
            result.Batch.Events) { CompletedOperations = result.Batch.CompletedOperations };
        return result with { Batch = batch };
    }

    /// <summary>Creates a resolved latest anchor at the current complete-group frontier.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="stream">The retained stream state.</param>
    /// <returns>The resolved anchor.</returns>
    private static ServerSubscriptionInitialAnchor CaptureLatestAnchor(ServerStreamKey streamKey, ServerCommitStreamRecord? stream)
    {
        if (stream is null || stream.LastGroupSequence == 0)
        {
            return new(null, 0, true);
        }

        return stream.HasReceiveHistoryGap
            ? new(null, 0, false)
            : new(ServerReceiveGroupCursor.Create(streamKey, stream.LastGroupSequence), stream.LastGroupSequence, true);
    }

    /// <summary>Resolves Latest at offer time when registration found a legacy gap.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="stream">The retained stream state.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <returns>The resolution outcome.</returns>
    private static ServerSubscriptionAnchorResolution ResolveLatestAnchor(
        ServerStreamKey streamKey,
        ServerCommitStreamRecord? stream,
        out ServerSubscriptionInitialAnchor anchor)
    {
        anchor = CaptureLatestAnchor(streamKey, stream);
        return anchor.IsResolved ? ServerSubscriptionAnchorResolution.Resolved : ServerSubscriptionAnchorResolution.RetentionGap;
    }

    /// <summary>Resolves an event-sequence threshold to the complete group that contains it.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="sequence">The requested server event sequence.</param>
    /// <param name="stream">The retained stream state.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <returns>The resolution outcome.</returns>
    private static ServerSubscriptionAnchorResolution ResolveSequenceAnchor(
        ServerStreamKey streamKey,
        long sequence,
        ServerCommitStreamRecord? stream,
        out ServerSubscriptionInitialAnchor anchor)
    {
        anchor = default;
        if (sequence == 0)
        {
            anchor = new(null, 0, true);
            return ServerSubscriptionAnchorResolution.Resolved;
        }

        if (stream is null || sequence > stream.LastEventSequence)
        {
            return ServerSubscriptionAnchorResolution.Pending;
        }

        for (var index = 0; index < stream.Events.Count; index++)
        {
            var row = stream.Events[index];
            if (row.Sequence < sequence)
            {
                continue;
            }

            anchor = CreateBeforeGroupAnchor(streamKey, GetGroupSequence(row.Ledger));
            return row.Sequence == sequence || !stream.HasReceiveHistoryGap
                ? ServerSubscriptionAnchorResolution.Resolved
                : ServerSubscriptionAnchorResolution.RetentionGap;
        }

        return ServerSubscriptionAnchorResolution.RetentionGap;
    }

    /// <summary>Resolves a timestamp threshold to the first inclusive complete group.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="timestamp">The requested timestamp.</param>
    /// <param name="stream">The retained stream state.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <returns>The resolution outcome.</returns>
    private static ServerSubscriptionAnchorResolution ResolveTimestampAnchor(
        ServerStreamKey streamKey,
        DateTimeOffset timestamp,
        ServerCommitStreamRecord? stream,
        out ServerSubscriptionInitialAnchor anchor)
    {
        anchor = default;
        if (stream is null || stream.Groups.Count == 0)
        {
            return ServerSubscriptionAnchorResolution.Pending;
        }

        for (var index = 0; index < stream.Groups.Count; index++)
        {
            var group = stream.Groups[index];
            if (group.Entry.CommittedAtUtc < timestamp)
            {
                continue;
            }

            var groupSequence = GetGroupSequence(group);
            if (!HasProvenTimestampBoundary(stream, index, groupSequence))
            {
                return ServerSubscriptionAnchorResolution.RetentionGap;
            }

            anchor = CreateBeforeGroupAnchor(streamKey, groupSequence);
            return ServerSubscriptionAnchorResolution.Resolved;
        }

        return ServerSubscriptionAnchorResolution.Pending;
    }

    /// <summary>Returns whether retained history proves the earliest inclusive timestamp group.</summary>
    /// <param name="stream">The retained stream state.</param>
    /// <param name="index">The matching retained group index.</param>
    /// <param name="groupSequence">The matching durable group sequence.</param>
    /// <returns><see langword="true" /> when the matching group can be used as the timestamp anchor.</returns>
    private static bool HasProvenTimestampBoundary(ServerCommitStreamRecord stream, int index, long groupSequence)
    {
        if (!stream.HasReceiveHistoryGap)
        {
            return true;
        }

        if (index == 0)
        {
            return false;
        }

        return GetGroupSequence(stream.Groups[index - 1]) == groupSequence - 1;
    }

    /// <summary>Resolves an existing cursor to an initial anchor.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="cursor">The requested cursor.</param>
    /// <param name="stream">The retained stream state.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <returns>The resolution outcome.</returns>
    /// <exception cref="ArgumentException">The cursor does not identify a complete group.</exception>
    private static ServerSubscriptionAnchorResolution ResolveCursorAnchor(
        ServerStreamKey streamKey,
        string cursor,
        ServerCommitStreamRecord? stream,
        out ServerSubscriptionInitialAnchor anchor)
    {
        if (ServerReceiveGroupCursor.IsGroupCursor(cursor))
        {
            var groupSequence = ServerReceiveGroupCursor.Parse(streamKey, cursor);
            anchor = new(groupSequence == 0 ? null : cursor, groupSequence, true);
            return ServerSubscriptionAnchorResolution.Resolved;
        }

        if (stream is null)
        {
            anchor = default;
            return ServerSubscriptionAnchorResolution.RetentionGap;
        }

        for (var index = 0; index < stream.Groups.Count; index++)
        {
            var group = stream.Groups[index];
            var events = group.Entry.Events;
            if (events.Count == 0)
            {
                continue;
            }

            if (string.Equals(events[events.Count - 1].ServerCursor, cursor, StringComparison.Ordinal))
            {
                anchor = new(cursor, GetGroupSequence(group), true);
                return ServerSubscriptionAnchorResolution.Resolved;
            }

            if (ContainsNonFinalCursor(events, cursor))
            {
                throw new ArgumentException("The receive cursor does not identify a complete operation group.", nameof(cursor));
            }
        }

        anchor = default;
        return ServerSubscriptionAnchorResolution.RetentionGap;
    }

    /// <summary>Creates an anchor immediately before a selected group.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="groupSequence">The selected group sequence.</param>
    /// <returns>The anchor.</returns>
    private static ServerSubscriptionInitialAnchor CreateBeforeGroupAnchor(ServerStreamKey streamKey, long groupSequence)
    {
        var previousGroupSequence = checked(groupSequence - 1);
        return previousGroupSequence == 0
            ? new(null, 0, true)
            : new(ServerReceiveGroupCursor.Create(streamKey, previousGroupSequence), previousGroupSequence, true);
    }

    /// <summary>Checks whether a cursor belongs to a non-final event in a group.</summary>
    /// <param name="events">The retained events.</param>
    /// <param name="cursor">The cursor.</param>
    /// <returns>Whether the cursor is non-final.</returns>
    private static bool ContainsNonFinalCursor(IReadOnlyList<RemoteEvent> events, string cursor)
    {
        for (var index = 0; index < events.Count - 1; index++)
        {
            if (string.Equals(events[index].ServerCursor, cursor, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Gets a non-null group sequence.</summary>
    /// <param name="row">The row.</param>
    /// <returns>The group sequence.</returns>
    /// <exception cref="InvalidOperationException">The row is not a receive group.</exception>
    private static long GetGroupSequence(ServerCommitLedgerRow row) =>
        row.GroupSequence ?? throw new InvalidOperationException("The receive group sequence is missing.");
}
