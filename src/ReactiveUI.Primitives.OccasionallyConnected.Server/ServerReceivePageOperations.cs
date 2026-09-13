// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Builds bounded server receive pages from retained complete operation groups.</summary>
internal static class ServerReceivePageOperations
{
    /// <summary>The logical batch shell byte count.</summary>
    private const long GuidByteCount = 16;

    /// <summary>The logical integer byte count.</summary>
    private const long IntByteCount = 4;

    /// <summary>The logical timestamp byte count.</summary>
    private const long DateTimeOffsetByteCount = 16;

    /// <summary>The logical nullable marker byte count.</summary>
    private const long NullableMarkerByteCount = 1;

    /// <summary>Builds a receive page for a stream record.</summary>
    /// <param name="request">The page request.</param>
    /// <param name="stream">The retained stream, if any.</param>
    /// <returns>The page result.</returns>
    /// <exception cref="ArgumentException">The request is malformed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A request bound is invalid.</exception>
    /// <exception cref="InvalidOperationException">The cursor is ahead of the retained stream.</exception>
    /// <exception cref="QueueCapacityExceededException">The next group cannot fit the requested page bounds.</exception>
    internal static ServerReceivePageResult Create(ServerReceivePageRequest request, ServerCommitStreamRecord? stream)
    {
        ValidateRequest(request);
        if (!TryResolveRequestedSequence(request, stream, out var requestedSequence))
        {
            return new(ServerReceivePageStatus.RetentionGap, null, 0, stream?.LastGroupSequence ?? 0);
        }

        return stream is null
            ? CreateMissingStreamResult(requestedSequence)
            : CreateStreamResult(request, stream, requestedSequence);
    }

    /// <summary>Builds a result when the requested stream has no retained row.</summary>
    /// <param name="requestedSequence">The requested group sequence.</param>
    /// <returns>The receive page result.</returns>
    /// <exception cref="InvalidOperationException">The cursor is ahead of the retained stream.</exception>
    private static ServerReceivePageResult CreateMissingStreamResult(long requestedSequence) =>
        requestedSequence == 0
            ? new(ServerReceivePageStatus.EndOfStream, null, 0, 0)
            : throw new InvalidOperationException("The receive group cursor is ahead of the retained stream.");

    /// <summary>Builds a result for a retained stream row.</summary>
    /// <param name="request">The page request.</param>
    /// <param name="stream">The retained stream.</param>
    /// <param name="requestedSequence">The requested group sequence.</param>
    /// <returns>The receive page result.</returns>
    /// <exception cref="InvalidOperationException">The cursor is ahead of the retained stream.</exception>
    /// <exception cref="QueueCapacityExceededException">The next group cannot fit the requested page bounds.</exception>
    private static ServerReceivePageResult CreateStreamResult(
        ServerReceivePageRequest request,
        ServerCommitStreamRecord stream,
        long requestedSequence)
    {
        if (requestedSequence > stream.LastGroupSequence)
        {
            throw new InvalidOperationException("The receive group cursor is ahead of the retained stream.");
        }

        if (stream.HasReceiveHistoryGap && requestedSequence == 0)
        {
            return new(ServerReceivePageStatus.RetentionGap, null, requestedSequence, stream.LastGroupSequence);
        }

        if (requestedSequence == stream.LastGroupSequence)
        {
            return new(ServerReceivePageStatus.EndOfStream, null, requestedSequence, stream.LastGroupSequence);
        }

        var firstIndex = FindFirstGroupIndex(stream, requestedSequence);
        if (firstIndex < 0)
        {
            return new(ServerReceivePageStatus.RetentionGap, null, requestedSequence, stream.LastGroupSequence);
        }

        var firstGroupSequence = GetGroupSequence(stream.Groups[firstIndex]);
        return firstGroupSequence == requestedSequence + 1
            ? BuildPage(request, stream, firstIndex)
            : new(ServerReceivePageStatus.RetentionGap, null, requestedSequence, stream.LastGroupSequence);
    }

    /// <summary>Validates receive page bounds.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The request is malformed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A request bound is invalid.</exception>
    private static void ValidateRequest(ServerReceivePageRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ServerCommitJournalGuard.ValidateStreamKey(request.StreamKey);
        if (request.Cursor is not null)
        {
            ServerCommitJournalGuard.ValidateCursor(request.Cursor);
        }

        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(request.MaximumGroups);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(request.MaximumEvents);
        ThrowIfNonPositiveLogicalBytes(request.MaximumLogicalBytes);
    }

    /// <summary>Rejects a non-positive logical byte budget.</summary>
    /// <param name="maximumLogicalBytes">The logical byte budget.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ThrowIfNonPositiveLogicalBytes(long maximumLogicalBytes) =>
        _ = maximumLogicalBytes > 0 ? true : throw new ArgumentOutOfRangeException(nameof(maximumLogicalBytes), maximumLogicalBytes, null);

    /// <summary>Finds the first retained group after a requested sequence.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="requestedSequence">The requested sequence.</param>
    /// <returns>The group index, or -1.</returns>
    private static int FindFirstGroupIndex(ServerCommitStreamRecord stream, long requestedSequence)
    {
        for (var index = 0; index < stream.Groups.Count; index++)
        {
            if (GetGroupSequence(stream.Groups[index]) > requestedSequence)
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>Selects a contiguous prefix of complete groups that fits the page bounds.</summary>
    /// <param name="request">The page request.</param>
    /// <param name="stream">The stream.</param>
    /// <param name="firstIndex">The first group index.</param>
    /// <returns>The selected complete groups.</returns>
    private static List<ServerCommitLedgerRow> SelectGroups(
        ServerReceivePageRequest request,
        ServerCommitStreamRecord stream,
        int firstIndex)
    {
        var selectedGroups = new List<ServerCommitLedgerRow>();
        var eventCount = 0;
        var lastEventfulIndex = -1;
        for (var index = firstIndex; index < stream.Groups.Count && selectedGroups.Count < request.MaximumGroups; index++)
        {
            var row = stream.Groups[index];
            if (index > firstIndex && GetGroupSequence(row) != checked(GetGroupSequence(stream.Groups[index - 1]) + 1))
            {
                break;
            }

            var projectedEventCount = checked(eventCount + GetIncludedEventCount(row));
            if (projectedEventCount > request.MaximumEvents)
            {
                break;
            }

            selectedGroups.Add(row);
            if (GetIncludedEventCount(row) > 0)
            {
                lastEventfulIndex = selectedGroups.Count - 1;
            }

            var projectedEmittedGroupCount = lastEventfulIndex < 0 ? selectedGroups.Count : lastEventfulIndex + 1;
            if (GetBatchBytes(request, selectedGroups, projectedEmittedGroupCount) > request.MaximumLogicalBytes)
            {
                selectedGroups.RemoveAt(selectedGroups.Count - 1);
                break;
            }

            eventCount = projectedEventCount;
        }

        return selectedGroups;
    }

    /// <summary>Builds a non-empty page from the first retained group.</summary>
    /// <param name="request">The page request.</param>
    /// <param name="stream">The stream.</param>
    /// <param name="firstIndex">The first group index.</param>
    /// <returns>The page result.</returns>
    /// <exception cref="QueueCapacityExceededException">The next group cannot fit the requested page bounds.</exception>
    private static ServerReceivePageResult BuildPage(ServerReceivePageRequest request, ServerCommitStreamRecord stream, int firstIndex)
    {
        var selectedGroups = SelectGroups(request, stream, firstIndex);
        if (selectedGroups.Count == 0)
        {
            throw new QueueCapacityExceededException("The next receive group does not fit within the requested page bounds.", canFitWhenEmpty: false);
        }

        var lastEventfulIndex = FindLastEventfulIndex(selectedGroups);
        var emittedGroupCount = lastEventfulIndex < 0 ? selectedGroups.Count : lastEventfulIndex + 1;
        var events = new List<RemoteEvent>();
        var completions = new List<RemoteOperationCompletion>();
        for (var index = 0; index < emittedGroupCount; index++)
        {
            AppendGroup(selectedGroups[index], events, completions);
        }

        var lastGroup = selectedGroups[emittedGroupCount - 1];
        var lastSequence = GetGroupSequence(lastGroup);
        var nextCursor = GetNextCursor(request.StreamKey, lastGroup);
        var batch = new RemoteEventBatch(Guid.NewGuid(), request.StreamKey.StreamId, request.Cursor, nextCursor, events) { CompletedOperations = completions };
        return new(ServerReceivePageStatus.Page, batch, lastSequence, stream.LastGroupSequence);
    }

    /// <summary>Appends one complete operation group.</summary>
    /// <param name="row">The group row.</param>
    /// <param name="events">The page events.</param>
    /// <param name="completions">The page completions.</param>
    private static void AppendGroup(
        ServerCommitLedgerRow row,
        List<RemoteEvent> events,
        List<RemoteOperationCompletion> completions)
    {
        if (row.Entry.Result.Kind == OperationResultKind.Rejected)
        {
            return;
        }

        var eventIds = new Guid[row.Entry.Events.Count];
        for (var index = 0; index < row.Entry.Events.Count; index++)
        {
            var remoteEvent = row.Entry.Events[index];
            events.Add(remoteEvent);
            eventIds[index] = remoteEvent.EventId;
        }

        completions.Add(new(new(row.Entry.OperationKey.ClientId, row.Entry.OperationKey.OperationId), eventIds));
    }

    /// <summary>Finds the last selected group that includes canonical events.</summary>
    /// <param name="groups">The selected groups.</param>
    /// <returns>The last eventful index, or -1.</returns>
    private static int FindLastEventfulIndex(List<ServerCommitLedgerRow> groups)
    {
        for (var index = groups.Count - 1; index >= 0; index--)
        {
            if (GetIncludedEventCount(groups[index]) > 0)
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>Resolves a receive cursor into a complete operation group sequence.</summary>
    /// <param name="request">The page request.</param>
    /// <param name="stream">The retained stream, if any.</param>
    /// <param name="requestedSequence">The resolved group sequence.</param>
    /// <returns>Whether the cursor could be resolved without a retention gap.</returns>
    private static bool TryResolveRequestedSequence(
        ServerReceivePageRequest request,
        ServerCommitStreamRecord? stream,
        out long requestedSequence)
    {
        if (request.Cursor is null)
        {
            requestedSequence = 0;
            return true;
        }

        if (ServerReceiveGroupCursor.IsGroupCursor(request.Cursor))
        {
            requestedSequence = ServerReceiveGroupCursor.Parse(request.StreamKey, request.Cursor);
            return true;
        }

        return TryResolveFinalEventCursor(stream, request.Cursor, out requestedSequence);
    }

    /// <summary>Resolves a retained final event cursor into its complete operation group sequence.</summary>
    /// <param name="stream">The retained stream, if any.</param>
    /// <param name="cursor">The received cursor.</param>
    /// <param name="requestedSequence">The resolved group sequence.</param>
    /// <returns>Whether the cursor is a retained final event cursor.</returns>
    /// <exception cref="ArgumentException">The cursor points into the middle of a retained group.</exception>
    private static bool TryResolveFinalEventCursor(
        ServerCommitStreamRecord? stream,
        string cursor,
        out long requestedSequence)
    {
        requestedSequence = 0;
        if (stream is null)
        {
            return false;
        }

        for (var index = 0; index < stream.Groups.Count; index++)
        {
            var row = stream.Groups[index];
            var events = row.Entry.Events;
            if (events.Count == 0)
            {
                continue;
            }

            if (string.Equals(events[events.Count - 1].ServerCursor, cursor, StringComparison.Ordinal))
            {
                requestedSequence = GetGroupSequence(row);
                return true;
            }

            if (ContainsNonFinalEventCursor(events, cursor))
            {
                throw new ArgumentException("The receive cursor does not identify a complete operation group.", nameof(cursor));
            }
        }

        return false;
    }

    /// <summary>Checks whether a cursor belongs to a non-final event in a retained group.</summary>
    /// <param name="events">The group events.</param>
    /// <param name="cursor">The received cursor.</param>
    /// <returns>Whether the cursor is retained but not group-complete.</returns>
    private static bool ContainsNonFinalEventCursor(IReadOnlyList<RemoteEvent> events, string cursor)
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

    /// <summary>Gets the compatible next cursor for a completed page.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="row">The final emitted group.</param>
    /// <returns>The final event cursor for eventful pages, otherwise a group cursor.</returns>
    private static string GetNextCursor(ServerStreamKey streamKey, ServerCommitLedgerRow row)
    {
        var events = row.Entry.Events;
        return events.Count == 0
            ? ServerReceiveGroupCursor.Create(streamKey, GetGroupSequence(row))
            : events[events.Count - 1].ServerCursor;
    }

    /// <summary>Computes full logical bytes for an emitted receive batch.</summary>
    /// <param name="request">The request.</param>
    /// <param name="groups">The selected groups.</param>
    /// <param name="emittedGroupCount">The emitted group count.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetBatchBytes(
        ServerReceivePageRequest request,
        List<ServerCommitLedgerRow> groups,
        int emittedGroupCount)
    {
        var bytes = GuidByteCount + IntByteCount + IntByteCount;
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(request.StreamKey.StreamId.Value));
        if (request.Cursor is not null)
        {
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(request.Cursor));
        }

        var lastGroup = groups[emittedGroupCount - 1];
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(GetNextCursor(request.StreamKey, lastGroup)));
        for (var index = 0; index < emittedGroupCount; index++)
        {
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, GetGroupBytes(groups[index]));
        }

        return bytes;
    }

    /// <summary>Computes logical page bytes for one complete operation group.</summary>
    /// <param name="row">The group.</param>
    /// <returns>The byte count.</returns>
    private static long GetGroupBytes(ServerCommitLedgerRow row)
    {
        if (row.Entry.Result.Kind == OperationResultKind.Rejected)
        {
            return 0;
        }

        var bytes = GetOriginBytes(new(row.Entry.OperationKey.ClientId, row.Entry.OperationKey.OperationId));
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, IntByteCount);
        for (var index = 0; index < row.Entry.Events.Count; index++)
        {
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, GuidByteCount);
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, GetEventBytes(row.Entry.Events[index]));
        }

        return bytes;
    }

    /// <summary>Computes logical page bytes for one event.</summary>
    /// <param name="remoteEvent">The event.</param>
    /// <returns>The byte count.</returns>
    private static long GetEventBytes(RemoteEvent remoteEvent)
    {
        var bytes = GuidByteCount;
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(remoteEvent.StreamId.Value));
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(remoteEvent.ServerCursor));
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, DateTimeOffsetByteCount);
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, NullableMarkerByteCount);
        if (remoteEvent.CausedByOperationId.HasValue)
        {
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, GuidByteCount);
        }

        if (remoteEvent.Origin is not null)
        {
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, GetOriginBytes(remoteEvent.Origin));
        }

        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, IntByteCount);
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalSizer.GetPayloadBytes(remoteEvent.Payload));
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, IntByteCount);
        foreach (var item in remoteEvent.Metadata)
        {
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(item.Key));
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(item.Value));
        }

        return bytes;
    }

    /// <summary>Gets the number of canonical events included for a terminal operation group.</summary>
    /// <param name="row">The row.</param>
    /// <returns>The included event count.</returns>
    private static int GetIncludedEventCount(ServerCommitLedgerRow row) =>
        row.Entry.Result.Kind == OperationResultKind.Rejected ? 0 : row.Entry.Events.Count;

    /// <summary>Computes logical bytes for an origin field.</summary>
    /// <param name="origin">The event origin.</param>
    /// <returns>The logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetOriginBytes(RemoteEventOrigin origin) =>
        ServerCommitJournalSizer.AddLogicalBytes(ServerCommitJournalGuard.GetTextBytes(origin.ClientId), GuidByteCount);

    /// <summary>Gets a non-null group sequence.</summary>
    /// <param name="row">The row.</param>
    /// <returns>The group sequence.</returns>
    /// <exception cref="InvalidOperationException">The row is not a receive group.</exception>
    private static long GetGroupSequence(ServerCommitLedgerRow row) =>
        row.GroupSequence ?? throw new InvalidOperationException("The receive group sequence is missing.");
}
