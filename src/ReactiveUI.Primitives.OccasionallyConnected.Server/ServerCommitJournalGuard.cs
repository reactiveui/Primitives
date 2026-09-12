// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Validates and sizes server commit journal inputs before gated mutation.</summary>
internal static class ServerCommitJournalGuard
{
    /// <summary>The largest trusted textual identity in UTF-16 characters.</summary>
    private const int MaximumIdentifierCharacters = 256;

    /// <summary>The largest opaque cursor in strict UTF-8 bytes.</summary>
    private const int MaximumCursorUtf8Bytes = 4096;

    /// <summary>The canonical strict string encoding used for logical byte accounting.</summary>
    private static readonly Encoding TextEncoding = new UTF8Encoding(false, true);

    /// <summary>Validates and sizes a prepared plan.</summary>
    /// <param name="plan">The prepared plan.</param>
    /// <param name="options">The journal options.</param>
    /// <returns>The validated commit.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The plan exceeds configured bounds.</exception>
    /// <exception cref="InvalidOperationException">The plan is not a valid terminal commit.</exception>
    internal static ServerCommitValidationResult ValidatePlan(ServerCommitPlan plan, ServerCommitJournalOptions options)
    {
        ValidateStreamKey(plan.StreamKey);
        ValidateExpectedRevision(plan.ExpectedRevision);
        ValidateState(plan.StreamKey, plan.NewState);
        var entries = CaptureEntries(plan.Entries, options.MaximumOperationCaptureCount);
        var operationKeys = new ServerOperationKey[entries.Length];
        var entryBytes = new long[entries.Length];
        var operationSet = new HashSet<ServerOperationKey>();
        var eventIds = new HashSet<Guid>();
        var cursors = new HashSet<string>(StringComparer.Ordinal);
        var eventCount = 0;
        string? lastCursor = null;
        long ledgerBytes = 0;
        for (var index = 0; index < entries.Length; index++)
        {
            var entry = entries[index];
            ValidateEntry(plan.StreamKey, entry, operationSet, eventIds, cursors, options);
            operationKeys[index] = entry.OperationKey;
            entryBytes[index] = ServerCommitJournalSizer.GetEntryBytes(entry);
            ledgerBytes = ServerCommitJournalSizer.AddLogicalBytes(ledgerBytes, entryBytes[index]);
            eventCount = AddCount(eventCount, entry.Events.Count);
            lastCursor = GetLastCursor(entry, lastCursor);
        }

        ValidateWriteStamp(plan.NewWriteStamp, operationSet);
        return new()
        {
            StreamKey = plan.StreamKey,
            ExpectedRevision = plan.ExpectedRevision,
            NewState = plan.NewState,
            NewWriteStamp = plan.NewWriteStamp,
            Entries = entries,
            OperationKeys = operationKeys,
            EntryBytes = entryBytes,
            LedgerBytes = ledgerBytes,
            EventCount = eventCount,
            LastCursor = lastCursor,
            LastCursorBytes = lastCursor is null ? 0 : GetTextBytes(lastCursor),
            StateBytes = plan.NewState is null ? 0 : ServerCommitJournalSizer.GetStateBytes(plan.NewState),
        };
    }

    /// <summary>Validates and freezes operation keys before entering the gate.</summary>
    /// <param name="operationKeys">The requested operation keys.</param>
    /// <param name="maximumCount">The finite maximum count.</param>
    /// <returns>The owned operation key array.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Too many operation keys were requested.</exception>
    internal static ServerOperationKey[] CaptureOperationKeys(
        IReadOnlyList<ServerOperationKey> operationKeys,
        int maximumCount)
    {
        ArgumentExceptionHelper.ThrowIfNull(operationKeys);
        var count = operationKeys.Count;
        if (count < 0 || count > maximumCount)
        {
            throw new ArgumentOutOfRangeException(nameof(operationKeys), count, "The requested operation key count is outside the supported bounds.");
        }

        var copy = new ServerOperationKey[count];
        for (var index = 0; index < count; index++)
        {
            var operationKey = operationKeys[index];
            ValidateOperationKey(operationKey);
            copy[index] = operationKey;
        }

        return copy;
    }

    /// <summary>Validates an authenticated stream key.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <exception cref="ArgumentException">The stream key is invalid.</exception>
    internal static void ValidateStreamKey(ServerStreamKey streamKey)
    {
        ValidateText(streamKey.TenantId, nameof(streamKey.TenantId));
        if (streamKey.StreamId.Value is null)
        {
            throw new ArgumentException("A stream key must contain a valid stream identifier.", nameof(streamKey));
        }

        ValidateText(streamKey.StreamId.Value, nameof(streamKey.StreamId));
    }

    /// <summary>Validates bounded text before strict UTF-8 accounting.</summary>
    /// <param name="value">The value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The text is invalid.</exception>
    internal static void ValidateText(string value, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(value, parameterName);
        if (!IsInvalidText(value))
        {
            return;
        }

        throw new ArgumentException("Server journal text is invalid.", parameterName);
    }

    /// <summary>Computes strict UTF-8 bytes for validated text.</summary>
    /// <param name="value">The text.</param>
    /// <returns>The byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetTextBytes(string value) => TextEncoding.GetByteCount(value);

    /// <summary>Validates an opaque server cursor using the protocol UTF-8 byte bound.</summary>
    /// <param name="cursor">The cursor.</param>
    /// <exception cref="ArgumentException">The cursor is invalid.</exception>
    internal static void ValidateCursor(string cursor)
    {
        ArgumentExceptionHelper.ThrowIfNull(cursor);
        if (!IsInvalidCursor(cursor))
        {
            return;
        }

        throw new ArgumentException("A server cursor is invalid.", nameof(cursor));
    }

    /// <summary>Validates the expected revision.</summary>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <exception cref="ArgumentOutOfRangeException">The revision is negative.</exception>
    private static void ValidateExpectedRevision(long expectedRevision)
    {
        if (expectedRevision >= 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(expectedRevision), expectedRevision, "Expected revisions cannot be negative.");
    }

    /// <summary>Validates one prepared ledger entry.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The ledger entry.</param>
    /// <param name="operationSet">The current operation set.</param>
    /// <param name="eventIds">The current event identifier set.</param>
    /// <param name="cursors">The current cursor set.</param>
    /// <param name="options">The journal options.</param>
    /// <exception cref="InvalidOperationException">The entry is not valid for the stream.</exception>
    private static void ValidateEntry(
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        HashSet<ServerOperationKey> operationSet,
        HashSet<Guid> eventIds,
        HashSet<string> cursors,
        ServerCommitJournalOptions options)
    {
        ValidateOperationKey(entry.OperationKey);
        if (!operationSet.Add(entry.OperationKey))
        {
            throw new InvalidOperationException("A server commit cannot contain duplicate operation keys.");
        }

        if (entry.IsCommitted)
        {
            throw new InvalidOperationException("A server commit can only admit new prepared ledger entries.");
        }

        ValidateResult(entry);
        ValidateConflicts(entry, options);
        ValidateEvents(streamKey, entry, eventIds, cursors, options);
    }

    /// <summary>Validates a terminal operation result.</summary>
    /// <param name="entry">The containing ledger entry.</param>
    /// <exception cref="InvalidOperationException">The result is not terminal or does not match the entry.</exception>
    private static void ValidateResult(ServerLedgerEntry entry)
    {
        if (entry.Result.OperationId != entry.OperationKey.OperationId)
        {
            throw new InvalidOperationException("A terminal result must match its operation key.");
        }

        if (entry.Result.Kind is OperationResultKind.Accepted or OperationResultKind.Conflict or OperationResultKind.Rejected)
        {
            return;
        }

        throw new InvalidOperationException("Only terminal operation results can be retained by the process-local server journal.");
    }

    /// <summary>Validates resolved conflicts for one terminal operation.</summary>
    /// <param name="entry">The containing ledger entry.</param>
    /// <param name="options">The journal options.</param>
    /// <exception cref="ArgumentOutOfRangeException">Too many conflicts were supplied.</exception>
    /// <exception cref="InvalidOperationException">A conflict does not match the entry.</exception>
    private static void ValidateConflicts(ServerLedgerEntry entry, ServerCommitJournalOptions options)
    {
        var count = entry.Conflicts.Count;
        if (count > options.MaximumOperationCaptureCount)
        {
            throw new ArgumentOutOfRangeException(nameof(entry), count, "Too many conflicts were supplied.");
        }

        for (var index = 0; index < count; index++)
        {
            ValidateConflict(entry, entry.Conflicts[index]);
        }
    }

    /// <summary>Validates a single resolved conflict.</summary>
    /// <param name="entry">The containing entry.</param>
    /// <param name="conflict">The conflict.</param>
    /// <exception cref="InvalidOperationException">The conflict does not match the entry.</exception>
    private static void ValidateConflict(ServerLedgerEntry entry, ResolvedConflict conflict)
    {
        ArgumentExceptionHelper.ThrowIfNull(conflict);
        if (conflict.OperationId != entry.OperationKey.OperationId)
        {
            throw new InvalidOperationException("A resolved conflict must match its operation key.");
        }

        ValidateText(conflict.ResolutionCode, nameof(conflict.ResolutionCode));
    }

    /// <summary>Validates remote events for one terminal operation.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The containing ledger entry.</param>
    /// <param name="eventIds">The current event identifier set.</param>
    /// <param name="cursors">The current cursor set.</param>
    /// <param name="options">The journal options.</param>
    /// <exception cref="ArgumentOutOfRangeException">Too many events were supplied.</exception>
    /// <exception cref="InvalidOperationException">An event is invalid for the entry.</exception>
    private static void ValidateEvents(
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        HashSet<Guid> eventIds,
        HashSet<string> cursors,
        ServerCommitJournalOptions options)
    {
        var count = entry.Events.Count;
        if (count > options.MaximumEntryEventCount)
        {
            throw new ArgumentOutOfRangeException(nameof(entry), count, "Too many events were supplied.");
        }

        for (var index = 0; index < count; index++)
        {
            ValidateEvent(streamKey, entry, entry.Events[index], eventIds, cursors);
        }
    }

    /// <summary>Validates a single remote event.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The containing ledger entry.</param>
    /// <param name="remoteEvent">The event.</param>
    /// <param name="eventIds">The current event identifier set.</param>
    /// <param name="cursors">The current cursor set.</param>
    /// <exception cref="InvalidOperationException">The event is invalid.</exception>
    private static void ValidateEvent(
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        RemoteEvent remoteEvent,
        HashSet<Guid> eventIds,
        HashSet<string> cursors)
    {
        ArgumentExceptionHelper.ThrowIfNull(remoteEvent);
        if (remoteEvent.StreamId != streamKey.StreamId)
        {
            throw new InvalidOperationException("A remote event must belong to the committed stream.");
        }

        ValidateEventIdentity(remoteEvent, eventIds, cursors);
        ValidateEventCause(entry, remoteEvent);
    }

    /// <summary>Validates a remote event identity and cursor.</summary>
    /// <param name="remoteEvent">The event.</param>
    /// <param name="eventIds">The current event identifier set.</param>
    /// <param name="cursors">The current cursor set.</param>
    /// <exception cref="InvalidOperationException">The event identity is invalid.</exception>
    private static void ValidateEventIdentity(
        RemoteEvent remoteEvent,
        HashSet<Guid> eventIds,
        HashSet<string> cursors)
    {
        if (remoteEvent.EventId == Guid.Empty || !eventIds.Add(remoteEvent.EventId))
        {
            throw new InvalidOperationException("A remote event identifier must be non-empty and unique.");
        }

        ValidateCursor(remoteEvent.ServerCursor);
        if (cursors.Add(remoteEvent.ServerCursor))
        {
            return;
        }

        throw new InvalidOperationException("A remote event cursor must be unique.");
    }

    /// <summary>Validates a remote event cause.</summary>
    /// <param name="entry">The containing entry.</param>
    /// <param name="remoteEvent">The event.</param>
    /// <exception cref="InvalidOperationException">The event cause does not match.</exception>
    private static void ValidateEventCause(ServerLedgerEntry entry, RemoteEvent remoteEvent)
    {
        if (!remoteEvent.CausedByOperationId.HasValue || remoteEvent.CausedByOperationId.Value == entry.OperationKey.OperationId)
        {
            return;
        }

        throw new InvalidOperationException("A remote event cause must match its operation key.");
    }

    /// <summary>Validates an optional new state.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="state">The optional state.</param>
    /// <exception cref="InvalidOperationException">The state belongs to another stream.</exception>
    private static void ValidateState(ServerStreamKey streamKey, ServerState? state)
    {
        if (state is null)
        {
            return;
        }

        if (state.StreamId != streamKey.StreamId)
        {
            throw new InvalidOperationException("A server state must belong to the committed stream.");
        }

        ValidateText(state.Version, nameof(state.Version));
    }

    /// <summary>Validates an optional new write stamp.</summary>
    /// <param name="writeStamp">The candidate write stamp.</param>
    /// <param name="operationSet">The new operation keys.</param>
    /// <exception cref="InvalidOperationException">The stamp does not identify a committed operation.</exception>
    private static void ValidateWriteStamp(ServerWriteStamp? writeStamp, HashSet<ServerOperationKey> operationSet)
    {
        if (!writeStamp.HasValue)
        {
            return;
        }

        var stamp = writeStamp.Value;
        var operationKey = new ServerOperationKey(stamp.ClientId, stamp.OperationId);
        ValidateOperationKey(operationKey);
        if (operationSet.Contains(operationKey))
        {
            return;
        }

        throw new InvalidOperationException("A write stamp must identify one operation in the committed plan.");
    }

    /// <summary>Gets the final event cursor produced by an entry.</summary>
    /// <param name="entry">The ledger entry.</param>
    /// <param name="current">The current candidate cursor.</param>
    /// <returns>The final cursor.</returns>
    private static string? GetLastCursor(ServerLedgerEntry entry, string? current)
    {
        var count = entry.Events.Count;
        return count == 0 ? current : entry.Events[count - 1].ServerCursor;
    }

    /// <summary>Validates an authenticated operation key.</summary>
    /// <param name="operationKey">The operation key.</param>
    /// <exception cref="ArgumentException">The operation key is invalid.</exception>
    private static void ValidateOperationKey(ServerOperationKey operationKey)
    {
        ValidateText(operationKey.ClientId, nameof(operationKey.ClientId));
        if (operationKey.OperationId.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("An operation key must contain a non-empty operation identifier.", nameof(operationKey));
    }

    /// <summary>Validates and freezes plan entries before entering the gate.</summary>
    /// <param name="entries">The candidate entries.</param>
    /// <param name="maximumCount">The maximum entry count.</param>
    /// <returns>The owned entry array.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Too many entries were supplied.</exception>
    /// <exception cref="InvalidOperationException">No entries were supplied.</exception>
    private static ServerLedgerEntry[] CaptureEntries(IReadOnlyList<ServerLedgerEntry> entries, int maximumCount)
    {
        ArgumentExceptionHelper.ThrowIfNull(entries);
        var count = entries.Count;
        if (count == 0)
        {
            throw new InvalidOperationException("A server commit must include at least one terminal entry.");
        }

        if (count > maximumCount)
        {
            throw new ArgumentOutOfRangeException(nameof(entries), count, "The operation entry count is outside the supported bounds.");
        }

        var copy = new ServerLedgerEntry[count];
        for (var index = 0; index < count; index++)
        {
            var entry = entries[index];
            ArgumentExceptionHelper.ThrowIfNull(entry, nameof(entries));
            copy[index] = entry;
        }

        return copy;
    }

    /// <summary>Checks whether text is blank, oversized or malformed.</summary>
    /// <param name="value">The text.</param>
    /// <returns>Whether the text is invalid.</returns>
    private static bool IsInvalidText(string value) =>
        string.IsNullOrWhiteSpace(value) || value.Length > MaximumIdentifierCharacters || HasMalformedSurrogate(value);

    /// <summary>Checks whether a cursor is blank, oversized or malformed.</summary>
    /// <param name="cursor">The cursor.</param>
    /// <returns>Whether the cursor is invalid.</returns>
    private static bool IsInvalidCursor(string cursor) =>
        string.IsNullOrWhiteSpace(cursor) || HasMalformedSurrogate(cursor) || GetTextBytes(cursor) > MaximumCursorUtf8Bytes;

    /// <summary>Detects malformed surrogate pairs before strict UTF-8 accounting.</summary>
    /// <param name="value">The text.</param>
    /// <returns>Whether the text has malformed UTF-16.</returns>
    private static bool HasMalformedSurrogate(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (char.IsLowSurrogate(character))
            {
                return true;
            }

            if (!char.IsHighSurrogate(character))
            {
                continue;
            }

            if (index + 1 < value.Length && char.IsLowSurrogate(value[index + 1]))
            {
                index++;
                continue;
            }

            return true;
        }

        return false;
    }

    /// <summary>Adds counts with overflow protection.</summary>
    /// <param name="current">The current count.</param>
    /// <param name="delta">The delta.</param>
    /// <returns>The new count.</returns>
    /// <exception cref="InvalidOperationException">The count overflowed.</exception>
    private static int AddCount(int current, int delta) => checked(current + delta);
}
