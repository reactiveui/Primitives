// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Provides stateless operations used by <see cref="InMemoryServerCommitJournal"/>.</summary>
internal static class ServerCommitJournalOperations
{
    /// <summary>Computes the pre-commit status for a stream.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The commit status.</returns>
    internal static ServerCommitStatus GetPreCommitStatus(
        ServerCommitStreamRecord stream,
        ServerCommitValidationResult commit)
    {
        if (stream.Revision != commit.ExpectedRevision)
        {
            return ServerCommitStatus.StaleRevision;
        }

        if (stream.Revision == long.MaxValue)
        {
            return ServerCommitStatus.RevisionOverflow;
        }

        return !CanAdvanceSequences(stream, commit) ? ServerCommitStatus.EventSequenceOverflow : CheckDuplicateKeys(stream, commit);
    }

    /// <summary>Applies optional state and stamp changes.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    internal static void ApplyState(ServerCommitStreamRecord stream, ServerCommitValidationResult commit)
    {
        if (commit.NewState is null)
        {
            ApplyAppendOnlyStamp(stream, commit);
            return;
        }

        stream.State = commit.NewState;
        stream.LastWriteStamp = commit.NewWriteStamp;
        stream.StateBytes = commit.StateBytes;
    }

    /// <summary>Adds a committed ledger row and sidecar event rows.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The committed entry.</param>
    /// <param name="logicalBytes">The retained logical bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void AddLedgerRow(
        ServerCommitStreamRecord stream,
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        long logicalBytes) =>
        AddLedgerRow(stream, streamKey, entry, logicalBytes, checked(stream.LastGroupSequence + 1));

    /// <summary>Adds a committed ledger row with an optional durable receive group sequence.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The committed entry.</param>
    /// <param name="logicalBytes">The retained logical bytes.</param>
    /// <param name="groupSequence">The receive group sequence.</param>
    internal static void AddLedgerRow(
        ServerCommitStreamRecord stream,
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        long logicalBytes,
        long? groupSequence)
    {
        var row = new ServerCommitLedgerRow(streamKey, entry, logicalBytes, groupSequence);
        stream.Ledger.Add(entry.OperationKey, row);
        if (groupSequence.HasValue)
        {
            stream.LastGroupSequence = groupSequence.Value;
            stream.Groups.Add(row);
        }
        else
        {
            stream.HasReceiveHistoryGap = true;
        }

        for (var index = 0; index < entry.Events.Count; index++)
        {
            AddEventRow(stream, row, entry.Events[index]);
        }
    }

    /// <summary>Adds a committed legacy ledger row whose original receive group order is unavailable.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="entry">The committed entry.</param>
    /// <param name="logicalBytes">The retained logical bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void AddUnsequencedLedgerRow(
        ServerCommitStreamRecord stream,
        ServerStreamKey streamKey,
        ServerLedgerEntry entry,
        long logicalBytes) =>
        AddLedgerRow(stream, streamKey, entry, logicalBytes, null);

    /// <summary>Creates committed entries from owned data without invoking caller callbacks.</summary>
    /// <param name="entries">The validated prepared entries.</param>
    /// <param name="committedUtc">The commit timestamp.</param>
    /// <param name="expiresUtc">The replay expiry.</param>
    /// <returns>The committed entries.</returns>
    internal static ServerLedgerEntry[] CommitEntries(
        ServerLedgerEntry[] entries,
        DateTimeOffset committedUtc,
        DateTimeOffset expiresUtc)
    {
        var committed = new ServerLedgerEntry[entries.Length];
        for (var index = 0; index < entries.Length; index++)
        {
            committed[index] = entries[index].Commit(committedUtc, expiresUtc);
        }

        return committed;
    }

    /// <summary>Gets the later of two timestamps.</summary>
    /// <param name="left">The first timestamp.</param>
    /// <param name="right">The second timestamp.</param>
    /// <returns>The later timestamp.</returns>
    internal static DateTimeOffset Max(DateTimeOffset left, DateTimeOffset right) => left >= right ? left : right;

    /// <summary>Creates a snapshot from the current stream record.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="stream">The optional stream record.</param>
    /// <param name="requested">The requested operation keys.</param>
    /// <returns>The atomic snapshot.</returns>
    internal static ServerCommitSnapshot CreateSnapshot(
        ServerStreamKey streamKey,
        ServerCommitStreamRecord? stream,
        ServerOperationKey[] requested)
    {
        if (stream is null)
        {
            return new(streamKey, 0, null, null, [], null, 0);
        }

        var entries = new List<ServerLedgerEntry>(requested.Length);
        for (var index = 0; index < requested.Length; index++)
        {
            if (stream.Ledger.TryGetValue(requested[index], out var row))
            {
                entries.Add(row.Entry);
            }
        }

        return new(streamKey, stream.Revision, stream.State, stream.LastWriteStamp, entries, stream.LastCursor, stream.LastEventSequence);
    }

    /// <summary>Collects expired ledger rows from one stream.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <param name="expired">The projected expired rows.</param>
    internal static void CollectExpiredLedgerRows(
        ServerCommitStreamRecord stream,
        DateTimeOffset utcNow,
        ServerCommitExpiredRows expired)
    {
        foreach (var ledgerPair in stream.Ledger)
        {
            if (ledgerPair.Value.Entry.ExpiresAtUtc >= utcNow)
            {
                continue;
            }

            expired.LedgerRows.Add(ledgerPair.Value);
            expired.LogicalBytes += ledgerPair.Value.LogicalBytes;
        }
    }

    /// <summary>Collects expired event rows from one stream.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <param name="expired">The projected expired rows.</param>
    internal static void CollectExpiredEventRows(
        ServerCommitStreamRecord stream,
        DateTimeOffset utcNow,
        ServerCommitExpiredRows expired)
    {
        for (var index = 0; index < stream.Events.Count; index++)
        {
            var eventRow = stream.Events[index];
            if (eventRow.Ledger.Entry.ExpiresAtUtc < utcNow)
            {
                expired.EventRows.Add(eventRow);
            }
        }
    }

    /// <summary>Applies an append-only stamp.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    private static void ApplyAppendOnlyStamp(ServerCommitStreamRecord stream, ServerCommitValidationResult commit)
    {
        if (!commit.NewWriteStamp.HasValue)
        {
            return;
        }

        stream.LastWriteStamp = commit.NewWriteStamp;
    }

    /// <summary>Adds a committed sidecar event row.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="row">The containing ledger row.</param>
    /// <param name="remoteEvent">The committed remote event.</param>
    private static void AddEventRow(ServerCommitStreamRecord stream, ServerCommitLedgerRow row, RemoteEvent remoteEvent)
    {
        checked
        {
            stream.LastEventSequence++;
        }

        var eventRow = new ServerCommitEventRow(row, remoteEvent, stream.LastEventSequence);
        stream.Events.Add(eventRow);
        _ = stream.EventIds.Add(remoteEvent.EventId);
        _ = stream.Cursors.Add(remoteEvent.ServerCursor);
        stream.LastCursor = remoteEvent.ServerCursor;
    }

    /// <summary>Checks whether the event and receive group sequences can advance.</summary>
    /// <param name="stream">The stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>Whether the sequences can advance without overflowing.</returns>
    private static bool CanAdvanceSequences(ServerCommitStreamRecord stream, ServerCommitValidationResult commit) =>
        commit.EventCount <= long.MaxValue - stream.LastEventSequence
        && commit.Entries.Length <= long.MaxValue - stream.LastGroupSequence;

    /// <summary>Checks for duplicate ledger keys and retained event identifiers.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The commit status.</returns>
    private static ServerCommitStatus CheckDuplicateKeys(ServerCommitStreamRecord stream, ServerCommitValidationResult commit)
    {
        for (var index = 0; index < commit.Entries.Length; index++)
        {
            var entry = commit.Entries[index];
            if (stream.Ledger.TryGetValue(entry.OperationKey, out var existing))
            {
                return existing.Entry.Fingerprint.Matches(entry.Fingerprint) ? ServerCommitStatus.StaleRevision : ServerCommitStatus.IntentMismatch;
            }

            if (HasRetainedEventConflict(stream, entry))
            {
                return ServerCommitStatus.IntentMismatch;
            }
        }

        return ServerCommitStatus.Committed;
    }

    /// <summary>Checks whether an entry conflicts with retained event identifiers or cursors.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="entry">The candidate entry.</param>
    /// <returns>Whether a conflict exists.</returns>
    private static bool HasRetainedEventConflict(ServerCommitStreamRecord stream, ServerLedgerEntry entry)
    {
        for (var index = 0; index < entry.Events.Count; index++)
        {
            var remoteEvent = entry.Events[index];
            if (stream.EventIds.Contains(remoteEvent.EventId) || stream.Cursors.Contains(remoteEvent.ServerCursor))
            {
                return true;
            }
        }

        return false;
    }
}
