// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores atomic process-local server state, events and terminal operation replays.</summary>
/// <remarks>
/// Authenticated tenant and client identifiers are trusted inputs from the host. This journal does not perform
/// authorization, durability, cross-process coordination or capability advertisement.
/// </remarks>
internal sealed class InMemoryServerCommitJournal : IServerCommitJournal
{
    /// <summary>Protects stream state and retained journal accounting.</summary>
    private readonly Lock _gate = new();

    /// <summary>The retained process-local stream records.</summary>
    private readonly Dictionary<ServerStreamKey, ServerCommitStreamRecord> _streams = [];

    /// <summary>The journal options.</summary>
    private readonly ServerCommitJournalOptions _options;

    /// <summary>The retained terminal entry count.</summary>
    private int _ledgerEntryCount;

    /// <summary>The retained event count.</summary>
    private int _eventCount;

    /// <summary>The retained logical encoded bytes.</summary>
    private long _logicalBytes;

    /// <summary>The latest clock value accepted by commit or compaction.</summary>
    private DateTimeOffset _latestUtc = DateTimeOffset.MinValue;

    /// <summary>Initializes a new instance of the <see cref="InMemoryServerCommitJournal"/> class.</summary>
    /// <param name="options">The finite journal bounds.</param>
    internal InMemoryServerCommitJournal(ServerCommitJournalOptions? options = null)
    {
        _options = options ?? new();
        _options.Validate();
    }

    /// <summary>Gets the current retained stream count.</summary>
    internal int StreamCount
    {
        get
        {
            lock (_gate)
            {
                return _streams.Count;
            }
        }
    }

    /// <summary>Gets the current retained terminal entry count.</summary>
    internal int LedgerEntryCount
    {
        get
        {
            lock (_gate)
            {
                return _ledgerEntryCount;
            }
        }
    }

    /// <summary>Gets the current retained event count.</summary>
    internal int EventCount
    {
        get
        {
            lock (_gate)
            {
                return _eventCount;
            }
        }
    }

    /// <summary>Gets the retained logical encoded byte count.</summary>
    internal long LogicalBytes
    {
        get
        {
            lock (_gate)
            {
                return _logicalBytes;
            }
        }
    }

    /// <summary>Reads a stream revision and requested terminal operation entries atomically.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="operationKeys">The bounded operation keys requested for replay.</param>
    /// <returns>The atomic stream snapshot.</returns>
    internal ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys)
    {
        ServerCommitJournalGuard.ValidateStreamKey(streamKey);
        var requested = ServerCommitJournalGuard.CaptureOperationKeys(operationKeys, _options.MaximumOperationCaptureCount);
        lock (_gate)
        {
            _ = _streams.TryGetValue(streamKey, out var stream);
            return ServerCommitJournalOperations.CreateSnapshot(streamKey, stream, requested);
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerCommitSnapshot IServerCommitJournal.Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys) =>
        Read(streamKey, operationKeys);

    /// <summary>Attempts to atomically admit a fully prepared terminal server commit.</summary>
    /// <param name="plan">The prepared commit plan.</param>
    /// <returns>The result and atomic stream snapshot observed by the attempt.</returns>
    internal ServerCommitResult TryCommit(ServerCommitPlan plan)
    {
        ArgumentExceptionHelper.ThrowIfNull(plan);
        var commit = ServerCommitJournalGuard.ValidatePlan(plan, _options);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            return TryCommitUnderGate(commit, observedUtc);
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerCommitResult IServerCommitJournal.TryCommit(ServerCommitPlan plan) => TryCommit(plan);

    /// <summary>Compacts expired terminal ledger entries and event rows using the journal clock.</summary>
    /// <returns>The number of terminal entries removed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int Compact() => Compact(null);

    /// <summary>Compacts expired terminal ledger entries and event rows.</summary>
    /// <param name="utcNow">The optional caller-sampled timestamp.</param>
    /// <returns>The number of terminal entries removed.</returns>
    internal int Compact(DateTimeOffset? utcNow)
    {
        var sampledUtc = utcNow ?? _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            var compactUtc = ServerCommitJournalOperations.Max(_latestUtc, sampledUtc);
            var expired = GetExpiredRows(compactUtc);
            ApplyExpired(expired);
            _latestUtc = compactUtc;
            return expired.LedgerRows.Count;
        }
    }

    /// <summary>Computes the retained cursor byte delta for a commit.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The retained cursor byte delta.</returns>
    private static long GetLastCursorDelta(ServerCommitStreamRecord stream, ServerCommitValidationResult commit) =>
        commit.LastCursor is null ? 0 : commit.LastCursorBytes - stream.LastCursorBytes;

    /// <summary>Applies retained cursor byte accounting to the stream.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    private static void ApplyLastCursorBytes(ServerCommitStreamRecord stream, ServerCommitValidationResult commit)
    {
        if (commit.LastCursor is null)
        {
            return;
        }

        stream.LastCursorBytes = commit.LastCursorBytes;
    }

    /// <summary>Performs the gated compare-and-swap commit.</summary>
    /// <param name="commit">The validated commit.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <returns>The commit result.</returns>
    private ServerCommitResult TryCommitUnderGate(ServerCommitValidationResult commit, DateTimeOffset observedUtc)
    {
        var streamExists = _streams.TryGetValue(commit.StreamKey, out var stream);
        stream ??= new();
        var status = ServerCommitJournalOperations.GetPreCommitStatus(stream, commit);
        if (status != ServerCommitStatus.Committed)
        {
            return new(status, ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys));
        }

        var committedUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
        var stateDelta = ServerCommitJournalSizer.GetStateDelta(stream, commit);
        var streamDelta = streamExists ? 0 : ServerCommitJournalSizer.GetStreamKeyBytes(commit.StreamKey);
        var lastCursorDelta = GetLastCursorDelta(stream, commit);
        var expired = GetExpiredRows(committedUtc);
        if (!HasCapacity(commit, stateDelta, streamDelta, lastCursorDelta, null)
            && !HasCapacity(commit, stateDelta, streamDelta, lastCursorDelta, expired))
        {
            return new(ServerCommitStatus.CapacityExceeded, ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys));
        }

        var expiresUtc = GetExpiry(committedUtc);
        var committedEntries = ServerCommitJournalOperations.CommitEntries(commit.Entries, committedUtc, expiresUtc);
        ApplyExpired(expired);
        AddStreamIfNeeded(commit.StreamKey, stream, streamExists, streamDelta);
        ApplyCommit(stream, commit, committedEntries, stateDelta, lastCursorDelta);
        _latestUtc = committedUtc;
        return new(ServerCommitStatus.Committed, ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys));
    }

    /// <summary>Applies a validated commit to the stream.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <param name="committedEntries">The committed entries.</param>
    /// <param name="stateDelta">The retained state byte delta.</param>
    /// <param name="lastCursorDelta">The retained cursor byte delta.</param>
    private void ApplyCommit(
        ServerCommitStreamRecord stream,
        ServerCommitValidationResult commit,
        ServerLedgerEntry[] committedEntries,
        long stateDelta,
        long lastCursorDelta)
    {
        ServerCommitJournalOperations.ApplyState(stream, commit);
        for (var index = 0; index < committedEntries.Length; index++)
        {
            ServerCommitJournalOperations.AddLedgerRow(stream, commit.StreamKey, committedEntries[index], commit.EntryBytes[index]);
        }

        stream.Revision++;
        _ledgerEntryCount += committedEntries.Length;
        _eventCount += commit.EventCount;
        ApplyLastCursorBytes(stream, commit);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, commit.LedgerBytes);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, stateDelta);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, lastCursorDelta);
    }

    /// <summary>Adds a stream after successful capacity admission.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The stream record.</param>
    /// <param name="streamExists">Whether the stream already exists.</param>
    /// <param name="streamDelta">The stream logical bytes.</param>
    private void AddStreamIfNeeded(
        ServerStreamKey streamKey,
        ServerCommitStreamRecord stream,
        bool streamExists,
        long streamDelta)
    {
        if (streamExists)
        {
            return;
        }

        _streams.Add(streamKey, stream);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, streamDelta);
    }

    /// <summary>Checks whether the commit can fit after an optional expired-row reclamation.</summary>
    /// <param name="commit">The validated commit.</param>
    /// <param name="stateDelta">The retained state byte delta.</param>
    /// <param name="streamDelta">The new stream logical byte delta.</param>
    /// <param name="lastCursorDelta">The retained cursor byte delta.</param>
    /// <param name="expired">The optional projected expired rows.</param>
    /// <returns>Whether capacity remains.</returns>
    private bool HasCapacity(
        ServerCommitValidationResult commit,
        long stateDelta,
        long streamDelta,
        long lastCursorDelta,
        ServerCommitExpiredRows? expired)
    {
        var streamCount = checked((long)_streams.Count + (streamDelta == 0 ? 0 : 1));
        var ledgerCount = checked((long)_ledgerEntryCount + commit.Entries.Length - (expired?.LedgerRows.Count ?? 0));
        var eventCount = checked((long)_eventCount + commit.EventCount - (expired?.EventRows.Count ?? 0));
        var logicalBytes = checked(_logicalBytes + commit.LedgerBytes + stateDelta + streamDelta + lastCursorDelta - (expired?.LogicalBytes ?? 0));
        return HasCountCapacity(streamCount, ledgerCount, eventCount) && logicalBytes <= _options.MaximumLogicalBytes;
    }

    /// <summary>Checks retained count capacity.</summary>
    /// <param name="streamCount">The projected stream count.</param>
    /// <param name="ledgerCount">The projected ledger count.</param>
    /// <param name="eventCount">The projected event count.</param>
    /// <returns>Whether count capacity remains.</returns>
    private bool HasCountCapacity(long streamCount, long ledgerCount, long eventCount) =>
        streamCount <= _options.MaximumStreams
        && ledgerCount <= _options.MaximumLedgerEntries
        && eventCount <= _options.MaximumEvents;

    /// <summary>Collects expired rows without mutating journal state.</summary>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>The projected expired rows.</returns>
    private ServerCommitExpiredRows GetExpiredRows(DateTimeOffset utcNow)
    {
        var expired = new ServerCommitExpiredRows();
        foreach (var streamPair in _streams)
        {
            ServerCommitJournalOperations.CollectExpiredLedgerRows(streamPair.Value, utcNow, expired);
            ServerCommitJournalOperations.CollectExpiredEventRows(streamPair.Value, utcNow, expired);
        }

        return expired;
    }

    /// <summary>Applies projected expired-row cleanup.</summary>
    /// <param name="expired">The expired rows.</param>
    private void ApplyExpired(ServerCommitExpiredRows expired)
    {
        for (var index = 0; index < expired.LedgerRows.Count; index++)
        {
            ApplyExpiredLedger(expired.LedgerRows[index]);
        }

        for (var index = 0; index < expired.EventRows.Count; index++)
        {
            ApplyExpiredEvent(expired.EventRows[index]);
        }
    }

    /// <summary>Applies one expired ledger row.</summary>
    /// <param name="ledgerRow">The ledger row.</param>
    private void ApplyExpiredLedger(ServerCommitLedgerRow ledgerRow)
    {
        var stream = _streams[ledgerRow.StreamKey];
        _ = stream.Ledger.Remove(ledgerRow.Entry.OperationKey);
        _ledgerEntryCount--;
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, -ledgerRow.LogicalBytes);
    }

    /// <summary>Applies one expired event row.</summary>
    /// <param name="eventRow">The event row.</param>
    private void ApplyExpiredEvent(ServerCommitEventRow eventRow)
    {
        var stream = _streams[eventRow.Ledger.StreamKey];
        _ = stream.Events.Remove(eventRow);
        _eventCount--;
        _ = stream.EventIds.Remove(eventRow.RemoteEvent.EventId);
        _ = stream.Cursors.Remove(eventRow.RemoteEvent.ServerCursor);
    }

    /// <summary>Computes an inclusive replay expiry for a successful commit.</summary>
    /// <param name="committedUtc">The successful commit timestamp.</param>
    /// <returns>The inclusive expiry timestamp.</returns>
    private DateTimeOffset GetExpiry(DateTimeOffset committedUtc)
    {
        try
        {
            return committedUtc.Add(_options.OperationRetention);
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MaxValue;
        }
    }
}
