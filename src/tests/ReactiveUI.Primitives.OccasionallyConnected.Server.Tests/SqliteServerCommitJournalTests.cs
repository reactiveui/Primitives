// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="SqliteServerCommitJournal"/>.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The default authenticated tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The default authenticated client.</summary>
    private const string Client = "client";

    /// <summary>The alternate authenticated client.</summary>
    private const string OtherClient = "client-b";

    /// <summary>The first committed cursor.</summary>
    private const string FirstCursor = "cursor-1";

    /// <summary>The second committed cursor.</summary>
    private const string SecondCursor = "cursor-2";

    /// <summary>The third committed cursor.</summary>
    private const string ThirdCursor = "cursor-3";

    /// <summary>The first state version.</summary>
    private const string FirstVersion = "v1";

    /// <summary>The second state version.</summary>
    private const string SecondVersion = "v2";

    /// <summary>The default payload text.</summary>
    private const string EventPayload = "event";

    /// <summary>The payload contract identifier.</summary>
    private const string PayloadContract = "contract";

    /// <summary>The payload content type.</summary>
    private const string PayloadContentType = "text/plain";

    /// <summary>The first operation seed.</summary>
    private const int FirstOperationSeed = 1;

    /// <summary>The second operation seed.</summary>
    private const int SecondOperationSeed = 2;

    /// <summary>The third operation seed.</summary>
    private const int ThirdOperationSeed = 3;

    /// <summary>The mismatched fingerprint seed.</summary>
    private const int MismatchFingerprintSeed = 9;

    /// <summary>The expected single-entry count.</summary>
    private const int SingleEntryCount = 1;

    /// <summary>The expected double-entry count.</summary>
    private const int DoubleEntryCount = 2;

    /// <summary>The default retained stream limit.</summary>
    private const int DefaultMaximumStreams = 4;

    /// <summary>The default retained event limit.</summary>
    private const int DefaultMaximumEvents = 16;

    /// <summary>The default retained ledger limit.</summary>
    private const int DefaultMaximumLedgerEntries = 16;

    /// <summary>The default retained logical byte limit.</summary>
    private const long DefaultMaximumLogicalBytes = 4096;

    /// <summary>The default retention duration in minutes.</summary>
    private const int DefaultRetentionMinutes = 5;

    /// <summary>The child process mode marker environment variable.</summary>
    private const string CrashChildModeVariable = "RXUI_SERVER_SQLITE_CRASH_CHILD";

    /// <summary>The child database path environment variable.</summary>
    private const string CrashDatabasePathVariable = "RXUI_SERVER_SQLITE_CRASH_DATABASE";

    /// <summary>The child signal path environment variable.</summary>
    private const string CrashSignalPathVariable = "RXUI_SERVER_SQLITE_CRASH_SIGNAL";

    /// <summary>The child operation identifier environment variable.</summary>
    private const string CrashOperationIdVariable = "RXUI_SERVER_SQLITE_CRASH_OPERATION";

    /// <summary>The child mode for crashing after an acknowledged journal commit.</summary>
    private const string CrashAfterCommitMode = "after-commit";

    /// <summary>The child mode for crashing with uncommitted raw rows.</summary>
    private const string CrashBeforeCommitMode = "before-commit";

    /// <summary>The signal file polling interval in milliseconds.</summary>
    private const int SignalPollIntervalMilliseconds = 100;

    /// <summary>The concurrent commit readiness polling interval in milliseconds.</summary>
    private const int ReadyPollIntervalMilliseconds = 10;

    /// <summary>The test assembly file name used by direct MTP execution.</summary>
    private const string TestAssemblyFileName = "ReactiveUI.Primitives.OccasionallyConnected.Server.Tests.dll";

    /// <summary>The child test tree node filter.</summary>
    private const string ChildTestTreeNodeFilter = $"/*/*/*/{nameof(CrashChildPublishesSignalAndWaits)}";

    /// <summary>The fixed start instant.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 12, 10, 0, 0, TimeSpan.Zero);

    /// <summary>The default stream.</summary>
    private static readonly StreamId Stream = new("stream");

    /// <summary>The maximum time to wait for the child to publish a signal.</summary>
    private static readonly TimeSpan SignalWaitTimeout = TimeSpan.FromSeconds(20);

    /// <summary>The maximum time to wait for the killed child process to exit.</summary>
    private static readonly TimeSpan ChildExitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Verifies committed data reopens with the complete replay payload and immutable collections.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReopenReconstructsOriginalTerminalReplayAndCounters()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var conflict = new ResolvedConflict(key.OperationId, "merge", Payload("resolved"));
        var remoteEvent = Event(key.OperationId, FirstCursor, EventPayload);
        var entry = Entry(key, OperationResultKind.Conflict, FirstOperationSeed, [conflict], [remoteEvent]);
        using (var journal = CreateJournal(database.Path))
        {
            var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), entry));
            await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        }

        using var reopened = CreateJournal(database.Path);
        var replay = reopened.Read(StreamKey(), [key]);

        await Assert.That(reopened.StreamCount).IsEqualTo(SingleEntryCount);
        await Assert.That(reopened.LedgerEntryCount).IsEqualTo(SingleEntryCount);
        await Assert.That(reopened.EventCount).IsEqualTo(SingleEntryCount);
        await Assert.That(reopened.LogicalBytes).IsGreaterThan(0);
        await Assert.That(replay.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(replay.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(replay.LastWriteStamp?.OperationId).IsEqualTo(key.OperationId);
        await Assert.That(replay.LastCursor).IsEqualTo(FirstCursor);
        await Assert.That(replay.LastEventSequence).IsEqualTo(SingleEntryCount);
        await Assert.That(replay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(replay.Entries[0].Result.Kind).IsEqualTo(OperationResultKind.Conflict);
        await Assert.That(replay.Entries[0].Conflicts[0].ResolutionCode).IsEqualTo(conflict.ResolutionCode);
        await Assert.That(PayloadBytesEqual(replay.Entries[0].Conflicts[0].ResolvedPayload, conflict.ResolvedPayload)).IsTrue();
        await Assert.That(replay.Entries[0].Events[0].EventId).IsEqualTo(remoteEvent.EventId);
        await Assert.That(PayloadBytesEqual(replay.Entries[0].Events[0].Payload, remoteEvent.Payload)).IsTrue();
        await Assert.That(replay.Entries[0].Events[0].Metadata["kind"]).IsEqualTo(EventPayload);
        await Assert.That(() => ((IList)replay.Entries).Add(entry)).ThrowsExactly<NotSupportedException>();
        await Assert.That(() => ((IList)replay.Entries[0].Events).Add(remoteEvent)).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Verifies separate SQLite instances serialize compare-and-swap commits.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompetingInstancesRejectStalePreparedEffects()
    {
        using var database = new TemporaryDatabase();
        using var first = CreateJournal(database.Path);
        using var second = CreateJournal(database.Path);
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);

        var committed = first.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        var stale = second.TryCommit(Plan(0, State(SecondVersion), Stamp(secondKey), Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));
        var snapshot = second.Read(StreamKey(), [firstKey, secondKey]);

        await Assert.That(committed.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(stale.Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].OperationKey).IsEqualTo(firstKey);
    }

    /// <summary>Verifies mixed duplicate and new plans do not apply combined precomputed effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MixedDuplicateAndNewPlanRejectsAtomically()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var duplicate = OperationKey(FirstOperationSeed);
        var fresh = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(duplicate), Entry(duplicate, OperationResultKind.Accepted, FirstOperationSeed)));

        var result = journal.TryCommit(new(
            StreamKey(),
            0,
            State(SecondVersion),
            Stamp(fresh),
            [Entry(duplicate, OperationResultKind.Accepted, FirstOperationSeed), Entry(fresh, OperationResultKind.Accepted, SecondOperationSeed)]));
        var snapshot = journal.Read(StreamKey(), [duplicate, fresh]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].OperationKey).IsEqualTo(duplicate);
    }

    /// <summary>Verifies same-scope duplicate intent mismatch leaves state and ledger untouched.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DuplicateIntentMismatchRejectsWithoutMutation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));

        var mismatch = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, MismatchFingerprintSeed)));
        var snapshot = journal.Read(StreamKey(), [key]);

        await Assert.That(mismatch.Status).IsEqualTo(ServerCommitStatus.IntentMismatch);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].Fingerprint.Matches(Fingerprint(FirstOperationSeed))).IsTrue();
    }

    /// <summary>Verifies retention uses UTC high-water timestamps and preserves state after replay expiry.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompactUsesUtcRetentionAndKeepsStateHighWater()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start.AddTicks(DoubleEntryCount));
        using var journal = CreateJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start);

        await Assert.That(journal.Compact(Start.AddTicks(DoubleEntryCount))).IsEqualTo(0);
        await Assert.That(journal.Compact(Start.AddTicks(DoubleEntryCount + DoubleEntryCount))).IsEqualTo(SingleEntryCount);
        var snapshot = journal.Read(StreamKey(), [key]);

        await Assert.That(snapshot.Entries).Count().IsEqualTo(0);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.LastCursor).IsEqualTo(FirstCursor);
        await Assert.That(snapshot.LastEventSequence).IsEqualTo(SingleEntryCount);
        await Assert.That(journal.EventCount).IsEqualTo(0);
        await Assert.That(journal.LedgerEntryCount).IsEqualTo(0);
    }

    /// <summary>Verifies retained logical capacity accounts for durable rows and expired cleanup.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RetainedLogicalCapacityRejectsUntilExpiredRowsCompact()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        using var journal = CreateJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount), maximumLedgerEntries: SingleEntryCount);
        var first = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));

        var rejected = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(secondKey), Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        var admitted = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(secondKey), Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));

        await Assert.That(first.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(rejected.Status).IsEqualTo(ServerCommitStatus.CapacityExceeded);
        await Assert.That(admitted.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(journal.Read(StreamKey(), [firstKey, secondKey]).Entries).Count().IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies conflicts without a resolved payload survive durable replay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConflictWithoutResolvedPayloadRoundTrips()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var conflict = new ResolvedConflict(key.OperationId, "manual", null);
        using (var journal = CreateJournal(database.Path))
        {
            var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Conflict, FirstOperationSeed, [conflict])));
            await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        }

        using var reopened = CreateJournal(database.Path);
        var replay = reopened.Read(StreamKey(), [key]);

        await Assert.That(replay.Entries[0].Conflicts[0].ResolutionCode).IsEqualTo("manual");
        await Assert.That(replay.Entries[0].Conflicts[0].ResolvedPayload).IsNull();
    }

    /// <summary>Verifies rejected replay rows preserve nullable server version and unoriginated events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RejectedReplayWithReasonAndUnoriginatedEventRoundTrips()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var remoteEvent = new RemoteEvent(Guid.NewGuid(), Stream, FirstCursor, Start, null, Payload(EventPayload), new Dictionary<string, string>());
        var entry = new ServerLedgerEntry(
            key,
            Fingerprint(FirstOperationSeed),
            new(key.OperationId, OperationResultKind.Rejected, "denied", null),
            [],
            [remoteEvent]);
        using (var journal = CreateJournal(database.Path))
        {
            var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), entry));
            await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        }

        using var reopened = CreateJournal(database.Path);
        var replay = reopened.Read(StreamKey(), [key]);

        await Assert.That(replay.Entries[0].Result.Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(replay.Entries[0].Result.ReasonCode).IsEqualTo("denied");
        await Assert.That(replay.Entries[0].Result.ServerVersion).IsNull();
        await Assert.That(replay.Entries[0].Events[0].CausedByOperationId).IsNull();
        await Assert.That(replay.Entries[0].Events[0].Origin).IsNull();
        await Assert.That(replay.Entries[0].Events[0].Metadata).Count().IsEqualTo(0);
    }

    /// <summary>Verifies default options, no-argument compaction and null optional commit data.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DefaultOptionsCompactAndNullOptionalDataRoundTrip()
    {
        using var database = new TemporaryDatabase();
        using var journal = new SqliteServerCommitJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var result = journal.TryCommit(Plan(0, null, null, Entry(key, OperationResultKind.Accepted, FirstOperationSeed, events: [])));
        var removed = journal.Compact();
        var replay = journal.Read(StreamKey(), [key]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(removed).IsEqualTo(0);
        await Assert.That(replay.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(replay.State).IsNull();
        await Assert.That(replay.LastWriteStamp).IsNull();
        await Assert.That(replay.LastCursor).IsNull();
        await Assert.That(replay.LastEventSequence).IsEqualTo(0);
        await Assert.That(replay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(replay.Entries[0].Events).Count().IsEqualTo(0);
    }

    /// <summary>Verifies expiry arithmetic saturates when commit timestamps approach the maximum value.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ExpiryOverflowSaturatesReplayRetention()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(DateTimeOffset.MaxValue);
        using var journal = CreateJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var key = OperationKey(FirstOperationSeed);
        var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(journal.Compact(DateTimeOffset.MaxValue)).IsEqualTo(0);
        await Assert.That(journal.Read(StreamKey(), [key]).Entries).Count().IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies unsupported and non-openable SQLite paths fail without silent fallback.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UnsupportedPathsFailClearly()
    {
        using var database = new TemporaryDatabase();
        var directoryPath = System.IO.Path.GetDirectoryName(database.Path) ?? database.Path;

        await Assert.That(static () => new SqliteServerCommitJournal(string.Empty)).ThrowsExactly<ArgumentException>();
        await Assert.That(static () => new SqliteServerCommitJournal(":memory:")).ThrowsExactly<ArgumentException>();
        await Assert.That(() => new SqliteServerCommitJournal(directoryPath)).ThrowsExactly<SqliteException>();
    }

    /// <summary>Verifies unsupported SQLite user versions are rejected independently from local store schema versions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UnsupportedUserVersionFailsClearly()
    {
        using var database = new TemporaryDatabase();
        WriteUnsupportedUserVersion(database.Path);

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies metadata schema version corruption is rejected after table validation succeeds.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MetadataSchemaVersionMismatchFailsClearly()
    {
        using var database = new TemporaryDatabase();
        using (var initialized = CreateJournal(database.Path))
        {
            await Assert.That(initialized.StreamCount).IsEqualTo(0);
        }

        WriteUnsupportedMetadataSchemaVersion(database.Path);

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing metadata rows are rejected after table validation succeeds.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MissingMetadataSchemaVersionFailsClearly()
    {
        using var database = new TemporaryDatabase();
        using (var initialized = CreateJournal(database.Path))
        {
            await Assert.That(initialized.StreamCount).IsEqualTo(0);
        }

        DeleteMetadataSchemaVersion(database.Path);

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a missing owned table is rejected after ordered table scanning completes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MissingOwnedTableFailsSchemaValidation()
    {
        using var database = new TemporaryDatabase();
        CreateMissingOwnedTableSchema(database.Path);

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies exact table SQL validation rejects matching table names with wrong definitions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task WrongTableDefinitionFailsSchemaValidation()
    {
        using var database = new TemporaryDatabase();
        CreateWrongTableDefinitionSchema(database.Path);

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a concurrent stream row disappearance fails instead of inserting detached sidecars.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task StreamUpdateGuardRejectsMissingRowAfterTriggerMutation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        CreateDeleteStreamBeforeUpdateTrigger(database.Path);

        await Assert.That(() => journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed))))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies compaction fails if metadata disappears during the high-water update.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MetadataUpdateGuardRejectsMissingRowAfterTriggerMutation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        CreateDeleteMetadataBeforeUpdateTrigger(database.Path);

        await Assert.That(() => journal.Compact()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies failed mid-write mutations roll back all ledger sidecars.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task StreamUpdateGuardRollsBackInsertedLedgerRows()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        CreateDeleteStreamBeforeUpdateTrigger(database.Path);

        await Assert.That(() => journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed))))
            .ThrowsExactly<InvalidOperationException>();

        var snapshot = journal.Read(StreamKey(), [key]);
        await Assert.That(snapshot.Revision).IsEqualTo(0);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(0);
        await Assert.That(journal.StreamCount).IsEqualTo(0);
        await Assert.That(journal.LedgerEntryCount).IsEqualTo(0);
        await Assert.That(journal.EventCount).IsEqualTo(0);
    }

    /// <summary>Verifies malformed durable rows fail closed when replay reconstruction reaches them.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CorruptedDurableRowsFailClosedDuringReplay()
    {
        foreach (var corruption in CreateReplayCorruptions())
        {
            using var database = new TemporaryDatabase();
            var key = SeedReplayRow(database.Path);
            ApplyReplayCorruption(database.Path, corruption);
            using var reopened = CreateJournal(database.Path);

            await Assert.That(() => reopened.Read(StreamKey(), [key])).ThrowsExactly<InvalidOperationException>();
        }
    }

    /// <summary>Verifies malformed retained metrics fail closed when counters inspect them.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CorruptedDurableMetricRowsFailClosedDuringCounterReads()
    {
        foreach (var corruption in CreateMetricCorruptions())
        {
            using var database = new TemporaryDatabase();
            _ = SeedReplayRow(database.Path);
            ApplyMetricCorruption(database.Path, corruption);
            using var reopened = CreateJournal(database.Path);

            await Assert.That(() => reopened.LogicalBytes).ThrowsExactly<InvalidOperationException>();
        }
    }

    /// <summary>Verifies an acknowledged server commit survives abrupt writer process termination.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task WhenWriterProcessDiesAfterAcknowledgedServerCommit_ThenReopenRecoversReplay()
    {
        using var database = new TemporaryDatabase();
        var signalPath = System.IO.Path.ChangeExtension(database.Path, $"after-{Guid.NewGuid():N}.signal");
        var operationId = OperationKey(FirstOperationSeed).OperationId.Value;

        await RunCrashChildUntilSignalAsync(database.Path, signalPath, operationId, CrashAfterCommitMode);

        using var reopened = CreateJournal(database.Path);
        var key = new ServerOperationKey(Client, new(operationId));
        var replay = reopened.Read(StreamKey(), [key]);

        await Assert.That(replay.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(replay.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(replay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(replay.Entries[0].OperationKey).IsEqualTo(key);
        await Assert.That(replay.Entries[0].Events[0].ServerCursor).IsEqualTo(FirstCursor);
    }

    /// <summary>Verifies uncommitted rows owned by a killed process roll back before reopen.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task WhenWriterProcessDiesBeforeCommit_ThenReopenIgnoresUncommittedRows()
    {
        using var database = new TemporaryDatabase();
        var signalPath = System.IO.Path.ChangeExtension(database.Path, $"before-{Guid.NewGuid():N}.signal");
        var operationId = OperationKey(FirstOperationSeed).OperationId.Value;
        using (var initialized = CreateJournal(database.Path))
        {
            await Assert.That(initialized.StreamCount).IsEqualTo(0);
        }

        await RunCrashChildUntilSignalAsync(database.Path, signalPath, operationId, CrashBeforeCommitMode);

        using var reopened = CreateJournal(database.Path);
        await Assert.That(reopened.StreamCount).IsEqualTo(0);
        await Assert.That(reopened.LedgerEntryCount).IsEqualTo(0);
        await Assert.That(reopened.EventCount).IsEqualTo(0);
        await Assert.That(reopened.Read(StreamKey(), [new(Client, new(operationId))]).Revision).IsEqualTo(0);
    }

    /// <summary>Child workflow used by the parent process crash tests.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child crash environment is incomplete.</exception>
    [Test]
    public async Task CrashChildPublishesSignalAndWaits()
    {
        var childContext = ReadCrashChildContext();
        if (childContext is null)
        {
            await Assert.That(Environment.GetEnvironmentVariable(CrashChildModeVariable)).IsNull();
            return;
        }

        IDisposable? uncommittedWrite = null;
        try
        {
            if (string.Equals(childContext.Mode, CrashAfterCommitMode, StringComparison.Ordinal))
            {
                using var journal = CreateJournal(childContext.DatabasePath);
                var key = new ServerOperationKey(Client, new(childContext.OperationId));
                var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
                if (result.Status != ServerCommitStatus.Committed)
                {
                    throw new InvalidOperationException("The child process server commit was not acknowledged.");
                }
            }
            else
            {
                uncommittedWrite = BeginUncommittedRawWrite(childContext.DatabasePath);
            }

            await PublishCrashSignalAsync(childContext.SignalPath, childContext.OperationId);
            await Task.Delay(Timeout.InfiniteTimeSpan);
        }
        finally
        {
            uncommittedWrite?.Dispose();
        }
    }

    /// <summary>Verifies corrupted durable schema fails clearly without resetting the database.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CorruptSchemaFailsWithoutDestructiveReset()
    {
        using var database = new TemporaryDatabase();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE oc_server_journal_streams (tenant_id TEXT NOT NULL); PRAGMA user_version = 1;";
            _ = command.ExecuteNonQuery();
        }

        await Assert.That(() => CreateJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
        await using var verify = OpenRawConnection(database.Path);
        await using var count = verify.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'oc_server_journal_streams';";
        await Assert.That(count.ExecuteScalar()).IsEqualTo(1L);
    }

    /// <summary>Checks whether payload byte sequences are identical.</summary>
    /// <param name="left">The first payload.</param>
    /// <param name="right">The second payload.</param>
    /// <returns>Whether both payload byte sequences are identical.</returns>
    private static bool PayloadBytesEqual(PayloadEnvelope? left, PayloadEnvelope? right) =>
        left is null ? right is null : right is not null && left.Payload.Span.SequenceEqual(right.Payload.Span);

    /// <summary>Creates a configured journal.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="clock">The optional clock.</param>
    /// <param name="retention">The optional retention.</param>
    /// <param name="maximumLedgerEntries">The ledger entry limit.</param>
    /// <param name="maximumLogicalBytes">The logical byte limit.</param>
    /// <returns>The configured journal.</returns>
    private static SqliteServerCommitJournal CreateJournal(
        string path,
        ManualTimeProvider? clock = null,
        TimeSpan? retention = null,
        int maximumLedgerEntries = DefaultMaximumLedgerEntries,
        long maximumLogicalBytes = DefaultMaximumLogicalBytes) =>
        new(path, new()
        {
            MaximumStreams = DefaultMaximumStreams,
            MaximumLedgerEntries = maximumLedgerEntries,
            MaximumEvents = DefaultMaximumEvents,
            MaximumLogicalBytes = maximumLogicalBytes,
            OperationRetention = retention ?? TimeSpan.FromMinutes(DefaultRetentionMinutes),
            TimeProvider = clock ?? new(Start),
        });

    /// <summary>Creates a commit plan for the default stream.</summary>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <param name="state">The optional new state.</param>
    /// <param name="stamp">The optional stamp.</param>
    /// <param name="entry">The single terminal entry.</param>
    /// <returns>The commit plan.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServerCommitPlan Plan(long expectedRevision, ServerState? state, ServerWriteStamp? stamp, ServerLedgerEntry entry) =>
        new(StreamKey(), expectedRevision, state, stamp, [entry]);

    /// <summary>Creates a ledger entry.</summary>
    /// <param name="key">The operation key.</param>
    /// <param name="kind">The result kind.</param>
    /// <param name="fingerprintSeed">The fingerprint seed.</param>
    /// <param name="conflicts">The conflicts.</param>
    /// <param name="events">The events.</param>
    /// <returns>The ledger entry.</returns>
    private static ServerLedgerEntry Entry(
        ServerOperationKey key,
        OperationResultKind kind,
        byte fingerprintSeed,
        IReadOnlyList<ResolvedConflict>? conflicts = null,
        IReadOnlyList<RemoteEvent>? events = null) =>
        new(
            key,
            Fingerprint(fingerprintSeed),
            new(key.OperationId, kind, null, FirstVersion),
            conflicts ?? [],
            events ?? [Event(key.OperationId, CursorForSeed(fingerprintSeed), EventPayload)]);

    /// <summary>Gets a deterministic cursor for an operation seed.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The deterministic cursor.</returns>
    private static string CursorForSeed(byte seed) => seed switch
    {
        FirstOperationSeed => FirstCursor,
        SecondOperationSeed => SecondCursor,
        ThirdOperationSeed => ThirdCursor,
        _ => string.Create(CultureInfo.InvariantCulture, $"cursor-{seed}"),
    };

    /// <summary>Creates a server state.</summary>
    /// <param name="version">The version.</param>
    /// <returns>The server state.</returns>
    private static ServerState State(string version) => new(Stream, version, Payload(version));

    /// <summary>Creates a remote event for the default stream.</summary>
    /// <param name="operationId">The causing operation.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="payload">The payload text.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent Event(OperationId operationId, string cursor, string payload) =>
        new(Guid.NewGuid(), Stream, cursor, Start, operationId, Payload(payload), new Dictionary<string, string> { ["kind"] = payload }) { Origin = new(Client, operationId) };

    /// <summary>Creates a payload envelope.</summary>
    /// <param name="value">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope Payload(string value) =>
        new(PayloadContract, SingleEntryCount, PayloadContentType, System.Text.Encoding.UTF8.GetBytes(value), value);

    /// <summary>Creates a write stamp.</summary>
    /// <param name="key">The operation key.</param>
    /// <returns>The write stamp.</returns>
    private static ServerWriteStamp Stamp(ServerOperationKey key) => new(Start, key.ClientId, key.OperationId);

    /// <summary>Creates the default stream key.</summary>
    /// <returns>The stream key.</returns>
    private static ServerStreamKey StreamKey() => new(Tenant, Stream);

    /// <summary>Creates an operation key.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation key.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServerOperationKey OperationKey(int seed) => OperationKey(seed == ThirdOperationSeed ? OtherClient : Client, seed);

    /// <summary>Creates an operation key for a client.</summary>
    /// <param name="clientId">The authenticated client identifier.</param>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation key.</returns>
    private static ServerOperationKey OperationKey(string clientId, int seed) => new(
        clientId,
        new(new Guid(seed, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1])));

    /// <summary>Creates a trusted canonical fingerprint.</summary>
    /// <param name="seed">The fingerprint seed.</param>
    /// <returns>The fingerprint.</returns>
    private static ServerCommitFingerprint Fingerprint(byte seed)
    {
        var bytes = new byte[ServerCommitFingerprint.Length];
        bytes[0] = seed;
        return new(bytes);
    }

    /// <summary>Writes an unsupported schema user version.</summary>
    /// <param name="path">The database path.</param>
    private static void WriteUnsupportedUserVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA user_version = 2;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Writes an unsupported metadata schema version.</summary>
    /// <param name="path">The database path.</param>
    private static void WriteUnsupportedMetadataSchemaVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_server_journal_metadata SET value = '2' WHERE key = 'schema_version';";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes the schema metadata version row.</summary>
    /// <param name="path">The database path.</param>
    private static void DeleteMetadataSchemaVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM oc_server_journal_metadata WHERE key = 'schema_version';";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a partial owned table set.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateMissingOwnedTableSchema(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA user_version = 1;
            CREATE TABLE oc_server_journal_conflicts (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_event_metadata (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_events (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_ledger (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_metadata (id INTEGER NOT NULL);
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates all owned table names with intentionally wrong definitions.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateWrongTableDefinitionSchema(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA user_version = 1;
            CREATE TABLE oc_server_journal_conflicts (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_event_metadata (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_events (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_ledger (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_metadata (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_streams (id INTEGER NOT NULL);
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that removes a stream before it can be updated.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteStreamBeforeUpdateTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_journal_delete_stream_before_update
            BEFORE UPDATE ON oc_server_journal_streams
            BEGIN
                DELETE FROM oc_server_journal_streams WHERE tenant_id = OLD.tenant_id AND stream_id = OLD.stream_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that removes metadata before it can be updated.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteMetadataBeforeUpdateTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_journal_delete_metadata_before_update
            BEFORE UPDATE ON oc_server_journal_metadata
            BEGIN
                DELETE FROM oc_server_journal_metadata WHERE key = OLD.key;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Opens a raw SQLite connection for schema assertions.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The open connection.</returns>
    private static SqliteConnection OpenRawConnection(string path)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString();
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return connection;
    }

    /// <summary>Manual clock used by journal tests.</summary>
    /// <param name="utcNow">The initial timestamp.</param>
    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC timestamp.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Sets the current timestamp.</summary>
        /// <param name="utcNow">The new timestamp.</param>
        internal void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }

    /// <summary>Temporary SQLite database file.</summary>
    private sealed class TemporaryDatabase : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="TemporaryDatabase"/> class.</summary>
        internal TemporaryDatabase()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"rxui-server-journal-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(directory);
            DirectoryPath = directory;
            Path = System.IO.Path.Combine(directory, "journal.db");
        }

        /// <summary>Gets the database path.</summary>
        internal string Path { get; }

        /// <summary>Gets the owning directory.</summary>
        private string DirectoryPath { get; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(DirectoryPath, recursive: true);
    }
}
