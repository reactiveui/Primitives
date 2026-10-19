// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="InMemoryServerCommitJournal"/>.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>The default authenticated tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The alternate authenticated tenant.</summary>
    private const string OtherTenant = "tenant-b";

    /// <summary>The default authenticated client.</summary>
    private const string Client = "client";

    /// <summary>A short authenticated client.</summary>
    private const string ShortClient = "c";

    /// <summary>A longer authenticated client.</summary>
    private const string LongClient = "client-with-longer-retained-identity";

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

    /// <summary>The logical byte limit used for cursor-bound tests.</summary>
    private const long CursorTestMaximumLogicalBytes = 12_288;

    /// <summary>A later journal timestamp in ticks after start.</summary>
    private const int LaterCommitTicks = 10;

    /// <summary>The protocol cursor bound in UTF-8 bytes.</summary>
    private const int CursorUtf8Bound = 4096;

    /// <summary>The UTF-8 byte length of a euro sign.</summary>
    private const int EuroUtf8Bytes = 3;

    /// <summary>The euro sign used for multibyte cursor bounds.</summary>
    private const char EuroSign = '€';

    /// <summary>The first snapshot revision.</summary>
    private const long FirstRevision = 1;

    /// <summary>The second snapshot revision.</summary>
    private const long SecondRevision = 2;

    /// <summary>The guard timeout used by concurrency tests.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The fixed start instant.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 12, 10, 0, 0, TimeSpan.Zero);

    /// <summary>The default stream.</summary>
    private static readonly StreamId Stream = new("stream");

    /// <summary>The alternate stream.</summary>
    private static readonly StreamId OtherStream = new("stream-b");

    /// <summary>Verifies duplicate-response replay keeps the original result, conflicts and events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task LostResponseReadReturnsOriginalTerminalResultConflictsAndEvents()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var conflict = new ResolvedConflict(key.OperationId, "merge", Payload("resolved"));
        var remoteEvent = Event(key.OperationId, FirstCursor, EventPayload);
        var entry = Entry(key, OperationResultKind.Conflict, FirstOperationSeed, [conflict], [remoteEvent]);

        var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), entry));
        var replay = journal.Read(StreamKey(), [key]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(replay.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(replay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(replay.Entries[0].Result.Kind).IsEqualTo(OperationResultKind.Conflict);
        await Assert.That(replay.Entries[0].Conflicts[0]).IsEqualTo(conflict);
        await Assert.That(replay.Entries[0].Events[0]).IsEqualTo(remoteEvent);
        await Assert.That(replay.LastCursor).IsEqualTo(FirstCursor);
        await Assert.That(replay.LastEventSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies identical operation identifiers are scoped by tenant and stream.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SameOperationKeyAcrossScopesIsIsolated()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var first = journal.TryCommit(Plan(StreamKey(), 0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var secondStream = new ServerStreamKey(OtherTenant, OtherStream);
        var secondEntry = Entry(
            key,
            OperationResultKind.Accepted,
            SecondOperationSeed,
            [],
            [Event(OtherStream, key.OperationId, SecondCursor, "other")]);
        var second = journal.TryCommit(Plan(secondStream, 0, State(SecondVersion, OtherStream), Stamp(key), secondEntry));

        await Assert.That(first.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(second.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(journal.Read(StreamKey(), [key]).State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(journal.Read(secondStream, [key]).State?.Version).IsEqualTo(SecondVersion);
    }

    /// <summary>Verifies compare-and-swap fences two candidates built from the same revision.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task TwoCandidatesFromSameExpectedRevisionAllowOnlyOneCommit()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var first = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        var second = journal.TryCommit(Plan(0, State(SecondVersion), Stamp(secondKey), Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));

        var snapshot = journal.Read(StreamKey(), [firstKey, secondKey]);

        await Assert.That(first.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(second.Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].OperationKey).IsEqualTo(firstKey);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies an append-only stale plan rejects rather than applying prepared effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AppendOnlyStalePlanRejectsPreparedEffects()
    {
        var journal = CreateJournal();
        var stateKey = OperationKey(FirstOperationSeed);
        var appendKey = OperationKey(SecondOperationSeed);
        var staleKey = OperationKey(ThirdOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(stateKey), Entry(stateKey, OperationResultKind.Accepted, FirstOperationSeed)));
        var stale = journal.Read(StreamKey(), [staleKey]);
        _ = journal.TryCommit(Plan(SingleEntryCount, null, Stamp(appendKey), Entry(appendKey, OperationResultKind.Rejected, SecondOperationSeed)));

        var result = journal.TryCommit(Plan(stale.Revision, null, Stamp(staleKey), Entry(staleKey, OperationResultKind.Rejected, ThirdOperationSeed)));
        var snapshot = journal.Read(StreamKey(), [appendKey, staleKey]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(snapshot.Revision).IsEqualTo(DoubleEntryCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].OperationKey).IsEqualTo(appendKey);
    }

    /// <summary>Verifies a mixed duplicate and new stale race rejects all prepared state and events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MixedNewAndDuplicateRaceRejectsAllPreparedEffects()
    {
        var journal = CreateJournal();
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

    /// <summary>Verifies a same-operation fingerprint mismatch rejects atomically.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FingerprintMismatchRejectsWithoutMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));

        var mismatch = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, MismatchFingerprintSeed)));
        var snapshot = journal.Read(StreamKey(), [key]);

        await Assert.That(mismatch.Status).IsEqualTo(ServerCommitStatus.IntentMismatch);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.Entries[0].Fingerprint.Matches(Fingerprint(FirstOperationSeed))).IsTrue();
    }

    /// <summary>Verifies a matching duplicate at the current revision replays as stale without mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MatchingDuplicateAtCurrentRevisionRejectsWithoutMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));

        var duplicate = journal.TryCommit(Plan(FirstRevision, State(SecondVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var snapshot = journal.Read(StreamKey(), [key]);

        await Assert.That(duplicate.Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(snapshot.Revision).IsEqualTo(FirstRevision);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies expiry is inclusive and clock rollback does not reopen compacted entries.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReplayExpiryBoundaryAndClockRollbackKeepStateAndSequence()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(1));
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));

        await Assert.That(journal.Compact(Start.AddTicks(SingleEntryCount))).IsEqualTo(0);
        await Assert.That(journal.Read(StreamKey(), [key]).Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(journal.Compact(Start.AddTicks(DoubleEntryCount))).IsEqualTo(SingleEntryCount);
        clock.SetUtcNow(Start);

        var snapshot = journal.Read(StreamKey(), [key]);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(0);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.LastEventSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies rollback commits on another stream use the retained highwater before compaction.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RollbackCommitOnAnotherStreamKeepsHighWaterAcrossCompaction()
    {
        var clock = new ManualTimeProvider(Start.AddTicks(LaterCommitTicks));
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var futureKey = OperationKey(FirstOperationSeed);
        var rollbackKey = OperationKey(SecondOperationSeed);
        var secondStream = new ServerStreamKey(OtherTenant, OtherStream);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(futureKey), Entry(futureKey, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start);

        var rollback = journal.TryCommit(Plan(
            secondStream,
            0,
            State(SecondVersion, OtherStream),
            Stamp(rollbackKey),
            Entry(rollbackKey, OperationResultKind.Accepted, SecondOperationSeed, [], [Event(OtherStream, rollbackKey.OperationId, SecondCursor, EventPayload)])));
        var earlyCompact = journal.Compact(Start.AddTicks(DoubleEntryCount));
        var snapshot = journal.Read(secondStream, [rollbackKey]);

        await Assert.That(rollback.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(earlyCompact).IsEqualTo(0);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].CommittedAtUtc).IsEqualTo(Start.AddTicks(LaterCommitTicks));
        await Assert.That(snapshot.Entries[0].ExpiresAtUtc).IsEqualTo(Start.AddTicks(LaterCommitTicks + SingleEntryCount));
    }

    /// <summary>Verifies accounting properties and default-clock compaction reflect retained rows.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AccountingPropertiesAndDefaultCompactReflectRetainedRows()
    {
        var empty = new InMemoryServerCommitJournal();
        var clock = new ManualTimeProvider(Start);
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));

        await Assert.That(empty.StreamCount).IsEqualTo(0);
        await Assert.That(journal.StreamCount).IsEqualTo(SingleEntryCount);
        await Assert.That(journal.LedgerEntryCount).IsEqualTo(SingleEntryCount);
        await Assert.That(journal.EventCount).IsEqualTo(SingleEntryCount);
        await Assert.That(journal.LogicalBytes).IsGreaterThan(0);

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        await Assert.That(journal.Compact()).IsEqualTo(SingleEntryCount);
        await Assert.That(journal.LedgerEntryCount).IsEqualTo(0);
        await Assert.That(journal.EventCount).IsEqualTo(0);
    }

    /// <summary>Verifies retained last cursor bytes survive event-row expiry.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task LastCursorBytesRemainAccountedAfterEventRowsExpire()
    {
        var eventClock = new ManualTimeProvider(Start);
        var eventJournal = CreateJournal(eventClock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var noEventClock = new ManualTimeProvider(Start);
        var noEventJournal = CreateJournal(noEventClock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var eventKey = OperationKey(FirstOperationSeed);
        var noEventKey = OperationKey(FirstOperationSeed);
        var eventEntry = Entry(eventKey, OperationResultKind.Accepted, FirstOperationSeed);
        var noEventEntry = Entry(noEventKey, OperationResultKind.Accepted, FirstOperationSeed, [], []);

        _ = eventJournal.TryCommit(Plan(0, State(FirstVersion), Stamp(eventKey), eventEntry));
        _ = noEventJournal.TryCommit(Plan(0, State(FirstVersion), Stamp(noEventKey), noEventEntry));
        eventClock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        noEventClock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = eventJournal.Compact();
        _ = noEventJournal.Compact();

        var snapshot = eventJournal.Read(StreamKey(), [eventKey]);
        var retainedCursorBytes = ServerCommitJournalGuard.GetTextBytes(FirstCursor);

        await Assert.That(snapshot.LastCursor).IsEqualTo(FirstCursor);
        await Assert.That(eventJournal.LogicalBytes).IsEqualTo(noEventJournal.LogicalBytes + retainedCursorBytes);
    }

    /// <summary>Verifies retained last cursor bytes still count against the next admission after expiry.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RetainedLastCursorBytesRejectAppendThatWouldExceedLogicalCapacity()
    {
        var noEventClock = new ManualTimeProvider(Start);
        var noEventJournal = CreateJournal(noEventClock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var firstKey = OperationKey(FirstOperationSeed);
        var firstEntry = Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed);
        var noEventEntry = Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, [], []);
        _ = noEventJournal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), noEventEntry));
        noEventClock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = noEventJournal.Compact();
        var secondKey = OperationKey(SecondOperationSeed);
        var secondEntry = Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, [], [Event(secondKey.OperationId, "cursor-22", "event-2")]);
        var maximumBytes = noEventJournal.LogicalBytes
            + ServerCommitJournalGuard.GetTextBytes(FirstCursor)
            + ServerCommitJournalSizer.GetEntryBytes(firstEntry);
        var clock = new ManualTimeProvider(Start);
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount), maximumLogicalBytes: maximumBytes);
        var first = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), firstEntry));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();

        var rejected = journal.TryCommit(Plan(FirstRevision, null, null, secondEntry));
        var snapshot = journal.Read(StreamKey(), [firstKey, secondKey]);

        await Assert.That(ServerCommitJournalSizer.GetEntryBytes(secondEntry)).IsGreaterThan(ServerCommitJournalSizer.GetEntryBytes(firstEntry));
        await Assert.That(first.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(rejected.Status).IsEqualTo(ServerCommitStatus.CapacityExceeded);
        await Assert.That(snapshot.Revision).IsEqualTo(FirstRevision);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(0);
        await Assert.That(snapshot.LastCursor).IsEqualTo(FirstCursor);
    }

    /// <summary>Verifies a trailing eventless entry does not clear the last cursor produced earlier in the plan.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EventlessTrailingEntryKeepsLastCursorFromEarlierPlanEntry()
    {
        var journal = CreateJournal();
        var eventKey = OperationKey(FirstOperationSeed);
        var eventlessKey = OperationKey(SecondOperationSeed);
        var eventEntry = Entry(eventKey, OperationResultKind.Accepted, FirstOperationSeed);
        var eventlessEntry = Entry(eventlessKey, OperationResultKind.Accepted, SecondOperationSeed, [], []);

        var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(eventlessKey), [eventEntry, eventlessEntry]));
        var snapshot = journal.Read(StreamKey(), [eventKey, eventlessKey]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(DoubleEntryCount);
        await Assert.That(snapshot.LastCursor).IsEqualTo(FirstCursor);
        await Assert.That(snapshot.LastEventSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies replay expiry is clamped when a commit arrives near the maximum timestamp.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MaximumTimestampCommitClampsReplayExpiry()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.MaxValue);
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var key = OperationKey(FirstOperationSeed);

        var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var snapshot = journal.Read(StreamKey(), [key]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(snapshot.Entries[0].CommittedAtUtc).IsEqualTo(DateTimeOffset.MaxValue);
        await Assert.That(snapshot.Entries[0].ExpiresAtUtc).IsEqualTo(DateTimeOffset.MaxValue);
    }

    /// <summary>Verifies bounded capacity rejection preserves the prior view and compaction reclaims rows.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BoundedCapacityRejectionPreservesPriorViewUntilCompactionReclaimsRows()
    {
        var journal = CreateJournal(retention: TimeSpan.FromTicks(1), maximumLedgerEntries: 1);
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));

        var rejected = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(secondKey), Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));
        var beforeCompact = journal.Read(StreamKey(), [firstKey, secondKey]);
        var compacted = journal.Compact(Start.AddTicks(DoubleEntryCount));
        var admitted = journal.TryCommit(Plan(SingleEntryCount, null, Stamp(secondKey), Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));
        var after = journal.Read(StreamKey(), [firstKey, secondKey]);

        await Assert.That(rejected.Status).IsEqualTo(ServerCommitStatus.CapacityExceeded);
        await Assert.That(beforeCompact.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(beforeCompact.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(compacted).IsEqualTo(SingleEntryCount);
        await Assert.That(admitted.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(after.Revision).IsEqualTo(DoubleEntryCount);
        await Assert.That(after.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(after.LastEventSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(after.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(after.Entries[0].OperationKey).IsEqualTo(secondKey);
    }

    /// <summary>Verifies retained logical bytes include current write-stamp client identity deltas.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task LogicalBytesTrackWriteStampClientReplacementAndRemoval()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var shortKey = OperationKey(ShortClient, FirstOperationSeed);
        var longKey = OperationKey(LongClient, SecondOperationSeed);
        var nullStampKey = OperationKey(ShortClient, ThirdOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(shortKey), Entry(shortKey, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();
        var shortBytes = journal.LogicalBytes;

        _ = journal.TryCommit(Plan(FirstRevision, State(FirstVersion), Stamp(longKey), Entry(longKey, OperationResultKind.Accepted, SecondOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(LaterCommitTicks));
        _ = journal.Compact();
        var longBytes = journal.LogicalBytes;

        _ = journal.TryCommit(Plan(SecondRevision, State(FirstVersion), null, Entry(nullStampKey, OperationResultKind.Accepted, ThirdOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(LaterCommitTicks + LaterCommitTicks));
        _ = journal.Compact();

        await Assert.That(longBytes).IsGreaterThan(shortBytes);
        await Assert.That(journal.LogicalBytes).IsLessThan(longBytes);
    }

    /// <summary>Verifies owned immutable outputs and source list copies.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CapturedCollectionsAreOwnedAndReadOnly()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var conflicts = new List<ResolvedConflict> { new(key.OperationId, "merge", Payload("resolved")) };
        var events = new List<RemoteEvent> { Event(key.OperationId, FirstCursor, EventPayload) };
        var entry = Entry(key, OperationResultKind.Conflict, FirstOperationSeed, conflicts, events);
        var entries = new List<ServerLedgerEntry> { entry };
        var plan = Plan(0, State(FirstVersion), Stamp(key), entries);
        conflicts.Clear();
        events.Clear();
        entries.Clear();

        _ = journal.TryCommit(plan);
        var snapshot = journal.Read(StreamKey(), [key]);
        var stored = snapshot.Entries[0];

        await Assert.That(stored.Conflicts).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(stored.Events).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(() => ((IList)stored.Conflicts).Add(new ResolvedConflict(key.OperationId, "x", null))).ThrowsExactly<NotSupportedException>();
        await Assert.That(() => ((IList)stored.Events).Add(Event(key.OperationId, SecondCursor, "x"))).ThrowsExactly<NotSupportedException>();
        await Assert.That(() => ((IList)snapshot.Entries).Add(entry)).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Verifies an opaque multibyte cursor is accepted at the UTF-8 protocol bound.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MultibyteCursorAtUtf8BoundIsAccepted()
    {
        var journal = CreateJournal(maximumLogicalBytes: CursorTestMaximumLogicalBytes);
        var key = OperationKey(FirstOperationSeed);
        var exactCursor = $"{new string(EuroSign, CursorUtf8Bound / EuroUtf8Bytes)}a";
        var entry = Entry(key, OperationResultKind.Accepted, FirstOperationSeed, [], [Event(key.OperationId, exactCursor, EventPayload)]);

        var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), entry));
        var snapshot = journal.Read(StreamKey(), [key]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(snapshot.LastCursor).IsEqualTo(exactCursor);
    }

    /// <summary>Verifies invalid capture counts and stream identity are rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task InvalidCaptureCountsAndOversizedStreamIdentityRejectBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var invalidKey = new ServerStreamKey(Tenant, default);
        var result = new OperationSyncResult(key.OperationId, OperationResultKind.Accepted, null, FirstVersion);

        var operationKeys = new NegativeCountList<ServerOperationKey>(NegativeCountList<ServerOperationKey>.InvalidCount);
        var entries = new NegativeCountList<ServerLedgerEntry>(NegativeCountList<ServerLedgerEntry>.InvalidCount);
        var conflicts = new NegativeCountList<ResolvedConflict>(NegativeCountList<ResolvedConflict>.InvalidCount);
        await Assert.That(() => journal.Read(StreamKey(), operationKeys)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServerCommitPlan(StreamKey(), 0, null, null, entries)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => new ServerLedgerEntry(key, Fingerprint(FirstOperationSeed), result, conflicts, [])).ThrowsExactly<ArgumentOutOfRangeException>();
        var invalidPlan = Plan(invalidKey, 0, null, Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed));

        await Assert.That(() => journal.TryCommit(invalidPlan)).ThrowsExactly<ArgumentException>();
        await Assert.That(journal.StreamCount).IsEqualTo(0);
    }

    /// <summary>Verifies invalid durable terminal inputs are rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task InvalidTerminalInputsRejectBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var retryable = Entry(key, OperationResultKind.Retryable, FirstOperationSeed);
        var wrongStream = Entry(key, OperationResultKind.Accepted, FirstOperationSeed, [], [Event(OtherStream, key.OperationId, FirstCursor, "other")]);

        await Assert.That(() => journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), retryable))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), wrongStream))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.TryCommit(new(StreamKey(), 0, State(FirstVersion), null, []))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(journal.Read(StreamKey(), [key]).Revision).IsEqualTo(0);
    }

    /// <summary>Verifies concurrent reads observe no partial commit view.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConcurrentReadAndCommitNeverExposePartialView()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        using var start = new ManualResetEventSlim();
        using var firstRead = new ManualResetEventSlim();
        using var stop = new CancellationTokenSource();
        var readerState = (journal, firstKey, secondKey, start, firstRead, stop.Token);
        var reader = Task.Factory.StartNew(
            static state =>
            {
                var context = ((InMemoryServerCommitJournal Journal, ServerOperationKey FirstKey, ServerOperationKey SecondKey,
                    ManualResetEventSlim Start, ManualResetEventSlim FirstRead, CancellationToken Token))state!;
                return ReadUntilStopped(context.Journal, context.FirstKey, context.SecondKey, context.Start, context.FirstRead, context.Token);
            },
            readerState,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        var writerState = (Journal: journal, SecondKey: secondKey, FirstRead: firstRead);
        var writer = Task.Factory.StartNew(
            static state =>
            {
                var context = ((InMemoryServerCommitJournal Journal, ServerOperationKey SecondKey, ManualResetEventSlim FirstRead))state!;
                if (!context.FirstRead.Wait(GuardTimeout, CancellationToken.None))
                {
                    throw new TimeoutException("The reader did not observe the pre-commit view.");
                }

                var entry = Entry(context.SecondKey, OperationResultKind.Accepted, SecondOperationSeed);
                return context.Journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(context.SecondKey), entry));
            },
            writerState,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

        var reads = 0;
        start.Set();
        try
        {
            var result = await writer.WaitAsync(GuardTimeout);

            await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
            await Assert.That(journal.Read(StreamKey(), [firstKey, secondKey]).Entries).Count().IsEqualTo(DoubleEntryCount);
        }
        finally
        {
            await stop.CancelAsync();
            reads = await reader.WaitAsync(GuardTimeout);
        }

        await Assert.That(reads).IsGreaterThan(0);
    }

    /// <summary>Reads until cancellation and checks every snapshot is internally consistent.</summary>
    /// <param name="journal">The journal.</param>
    /// <param name="firstKey">The first key.</param>
    /// <param name="secondKey">The second key.</param>
    /// <param name="start">The start signal.</param>
    /// <param name="firstRead">The first-read signal.</param>
    /// <param name="token">The cancellation token.</param>
    /// <returns>The read count.</returns>
    /// <exception cref="InvalidOperationException">A partial view was observed.</exception>
    private static int ReadUntilStopped(
        InMemoryServerCommitJournal journal,
        ServerOperationKey firstKey,
        ServerOperationKey secondKey,
        ManualResetEventSlim start,
        ManualResetEventSlim firstRead,
        CancellationToken token)
    {
        _ = start.Wait(GuardTimeout, CancellationToken.None);
        var reads = 0;
        while (!token.IsCancellationRequested)
        {
            var snapshot = journal.Read(StreamKey(), [firstKey, secondKey]);
            if (reads == 0)
            {
                firstRead.Set();
            }

            if (snapshot.Revision == FirstRevision && snapshot.Entries.Count != SingleEntryCount)
            {
                throw new InvalidOperationException("Revision 1 must expose only the first operation.");
            }

            if (IsInvalidSecondRevisionSnapshot(snapshot))
            {
                throw new InvalidOperationException("Revision 2 must expose the second operation, state and event sequence together.");
            }

            reads++;
        }

        return reads;
    }

    /// <summary>Checks whether a second-revision snapshot is internally inconsistent.</summary>
    /// <param name="snapshot">The snapshot.</param>
    /// <returns>Whether the snapshot is inconsistent.</returns>
    private static bool IsInvalidSecondRevisionSnapshot(ServerCommitSnapshot snapshot) =>
        snapshot.Revision == SecondRevision
        && (snapshot.Entries.Count != DoubleEntryCount || snapshot.State?.Version != SecondVersion || snapshot.LastEventSequence != SecondRevision);

    /// <summary>Creates a configured journal.</summary>
    /// <param name="clock">The optional clock.</param>
    /// <param name="retention">The optional retention.</param>
    /// <param name="maximumLedgerEntries">The ledger entry limit.</param>
    /// <param name="maximumLogicalBytes">The logical byte limit.</param>
    /// <returns>The configured journal.</returns>
    private static InMemoryServerCommitJournal CreateJournal(
        ManualTimeProvider? clock = null,
        TimeSpan? retention = null,
        int maximumLedgerEntries = DefaultMaximumLedgerEntries,
        long maximumLogicalBytes = DefaultMaximumLogicalBytes) =>
        new(new()
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
        Plan(StreamKey(), expectedRevision, state, stamp, entry);

    /// <summary>Creates a commit plan for a stream.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <param name="state">The optional new state.</param>
    /// <param name="stamp">The optional stamp.</param>
    /// <param name="entry">The single terminal entry.</param>
    /// <returns>The commit plan.</returns>
    private static ServerCommitPlan Plan(
        ServerStreamKey streamKey,
        long expectedRevision,
        ServerState? state,
        ServerWriteStamp? stamp,
        ServerLedgerEntry entry) =>
        new(streamKey, expectedRevision, state, stamp, [entry]);

    /// <summary>Creates a commit plan with an entry list.</summary>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <param name="state">The optional new state.</param>
    /// <param name="stamp">The optional stamp.</param>
    /// <param name="entries">The terminal entries.</param>
    /// <returns>The commit plan.</returns>
    private static ServerCommitPlan Plan(
        long expectedRevision,
        ServerState? state,
        ServerWriteStamp? stamp,
        IReadOnlyList<ServerLedgerEntry> entries) =>
        new(StreamKey(), expectedRevision, state, stamp, entries);

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
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The server state.</returns>
    private static ServerState State(string version, StreamId? streamId = null) => new(streamId ?? Stream, version, Payload(version));

    /// <summary>Creates a remote event for the default stream.</summary>
    /// <param name="operationId">The causing operation.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="payload">The payload text.</param>
    /// <returns>The remote event.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemoteEvent Event(OperationId operationId, string cursor, string payload) => Event(Stream, operationId, cursor, payload);

    /// <summary>Creates a remote event.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="operationId">The causing operation.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="payload">The payload text.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent Event(StreamId streamId, OperationId operationId, string cursor, string payload) =>
        new(Guid.NewGuid(), streamId, cursor, Start, operationId, Payload(payload), new Dictionary<string, string> { ["kind"] = payload });

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
    private static ServerOperationKey OperationKey(int seed) => OperationKey(Client, seed);

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

    /// <summary>Custom list that reports an invalid negative count.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="count">The reported count.</param>
    private sealed class NegativeCountList<T>(int count) : IReadOnlyList<T>
    {
        /// <summary>An invalid negative collection count.</summary>
        internal const int InvalidCount = -1;

        /// <inheritdoc/>
        public int Count => count;

        /// <inheritdoc/>
        public T this[int index] => throw new InvalidOperationException("Negative-count lists cannot be indexed.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator()
        {
            yield break;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
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
}
