// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Completes coverage for in-memory durable subscription start positions.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>The third server event sequence used by resolver coverage.</summary>
    private const long ResolverThirdEventSequence = 3;

    /// <summary>Verifies the internal registration overload dispatches through the acknowledgement interface.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionStartPositionInterfaceOverloadRegistersAndRejectsForeignOffer()
    {
        IServerSubscriptionAcknowledgementJournal journal = CreateSubscriptionJournal();
        var identity = SubscriptionIdentity(FirstSubscription);
        var foreign = new ServerSubscriptionIdentity(new(OtherTenant, Stream), Client, FirstSubscription);

        var state = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.Latest));

        await Assert.That(state.Identity.SubscriptionId).IsEqualTo(FirstSubscription);
        await Assert.That(() => journal.OfferReceivePage(new(foreign, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies future in-memory timestamp positions wait and then durably resolve their anchor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionFromTimestampFutureResolvesInMemoryAnchor()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(clock);
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        var identity = SubscriptionIdentity(FirstSubscription);
        var future = Start.AddTicks(DoubleEntryCount);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromTimestamp(future)));

        var empty = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        clock.SetUtcNow(future);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(batch.PreviousCursor).IsNull();
        await Assert.That(batch.NextCursor).IsEqualTo(SecondCursor);
    }

    /// <summary>Verifies future in-memory sequence positions wait when the stream does not exist.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionFromSequenceMissingInMemoryStreamWaitsAtZeroFrontier()
    {
        var journal = CreateSubscriptionJournal();
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(SingleEntryCount)));

        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(page.LastGroupSequence).IsEqualTo(0);
    }

    /// <summary>Verifies missing cursor registration fails closed over in-memory retained-history gaps.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionFromCursorInMemoryGapReturnsRetentionGap()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var first = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        _ = journal.Compact();
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromCursor(FirstCursor)));

        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(page.LastGroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Verifies shared start-position resolver branches over retained in-memory stream records.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionStartPositionResolverCoversRetainedAnchorBranches()
    {
        var stream = CreateResolverStream();
        var streamKey = StreamKey();
        var latestGap = new ServerCommitStreamRecord { LastGroupSequence = SingleEntryCount, HasReceiveHistoryGap = true };
        var timestampGap = CreateSingleResolverGroup(Start.AddTicks(DoubleEntryCount), hasGap: true);

        var latestResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.Latest, null, out var latestAnchor);
        var latestGapResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.Latest, latestGap, out _);
        var sequenceZeroResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromSequence(0), null, out var sequenceZeroAnchor);
        var sequencePendingResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromSequence(SingleEntryCount), null, out _);
        var sequenceResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromSequence(ResolverThirdEventSequence), stream, out var sequenceAnchor);
        var sequenceGap = CreateGappedSequenceStream();
        var sequenceEmpty = new ServerCommitStreamRecord { LastEventSequence = SingleEntryCount };
        var sequenceGapResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromSequence(SingleEntryCount), sequenceGap, out _);
        var sequenceEmptyResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromSequence(SingleEntryCount), sequenceEmpty, out _);
        var timestampPendingResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromTimestamp(Start), null, out _);
        var timestampAfterResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromTimestamp(Start.AddTicks(DoubleEntryCount)), stream, out _);
        var timestampGapResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromTimestamp(Start), timestampGap, out _);
        var timestampResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromTimestamp(Start), stream, out var timestampAnchor);
        var groupCursor = ServerReceiveGroupCursor.Create(streamKey, SingleEntryCount);
        var zeroGroupCursor = ServerReceiveGroupCursor.Create(streamKey, 0);
        var groupCursorResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromCursor(groupCursor), stream, out var groupCursorAnchor);
        var zeroGroupCursorResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromCursor(zeroGroupCursor), stream, out var zeroGroupCursorAnchor);
        var missingCursorResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromCursor(ThirdCursor), null, out _);
        var finalCursorResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromCursor(SecondCursor), stream, out var finalCursorAnchor);
        var absentCursorResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromCursor("cursor-absent"), stream, out _);

        await Assert.That(latestResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(latestAnchor.GroupSequence).IsEqualTo(0);
        await Assert.That(latestGapResolution).IsEqualTo(ServerSubscriptionAnchorResolution.RetentionGap);
        await Assert.That(sequenceZeroResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(sequenceZeroAnchor.GroupSequence).IsEqualTo(0);
        await Assert.That(sequencePendingResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Pending);
        await Assert.That(sequenceResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(sequenceAnchor.GroupSequence).IsEqualTo(SingleEntryCount);
        await Assert.That(sequenceGapResolution).IsEqualTo(ServerSubscriptionAnchorResolution.RetentionGap);
        await Assert.That(sequenceEmptyResolution).IsEqualTo(ServerSubscriptionAnchorResolution.RetentionGap);
        await Assert.That(timestampPendingResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Pending);
        await Assert.That(timestampAfterResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Pending);
        await Assert.That(timestampGapResolution).IsEqualTo(ServerSubscriptionAnchorResolution.RetentionGap);
        await Assert.That(timestampResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(timestampAnchor.GroupSequence).IsEqualTo(0);
        await Assert.That(groupCursorResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(groupCursorAnchor.Cursor).IsEqualTo(groupCursor);
        await Assert.That(zeroGroupCursorResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(zeroGroupCursorAnchor.Cursor).IsNull();
        await Assert.That(missingCursorResolution).IsEqualTo(ServerSubscriptionAnchorResolution.RetentionGap);
        await Assert.That(finalCursorResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(finalCursorAnchor.GroupSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(absentCursorResolution).IsEqualTo(ServerSubscriptionAnchorResolution.RetentionGap);
        await Assert.That(() => ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromCursor(ThirdCursor), stream, out _))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(() => ServerSubscriptionStartPositionOperations.TryResolveAnchor(streamKey, StartPosition.FromCursor(SecondCursor), CreateNullGroupSequenceStream(), out _))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies start-position cursor resolution skips retained operation groups that produced no events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionStartPositionResolverSkipsZeroEventGroupForEventCursor()
    {
        var streamKey = StreamKey();
        var stream = new ServerCommitStreamRecord();
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var secondEntry = Entry(
            second,
            OperationResultKind.Accepted,
            SecondOperationSeed,
            events: [Event(second.OperationId, SecondCursor, EventPayload)]);
        ServerCommitJournalOperations.AddLedgerRow(
            stream,
            streamKey,
            Entry(first, OperationResultKind.Accepted, FirstOperationSeed, events: []).Commit(Start, Start.AddMinutes(DefaultRetentionMinutes)),
            0);
        ServerCommitJournalOperations.AddLedgerRow(
            stream,
            streamKey,
            secondEntry.Commit(Start, Start.AddMinutes(DefaultRetentionMinutes)),
            0);

        var resolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(
            streamKey,
            StartPosition.FromCursor(SecondCursor),
            stream,
            out var anchor);

        await Assert.That(resolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(anchor.GroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Verifies timestamp anchors require a retained predecessor when receive history has gaps.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionStartPositionResolverRequiresTimestampBoundaryProof()
    {
        var streamKey = StreamKey();
        var threshold = Start.AddTicks(SingleEntryCount);
        var startPosition = StartPosition.FromTimestamp(threshold);
        var timestampInteriorGap = CreateTimestampInteriorGapStream();
        var timestampProvenAfterGap = CreateTimestampProvenAfterGapStream();

        var timestampInteriorGapResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(
            streamKey,
            startPosition,
            timestampInteriorGap,
            out _);
        var timestampProvenAfterGapResolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(
            streamKey,
            startPosition,
            timestampProvenAfterGap,
            out var timestampProvenAnchor);

        await Assert.That(timestampInteriorGapResolution).IsEqualTo(ServerSubscriptionAnchorResolution.RetentionGap);
        await Assert.That(timestampProvenAfterGapResolution).IsEqualTo(ServerSubscriptionAnchorResolution.Resolved);
        await Assert.That(timestampProvenAnchor.GroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Creates a retained stream with two eventful receive groups.</summary>
    /// <returns>The retained stream.</returns>
    private static ServerCommitStreamRecord CreateResolverStream()
    {
        var stream = new ServerCommitStreamRecord();
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var secondEntry = Entry(
            second,
            OperationResultKind.Accepted,
            SecondOperationSeed,
            events:
            [
                Event(second.OperationId, ThirdCursor, EventPayload),
                Event(second.OperationId, SecondCursor, EventPayload),
            ]);
        ServerCommitJournalOperations.AddLedgerRow(stream, StreamKey(), Entry(first, OperationResultKind.Accepted, FirstOperationSeed).Commit(Start, Start.AddMinutes(DefaultRetentionMinutes)), 0);
        ServerCommitJournalOperations.AddLedgerRow(stream, StreamKey(), secondEntry.Commit(Start, Start.AddMinutes(DefaultRetentionMinutes)), 0);
        return stream;
    }

    /// <summary>Creates a stream whose retained first event sequence cannot prove the requested sequence.</summary>
    /// <returns>The retained stream.</returns>
    private static ServerCommitStreamRecord CreateGappedSequenceStream()
    {
        var stream = new ServerCommitStreamRecord { LastEventSequence = DoubleEntryCount, LastGroupSequence = DoubleEntryCount, HasReceiveHistoryGap = true };
        var key = OperationKey(SecondOperationSeed);
        var entry = Entry(key, OperationResultKind.Accepted, SecondOperationSeed).Commit(Start, Start.AddMinutes(DefaultRetentionMinutes));
        var row = new ServerCommitLedgerRow(StreamKey(), entry, 0, DoubleEntryCount);
        stream.Groups.Add(row);
        stream.Events.Add(new(row, entry.Events[0], DoubleEntryCount));
        return stream;
    }

    /// <summary>Creates a stream with one group and a configurable gap marker.</summary>
    /// <param name="committedAtUtc">The commit timestamp.</param>
    /// <param name="hasGap">Whether history is incomplete.</param>
    /// <returns>The retained stream.</returns>
    private static ServerCommitStreamRecord CreateSingleResolverGroup(DateTimeOffset committedAtUtc, bool hasGap)
    {
        var stream = new ServerCommitStreamRecord { HasReceiveHistoryGap = hasGap };
        var key = OperationKey(FirstOperationSeed);
        ServerCommitJournalOperations.AddLedgerRow(
            stream,
            StreamKey(),
            Entry(key, OperationResultKind.Accepted, FirstOperationSeed).Commit(committedAtUtc, committedAtUtc.AddMinutes(DefaultRetentionMinutes)),
            0);
        return stream;
    }

    /// <summary>Creates a gapped stream where a missing interior group could satisfy the timestamp threshold.</summary>
    /// <returns>The retained stream.</returns>
    private static ServerCommitStreamRecord CreateTimestampInteriorGapStream()
    {
        var stream = new ServerCommitStreamRecord { HasReceiveHistoryGap = true };
        AddTimestampResolverGroup(stream, FirstOperationSeed, Start, 0);
        AddTimestampResolverGroup(stream, ThirdOperationSeed, Start.AddTicks(SingleEntryCount), ResolverThirdEventSequence);
        return stream;
    }

    /// <summary>Creates a gapped stream whose retained predecessor proves the timestamp boundary.</summary>
    /// <returns>The retained stream.</returns>
    private static ServerCommitStreamRecord CreateTimestampProvenAfterGapStream()
    {
        var stream = new ServerCommitStreamRecord { HasReceiveHistoryGap = true };
        AddTimestampResolverGroup(stream, FirstOperationSeed, Start.AddTicks(-SingleEntryCount), DoubleEntryCount);
        AddTimestampResolverGroup(stream, ThirdOperationSeed, Start.AddTicks(SingleEntryCount), ResolverThirdEventSequence);
        return stream;
    }

    /// <summary>Adds a timestamp resolver group with an explicit durable group sequence.</summary>
    /// <param name="stream">The stream to mutate.</param>
    /// <param name="seed">The operation seed.</param>
    /// <param name="committedAtUtc">The commit timestamp.</param>
    /// <param name="groupSequence">The durable group sequence.</param>
    private static void AddTimestampResolverGroup(ServerCommitStreamRecord stream, byte seed, DateTimeOffset committedAtUtc, long groupSequence)
    {
        var key = OperationKey(seed);
        ServerCommitJournalOperations.AddLedgerRow(
            stream,
            StreamKey(),
            Entry(key, OperationResultKind.Accepted, seed, events: []).Commit(committedAtUtc, committedAtUtc.AddMinutes(DefaultRetentionMinutes)),
            0,
            groupSequence);
    }

    /// <summary>Creates a malformed retained stream with an eventful row missing its group sequence.</summary>
    /// <returns>The retained stream.</returns>
    private static ServerCommitStreamRecord CreateNullGroupSequenceStream()
    {
        var stream = new ServerCommitStreamRecord { LastEventSequence = SingleEntryCount, LastGroupSequence = SingleEntryCount };
        var key = OperationKey(FirstOperationSeed);
        var entry = Entry(key, OperationResultKind.Accepted, FirstOperationSeed, events: [Event(key.OperationId, SecondCursor, EventPayload)]).Commit(Start, Start.AddMinutes(DefaultRetentionMinutes));
        var row = new ServerCommitLedgerRow(StreamKey(), entry, 0, null);
        stream.Groups.Add(row);
        stream.Events.Add(new(row, entry.Events[0], SingleEntryCount));
        return stream;
    }
}
