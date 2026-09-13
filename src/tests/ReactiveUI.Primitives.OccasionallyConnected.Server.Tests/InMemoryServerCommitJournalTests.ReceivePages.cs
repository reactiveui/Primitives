// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests receive paging for <see cref="InMemoryServerCommitJournal"/>.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>The logical GUID byte count used by receive transport validation.</summary>
    private const int ReceiveGuidByteCount = 16;

    /// <summary>The logical integer byte count used by receive transport validation.</summary>
    private const int ReceiveIntByteCount = 4;

    /// <summary>The logical timestamp byte count used by receive transport validation.</summary>
    private const int ReceiveDateTimeOffsetByteCount = 16;

    /// <summary>The logical nullable marker byte count used by receive transport validation.</summary>
    private const int ReceiveNullableMarkerByteCount = 1;

    /// <summary>The scale used for large cursor and hash text.</summary>
    private const int LargeTextScale = 32;

    /// <summary>The scale used for large metadata and payload text.</summary>
    private const int MediumTextScale = 64;

    /// <summary>The expected probe-page failure message.</summary>
    private const string MissingProbeBatchMessage = "The probe page did not return a batch.";

    /// <summary>Verifies receive paging returns complete operation groups, including zero-event acceptances.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesReturnCompleteGroupsAndZeroEventCompletions()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var firstEvent = Event(firstKey.OperationId, FirstCursor, EventPayload);
        var entries = new[]
        {
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [firstEvent]),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: []),
        };
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), entries));

        var firstPage = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var firstBatch = firstPage.Batch ?? throw new InvalidOperationException("The first page did not return a batch.");
        var secondPage = journal.ReadReceivePage(new(StreamKey(), firstBatch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var secondBatch = secondPage.Batch ?? throw new InvalidOperationException("The second page did not return a batch.");
        var end = journal.ReadReceivePage(new(StreamKey(), secondBatch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(firstPage.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(firstPage.NextGroupSequence).IsEqualTo(SingleEntryCount);
        await Assert.That(firstPage.LastGroupSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(firstBatch.PreviousCursor).IsNull();
        await Assert.That(firstBatch.NextCursor).IsEqualTo(firstEvent.ServerCursor);
        await Assert.That(firstBatch.Events).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(firstBatch.CompletedOperations).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(firstBatch.CompletedOperations[0].Origin).IsEqualTo(new(Client, firstKey.OperationId));
        await Assert.That(firstBatch.CompletedOperations[0].EventIds[0]).IsEqualTo(firstEvent.EventId);
        await Assert.That(secondPage.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(secondBatch.PreviousCursor).IsEqualTo(firstBatch.NextCursor);
        await Assert.That(secondBatch.Events).Count().IsEqualTo(0);
        await Assert.That(secondBatch.CompletedOperations).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(secondBatch.CompletedOperations[0].Origin).IsEqualTo(new(Client, secondKey.OperationId));
        await Assert.That(secondBatch.CompletedOperations[0].EventIds).Count().IsEqualTo(0);
        await Assert.That(end.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(end.NextGroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Verifies receive cursors are scoped to the authenticated tenant and stream.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesRejectForeignAndFutureCursors()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        var otherStream = new ServerStreamKey(Tenant, OtherStream);
        var otherStreamCursor = ServerReceiveGroupCursor.Create(StreamKey(), SingleEntryCount);

        await Assert.That(() => journal.ReadReceivePage(new(otherStream, otherStreamCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.ReadReceivePage(new(
                StreamKey(),
                ServerReceiveGroupCursor.Create(StreamKey(), ThirdOperationSeed),
                SingleEntryCount,
                DefaultMaximumEvents,
                DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies receive paging reports a retention gap instead of claiming completeness after compaction.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesReportRetentionGapAfterGroupExpiry()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();

        var page = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(page.Batch).IsNull();
        await Assert.That(page.LastGroupSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies event batches do not claim trailing zero-event groups behind a final event cursor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesLeaveTrailingZeroEventGroupsForGroupCursorPage()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var firstEvent = Event(firstKey.OperationId, FirstCursor, EventPayload);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), [
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [firstEvent]),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: []),
        ]));

        var firstPage = journal.ReadReceivePage(new(StreamKey(), null, DoubleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var firstBatch = firstPage.Batch ?? throw new InvalidOperationException("The first page did not return a batch.");
        var secondPage = journal.ReadReceivePage(new(StreamKey(), firstBatch.NextCursor, DoubleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var secondBatch = secondPage.Batch ?? throw new InvalidOperationException("The second page did not return a batch.");

        await Assert.That(firstPage.NextGroupSequence).IsEqualTo(SingleEntryCount);
        await Assert.That(firstBatch.NextCursor).IsEqualTo(firstEvent.ServerCursor);
        await Assert.That(firstBatch.CompletedOperations).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(secondPage.NextGroupSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(secondBatch.Events).Count().IsEqualTo(0);
        await Assert.That(secondBatch.CompletedOperations[0].Origin).IsEqualTo(new(Client, secondKey.OperationId));
    }

    /// <summary>Verifies retained cursors inside a group cannot advance the group cursor early.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesRejectNonFinalEventCursor()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var firstEvent = Event(firstKey.OperationId, FirstCursor, EventPayload);
        var secondEvent = Event(firstKey.OperationId, SecondCursor, EventPayload);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [firstEvent, secondEvent])));

        await Assert.That(() => journal.ReadReceivePage(new(StreamKey(), firstEvent.ServerCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies rejected terminal groups advance receive cursors without claiming included effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesDoNotCompleteRejectedOperations()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Rejected, FirstOperationSeed, events: [])));

        var page = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException("The page did not return a batch.");

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(batch.Events).Count().IsEqualTo(0);
        await Assert.That(batch.CompletedOperations).Count().IsEqualTo(0);
    }

    /// <summary>Verifies receive paging reports a gap when earlier groups expire but later groups remain.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesReportRetentionGapBeforeRemainingGroups()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(secondKey), Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));
        _ = journal.Compact();

        var page = journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(page.LastGroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Verifies receive paging rejects a first group that cannot fit event or logical byte bounds.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesRejectFirstGroupWhenPageBoundsCannotFit()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(
            firstKey,
            OperationResultKind.Accepted,
            FirstOperationSeed,
            events: [Event(firstKey.OperationId, FirstCursor, EventPayload), Event(firstKey.OperationId, SecondCursor, EventPayload)])));

        await Assert.That(() => journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, SingleEntryCount, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(() => journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, SingleEntryCount)))
            .ThrowsExactly<QueueCapacityExceededException>();
    }

    /// <summary>Verifies receive cursors reject malformed group cursors and unresolved event cursors safely.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesValidateGroupCursorShapeAndUnknownEventCursor()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(
            firstKey,
            OperationResultKind.Accepted,
            FirstOperationSeed,
            events: [Event(firstKey.OperationId, FirstCursor, EventPayload), Event(firstKey.OperationId, SecondCursor, EventPayload)])));

        var unknown = journal.ReadReceivePage(new(StreamKey(), ThirdCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(unknown.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(() => journal.ReadReceivePage(new(StreamKey(), string.Empty, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.ReadReceivePage(new(StreamKey(), "ocg:missing", SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, 0)))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => ServerReceiveGroupCursor.Create(StreamKey(), -SingleEntryCount))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies absent streams distinguish empty, future group, and unresolved event cursors.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesHandleMissingStreamCursors()
    {
        var request = new ServerReceivePageRequest(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes);
        var unknown = ServerReceivePageOperations.Create(new(StreamKey(), FirstCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes), null);
        var empty = ServerReceivePageOperations.Create(request, null);

        await Assert.That(empty.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(unknown.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(static () => ServerReceivePageOperations.Create(
                new(StreamKey(), ServerReceiveGroupCursor.Create(StreamKey(), SingleEntryCount), SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes),
                null))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies group cursors remain compact for maximum-length valid stream bindings.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesCreateCompactGroupCursorForLongStreamBindings()
    {
        const int CursorBindingScale = 17;
        var streamKey = new ServerStreamKey(new string('t', CursorUtf8Bound / CursorBindingScale), new(new string('s', CursorUtf8Bound / CursorBindingScale)));
        var cursor = ServerReceiveGroupCursor.Create(streamKey, long.MaxValue);
        var sequence = ServerReceiveGroupCursor.Parse(streamKey, cursor);

        await Assert.That(cursor.Length < CursorUtf8Bound).IsTrue();
        await Assert.That(sequence).IsEqualTo(long.MaxValue);
    }

    /// <summary>Verifies compact group cursors reject another valid stream binding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesRejectGroupCursorForAnotherStreamBinding()
    {
        var first = new ServerStreamKey("a\u001Fb", new("c"));
        var second = new ServerStreamKey("a", new("b-c"));
        var cursor = ServerReceiveGroupCursor.Create(first, SingleEntryCount);

        await Assert.That(ServerReceiveGroupCursor.Create(second, SingleEntryCount)).IsNotEqualTo(cursor);
        await Assert.That(() => ServerReceiveGroupCursor.Parse(second, cursor)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies receive page byte budgets include the final cursor and full event envelope at the exact boundary.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesRespectExactLogicalByteBoundary()
    {
        var firstKey = OperationKey(FirstOperationSeed);
        var remoteEvent = CreateSizedEvent(firstKey.OperationId);
        var probe = CreateJournal();
        _ = probe.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [remoteEvent])));
        var probedBatch = probe.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, CursorTestMaximumLogicalBytes)).Batch
            ?? throw new InvalidOperationException(MissingProbeBatchMessage);
        var exactBytes = CountReceiveBatchBytes(probedBatch);

        var exactJournal = CreateJournal();
        _ = exactJournal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [remoteEvent])));
        var exact = exactJournal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, exactBytes));
        var belowJournal = CreateJournal();
        _ = belowJournal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [remoteEvent])));

        await Assert.That(exact.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(() => belowJournal.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, exactBytes - SingleEntryCount)))
            .ThrowsExactly<QueueCapacityExceededException>();
    }

    /// <summary>Verifies a second oversized event group does not remove an already fitted eventful group.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesKeepFirstEventGroupWhenSecondEventGroupExceedsBytes()
    {
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var firstEvent = Event(firstKey.OperationId, FirstCursor, EventPayload);
        var secondEvent = CreateSizedEvent(secondKey.OperationId);
        var probe = CreateJournal();
        _ = probe.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), [
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [firstEvent]),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: [secondEvent]),
        ]));
        var firstOnlyBatch = probe.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, CursorTestMaximumLogicalBytes)).Batch
            ?? throw new InvalidOperationException(MissingProbeBatchMessage);
        var exactBytes = CountReceiveBatchBytes(firstOnlyBatch);

        var journal = CreateJournal();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), [
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: [firstEvent]),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: [secondEvent]),
        ]));
        var page = journal.ReadReceivePage(new(StreamKey(), null, DoubleEntryCount, DefaultMaximumEvents, exactBytes));

        await Assert.That(page.NextGroupSequence).IsEqualTo(SingleEntryCount);
        await Assert.That(page.Batch?.NextCursor).IsEqualTo(FirstCursor);
    }

    /// <summary>Verifies a fitted zero-event group remains pageable when a following event group exceeds bytes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected receive page is missing.</exception>
    [Test]
    public async Task ReceivePagesKeepZeroEventGroupWhenSecondEventGroupExceedsBytes()
    {
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var secondEvent = CreateSizedEvent(secondKey.OperationId);
        var probe = CreateJournal();
        _ = probe.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), [
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: []),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: [secondEvent]),
        ]));
        var firstOnlyBatch = probe.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, CursorTestMaximumLogicalBytes)).Batch
            ?? throw new InvalidOperationException(MissingProbeBatchMessage);
        var exactBytes = CountReceiveBatchBytes(firstOnlyBatch);

        var journal = CreateJournal();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), [
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: []),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: [secondEvent]),
        ]));
        var page = journal.ReadReceivePage(new(StreamKey(), null, DoubleEntryCount, DefaultMaximumEvents, exactBytes));
        var batch = page.Batch ?? throw new InvalidOperationException("The page did not return a batch.");

        await Assert.That(page.NextGroupSequence).IsEqualTo(SingleEntryCount);
        await Assert.That(batch.Events).Count().IsEqualTo(0);
        await Assert.That(batch.CompletedOperations[0].Origin).IsEqualTo(new(Client, firstKey.OperationId));
    }

    /// <summary>Verifies malformed retained group rows fail closed during receive paging.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesRejectRetainedGroupWithoutSequence()
    {
        var stream = new ServerCommitStreamRecord { LastGroupSequence = SingleEntryCount };
        var firstKey = OperationKey(FirstOperationSeed);
        stream.Groups.Add(new(StreamKey(), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed), SingleEntryCount, null));

        await Assert.That(() => ServerReceivePageOperations.Create(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes), stream))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies event cursor resolution skips retained operation groups that produced no events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesResolveEventCursorAfterZeroEventGroup()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var secondEvent = Event(secondKey.OperationId, SecondCursor, EventPayload);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), [
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed, events: []),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed, events: [secondEvent]),
        ]));

        var page = journal.ReadReceivePage(new(StreamKey(), SecondCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(page.NextGroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Verifies receive page dispatch through the journal interface.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReceivePagesUseCommitJournalInterface()
    {
        IServerCommitJournal journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));

        var snapshot = journal.Read(StreamKey(), [firstKey]);
        var page = ((IServerReceiveJournal)journal).ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.Page);
    }

    /// <summary>Creates a remote event with non-trivial cursor, metadata, origin and payload bytes.</summary>
    /// <param name="operationId">The causing operation.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent CreateSizedEvent(OperationId operationId)
    {
        var cursor = new string('c', CursorUtf8Bound / LargeTextScale);
        var payload = new PayloadEnvelope(
            PayloadContract,
            SingleEntryCount,
            PayloadContentType,
            System.Text.Encoding.UTF8.GetBytes(new string('p', CursorUtf8Bound / MediumTextScale)),
            new string('h', CursorUtf8Bound / LargeTextScale));
        var metadataKey = new string('k', CursorUtf8Bound / MediumTextScale);
        var metadataValue = new string('v', CursorUtf8Bound / MediumTextScale);
        Dictionary<string, string> metadata = [];
        metadata.Add(metadataKey, metadataValue);
        return new(Guid.NewGuid(), Stream, cursor, Start, operationId, payload, metadata) { Origin = new(Client, operationId) };
    }

    /// <summary>Counts logical receive batch bytes using the loopback transport envelope shape.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The logical byte count.</returns>
    private static long CountReceiveBatchBytes(RemoteEventBatch batch)
    {
        long total = ReceiveGuidByteCount + ReceiveIntByteCount + ReceiveIntByteCount;
        total += CountTextBytes(batch.StreamId.Value);
        total += batch.PreviousCursor is null ? 0 : CountTextBytes(batch.PreviousCursor);
        total += CountTextBytes(batch.NextCursor);
        foreach (var remoteEvent in batch.Events)
        {
            total += CountRemoteEventBytes(remoteEvent);
        }

        foreach (var completion in batch.CompletedOperations)
        {
            total += CountOriginBytes(completion.Origin) + ReceiveIntByteCount + (completion.EventIds.Count * ReceiveGuidByteCount);
        }

        return total;
    }

    /// <summary>Counts logical receive event bytes.</summary>
    /// <param name="remoteEvent">The event.</param>
    /// <returns>The logical byte count.</returns>
    private static long CountRemoteEventBytes(RemoteEvent remoteEvent)
    {
        long total = ReceiveGuidByteCount + CountTextBytes(remoteEvent.StreamId.Value) + CountTextBytes(remoteEvent.ServerCursor);
        total += ReceiveDateTimeOffsetByteCount + ReceiveNullableMarkerByteCount;
        total += remoteEvent.CausedByOperationId.HasValue ? ReceiveGuidByteCount : 0;
        total += remoteEvent.Origin is null ? 0 : CountOriginBytes(remoteEvent.Origin);
        total += ReceiveIntByteCount + CountTextBytes(remoteEvent.Payload.ContractId) + CountTextBytes(remoteEvent.Payload.ContentType);
        total += CountTextBytes(remoteEvent.Payload.PayloadHash) + remoteEvent.Payload.PayloadLength + ReceiveIntByteCount;
        foreach (var item in remoteEvent.Metadata)
        {
            total += CountTextBytes(item.Key) + CountTextBytes(item.Value);
        }

        return total;
    }

    /// <summary>Counts logical receive origin bytes.</summary>
    /// <param name="origin">The origin.</param>
    /// <returns>The logical byte count.</returns>
    private static long CountOriginBytes(RemoteEventOrigin origin) =>
        CountTextBytes(origin.ClientId) + ReceiveGuidByteCount;

    /// <summary>Counts strict UTF-8 text bytes.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountTextBytes(string value) => System.Text.Encoding.UTF8.GetByteCount(value);
}
