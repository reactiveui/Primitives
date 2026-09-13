// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests subscription acknowledgements for <see cref="SqliteServerCommitJournal"/>.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The shared failure message when a subscription page batch is required.</summary>
    private const string MissingSubscriptionBatchMessage = "The subscription page did not return a batch.";

    /// <summary>The logical byte limit used by subscription cursor tests.</summary>
    private const long SubscriptionCursorTestMaximumLogicalBytes = 12_288;

    /// <summary>The migrated schema version expected after opening a schema-two database.</summary>
    private const long MigratedSchemaVersion = 3;

    /// <summary>The first deterministic subscription.</summary>
    private static readonly SubscriptionId FirstSubscription = new(new Guid("20000000-0000-0000-0000-000000000001"));

    /// <summary>The second deterministic subscription.</summary>
    private static readonly SubscriptionId SecondSubscription = new(new Guid("20000000-0000-0000-0000-000000000002"));

    /// <summary>The alternate stream used for foreign-binding validation.</summary>
    private static readonly StreamId OtherStream = new("stream-b");

    /// <summary>Verifies an underfunded binding is rejected without persisting any subscription state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionAdmissionChargesRetentionTimestamp()
    {
        const long insufficientBindingBudget = 120;
        using var database = new TemporaryDatabase();
        using (var journal = new SqliteServerCommitJournal(database.Path, new() { MaximumLogicalBytes = insufficientBindingBudget }))
        {
            await Assert.That(() => journal.RegisterSubscription(SubscriptionIdentity(FirstSubscription)))
                .ThrowsExactly<QueueCapacityExceededException>();
            await Assert.That(journal.SubscriptionCount).IsEqualTo(0);
            await Assert.That(journal.LogicalBytes).IsEqualTo(0);
        }

        using var reopened = CreateSubscriptionJournal(database.Path);
        await Assert.That(reopened.SubscriptionCount).IsEqualTo(0);
        _ = reopened.RegisterSubscription(SubscriptionIdentity(FirstSubscription));
        await Assert.That(reopened.SubscriptionCount).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies retained acknowledged cursors remain charged after offer rows are pruned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementRetainsCursorLogicalBytesAfterOfferPrune()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var bytesBeforeOffer = journal.LogicalBytes;
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));

        await Assert.That(journal.SubscriptionOfferCount).IsEqualTo(0);
        await Assert.That(journal.LogicalBytes).IsGreaterThan(bytesBeforeOffer);
    }

    /// <summary>Verifies reoffering an older retained page does not move the durable frontier backwards.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionReofferPreservesHighestOfferedFrontier()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var firstPage = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var firstBatch = firstPage.Batch ?? throw new InvalidOperationException("The first subscription page did not return a batch.");
        var secondPage = journal.OfferReceivePage(new(identity, firstBatch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var secondBatch = secondPage.Batch ?? throw new InvalidOperationException("The second subscription page did not return a batch.");

        var reoffered = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(reoffered.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(journal.RegisterSubscription(identity).LatestOfferedCursor).IsEqualTo(secondBatch.NextCursor);
        await Assert.That(journal.RegisterSubscription(identity).LatestOfferedGroupSequence).IsEqualTo(DoubleEntryCount);
    }

    /// <summary>Verifies idle never-offered bindings cannot permanently consume subscription slots.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionCompactionExpiresIdleBindingsForClientChurn()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateSubscriptionJournal(
            database.Path,
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(SingleEntryCount),
            maximumSubscriptions: SingleEntryCount);
        _ = journal.RegisterSubscription(SubscriptionIdentity(FirstSubscription));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));

        _ = journal.Compact();
        var replacement = journal.RegisterSubscription(SubscriptionIdentity(SecondSubscription));

        await Assert.That(replacement.Identity.SubscriptionId).IsEqualTo(SecondSubscription);
        await Assert.That(journal.SubscriptionCount).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies offered unacknowledged bindings survive offer cleanup then expire by subscription retention after reopen.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionCompactionExpiresOfferedUnacknowledgedBindingsAfterSubscriptionRetention()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        var identity = SubscriptionIdentity(FirstSubscription);
        using (var journal = CreateSubscriptionJournal(
            database.Path,
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(DoubleEntryCount + SingleEntryCount),
            maximumSubscriptions: SingleEntryCount))
        {
            _ = journal.RegisterSubscription(identity);
            var key = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
            _ = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
            clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
            _ = journal.Compact();

            await Assert.That(journal.SubscriptionOfferCount).IsEqualTo(0);
            await Assert.That(() => journal.RegisterSubscription(SubscriptionIdentity(SecondSubscription))).ThrowsExactly<QueueCapacityExceededException>();
        }

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount + DoubleEntryCount));
        using var reopened = CreateSubscriptionJournal(
            database.Path,
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(DoubleEntryCount + SingleEntryCount),
            maximumSubscriptions: SingleEntryCount);
        _ = reopened.Compact();
        var replacement = reopened.RegisterSubscription(SubscriptionIdentity(SecondSubscription));

        await Assert.That(replacement.Identity.SubscriptionId).IsEqualTo(SecondSubscription);
        await Assert.That(reopened.SubscriptionCount).IsEqualTo(SingleEntryCount);
        await Assert.That(() => reopened.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, FirstCursor)))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies acknowledged bindings keep duplicate ACKs within subscription retention and expire after it across restart.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionCompactionExpiresAcknowledgedBindingsAfterSubscriptionRetention()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        var identity = SubscriptionIdentity(FirstSubscription);
        string acknowledgedCursor;
        using (var journal = CreateSubscriptionJournal(
            database.Path,
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(DoubleEntryCount + SingleEntryCount),
            maximumSubscriptions: SingleEntryCount))
        {
            _ = journal.RegisterSubscription(identity);
            var key = OperationKey(FirstOperationSeed);
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
            var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
            acknowledgedCursor = batch.NextCursor;
            _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, acknowledgedCursor)));
        }

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        using (var reopened = CreateSubscriptionJournal(
            database.Path,
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(DoubleEntryCount + SingleEntryCount),
            maximumSubscriptions: SingleEntryCount))
        {
            _ = reopened.Compact();
            var duplicate = reopened.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, acknowledgedCursor)));

            await Assert.That(duplicate.AcknowledgedCursor).IsEqualTo(acknowledgedCursor);
            await Assert.That(() => reopened.RegisterSubscription(SubscriptionIdentity(SecondSubscription))).ThrowsExactly<QueueCapacityExceededException>();
        }

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount + DoubleEntryCount + DoubleEntryCount));
        using var expired = CreateSubscriptionJournal(
            database.Path,
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(DoubleEntryCount + SingleEntryCount),
            maximumSubscriptions: SingleEntryCount);
        _ = expired.Compact();
        var replacement = expired.RegisterSubscription(SubscriptionIdentity(SecondSubscription));

        await Assert.That(replacement.Identity.SubscriptionId).IsEqualTo(SecondSubscription);
        await Assert.That(expired.SubscriptionCount).IsEqualTo(SingleEntryCount);
        await Assert.That(() => expired.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, acknowledgedCursor)))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies acknowledgements survive reopen and can be completed by another journal instance.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementsSurviveReopenCleanupAndMultipleInstances()
    {
        using var database = new TemporaryDatabase();
        var identity = SubscriptionIdentity(FirstSubscription);
        var clock = new ManualTimeProvider(Start);
        string acknowledgedCursor;
        using (var first = CreateSubscriptionJournal(database.Path, clock))
        using (var second = CreateSubscriptionJournal(database.Path, clock))
        {
            _ = first.RegisterSubscription(identity);
            var key = OperationKey(FirstOperationSeed);
            _ = first.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            var page = first.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
            var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

            await Assert.That(() => second.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, SecondCursor)))).ThrowsExactly<InvalidOperationException>();
            var acknowledged = second.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));
            acknowledgedCursor = batch.NextCursor;

            await Assert.That(acknowledged.AcknowledgedCursor).IsEqualTo(acknowledgedCursor);
            await Assert.That(() => first.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
                .ThrowsExactly<InvalidOperationException>();
        }

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        using var reopened = CreateSubscriptionJournal(database.Path, clock);
        _ = reopened.Compact();
        var duplicate = reopened.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, acknowledgedCursor)));

        await Assert.That(reopened.SubscriptionOfferCount).IsEqualTo(0);
        await Assert.That(duplicate.AcknowledgedCursor).IsEqualTo(acknowledgedCursor);
    }

    /// <summary>Verifies an aborted offer transaction does not leave a stray offer row.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionOfferTransactionAbortRollsBackInsertedOffer()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        CreateAbortLatestOfferTrigger(database.Path);

        await Assert.That(() => journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<Microsoft.Data.Sqlite.SqliteException>();
        await Assert.That(journal.SubscriptionOfferCount).IsEqualTo(0);
    }

    /// <summary>Verifies schema-two data migrates to schema three without losing existing replay or receive order.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionSchemaThreeMigrationPreservesSchemaTwoReplayAndReceivePages()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        using (var seeded = CreateSubscriptionJournal(database.Path))
        {
            _ = seeded.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        }

        DowngradeSchemaThreeToTwo(database.Path);
        using var migrated = CreateSubscriptionJournal(database.Path);
        var replay = migrated.Read(StreamKey(), [key]);
        var page = migrated.ReadReceivePage(new(StreamKey(), null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(MigratedSchemaVersion);
        await Assert.That(replay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(replay.LastCursor).IsEqualTo(FirstCursor);
        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(migrated.SubscriptionCount).IsEqualTo(0);
    }

    /// <summary>Verifies SQLite subscription guards reject malformed, foreign and missing bindings.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionAcknowledgementGuardsRejectMalformedMissingAndForeignBindings()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);

        await Assert.That(() => journal.RegisterSubscription(new(StreamKey(), " ", SecondSubscription))).ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.RegisterSubscription(new(StreamKey(), Client, new(Guid.Empty)))).ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.RegisterSubscription(new(new(Tenant, OtherStream), Client, FirstSubscription))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, 0))).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(SecondSubscription, Stream, FirstCursor)))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies finite SQLite subscription and offer capacity rejects without mutating retained state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementCapacityRejectsFullBindingsAndOffers()
    {
        using var subscriptionDatabase = new TemporaryDatabase();
        using var subscriptionJournal = CreateSubscriptionJournal(subscriptionDatabase.Path, maximumSubscriptions: SingleEntryCount);
        _ = subscriptionJournal.RegisterSubscription(SubscriptionIdentity(FirstSubscription));
        await Assert.That(() => subscriptionJournal.RegisterSubscription(SubscriptionIdentity(SecondSubscription))).ThrowsExactly<QueueCapacityExceededException>();

        using var offerDatabase = new TemporaryDatabase();
        using var offerJournal = CreateSubscriptionJournal(offerDatabase.Path, maximumSubscriptionOffers: SingleEntryCount);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = offerJournal.RegisterSubscription(identity);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = offerJournal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = offerJournal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var page = offerJournal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(() => offerJournal.OfferReceivePage(new(identity, batch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(offerJournal.SubscriptionOfferCount).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies SQLite subscription timestamp updates fail closed if a row vanishes mid-update.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionUpdateGuardsRejectMissingRowsAfterTriggerMutation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        CreateDeleteSubscriptionBeforeUpdatedAtTrigger(database.Path);

        await Assert.That(() => journal.RegisterSubscription(identity)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies SQLite subscription acknowledgement members dispatch through the internal interface.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementsUseJournalInterface()
    {
        using var database = new TemporaryDatabase();
        IServerSubscriptionAcknowledgementJournal journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var key = OperationKey(FirstOperationSeed);
        _ = ((IServerCommitJournal)journal).TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
        var acknowledged = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));

        await Assert.That(acknowledged.AcknowledgedCursor).IsEqualTo(batch.NextCursor);
    }

    /// <summary>Verifies acknowledging one of multiple offers reports the remaining offered cursor count.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementReportsRemainingOffers()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var firstPage = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var firstBatch = firstPage.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
        _ = journal.OfferReceivePage(new(identity, firstBatch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        var acknowledged = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, firstBatch.NextCursor)));

        await Assert.That(acknowledged.OfferCount).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies SQLite offer update guards fail closed when rows vanish mid-update.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionOfferUpdateGuardsRejectMissingRowsAfterTriggerMutation()
    {
        using var updateOfferDatabase = new TemporaryDatabase();
        using var updateOfferJournal = CreateSubscriptionJournal(updateOfferDatabase.Path);
        var updateOfferIdentity = SubscriptionIdentity(FirstSubscription);
        _ = updateOfferJournal.RegisterSubscription(updateOfferIdentity);
        _ = SeedOfferedPage(updateOfferJournal, updateOfferIdentity);
        CreateDeleteOfferBeforeOfferedAtTrigger(updateOfferDatabase.Path);
        await Assert.That(() => updateOfferJournal.OfferReceivePage(new(updateOfferIdentity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();

        using var latestOfferDatabase = new TemporaryDatabase();
        using var latestOfferJournal = CreateSubscriptionJournal(latestOfferDatabase.Path);
        var latestOfferIdentity = SubscriptionIdentity(FirstSubscription);
        _ = latestOfferJournal.RegisterSubscription(latestOfferIdentity);
        SeedCommittedEvent(latestOfferJournal);
        CreateDeleteSubscriptionBeforeLatestOfferTrigger(latestOfferDatabase.Path);
        await Assert.That(() => latestOfferJournal.OfferReceivePage(new(latestOfferIdentity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();

        using var acknowledgementDatabase = new TemporaryDatabase();
        using var acknowledgementJournal = CreateSubscriptionJournal(acknowledgementDatabase.Path);
        var acknowledgementIdentity = SubscriptionIdentity(FirstSubscription);
        _ = acknowledgementJournal.RegisterSubscription(acknowledgementIdentity);
        var cursor = SeedOfferedPage(acknowledgementJournal, acknowledgementIdentity);
        CreateDeleteSubscriptionBeforeAcknowledgementTrigger(acknowledgementDatabase.Path);
        await Assert.That(() => acknowledgementJournal.Acknowledge(new(StreamKey(), Client, new(acknowledgementIdentity.SubscriptionId, Stream, cursor))))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema-two metadata mismatches and malformed metadata fail before migration.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionSchemaTwoMigrationRejectsUnsupportedAndMalformedMetadata()
    {
        using var unsupportedDatabase = new TemporaryDatabase();
        using (var seeded = CreateSubscriptionJournal(unsupportedDatabase.Path))
        {
            SeedCommittedEvent(seeded);
        }

        DowngradeSchemaThreeToTwoWithMetadata(unsupportedDatabase.Path, "9");
        await Assert.That(() => CreateSubscriptionJournal(unsupportedDatabase.Path)).ThrowsExactly<InvalidOperationException>();

        using var malformedDatabase = new TemporaryDatabase();
        CreateMalformedSchemaTwoMetadata(malformedDatabase.Path);
        await Assert.That(() => CreateSubscriptionJournal(malformedDatabase.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies SQLite subscription expiry clamps when the monotonic clock is at its minimum.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionCompactionClampsMinimumExpiryBoundary()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(DateTimeOffset.MinValue);
        using var journal = CreateSubscriptionJournal(database.Path, clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        _ = journal.RegisterSubscription(SubscriptionIdentity(FirstSubscription));

        _ = journal.Compact();

        await Assert.That(journal.SubscriptionCount).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies smaller SQLite instances reject retained offer counts before reads.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionReadCapacityRejectsRetainedOfferOverflow()
    {
        using var subscriptionDatabase = new TemporaryDatabase();
        var subscriptionIdentity = SubscriptionIdentity(FirstSubscription);
        using (var seededSubscriptions = CreateSubscriptionJournal(subscriptionDatabase.Path))
        {
            SeedCommittedEvent(seededSubscriptions);
            _ = seededSubscriptions.RegisterSubscription(subscriptionIdentity);
            _ = seededSubscriptions.RegisterSubscription(SubscriptionIdentity(SecondSubscription));
        }

        using var smallerSubscriptionReader = CreateSubscriptionJournal(subscriptionDatabase.Path, maximumSubscriptions: SingleEntryCount);
        await Assert.That(() => smallerSubscriptionReader.Read(StreamKey(), [OperationKey(FirstOperationSeed)])).ThrowsExactly<InvalidOperationException>();

        using var database = new TemporaryDatabase();
        var identity = SubscriptionIdentity(FirstSubscription);
        var first = OperationKey(FirstOperationSeed);
        using (var seeded = CreateSubscriptionJournal(database.Path))
        {
            _ = seeded.RegisterSubscription(identity);
            var second = OperationKey(SecondOperationSeed);
            _ = seeded.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
            _ = seeded.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
            var firstPage = seeded.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
            var firstBatch = firstPage.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
            _ = seeded.OfferReceivePage(new(identity, firstBatch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        }

        using var smaller = CreateSubscriptionJournal(database.Path, maximumSubscriptionOffers: SingleEntryCount);

        await Assert.That(() => smaller.Read(StreamKey(), [first])).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema-three subscription table corruption fails validation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionSchemaThreeTableDefinitionCorruptionFailsValidation()
    {
        using var database = new TemporaryDatabase();
        var initialized = CreateSubscriptionJournal(database.Path);
        initialized.Dispose();

        CorruptSubscriptionTables(database.Path);

        await Assert.That(() => CreateSubscriptionJournal(database.Path)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a subscription identity for the default trusted context.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The identity.</returns>
    private static ServerSubscriptionIdentity SubscriptionIdentity(SubscriptionId subscriptionId) =>
        new(StreamKey(), Client, subscriptionId);

    /// <summary>Creates a trigger that aborts latest offer updates.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateAbortLatestOfferTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_subscription_abort_latest_offer
            BEFORE UPDATE OF latest_offered_cursor ON oc_server_journal_subscriptions
            BEGIN
                SELECT RAISE(ABORT, 'abort subscription offer');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that deletes an offer before its timestamp update.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteOfferBeforeOfferedAtTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_subscription_offer_delete_before_offered_at
            BEFORE UPDATE OF offered_at_utc ON oc_server_journal_subscription_offers
            BEGIN
                DELETE FROM oc_server_journal_subscription_offers
                WHERE subscription_id = OLD.subscription_id AND cursor = OLD.cursor;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that deletes a subscription before latest offer state changes.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteSubscriptionBeforeLatestOfferTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_subscription_delete_before_latest_offer
            BEFORE UPDATE OF latest_offered_cursor ON oc_server_journal_subscriptions
            BEGIN
                DELETE FROM oc_server_journal_subscriptions WHERE subscription_id = OLD.subscription_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that deletes a subscription before acknowledgement state changes.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteSubscriptionBeforeAcknowledgementTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_subscription_delete_before_acknowledgement
            BEFORE UPDATE OF acknowledged_cursor ON oc_server_journal_subscriptions
            BEGIN
                DELETE FROM oc_server_journal_subscriptions WHERE subscription_id = OLD.subscription_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that deletes a subscription before its update timestamp changes.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateDeleteSubscriptionBeforeUpdatedAtTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_server_subscription_delete_before_updated_at
            BEFORE UPDATE OF updated_at_utc ON oc_server_journal_subscriptions
            BEGIN
                DELETE FROM oc_server_journal_subscriptions WHERE subscription_id = OLD.subscription_id;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Removes schema-three subscription tables after seeding schema-two compatible data.</summary>
    /// <param name="path">The database path.</param>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static void DowngradeSchemaThreeToTwo(string path) => DowngradeSchemaThreeToTwoWithMetadata(path, "2");

    /// <summary>Removes schema-three subscription tables and writes a schema-two metadata value.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="metadataVersion">The metadata schema version.</param>
    private static void DowngradeSchemaThreeToTwoWithMetadata(string path, string metadataVersion)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DROP TABLE oc_server_journal_subscription_offers;
            DROP TABLE oc_server_journal_subscriptions;
            UPDATE oc_server_journal_metadata SET value = $metadataVersion WHERE key = 'schema_version';
            PRAGMA user_version = 2;
            """;
        _ = command.Parameters.AddWithValue("$metadataVersion", metadataVersion);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates schema-two table names with malformed metadata storage.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateMalformedSchemaTwoMetadata(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA user_version = 2;
            CREATE TABLE oc_server_journal_conflicts (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_event_metadata (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_events (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_ledger (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_metadata (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_streams (id INTEGER NOT NULL);
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Replaces schema-three subscription tables with invalid definitions.</summary>
    /// <param name="path">The database path.</param>
    private static void CorruptSubscriptionTables(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            DROP TABLE oc_server_journal_subscription_offers;
            DROP TABLE oc_server_journal_subscriptions;
            CREATE TABLE oc_server_journal_subscriptions (id INTEGER NOT NULL);
            CREATE TABLE oc_server_journal_subscription_offers (id INTEGER NOT NULL);
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Commits one accepted event and returns its offered cursor.</summary>
    /// <param name="journal">The journal.</param>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The offered cursor.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    private static string SeedOfferedPage(SqliteServerCommitJournal journal, ServerSubscriptionIdentity identity)
    {
        SeedCommittedEvent(journal);
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
        return batch.NextCursor;
    }

    /// <summary>Commits one accepted event.</summary>
    /// <param name="journal">The journal.</param>
    private static void SeedCommittedEvent(SqliteServerCommitJournal journal)
    {
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
    }

    /// <summary>Reads the SQLite user version.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The user version.</returns>
    private static long ReadUserVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA user_version;";
        return Convert.ToInt64(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Creates a configured subscription journal.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="clock">The optional clock.</param>
    /// <param name="retention">The optional retention.</param>
    /// <param name="subscriptionRetention">The optional subscription retention.</param>
    /// <param name="maximumSubscriptions">The subscription limit.</param>
    /// <param name="maximumSubscriptionOffers">The subscription offer limit.</param>
    /// <returns>The configured journal.</returns>
    private static SqliteServerCommitJournal CreateSubscriptionJournal(
        string path,
        ManualTimeProvider? clock = null,
        TimeSpan? retention = null,
        TimeSpan? subscriptionRetention = null,
        int maximumSubscriptions = DefaultMaximumStreams,
        int maximumSubscriptionOffers = DefaultMaximumLedgerEntries) =>
        new(path, new()
        {
            MaximumStreams = DefaultMaximumStreams,
            MaximumLedgerEntries = DefaultMaximumLedgerEntries,
            MaximumEvents = DefaultMaximumEvents,
            MaximumLogicalBytes = SubscriptionCursorTestMaximumLogicalBytes,
            MaximumSubscriptions = maximumSubscriptions,
            MaximumSubscriptionOffers = maximumSubscriptionOffers,
            OperationRetention = retention ?? TimeSpan.FromMinutes(DefaultRetentionMinutes),
            SubscriptionRetention = subscriptionRetention ?? TimeSpan.FromMinutes(DefaultRetentionMinutes + DefaultRetentionMinutes),
            TimeProvider = clock ?? new(Start),
        });
}
