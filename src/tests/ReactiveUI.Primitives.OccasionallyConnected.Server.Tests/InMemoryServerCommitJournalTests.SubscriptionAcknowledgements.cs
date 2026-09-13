// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests subscription acknowledgements for <see cref="InMemoryServerCommitJournal"/>.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>The shared failure message when a subscription page batch is required.</summary>
    private const string MissingSubscriptionBatchMessage = "The subscription page did not return a batch.";

    /// <summary>The first deterministic subscription.</summary>
    private static readonly SubscriptionId FirstSubscription = new(new Guid("10000000-0000-0000-0000-000000000001"));

    /// <summary>The second deterministic subscription.</summary>
    private static readonly SubscriptionId SecondSubscription = new(new Guid("10000000-0000-0000-0000-000000000002"));

    /// <summary>Verifies the retention timestamp participates in admission before a binding is retained.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionAdmissionChargesRetentionTimestamp()
    {
        const long insufficientBindingBudget = 120;
        var journal = new InMemoryServerCommitJournal(new() { MaximumLogicalBytes = insufficientBindingBudget });

        await Assert.That(() => journal.RegisterSubscription(SubscriptionIdentity(FirstSubscription)))
            .ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(journal.SubscriptionCount).IsEqualTo(0);
        await Assert.That(journal.LogicalBytes).IsEqualTo(0);
    }

    /// <summary>Verifies retained acknowledged cursors remain charged after offer rows are pruned.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementRetainsCursorLogicalBytesAfterOfferPrune()
    {
        var journal = CreateSubscriptionJournal();
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

    /// <summary>Verifies reoffering an older retained page does not move the frontier backwards.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionReofferPreservesHighestOfferedFrontier()
    {
        var journal = CreateSubscriptionJournal();
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
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(
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

    /// <summary>Verifies offered unacknowledged bindings are retained beyond offer cleanup then expire by subscription retention.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionCompactionExpiresOfferedUnacknowledgedBindingsAfterSubscriptionRetention()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(DoubleEntryCount + SingleEntryCount),
            maximumSubscriptions: SingleEntryCount);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        _ = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));

        _ = journal.Compact();

        await Assert.That(journal.SubscriptionOfferCount).IsEqualTo(0);
        await Assert.That(() => journal.RegisterSubscription(SubscriptionIdentity(SecondSubscription))).ThrowsExactly<QueueCapacityExceededException>();

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount + DoubleEntryCount));
        _ = journal.Compact();
        var replacement = journal.RegisterSubscription(SubscriptionIdentity(SecondSubscription));

        await Assert.That(replacement.Identity.SubscriptionId).IsEqualTo(SecondSubscription);
        await Assert.That(journal.SubscriptionCount).IsEqualTo(SingleEntryCount);
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, FirstCursor)))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies acknowledged bindings keep duplicate ACKs within subscription retention and expire after it.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionCompactionExpiresAcknowledgedBindingsAfterSubscriptionRetention()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(
            clock,
            retention: TimeSpan.FromTicks(SingleEntryCount),
            subscriptionRetention: TimeSpan.FromTicks(DoubleEntryCount + SingleEntryCount),
            maximumSubscriptions: SingleEntryCount);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));

        _ = journal.Compact();
        var duplicate = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));

        await Assert.That(duplicate.AcknowledgedCursor).IsEqualTo(batch.NextCursor);
        await Assert.That(() => journal.RegisterSubscription(SubscriptionIdentity(SecondSubscription))).ThrowsExactly<QueueCapacityExceededException>();

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount + DoubleEntryCount + DoubleEntryCount));
        _ = journal.Compact();
        var replacement = journal.RegisterSubscription(SubscriptionIdentity(SecondSubscription));

        await Assert.That(replacement.Identity.SubscriptionId).IsEqualTo(SecondSubscription);
        await Assert.That(journal.SubscriptionCount).IsEqualTo(SingleEntryCount);
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies acknowledgements can only target offered final cursors for the trusted binding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementsRejectUntrustedUnOfferedNonFinalAndRewindCursors()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(clock);
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var firstEntry = Entry(
            first,
            OperationResultKind.Accepted,
            FirstOperationSeed,
            events: [Event(first.OperationId, FirstCursor, EventPayload), Event(first.OperationId, SecondCursor, EventPayload)]);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), firstEntry));
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, ThirdOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        await Assert.That(() => journal.RegisterSubscription(new(new(OtherTenant, Stream), Client, identity.SubscriptionId))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, OtherStream, batch.NextCursor)))).ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, FirstCursor)))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, ThirdCursor)))).ThrowsExactly<InvalidOperationException>();

        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();
        var duplicate = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));

        await Assert.That(() => journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<InvalidOperationException>();

        var secondPage = journal.OfferReceivePage(new(identity, batch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var secondBatch = secondPage.Batch ?? throw new InvalidOperationException("The second subscription page did not return a batch.");
        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, secondBatch.NextCursor)));

        await Assert.That(duplicate.AcknowledgedCursor).IsEqualTo(batch.NextCursor);
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies subscription acknowledgement members dispatch through the internal interface.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementsUseJournalInterface()
    {
        IServerSubscriptionAcknowledgementJournal journal = CreateSubscriptionJournal();
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var key = OperationKey(FirstOperationSeed);
        _ = ((IServerCommitJournal)journal).TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
        var end = journal.OfferReceivePage(new(identity, batch.NextCursor, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var acknowledged = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.Page);
        await Assert.That(end.Status).IsEqualTo(ServerReceivePageStatus.EndOfStream);
        await Assert.That(acknowledged.AcknowledgedCursor).IsEqualTo(batch.NextCursor);
    }

    /// <summary>Verifies subscription input guards reject malformed trusted data before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionAcknowledgementGuardsRejectMalformedInputs()
    {
        var journal = CreateSubscriptionJournal();
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);

        await Assert.That(() => journal.RegisterSubscription(new(StreamKey(), " ", SecondSubscription))).ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.RegisterSubscription(new(StreamKey(), Client, new(Guid.Empty)))).ThrowsExactly<ArgumentException>();
        await Assert.That(() => journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, 0))).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => journal.Acknowledge(new(StreamKey(), Client, new(SecondSubscription, Stream, FirstCursor)))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies finite subscription and offer capacity rejects without mutating retained state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionAcknowledgementCapacityRejectsFullBindingsAndOffers()
    {
        var subscriptionJournal = CreateSubscriptionJournal(maximumSubscriptions: SingleEntryCount);
        _ = subscriptionJournal.RegisterSubscription(SubscriptionIdentity(FirstSubscription));
        await Assert.That(() => subscriptionJournal.RegisterSubscription(SubscriptionIdentity(SecondSubscription))).ThrowsExactly<QueueCapacityExceededException>();

        var offerJournal = CreateSubscriptionJournal(maximumSubscriptionOffers: SingleEntryCount);
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

    /// <summary>Verifies offering a page reserves both the offer and the subscription frontier before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionOfferAdmissionChargesFrontierCursor()
    {
        var identity = SubscriptionIdentity(FirstSubscription);
        var key = OperationKey(FirstOperationSeed);
        var plan = Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed));
        var request = new ServerSubscriptionPageRequest(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes);
        var probe = CreateJournal();
        _ = probe.RegisterSubscription(identity);
        _ = probe.TryCommit(plan);
        _ = probe.OfferReceivePage(request);
        var journal = CreateJournal(maximumLogicalBytes: probe.LogicalBytes - 1);
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(plan);
        var beforeOffer = journal.LogicalBytes;

        await Assert.That(() => journal.OfferReceivePage(request)).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(journal.LogicalBytes).IsEqualTo(beforeOffer);
        await Assert.That(journal.SubscriptionOfferCount).IsEqualTo(0);
        await Assert.That(journal.RegisterSubscription(identity).LatestOfferedCursor).IsNull();
    }

    /// <summary>Verifies expired unacknowledged offers are pruned and expiry underflow is clamped.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected page is missing.</exception>
    [Test]
    public async Task SubscriptionOfferCompactionPrunesExpiredOffersAndClampsMinimumTime()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(clock, retention: TimeSpan.FromTicks(SingleEntryCount));
        var identity = SubscriptionIdentity(FirstSubscription);
        _ = journal.RegisterSubscription(identity);
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        _ = page.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);

        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();
        var minimumClockJournal = CreateSubscriptionJournal(new(DateTimeOffset.MinValue), retention: TimeSpan.FromTicks(SingleEntryCount));
        var minimumIdentity = SubscriptionIdentity(SecondSubscription);
        _ = minimumClockJournal.RegisterSubscription(minimumIdentity);
        var minimumKey = OperationKey(SecondOperationSeed);
        _ = minimumClockJournal.TryCommit(Plan(0, State(SecondVersion), Stamp(minimumKey), Entry(minimumKey, OperationResultKind.Accepted, SecondOperationSeed)));
        var minimumPage = minimumClockJournal.OfferReceivePage(new(minimumIdentity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        _ = minimumPage.Batch ?? throw new InvalidOperationException(MissingSubscriptionBatchMessage);
        _ = minimumClockJournal.Compact();

        await Assert.That(journal.SubscriptionOfferCount).IsEqualTo(0);
        await Assert.That(minimumClockJournal.SubscriptionCount).IsEqualTo(SingleEntryCount);
        await Assert.That(minimumClockJournal.SubscriptionOfferCount).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies identity matching distinguishes every trusted binding component.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SubscriptionIdentityMatchingChecksSubscriptionClientAndStream()
    {
        var identity = SubscriptionIdentity(FirstSubscription);
        var record = new ServerSubscriptionRecord(identity, Start, ServerSubscriptionJournalOperations.GetSubscriptionBytes(identity));

        await Assert.That(ServerSubscriptionJournalOperations.IdentityMatches(identity, record)).IsTrue();
        await Assert.That(ServerSubscriptionJournalOperations.IdentityMatches(SubscriptionIdentity(SecondSubscription), record)).IsFalse();
        await Assert.That(ServerSubscriptionJournalOperations.IdentityMatches(new(StreamKey(), LongClient, FirstSubscription), record)).IsFalse();
        await Assert.That(ServerSubscriptionJournalOperations.IdentityMatches(new(new(OtherTenant, Stream), Client, FirstSubscription), record)).IsFalse();
    }

    /// <summary>Creates a subscription identity for the default trusted context.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The identity.</returns>
    private static ServerSubscriptionIdentity SubscriptionIdentity(SubscriptionId subscriptionId) =>
        new(StreamKey(), Client, subscriptionId);

    /// <summary>Creates a configured subscription journal.</summary>
    /// <param name="clock">The optional clock.</param>
    /// <param name="retention">The optional retention.</param>
    /// <param name="subscriptionRetention">The optional subscription retention.</param>
    /// <param name="maximumSubscriptions">The subscription limit.</param>
    /// <param name="maximumSubscriptionOffers">The subscription offer limit.</param>
    /// <returns>The configured journal.</returns>
    private static InMemoryServerCommitJournal CreateSubscriptionJournal(
        ManualTimeProvider? clock = null,
        TimeSpan? retention = null,
        TimeSpan? subscriptionRetention = null,
        int maximumSubscriptions = DefaultMaximumStreams,
        int maximumSubscriptionOffers = DefaultMaximumLedgerEntries) =>
        new(new()
        {
            MaximumStreams = DefaultMaximumStreams,
            MaximumLedgerEntries = DefaultMaximumLedgerEntries,
            MaximumEvents = DefaultMaximumEvents,
            MaximumLogicalBytes = CursorTestMaximumLogicalBytes,
            MaximumSubscriptions = maximumSubscriptions,
            MaximumSubscriptionOffers = maximumSubscriptionOffers,
            OperationRetention = retention ?? TimeSpan.FromMinutes(DefaultRetentionMinutes),
            SubscriptionRetention = subscriptionRetention ?? TimeSpan.FromMinutes(DefaultRetentionMinutes + DefaultRetentionMinutes),
            TimeProvider = clock ?? new(Start),
        });
}
