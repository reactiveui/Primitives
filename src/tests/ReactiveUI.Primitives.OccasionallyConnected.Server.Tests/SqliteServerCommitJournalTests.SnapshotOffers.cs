// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery offer tests.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The bounded canonical fingerprint byte budget used by snapshot offer fixtures.</summary>
    private const int SnapshotFingerprintBudget = 4096;

    /// <summary>The elapsed ticks used to expire one-tick subscription retention fixtures.</summary>
    private const int ExpiredSubscriptionTicks = 2;

    /// <summary>The schema version before durable snapshot offer fields were added.</summary>
    private const int SnapshotOfferSchemaFourVersion = 4;

    /// <summary>The durable subscription generation high-water metadata key.</summary>
    private const string SubscriptionGenerationHighWaterMetadataKey = "subscription_generation_high_water";

    /// <summary>The raw SQL parameter name for subscription identifiers.</summary>
    private const string RawSubscriptionIdParameterName = "$subscriptionId";

    /// <summary>The raw SQL parameter name for the generation metadata key.</summary>
    private const string RawGenerationKeyParameterName = "$generationKey";

    /// <summary>The default snapshot subscription identifier text.</summary>
    private const string SnapshotSubscriptionText = "00000014-0000-0000-0000-000000000001";

    /// <summary>The second snapshot subscription identifier text.</summary>
    private const string SecondSnapshotSubscriptionText = "00000014-0000-0000-0000-000000000002";

    /// <summary>Verifies durable reads expose the complete group frontier even when the last group produced no sidecar events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReopenReadExposesLastGroupSequenceForAcceptedEmptyGroup()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        using (var journal = CreateJournal(database.Path))
        {
            var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed, events: [])));
            await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        }

        using var reopened = CreateJournal(database.Path);
        var snapshot = reopened.Read(StreamKey(), [key]);

        await Assert.That(snapshot.LastEventSequence).IsEqualTo(0);
        await Assert.That(snapshot.LastGroupSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies a missing or expired subscription yields no fabricated snapshot offer subscription state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MissingSnapshotSubscriptionReturnsNullState()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var identity = SnapshotSubscription();
        var key = OperationKey(FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var snapshot = journal.Read(StreamKey(), [key]);
        var result = journal.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = SnapshotView(snapshot, null),
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId),
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot),
            Limits = SnapshotLimits(),
        });

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.MissingSubscription);
        await Assert.That(result.SubscriptionState).IsNull();
        await Assert.That(result.Cursor).IsNull();
    }

    /// <summary>Verifies durable recovery view reads return subscription generation and retained canonical operation proofs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadSnapshotRecoveryViewReturnsGenerationAndOperationProofs()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        _ = journal.RegisterSubscription(identity);

        var view = journal.ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });

        await Assert.That(view.SubscriptionState).IsNotNull();
        await Assert.That(view.SubscriptionState?.Generation ?? 0).IsGreaterThan(0);
        await Assert.That(view.OperationDispositions).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(view.OperationDispositions[0].Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
    }

    /// <summary>Verifies snapshot offer proof data is durable enough to replay an identical retry after reopen.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A successful offer did not return a cursor.</exception>
    [Test]
    public async Task SnapshotOfferRetryAfterReopenUsesPersistedProof()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        ServerSnapshotOfferRequest offer;
        string cursor;
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            var state = journal.RegisterSubscription(identity);
            var snapshot = journal.Read(StreamKey(), [key]);
            var request = SnapshotRequest(identity.SubscriptionId);
            offer = new()
            {
                StreamKey = StreamKey(),
                Subscription = identity,
                View = SnapshotView(snapshot, state),
                RecoveryRequest = request,
                RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot),
                Limits = SnapshotLimits(),
            };
            var first = journal.TryOfferSnapshot(offer);
            cursor = first.Cursor ?? throw new InvalidOperationException("A successful snapshot offer must include a cursor.");
            await Assert.That(first.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        }

        using var reopened = CreateJournal(database.Path);
        var retry = reopened.TryOfferSnapshot(offer);

        await Assert.That(retry.Status).IsEqualTo(ServerSnapshotOfferStatus.AlreadyOffered);
        await Assert.That(retry.Cursor).IsEqualTo(cursor);
    }

    /// <summary>Verifies subscription generations stay monotonic after the highest live generation expires and the database reopens.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferAfterReopenAndSubscriptionRecreateRejectsOldGenerationView()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        ServerSnapshotRecoveryView oldView;
        RemoteSnapshotRecoveryRequest request;
        using (var journal = new SqliteServerCommitJournal(database.Path, new() { TimeProvider = clock, SubscriptionRetention = TimeSpan.FromTicks(1) }))
        {
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            _ = journal.RegisterSubscription(identity);
            request = SnapshotRequest(identity.SubscriptionId);
            oldView = journal.ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
            clock.SetUtcNow(Start.AddTicks(ExpiredSubscriptionTicks));
            _ = journal.Compact();
        }

        using var reopened = new SqliteServerCommitJournal(database.Path, new() { TimeProvider = clock, SubscriptionRetention = TimeSpan.FromTicks(1) });
        var recreated = reopened.RegisterSubscription(identity);
        var result = reopened.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = oldView,
            RecoveryRequest = request,
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, oldView.Snapshot),
            Limits = SnapshotLimits(),
        });

        await Assert.That(oldView.SubscriptionState).IsNotNull();
        await Assert.That(recreated.Generation).IsGreaterThan(oldView.SubscriptionState?.Generation ?? 0);
        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
    }

    /// <summary>Verifies schema-four migration seeds the durable subscription generation allocator from retained rows.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotGenerationMigrationSeedsDurableAllocator()
    {
        using var database = new TemporaryDatabase();
        var first = SnapshotSubscription();
        var second = SnapshotSubscription(SecondSnapshotSubscriptionText);
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.RegisterSubscription(first);
        }

        RewriteSubscriptionsAsSchemaFour(database.Path);
        using var migrated = CreateJournal(database.Path);
        var retained = migrated.RegisterSubscription(first);
        var created = migrated.RegisterSubscription(second);

        await Assert.That(ReadSubscriptionGenerationHighWater(database.Path)).IsEqualTo(created.Generation);
        await Assert.That(created.Generation).IsGreaterThan(retained.Generation);
    }

    /// <summary>Verifies generation overflow fails before registering a partial subscription row.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotGenerationOverflowFailsBeforeRegistrationMutation()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateJournal(database.Path))
        {
            await Assert.That(journal.SubscriptionCount).IsEqualTo(0);
        }

        WriteSubscriptionGenerationHighWater(database.Path, long.MaxValue);
        using var reopened = CreateJournal(database.Path);

        await Assert.That(() => reopened.RegisterSubscription(SnapshotSubscription())).ThrowsExactly<InvalidOperationException>();
        await Assert.That(reopened.SubscriptionCount).IsEqualTo(0);
        await Assert.That(ReadSubscriptionGenerationHighWater(database.Path)).IsEqualTo(long.MaxValue);
    }

    /// <summary>Verifies missing current-schema generation metadata fails closed when deleted rows hide the true high-water.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MissingCurrentSchemaGenerationHighWaterAfterDeletedHighestGenerationFailsClosed()
    {
        using var database = new TemporaryDatabase();
        var first = SnapshotSubscription();
        var second = SnapshotSubscription(SecondSnapshotSubscriptionText);
        ServerSubscriptionState firstState;
        ServerSubscriptionState secondState;
        using (var journal = CreateJournal(database.Path))
        {
            firstState = journal.RegisterSubscription(first);
            secondState = journal.RegisterSubscription(second);
        }

        DeleteSubscriptionAndGenerationHighWater(database.Path, second.SubscriptionId);

        await Assert.That(secondState.Generation).IsGreaterThan(firstState.Generation);
        await Assert.That(() => CreateJournal(database.Path).Dispose()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies malformed current-schema generation metadata fails closed as corruption.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MalformedCurrentSchemaGenerationHighWaterFailsClosed()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.RegisterSubscription(SnapshotSubscription());
        }

        WriteSubscriptionGenerationHighWater(database.Path, "not-a-generation");

        await Assert.That(() => CreateJournal(database.Path).Dispose()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies negative current-schema generation metadata fails closed as corruption.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task NegativeCurrentSchemaGenerationHighWaterFailsClosed()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.RegisterSubscription(SnapshotSubscription());
        }

        WriteSubscriptionGenerationHighWater(database.Path, -1);

        await Assert.That(() => CreateJournal(database.Path).Dispose()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies disposed SQLite snapshot recovery interface reads and offers fail before transactions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposedSnapshotRecoveryJournalInterfaceCallsThrow()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        ServerSnapshotRecoveryReadRequest readRequest;
        ServerSnapshotOfferRequest offerRequest;
        var journal = CreateJournal(database.Path);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        readRequest = new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = SnapshotRequest(identity.SubscriptionId), Limits = SnapshotLimits() };
        offerRequest = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot);
        var recovery = (IServerSnapshotRecoveryJournal)journal;
        journal.Dispose();

        await Assert.That(() => recovery.ReadSnapshotRecoveryView(readRequest)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => recovery.TryOfferSnapshot(offerRequest)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies durable recovery view reads retain the matching expired cursor offer proof.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task ReadSnapshotRecoveryViewReturnsExpiredCursorOfferProof()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SnapshotSubscription();
        _ = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        const string expiredCursor = FirstCursor;

        var view = journal.ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [], expiredCursor),
            Limits = SnapshotLimits(),
        });

        await Assert.That(view.RequestedExpiredCursor).IsEqualTo(expiredCursor);
        await Assert.That(view.ExpiredCursorOffer?.Cursor).IsEqualTo(expiredCursor);
    }

    /// <summary>Verifies durable recovery view reads preserve an unknown expired cursor without fabricating an offer proof.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task ReadSnapshotRecoveryViewReturnsNullExpiredCursorOfferForUnknownCursor()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SnapshotSubscription();
        _ = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        var expiredCursor = ServerReceiveGroupCursor.Create(StreamKey(), DoubleEntryCount);

        var view = journal.ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [], expiredCursor),
            Limits = SnapshotLimits(),
        });

        await Assert.That(view.RequestedExpiredCursor).IsEqualTo(expiredCursor);
        await Assert.That(view.ExpiredCursorOffer).IsNull();
    }

    /// <summary>Verifies offers with a foreign trusted identity are rejected before durable mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsForeignSubscriptionIdentityBeforeMutation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var foreign = new ServerSubscriptionIdentity(StreamKey(), OtherClient, identity.SubscriptionId);

        var result = journal.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = foreign,
            View = SnapshotView(snapshot, state),
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId),
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(result.SubscriptionState).IsNull();
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies non-recovered snapshot responses do not start a durable offer transaction.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsNonRecoveredResultBeforeTransaction()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);

        var result = journal.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = SnapshotView(snapshot, state),
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId),
            RecoveryResult = new() { Status = RemoteSnapshotRecoveryStatus.RetentionExpired },
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies altered durable pending proof counts are rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsAlteredCapturedProofCountBeforeTransaction()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var view = journal.ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });
        var alteredView = view with { OperationDispositions = [] };

        var result = journal.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = alteredView,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            RecoveryResult = SnapshotRecoveryResult(
                identity.SubscriptionId,
                view.Snapshot,
                [CreateClientDisposition(view.OperationDispositions[0])]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies altered durable positive proof fingerprints are rejected against the current ledger.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsAlteredPositiveProofFingerprintBeforeTransaction()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var view = journal.ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });
        var alteredProof = view.OperationDispositions[0] with { Fingerprint = MismatchedFingerprint() };
        var alteredView = view with { OperationDispositions = [alteredProof] };

        var result = journal.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = alteredView,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, [CreateClientDisposition(alteredProof)]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies snapshot offers with missing captured state report a concurrent change.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferWithMissingViewStateReportsConcurrentChange()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);

        var result = journal.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = SnapshotView(snapshot, null),
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId),
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(result.SubscriptionState?.Generation ?? 0).IsEqualTo(state.Generation);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies an intervening stream commit rejects a stale durable snapshot view without mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferReportsConcurrentChangeWhenStreamAdvancesBeforeOffer()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [first]);
        _ = journal.TryCommit(Plan(
            SingleEntryCount,
            State(SecondVersion),
            Stamp(second),
            Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));

        var result = journal.TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot));
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies a cursor already offered by receive paging rejects a snapshot proof without mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferReportsConcurrentChangeForExistingReceiveCursor()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SnapshotSubscription();
        var snapshot = SeedOneCommitAndOfferFirstPage(journal, identity);
        var state = journal.RegisterSubscription(identity);

        var result = journal.TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot));
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies a recovered checkpoint with a stale durable frontier cursor is rejected without subscription mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A recovered snapshot offer did not include a checkpoint.</exception>
    [Test]
    public async Task SnapshotOfferRejectsMismatchedCheckpointCursorBeforeMutation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot);
        var checkpoint = offer.RecoveryResult.Checkpoint ?? throw new InvalidOperationException("A recovered snapshot offer must include a checkpoint.");

        var result = journal.TryOfferSnapshot(offer with
        {
            RecoveryResult = offer.RecoveryResult with
            {
                Checkpoint = checkpoint with { FrontierCursor = ServerReceiveGroupCursor.Create(StreamKey(), DoubleEntryCount) },
            },
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies an intervening acknowledgement revision fences an otherwise current snapshot view.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferReportsConcurrentChangeWhenSubscriptionRevisionChanges()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var identity = SnapshotSubscription();
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(SingleEntryCount, State(SecondVersion), Stamp(second), Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [first, second]);
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException("The expected first subscription page was missing.");
        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));

        var result = journal.TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot));

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(result.SubscriptionState?.Revision ?? 0).IsGreaterThan(state.Revision);
    }

    /// <summary>Verifies durable snapshot offer capacity failure leaves the subscription unchanged.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferReturnsCapacityExceededWithoutMutationWhenOfferCapacityFull()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path, maximumSubscriptionOffers: SingleEntryCount);
        var identity = SnapshotSubscription();
        var snapshot = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        var state = journal.RegisterSubscription(identity);

        var result = journal.TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot));
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.CapacityExceeded);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies durable snapshot offer admission compacts expired offers when capacity is initially full.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferCompactsExpiredOfferWhenCapacityIsFull()
    {
        using var database = new TemporaryDatabase();
        var clock = new ManualTimeProvider(Start);
        using var journal = CreateSubscriptionJournal(
            database.Path,
            clock,
            retention: TimeSpan.FromTicks(1),
            maximumSubscriptionOffers: SingleEntryCount);
        var identity = SnapshotSubscription();
        var snapshot = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        clock.SetUtcNow(Start.AddTicks(ExpiredSubscriptionTicks));
        var state = journal.RegisterSubscription(identity);

        var result = journal.TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot));

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(result.Cursor).IsEqualTo(SecondCursor);
        await Assert.That(result.SubscriptionState?.OfferCount ?? 0).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies durable snapshot offers can be issued when the normal receive cursor is already acknowledged.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferAfterAcknowledgedNormalCursorDoesNotChargeLatestCursorDelta()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateSubscriptionJournal(database.Path);
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException("The expected first subscription page was missing.");
        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));
        var request = SnapshotRequest(identity.SubscriptionId);
        var view = journal.ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });

        var result = journal.TryOfferSnapshot(CreateSnapshotOffer(identity, view, request, view.Snapshot));

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(result.Cursor).IsEqualTo(batch.NextCursor);
        await Assert.That(result.SubscriptionState?.OfferCount ?? 0).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies snapshot acknowledgement rejects an offer from a stale subscription generation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A successful snapshot offer did not return a cursor.</exception>
    [Test]
    public async Task AcknowledgeSnapshotOfferRejectsGenerationMismatch()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        string cursor;
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            var state = journal.RegisterSubscription(identity);
            var snapshot = journal.Read(StreamKey(), [key]);
            var offer = journal.TryOfferSnapshot(
                CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot));
            cursor = offer.Cursor ?? throw new InvalidOperationException("A successful snapshot offer must include a cursor.");
        }

        IncrementSubscriptionGeneration(database.Path, identity.SubscriptionId);
        using var reopened = CreateJournal(database.Path);

        await Assert
            .That(() => reopened.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, cursor))))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies snapshot offer revision overflow fails before inserting durable offer state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRevisionOverflowFailsBeforeMutation()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        using (var seeded = CreateJournal(database.Path))
        {
            _ = seeded.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            _ = seeded.RegisterSubscription(identity);
        }

        WriteSubscriptionRevision(database.Path, identity.SubscriptionId, long.MaxValue);
        using var journal = CreateJournal(database.Path);
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot);

        await Assert.That(() => journal.TryOfferSnapshot(offer)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(journal.RegisterSubscription(identity).OfferCount).IsEqualTo(0);
    }

    /// <summary>Verifies a storage conflict during snapshot offer state update rolls the inserted offer back.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRollsBackInsertedOfferWhenRevisionUpdateLosesSubscription()
    {
        using var database = new TemporaryDatabase();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        using (var seeded = CreateJournal(database.Path))
        {
            _ = seeded.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
            _ = seeded.RegisterSubscription(identity);
        }

        using var journal = CreateJournal(database.Path);
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId), snapshot);

        CreateDeleteSubscriptionBeforeRevisionUpdateTrigger(database.Path);
        await Assert.That(() => journal.TryOfferSnapshot(offer)).ThrowsExactly<InvalidOperationException>();
        DropDeleteSubscriptionBeforeRevisionUpdateTrigger(database.Path);
        await Assert.That(journal.RegisterSubscription(identity).OfferCount).IsEqualTo(0);
    }

    /// <summary>Verifies schema-four migration rejects unsupported snapshot-offer metadata.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferSchemaFourMigrationRejectsUnsupportedMetadata()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.RegisterSubscription(SnapshotSubscription());
        }

        RewriteSubscriptionsAsSchemaFour(database.Path);
        WriteSchemaVersionMetadata(database.Path, "9");

        await Assert.That(() => CreateJournal(database.Path).Dispose()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema-four migration rejects malformed snapshot-offer metadata storage.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferSchemaFourMigrationRejectsMalformedMetadata()
    {
        using var database = new TemporaryDatabase();
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.RegisterSubscription(SnapshotSubscription());
        }

        RewriteSubscriptionsAsSchemaFour(database.Path);
        CorruptSchemaFourMetadataTable(database.Path);

        await Assert.That(() => CreateJournal(database.Path).Dispose()).ThrowsExactly<InvalidOperationException>();
    }
}
