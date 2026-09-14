// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery offer tests.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>The bounded canonical fingerprint byte budget used by snapshot offer fixtures.</summary>
    private const int SnapshotFingerprintBudget = 4096;

    /// <summary>The elapsed ticks used to expire one-tick subscription retention fixtures.</summary>
    private const int ExpiredSubscriptionTicks = 2;

    /// <summary>The first count that exceeds the fixed snapshot recovery view ownership ceiling.</summary>
    private const int OwnedCollectionOverflowCount = 4097;

    /// <summary>Verifies snapshots expose the complete group frontier even when a group produced no sidecar events.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadExposesLastGroupSequenceForAcceptedEmptyGroup()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);

        var result = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed, events: [])));
        var snapshot = journal.Read(StreamKey(), [key]);

        await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        await Assert.That(snapshot.LastEventSequence).IsEqualTo(0);
        await Assert.That(snapshot.LastGroupSequence).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies recovery view reads return durable subscription generation and retained operation proofs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadSnapshotRecoveryViewReturnsGenerationAndOperationProofs()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        _ = journal.RegisterSubscription(identity);

        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
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

    /// <summary>Verifies an identical snapshot offer retry replays the retained proof without charging capacity again.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRetryReplaysWithoutMutationOrCapacityCharge()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var view = SnapshotView(snapshot, state);
        var result = SnapshotRecoveryResult(identity.SubscriptionId, snapshot, []);
        var offer = new ServerSnapshotOfferRequest
            { StreamKey = StreamKey(), Subscription = identity, View = view, RecoveryRequest = request, RecoveryResult = result, Limits = SnapshotLimits() };

        var first = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer);
        var retry = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer);

        await Assert.That(first.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(retry.Status).IsEqualTo(ServerSnapshotOfferStatus.AlreadyOffered);
        await Assert.That(retry.SubscriptionState?.Revision).IsEqualTo(first.SubscriptionState?.Revision);
        await Assert.That(retry.Cursor).IsEqualTo(first.Cursor);
    }

    /// <summary>Verifies ACK is a semantic mutation that fences later retries of the same recovered view.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A successful offer did not return a cursor.</exception>
    [Test]
    public async Task SnapshotOfferRetryAfterAcknowledgementReportsConcurrentChange()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var offer = new ServerSnapshotOfferRequest
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = SnapshotView(snapshot, state),
            RecoveryRequest = request,
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot, []),
            Limits = SnapshotLimits(),
        };
        var first = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer);
        var cursor = first.Cursor ?? throw new InvalidOperationException("A successful snapshot offer must include a cursor.");

        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, cursor)));
        var retry = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer);

        await Assert.That(first.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(retry.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
    }

    /// <summary>Verifies a recreated subscription id is fenced by durable generation instead of timestamps.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRetryAfterSubscriptionRecreateReportsConcurrentChange()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = new InMemoryServerCommitJournal(new() { TimeProvider = clock, SubscriptionRetention = TimeSpan.FromTicks(1) });
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var offer = new ServerSnapshotOfferRequest
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = SnapshotView(snapshot, state),
            RecoveryRequest = request,
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot, []),
            Limits = SnapshotLimits(),
        };

        clock.SetUtcNow(Start.AddTicks(ExpiredSubscriptionTicks));
        _ = journal.Compact();
        var recreated = journal.RegisterSubscription(identity);
        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer);

        await Assert.That(recreated.Generation).IsGreaterThan(state.Generation);
        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
    }

    /// <summary>Verifies changed same-id pending operation intent is rejected against the captured view proof before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsMutatedPendingIntentBeforeMutation()
    {
        var baseline = OfferUnchangedPendingIntent();
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });
        var mutated = operation with { Payload = Payload("mutated-intent") };
        var proof = view.OperationDispositions[0];

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [mutated]),
            RecoveryResult = SnapshotRecoveryResult(
                identity.SubscriptionId,
                view.Snapshot,
                [CreateClientDisposition(proof)]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(baseline.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies changed same-id unknown pending intent is rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsMutatedUnknownPendingIntentBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var committedKey = OperationKey(SecondOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(
            0,
            State(FirstVersion),
            Stamp(committedKey),
            Entry(committedKey, OperationResultKind.Accepted, SecondOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });
        var mutated = operation with { Payload = Payload("mutated-unknown-intent") };
        var proof = view.OperationDispositions[0];

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [mutated]),
            RecoveryResult = SnapshotRecoveryResult(
                identity.SubscriptionId,
                view.Snapshot,
                [CreateClientDisposition(proof)]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(proof.Kind).IsEqualTo(SnapshotOperationDispositionKind.Unknown);
        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies a recovery view captured for another stream is rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsMismatchedViewStreamBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
        var otherStreamKey = new ServerStreamKey(OtherTenant, OtherStream);
        var otherRequest = SnapshotRequest(
            otherStreamKey,
            identity.SubscriptionId,
            [],
            view.RequestedExpiredCursor);

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = otherStreamKey,
            Subscription = new(otherStreamKey, Client, identity.SubscriptionId),
            View = view,
            RecoveryRequest = otherRequest,
            RecoveryResult = SnapshotRecoveryResult(OtherStream, identity.SubscriptionId, view.Snapshot, []),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies a changed expired cursor is rejected against the captured view before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsChangedExpiredCursorBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var recoveryJournal = (IServerSnapshotRecoveryJournal)journal;
        var view = recoveryJournal.ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
        var changedRequest = SnapshotRequest(StreamKey(), identity.SubscriptionId, [], null);
        var recoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, []);
        var offerRequest = CreateSnapshotOffer(identity, view, request, view.Snapshot) with { RecoveryRequest = changedRequest, RecoveryResult = recoveryResult };

        var result = recoveryJournal.TryOfferSnapshot(offerRequest);
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies recovery view reads retain the matching expired cursor offer proof.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task ReadSnapshotRecoveryViewReturnsExpiredCursorOfferProof()
    {
        var journal = CreateSubscriptionJournal();
        var identity = SnapshotSubscription();
        _ = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        const string expiredCursor = FirstCursor;

        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(StreamKey(), identity.SubscriptionId, [], expiredCursor),
            Limits = SnapshotLimits(),
        });

        await Assert.That(view.RequestedExpiredCursor).IsEqualTo(expiredCursor);
        await Assert.That(view.ExpiredCursorOffer?.Cursor).IsEqualTo(expiredCursor);
    }

    /// <summary>Verifies recovery view reads preserve an unknown expired cursor without fabricating an offer proof.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task ReadSnapshotRecoveryViewReturnsNullExpiredCursorOfferForUnknownCursor()
    {
        var journal = CreateSubscriptionJournal();
        var identity = SnapshotSubscription();
        _ = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        var expiredCursor = ServerReceiveGroupCursor.Create(StreamKey(), DoubleEntryCount);

        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(StreamKey(), identity.SubscriptionId, [], expiredCursor),
            Limits = SnapshotLimits(),
        });

        await Assert.That(view.RequestedExpiredCursor).IsEqualTo(expiredCursor);
        await Assert.That(view.ExpiredCursorOffer).IsNull();
    }

    /// <summary>Verifies recovery view reads reject a request not bound to the trusted subscription stream.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadSnapshotRecoveryViewRejectsUnboundRequestBeforeStateRead()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var otherStreamKey = new ServerStreamKey(OtherTenant, OtherStream);

        await Assert
            .That(() => ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
            {
                StreamKey = otherStreamKey,
                Subscription = identity,
                RecoveryRequest = SnapshotRequest(identity.SubscriptionId, []),
                Limits = SnapshotLimits(),
            }))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies snapshot offer requests reject an unbound recovery subscription before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsUnboundRecoveryRequestBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var otherSubscription = new SubscriptionId(new Guid("00000014-0000-0000-0000-000000000002"));
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var view = SnapshotView(snapshot, state);

        await Assert
            .That(() => ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
            {
                StreamKey = StreamKey(),
                Subscription = identity,
                View = view,
                RecoveryRequest = SnapshotRequest(otherSubscription, []),
                RecoveryResult = SnapshotRecoveryResult(otherSubscription, snapshot, []),
                Limits = SnapshotLimits(),
            }))
            .ThrowsExactly<ArgumentException>();
        await Assert.That(journal.RegisterSubscription(identity).Revision).IsEqualTo(state.Revision);
    }

    /// <summary>Verifies recovery view reads reject pending operations above configured structural limits.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReadSnapshotRecoveryViewRejectsPendingOperationsAboveLimit()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var first = SnapshotOperation(OperationKey(FirstOperationSeed), FirstOperationSeed);
        var second = SnapshotOperation(OperationKey(SecondOperationSeed), SecondOperationSeed);

        await Assert
            .That(() => ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
            {
                StreamKey = StreamKey(),
                Subscription = identity,
                RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [first, second]),
                Limits = new() { MaximumPendingOperations = SingleEntryCount, MaximumLogicalBytes = DefaultMaximumLogicalBytes },
            }))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies internally owned recovery view proof lists enforce the fixed copy ceiling.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotRecoveryViewRejectsOversizedOwnedProofList()
    {
        var journal = CreateJournal();
        var snapshot = journal.Read(StreamKey(), []);
        var state = journal.RegisterSubscription(SnapshotSubscription());

        await Assert
            .That(() => new ServerSnapshotRecoveryView
            {
                Snapshot = snapshot,
                SubscriptionState = state,
                ExpiredCursorOffer = null,
                RequestedExpiredCursor = null,
                OperationDispositions = new ServerSnapshotOperationDisposition[OwnedCollectionOverflowCount],
                OperationFingerprints = [],
            })
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies a missing captured subscription state reports concurrent change without mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferWithMissingViewStateReportsConcurrentChange()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
        var state = journal.RegisterSubscription(identity);

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(
            CreateSnapshotOffer(identity, view, request, view.Snapshot));
        var after = journal.RegisterSubscription(identity);

        await Assert.That(view.SubscriptionState).IsNull();
        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(result.SubscriptionState?.Generation ?? 0).IsEqualTo(state.Generation);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies offers with a foreign trusted identity are rejected before subscription mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsForeignSubscriptionIdentityBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var foreign = new ServerSubscriptionIdentity(StreamKey(), ShortClient, identity.SubscriptionId);

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = foreign,
            View = SnapshotView(snapshot, state),
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, []),
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot, []),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(result.SubscriptionState).IsNull();
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies altered captured pending proof counts are rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsAlteredCapturedProofCountBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });
        var alteredView = view with { OperationDispositions = [] };

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
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

    /// <summary>Verifies altered positive proof fingerprints are rejected against the current ledger.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsAlteredPositiveProofFingerprintBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });
        var alteredProof = view.OperationDispositions[0] with { Fingerprint = MismatchedFingerprint() };
        var alteredView = view with { OperationDispositions = [alteredProof] };

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
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

    /// <summary>Verifies unknown operation proofs are ignored while matching positive proofs are checked.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PositiveProofsMatchSkipsUnknownDispositions()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var state = journal.RegisterSubscription(identity);
        var disposition = new ServerSnapshotOperationDisposition { OperationId = OperationId.New(), Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var view = SnapshotView(journal.Read(StreamKey(), []), state) with
        {
            OperationDispositions = [disposition],
        };

        var matches = ServerSnapshotRecoveryJournalOperations.PositiveProofsMatch(identity, view, view.Snapshot);

        await Assert.That(matches).IsTrue();
    }

    /// <summary>Verifies missing retained ledger entries reject positive snapshot proofs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PositiveProofsMatchRejectsMissingRetainedEntry()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var retainedKey = OperationKey(SecondOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        var entry = SnapshotEntry(key, operation, OperationResultKind.Accepted);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(retainedKey), Entry(retainedKey, OperationResultKind.Accepted, SecondOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [retainedKey]);
        var disposition = new ServerSnapshotOperationDisposition
        {
            OperationId = key.OperationId,
            Kind = SnapshotOperationDispositionKind.IncludedAccepted,
            Result = entry.Result,
            Fingerprint = entry.Fingerprint,
        };
        var view = SnapshotView(snapshot, state) with
        {
            OperationDispositions = [disposition],
        };

        var matches = ServerSnapshotRecoveryJournalOperations.PositiveProofsMatch(identity, view, view.Snapshot);

        await Assert.That(matches).IsFalse();
    }

    /// <summary>Verifies recovered result disposition count mismatches are rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RecoveryResultMatchesViewRejectsDispositionCountMismatch()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var state = journal.RegisterSubscription(identity);
        var disposition = new ServerSnapshotOperationDisposition { OperationId = OperationId.New(), Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var view = SnapshotView(journal.Read(StreamKey(), []), state) with
        {
            OperationDispositions = [disposition],
        };
        var result = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, []);

        var matches = ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(view, result);

        await Assert.That(matches).IsFalse();
    }

    /// <summary>Verifies non-recovered snapshot responses do not mutate a durable subscription.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsNonRecoveredResultBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = SnapshotView(snapshot, state),
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, []),
            RecoveryResult = new() { Status = RemoteSnapshotRecoveryStatus.RetentionExpired },
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies a cursor already offered by receive paging rejects a snapshot proof without mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferReportsConcurrentChangeForExistingReceiveCursor()
    {
        var journal = CreateSubscriptionJournal();
        var identity = SnapshotSubscription();
        var snapshot = SeedOneCommitAndOfferFirstPage(journal, identity);
        var state = journal.RegisterSubscription(identity);

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId, []), snapshot));
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies a recovered checkpoint with a stale frontier cursor is rejected without subscription mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A recovered snapshot offer did not include a checkpoint.</exception>
    [Test]
    public async Task SnapshotOfferRejectsMismatchedCheckpointCursorBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), request, snapshot);
        var checkpoint = offer.RecoveryResult.Checkpoint ?? throw new InvalidOperationException("A recovered snapshot offer must include a checkpoint.");

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer with
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

    /// <summary>Verifies mismatched recovered operation proofs are rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsMismatchedRecoveredDispositionBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            Limits = SnapshotLimits(),
        });

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            RecoveryResult = SnapshotRecoveryResult(
                identity.SubscriptionId,
                view.Snapshot,
                [new() { OperationId = operation.OperationId, Kind = SnapshotOperationDispositionKind.Unknown }]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(view.OperationDispositions[0].Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies an expired subscription yields a missing snapshot offer without fabricated state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ExpiredSnapshotSubscriptionOfferReturnsMissingWithoutState()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = new InMemoryServerCommitJournal(new() { TimeProvider = clock, SubscriptionRetention = TimeSpan.FromTicks(1) });
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId, []), snapshot);

        clock.SetUtcNow(Start.AddTicks(ExpiredSubscriptionTicks));
        _ = journal.Compact();
        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.MissingSubscription);
        await Assert.That(result.SubscriptionState).IsNull();
        await Assert.That(result.Cursor).IsNull();
    }

    /// <summary>Verifies an intervening stream commit rejects the stale snapshot view without subscription mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferReportsConcurrentChangeWhenStreamAdvancesBeforeOffer()
    {
        var journal = CreateJournal();
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

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId, []), snapshot));
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies snapshot offer capacity failure leaves the subscription unchanged.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferReturnsCapacityExceededWithoutMutationWhenOfferCapacityFull()
    {
        var journal = CreateSubscriptionJournal(maximumSubscriptionOffers: SingleEntryCount);
        var identity = SnapshotSubscription();
        var snapshot = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        var state = journal.RegisterSubscription(identity);

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId, []), snapshot));
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.CapacityExceeded);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies snapshot offer admission compacts expired offers when capacity is initially full.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferCompactsExpiredOfferWhenCapacityIsFull()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(
            clock,
            retention: TimeSpan.FromTicks(1),
            maximumSubscriptionOffers: SingleEntryCount);
        var identity = SnapshotSubscription();
        var snapshot = SeedTwoCommitsAndOfferFirstPage(journal, identity);
        clock.SetUtcNow(Start.AddTicks(ExpiredSubscriptionTicks));
        var state = journal.RegisterSubscription(identity);

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(
            CreateSnapshotOffer(identity, SnapshotView(snapshot, state), SnapshotRequest(identity.SubscriptionId, []), snapshot));

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(result.Cursor).IsEqualTo(SecondCursor);
        await Assert.That(result.SubscriptionState?.OfferCount ?? 0).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies later normal offers do not invalidate acknowledgement of an earlier snapshot proof from the same generation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A successful offer did not return a cursor.</exception>
    [Test]
    public async Task AcknowledgeSnapshotOfferSurvivesLaterNormalOfferInSameGeneration()
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [firstKey]);
        var view = SnapshotView(snapshot, state);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var offer = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = request,
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot, []),
            Limits = SnapshotLimits(),
        });
        _ = journal.TryCommit(Plan(
            SingleEntryCount,
            State(SecondVersion),
            Stamp(secondKey),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed)));
        _ = journal.OfferReceivePage(new(identity, null, DefaultMaximumLedgerEntries, DefaultMaximumEvents, DefaultMaximumLogicalBytes));

        var cursor = offer.Cursor ?? throw new InvalidOperationException("A successful snapshot offer must include a cursor.");
        var acknowledged = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, cursor)));

        await Assert.That(offer.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(acknowledged.Generation).IsEqualTo(state.Generation);
        await Assert.That(acknowledged.AcknowledgedCursor).IsEqualTo(offer.Cursor);
    }

    /// <summary>Verifies snapshot offers can be issued when the normal receive cursor is already acknowledged.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected normal receive page is missing.</exception>
    [Test]
    public async Task SnapshotOfferAfterAcknowledgedNormalCursorDoesNotChargeLatestCursorDelta()
    {
        var journal = CreateSubscriptionJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        var batch = page.Batch ?? throw new InvalidOperationException("The expected first subscription page was missing.");
        _ = journal.Acknowledge(new(StreamKey(), Client, new(identity.SubscriptionId, Stream, batch.NextCursor)));
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(
            CreateSnapshotOffer(identity, view, request, view.Snapshot));

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.Offered);
        await Assert.That(result.Cursor).IsEqualTo(batch.NextCursor);
        await Assert.That(result.SubscriptionState?.OfferCount ?? 0).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies the shared snapshot-offer generation guard allows normal and matching snapshot offers.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferGenerationGuardAllowsUnboundAndMatchingGeneration()
    {
        var normalOffer = new ServerSubscriptionOffer { Cursor = FirstCursor, GroupSequence = SingleEntryCount, OfferedAtUtc = Start, LogicalBytes = SingleEntryCount };
        var snapshotOffer = normalOffer with { SnapshotSubscriptionGeneration = SingleEntryCount };

        ServerSnapshotRecoveryJournalOperations.ThrowIfSnapshotOfferGenerationMismatch(normalOffer, SingleEntryCount);
        ServerSnapshotRecoveryJournalOperations.ThrowIfSnapshotOfferGenerationMismatch(snapshotOffer, SingleEntryCount);
        await Assert.That(snapshotOffer.SnapshotSubscriptionGeneration).IsEqualTo(SingleEntryCount);
    }

    /// <summary>Verifies the shared snapshot-offer generation guard rejects stale durable proof generation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferGenerationGuardRejectsMismatchedGeneration()
    {
        ServerSubscriptionOffer snapshotOffer = new()
        {
            Cursor = FirstCursor,
            GroupSequence = SingleEntryCount,
            OfferedAtUtc = Start,
            LogicalBytes = SingleEntryCount,
            SnapshotSubscriptionGeneration = SingleEntryCount,
        };

        await Assert
            .That(() => ServerSnapshotRecoveryJournalOperations.ThrowIfSnapshotOfferGenerationMismatch(
                snapshotOffer,
                DoubleEntryCount))
            .ThrowsExactly<InvalidOperationException>();
    }
}
