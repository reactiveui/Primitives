// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery offer tests for replay-only operation proof reconciliation.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>Verifies changed same-id replay-only operation intent is rejected against the captured union proof before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsMutatedReplayIntentBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, [], [operation]);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
        var mutated = operation with { Payload = Payload("mutated-replay-intent") };

        await Assert.That(view.OperationDispositions).Count().IsEqualTo(SingleEntryCount);
        var proof = view.OperationDispositions[0];

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [], [mutated]),
            RecoveryResult = SnapshotRecoveryResult(
                identity.SubscriptionId,
                view.Snapshot,
                [CreateClientDisposition(proof)]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(proof.Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies altered replay-only positive proof fingerprints are rejected before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotOfferRejectsAlteredReplayProofFingerprintBeforeMutation()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, [], [operation]);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });

        await Assert.That(view.OperationDispositions).Count().IsEqualTo(SingleEntryCount);
        var alteredProof = view.OperationDispositions[0] with { Fingerprint = MismatchedFingerprint() };
        var alteredView = view with { OperationDispositions = [alteredProof] };

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = alteredView,
            RecoveryRequest = request,
            RecoveryResult = SnapshotRecoveryResult(
                identity.SubscriptionId,
                view.Snapshot,
                [CreateClientDisposition(alteredProof)]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ConcurrentChange);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
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
        var view = SnapshotView(journal.Read(StreamKey(), []), state) with { OperationDispositions = [disposition] };
        var result = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, []);

        var matches = ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(view, result);

        await Assert.That(matches).IsFalse();
    }

    /// <summary>Verifies exact recovered disposition identity sets match even when the public order differs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RecoveryResultMatchesViewAcceptsReorderedExactDispositionIdentities()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var state = journal.RegisterSubscription(identity);
        var first = new ServerSnapshotOperationDisposition { OperationId = OperationId.New(), Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var second = new ServerSnapshotOperationDisposition { OperationId = OperationId.New(), Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var view = SnapshotView(journal.Read(StreamKey(), []), state) with { OperationDispositions = [first, second] };
        var result = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, [CreateClientDisposition(second), CreateClientDisposition(first)]);

        var matches = ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(view, result);

        await Assert.That(matches).IsTrue();
    }

    /// <summary>Verifies malformed recovered disposition identity sets reject before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RecoveryResultMatchesViewRejectsMalformedDispositionIdentitySet()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var state = journal.RegisterSubscription(identity);
        var first = new ServerSnapshotOperationDisposition { OperationId = OperationId.New(), Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var second = new ServerSnapshotOperationDisposition { OperationId = OperationId.New(), Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var extra = new ServerSnapshotOperationDisposition { OperationId = OperationId.New(), Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var view = SnapshotView(journal.Read(StreamKey(), []), state) with { OperationDispositions = [first, second] };
        var duplicate = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, [CreateClientDisposition(first), CreateClientDisposition(first)]);
        var missing = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, [CreateClientDisposition(first)]);
        var extraResult = SnapshotRecoveryResult(
            identity.SubscriptionId,
            view.Snapshot,
            [CreateClientDisposition(first), CreateClientDisposition(second), CreateClientDisposition(extra)]);

        var duplicateMatches = ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(view, duplicate);
        var missingMatches = ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(view, missing);
        var extraMatches = ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(view, extraResult);

        await Assert.That(duplicateMatches).IsFalse();
        await Assert.That(missingMatches).IsFalse();
        await Assert.That(extraMatches).IsFalse();
    }

    /// <summary>Verifies duplicate captured disposition identities reject before matching remote proofs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RecoveryResultMatchesViewRejectsDuplicateCapturedDispositionIdentity()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var state = journal.RegisterSubscription(identity);
        var operationId = OperationId.New();
        var first = new ServerSnapshotOperationDisposition { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var second = new ServerSnapshotOperationDisposition { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown, Result = null, Fingerprint = null };
        var view = SnapshotView(journal.Read(StreamKey(), []), state) with { OperationDispositions = [first, second] };
        var result = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, [CreateClientDisposition(first), CreateClientDisposition(second)]);

        var matches = ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(view, result);

        await Assert.That(matches).IsFalse();
    }

    /// <summary>Verifies captured pending/replay boundary mismatches fail replay-only proof acceptance.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReplayOnlyProofsAreAcceptedRejectsCapturedPendingCountMismatch()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        _ = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, [], [operation]);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
        var alteredView = view with { CapturedPendingOperationCount = SingleEntryCount };

        var accepted = ServerSnapshotRecoveryJournalOperations.ReplayOnlyProofsAreAccepted(alteredView, request);

        await Assert.That(accepted).IsFalse();
    }

    /// <summary>Verifies captured operation proof count mismatches fail replay-only proof acceptance.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReplayOnlyProofsAreAcceptedRejectsCapturedDispositionCountMismatch()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        _ = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, [], [operation]);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
        var alteredView = view with { OperationDispositions = [] };

        var accepted = ServerSnapshotRecoveryJournalOperations.ReplayOnlyProofsAreAccepted(alteredView, request);

        await Assert.That(accepted).IsFalse();
    }

    /// <summary>Verifies invalid UTF-16 recovered results reject before durable offer mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A recovered snapshot offer did not include a checkpoint.</exception>
    [Test]
    public async Task SnapshotOfferRejectsInvalidUtf16RecoveredResultBeforeMutation()
    {
        var journal = CreateJournal();
        var identity = SnapshotSubscription();
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), []);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), request, snapshot);
        var checkpoint = offer.RecoveryResult.Checkpoint
            ?? throw new InvalidOperationException("A recovered snapshot offer must include a checkpoint.");
        var invalidResult = offer.RecoveryResult with
        {
            Checkpoint = checkpoint with { ServerVersion = new('\uD800', 1) },
        };

        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(offer with { RecoveryResult = invalidResult });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies replay-only rejected and conflict proofs fail closed before durable offer mutation.</summary>
    /// <param name="operationResultKind">The retained replay-only proof kind.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(OperationResultKind.Rejected)]
    [Arguments(OperationResultKind.Conflict)]
    public async Task SnapshotOfferRejectsReplayOnlyContradictoryProofBeforeMutation(OperationResultKind operationResultKind)
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, operationResultKind)));
        var state = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, [], [operation]);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });

        await Assert.That(view.OperationDispositions).Count().IsEqualTo(SingleEntryCount);
        var result = ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = request,
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

    /// <summary>Verifies moving the pending/replay boundary with the same ordered operations rejects before mutation.</summary>
    /// <param name="moveReplayIntoPending">Whether the changed offer request moves a captured replay operation into pending.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotOfferRejectsOperationRoleBoundaryChangeBeforeMutation(bool moveReplayIntoPending)
    {
        var journal = CreateJournal();
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var identity = SnapshotSubscription();
        var firstOperation = SnapshotOperation(firstKey, FirstOperationSeed);
        var secondOperation = SnapshotOperation(secondKey, SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(firstKey), SnapshotEntry(firstKey, firstOperation, OperationResultKind.Accepted)));
        _ = journal.TryCommit(Plan(
            SingleEntryCount,
            State(SecondVersion),
            Stamp(secondKey),
            SnapshotEntry(secondKey, secondOperation, OperationResultKind.Accepted)));
        var state = journal.RegisterSubscription(identity);
        var recoveryJournal = (IServerSnapshotRecoveryJournal)journal;
        var capturedRequest = moveReplayIntoPending
            ? SnapshotRequest(identity.SubscriptionId, [firstOperation], [secondOperation])
            : SnapshotRequest(identity.SubscriptionId, [firstOperation, secondOperation], []);
        var changedRequest = moveReplayIntoPending
            ? SnapshotRequest(identity.SubscriptionId, [firstOperation, secondOperation], [])
            : SnapshotRequest(identity.SubscriptionId, [firstOperation], [secondOperation]);
        var view = recoveryJournal.ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = capturedRequest, Limits = SnapshotLimits() });

        await Assert.That(view.OperationDispositions).Count().IsEqualTo(DoubleEntryCount);
        var result = recoveryJournal.TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = changedRequest,
            RecoveryResult = SnapshotRecoveryResult(
                identity.SubscriptionId,
                view.Snapshot,
                [CreateClientDisposition(view.OperationDispositions[0]), CreateClientDisposition(view.OperationDispositions[1])]),
            Limits = SnapshotLimits(),
        });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }
}
