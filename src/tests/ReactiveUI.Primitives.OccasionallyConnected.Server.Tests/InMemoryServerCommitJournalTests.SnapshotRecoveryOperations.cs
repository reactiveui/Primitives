// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery operation helper tests.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>Verifies payload proof comparison rejects every persisted checkpoint identity field independently.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PayloadMatchesRejectsEachCheckpointPayloadMismatch()
    {
        var expected = Payload("checkpoint");
        var differentContract = expected with { ContractId = $"{expected.ContractId}.other" };
        var differentSchema = expected with { SchemaVersion = expected.SchemaVersion + 1 };
        var differentContentType = expected with { ContentType = "application/octet-stream" };
        var differentHash = expected with { PayloadHash = $"{expected.PayloadHash}.other" };
        var differentBytes = expected with { Payload = "different-checkpoint"u8.ToArray() };

        var matched = ServerSnapshotRecoveryJournalOperations.PayloadMatches(expected, expected);
        var nullPayloadMatched = ServerSnapshotRecoveryJournalOperations.PayloadMatches(null, expected);
        var contractMatched = ServerSnapshotRecoveryJournalOperations.PayloadMatches(differentContract, expected);
        var schemaMatched = ServerSnapshotRecoveryJournalOperations.PayloadMatches(differentSchema, expected);
        var contentTypeMatched = ServerSnapshotRecoveryJournalOperations.PayloadMatches(differentContentType, expected);
        var hashMatched = ServerSnapshotRecoveryJournalOperations.PayloadMatches(differentHash, expected);
        var bytesMatched = ServerSnapshotRecoveryJournalOperations.PayloadMatches(differentBytes, expected);

        await Assert.That(matched).IsTrue();
        await Assert.That(nullPayloadMatched).IsFalse();
        await Assert.That(contractMatched).IsFalse();
        await Assert.That(schemaMatched).IsFalse();
        await Assert.That(contentTypeMatched).IsFalse();
        await Assert.That(hashMatched).IsFalse();
        await Assert.That(bytesMatched).IsFalse();
    }

    /// <summary>Verifies frontier cursor creation falls back when no retained group can supply an event cursor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateFrontierCursorFallsBackForMissingOrEmptyStreamGroups()
    {
        var journal = CreateJournal();
        var snapshot = journal.Read(StreamKey(), []);
        var expectedCursor = ServerReceiveGroupCursor.Create(StreamKey(), snapshot.LastGroupSequence);

        var missingStreamCursor = ServerSnapshotRecoveryJournalOperations.CreateFrontierCursor(StreamKey(), null, snapshot);
        var emptyStreamCursor = ServerSnapshotRecoveryJournalOperations.CreateFrontierCursor(StreamKey(), new(), snapshot);

        await Assert.That(missingStreamCursor).IsEqualTo(expectedCursor);
        await Assert.That(emptyStreamCursor).IsEqualTo(expectedCursor);
    }

    /// <summary>Verifies offer request validation rejects each authenticated binding mismatch before mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateOfferRequestRejectsEachAuthenticatedBindingMismatch()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), [key]);
        var request = SnapshotRequest(identity.SubscriptionId, []);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), request, snapshot);
        var otherStreamKey = new ServerStreamKey(OtherTenant, OtherStream);
        var otherSubscriptionId = new SubscriptionId(new Guid("00000014-0000-0000-0000-000000000002"));

        await Assert
            .That(() => ServerSnapshotRecoveryJournalOperations.ValidateOfferRequest(offer with { Subscription = new(otherStreamKey, Client, identity.SubscriptionId) }))
            .ThrowsExactly<ArgumentException>();
        await Assert
            .That(() => ServerSnapshotRecoveryJournalOperations.ValidateOfferRequest(offer with
            {
                RecoveryRequest = SnapshotRequest(otherStreamKey, identity.SubscriptionId, [], ServerReceiveGroupCursor.Create(StreamKey(), 0)),
            }))
            .ThrowsExactly<ArgumentException>();
        await Assert
            .That(() => ServerSnapshotRecoveryJournalOperations.ValidateOfferRequest(offer with
            {
                RecoveryRequest = SnapshotRequest(otherSubscriptionId, []),
                RecoveryResult = SnapshotRecoveryResult(otherSubscriptionId, snapshot, []),
            }))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies rejected retained entries produce terminal rejected operation dispositions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateOperationDispositionsMarksRejectedEntriesTerminalRejected()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        var entry = SnapshotEntry(key, operation, OperationResultKind.Rejected);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), entry));
        var snapshot = journal.Read(StreamKey(), [key]);
        var fingerprints = ServerSnapshotRecoveryJournalOperations.CaptureOperationFingerprints(
            StreamKey(),
            identity,
            SnapshotRequest(identity.SubscriptionId, [operation]),
            SnapshotLimits());

        var dispositions = ServerSnapshotRecoveryJournalOperations.CreateOperationDispositions(snapshot, [key], fingerprints);

        await Assert.That(dispositions).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(dispositions[0].Kind).IsEqualTo(SnapshotOperationDispositionKind.TerminalRejected);
        await Assert.That(dispositions[0].Result).IsEqualTo(entry.Result);
    }

    /// <summary>Verifies captured operation identity mismatches reject an otherwise unchanged offer request.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OfferRequestMatchesViewRejectsCapturedOperationIdMismatch()
    {
        var journal = CreateJournal();
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), SnapshotEntry(key, operation, OperationResultKind.Accepted)));
        _ = journal.RegisterSubscription(identity);
        var request = SnapshotRequest(identity.SubscriptionId, [operation]);
        var view = ((IServerSnapshotRecoveryJournal)journal).ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = request, Limits = SnapshotLimits() });
        var alteredProof = view.OperationDispositions[0] with { OperationId = OperationId.New() };
        var alteredView = view with { OperationDispositions = [alteredProof] };
        var offer = CreateSnapshotOffer(identity, alteredView, request, view.Snapshot);

        var matches = ServerSnapshotRecoveryJournalOperations.OfferRequestMatchesView(offer);

        await Assert.That(matches).IsFalse();
    }

    /// <summary>Verifies canonical fingerprint capture caps oversized logical byte budgets at the finite hashing bound.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CaptureOperationFingerprintsCapsOversizedLogicalByteBudget()
    {
        var key = OperationKey(FirstOperationSeed);
        var identity = SnapshotSubscription();
        var operation = SnapshotOperation(key, FirstOperationSeed);
        var request = SnapshotRequest(identity.SubscriptionId, [operation]);
        var expected = new ServerCommitFingerprint(CanonicalOperationFingerprint.Compute(Tenant, Client, operation, int.MaxValue));

        var fingerprints = ServerSnapshotRecoveryJournalOperations.CaptureOperationFingerprints(
            StreamKey(),
            identity,
            request,
            SnapshotLimits() with { MaximumLogicalBytes = (long)int.MaxValue + 1 });

        await Assert.That(fingerprints).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(fingerprints[0].Matches(expected)).IsTrue();
    }
}
