// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Durable snapshot recovery offer tests for replay-only operation proof reconciliation.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>Verifies invalid UTF-16 recovered results reject before durable offer mutation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">A recovered snapshot offer did not include a checkpoint.</exception>
    [Test]
    public async Task SnapshotOfferRejectsInvalidUtf16RecoveredResultBeforeMutation()
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
        var identity = SnapshotSubscription();
        var state = journal.RegisterSubscription(identity);
        var snapshot = journal.Read(StreamKey(), []);
        var request = SnapshotRequest(identity.SubscriptionId);
        var offer = CreateSnapshotOffer(identity, SnapshotView(snapshot, state), request, snapshot);
        var checkpoint = offer.RecoveryResult.Checkpoint
            ?? throw new InvalidOperationException("A recovered snapshot offer must include a checkpoint.");
        var invalidResult = offer.RecoveryResult with
        {
            Checkpoint = checkpoint with { ServerVersion = new('\uD800', 1) },
        };

        var result = journal.TryOfferSnapshot(offer with { RecoveryResult = invalidResult });
        var after = journal.RegisterSubscription(identity);

        await Assert.That(result.Status).IsEqualTo(ServerSnapshotOfferStatus.ValidationRejected);
        await Assert.That(after.Revision).IsEqualTo(state.Revision);
        await Assert.That(after.OfferCount).IsEqualTo(state.OfferCount);
    }

    /// <summary>Verifies moving the pending/replay boundary with the same ordered operations rejects before durable mutation.</summary>
    /// <param name="moveReplayIntoPending">Whether the changed offer request moves a captured replay operation into pending.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotOfferRejectsOperationRoleBoundaryChangeBeforeMutation(bool moveReplayIntoPending)
    {
        using var database = new TemporaryDatabase();
        using var journal = CreateJournal(database.Path);
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
        var capturedRequest = moveReplayIntoPending
            ? SnapshotRequest(identity.SubscriptionId, [firstOperation], [secondOperation])
            : SnapshotRequest(identity.SubscriptionId, [firstOperation, secondOperation], []);
        var changedRequest = moveReplayIntoPending
            ? SnapshotRequest(identity.SubscriptionId, [firstOperation, secondOperation], [])
            : SnapshotRequest(identity.SubscriptionId, [firstOperation], [secondOperation]);
        var view = journal.ReadSnapshotRecoveryView(new() { StreamKey = StreamKey(), Subscription = identity, RecoveryRequest = capturedRequest, Limits = SnapshotLimits() });

        await Assert.That(view.OperationDispositions).Count().IsEqualTo(DoubleEntryCount);
        var result = journal.TryOfferSnapshot(new()
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
