// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Snapshot recovery offer tests.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>Creates the snapshot subscription identity.</summary>
    /// <returns>The trusted identity.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static ServerSubscriptionIdentity SnapshotSubscription() =>
        new(StreamKey(), Client, new(new Guid(20, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1])));

    /// <summary>Offers a snapshot whose current request exactly matches the captured pending intent.</summary>
    /// <returns>The snapshot offer result.</returns>
    private static ServerSnapshotOfferResult OfferUnchangedPendingIntent()
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
        var proof = view.OperationDispositions[0];
        return ((IServerSnapshotRecoveryJournal)journal).TryOfferSnapshot(new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = SnapshotRequest(identity.SubscriptionId, [operation]),
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, view.Snapshot, [CreateClientDisposition(proof)]),
            Limits = SnapshotLimits(),
        });
    }

    /// <summary>Creates a structurally valid snapshot offer request.</summary>
    /// <param name="identity">The trusted subscription identity.</param>
    /// <param name="view">The captured recovery view.</param>
    /// <param name="request">The recovery request bound to the view.</param>
    /// <param name="snapshot">The captured stream snapshot.</param>
    /// <returns>The offer request.</returns>
    private static ServerSnapshotOfferRequest CreateSnapshotOffer(
        ServerSubscriptionIdentity identity,
        ServerSnapshotRecoveryView view,
        RemoteSnapshotRecoveryRequest request,
        ServerCommitSnapshot snapshot) =>
        new()
        {
            StreamKey = StreamKey(),
            Subscription = identity,
            View = view,
            RecoveryRequest = request,
            RecoveryResult = SnapshotRecoveryResult(identity.SubscriptionId, snapshot, []),
            Limits = SnapshotLimits(),
        };

    /// <summary>Seeds two committed groups and retains a normal offer for the first group.</summary>
    /// <param name="journal">The journal to seed.</param>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The stream snapshot after the second group.</returns>
    /// <exception cref="InvalidOperationException">The first page is missing.</exception>
    private static ServerCommitSnapshot SeedTwoCommitsAndOfferFirstPage(
        InMemoryServerCommitJournal journal,
        ServerSubscriptionIdentity identity)
    {
        var first = OperationKey(FirstOperationSeed);
        var second = OperationKey(SecondOperationSeed);
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(first), Entry(first, OperationResultKind.Accepted, FirstOperationSeed)));
        _ = journal.TryCommit(Plan(
            SingleEntryCount,
            State(SecondVersion),
            Stamp(second),
            Entry(second, OperationResultKind.Accepted, SecondOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        _ = page.Batch ?? throw new InvalidOperationException("The expected first subscription page was missing.");
        return journal.Read(StreamKey(), [first, second]);
    }

    /// <summary>Seeds one committed group and retains a normal offer for that group.</summary>
    /// <param name="journal">The journal to seed.</param>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The stream snapshot after the first group.</returns>
    /// <exception cref="InvalidOperationException">The first page is missing.</exception>
    private static ServerCommitSnapshot SeedOneCommitAndOfferFirstPage(
        InMemoryServerCommitJournal journal,
        ServerSubscriptionIdentity identity)
    {
        var key = OperationKey(FirstOperationSeed);
        _ = journal.RegisterSubscription(identity);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed)));
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes));
        _ = page.Batch ?? throw new InvalidOperationException("The expected first subscription page was missing.");
        return journal.Read(StreamKey(), [key]);
    }

    /// <summary>Creates a valid fingerprint value that cannot match a canonical operation hash in these fixtures.</summary>
    /// <returns>The mismatched fingerprint.</returns>
    private static ServerCommitFingerprint MismatchedFingerprint() => new(new byte[ServerCommitFingerprint.Length]);

    /// <summary>Creates a structurally valid recovery request.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pendingOperations">The owned pending operations.</param>
    /// <returns>The recovery request.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static RemoteSnapshotRecoveryRequest SnapshotRequest(SubscriptionId subscriptionId, IReadOnlyList<SyncOperation> pendingOperations) =>
        SnapshotRequest(
            StreamKey(),
            subscriptionId,
            pendingOperations,
            ServerReceiveGroupCursor.Create(StreamKey(), 0));

    /// <summary>Creates a structurally valid recovery request for a stream key.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="pendingOperations">The owned pending operations.</param>
    /// <param name="expiredCursor">The claimed expired cursor.</param>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest SnapshotRequest(
        ServerStreamKey streamKey,
        SubscriptionId subscriptionId,
        IReadOnlyList<SyncOperation> pendingOperations,
        string? expiredCursor) =>
        new()
        {
            StreamId = streamKey.StreamId,
            SubscriptionId = subscriptionId,
            ExpiredCursor = expiredCursor,
            ClientStateContractId = PayloadContract,
            ClientStateSchemaVersion = SingleEntryCount,
            SnapshotFormatVersion = SingleEntryCount,
            PendingOperations = pendingOperations,
            MaximumResponseBytes = DefaultMaximumLogicalBytes,
        };

    /// <summary>Creates a pending snapshot recovery operation.</summary>
    /// <param name="key">The server operation key.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The pending operation.</returns>
    private static SyncOperation SnapshotOperation(ServerOperationKey key, long sequence) =>
        new()
        {
            OperationId = key.OperationId,
            StreamId = Stream,
            ClientSequence = sequence,
            TimestampUtc = Start,
            BaseVersion = FirstVersion,
            Type = SyncOperationType.Update,
            Payload = Payload($"pending-{sequence}"),
        };

    /// <summary>Creates a retained ledger entry with the canonical fingerprint for the exact pending operation.</summary>
    /// <param name="key">The server operation key.</param>
    /// <param name="operation">The pending operation used by the recovery request.</param>
    /// <param name="kind">The terminal operation result kind.</param>
    /// <returns>The retained ledger entry.</returns>
    private static ServerLedgerEntry SnapshotEntry(ServerOperationKey key, SyncOperation operation, OperationResultKind kind) =>
        new(
            key,
            new(CanonicalOperationFingerprint.Compute(Tenant, key.ClientId, operation, SnapshotFingerprintBudget)),
            new(key.OperationId, kind, null, FirstVersion),
            [],
            []);

    /// <summary>Creates the client-visible disposition corresponding to a trusted server proof.</summary>
    /// <param name="proof">The server proof captured in the recovery view.</param>
    /// <returns>The client-visible operation disposition.</returns>
    private static SnapshotOperationDisposition CreateClientDisposition(ServerSnapshotOperationDisposition proof) =>
        new() { OperationId = proof.OperationId, Kind = proof.Kind, Result = proof.Result };

    /// <summary>Creates a structurally valid recovered result.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="snapshot">The captured snapshot frontier.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <returns>The recovery result.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static RemoteSnapshotRecoveryResult SnapshotRecoveryResult(
        SubscriptionId subscriptionId,
        ServerCommitSnapshot snapshot,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) =>
        SnapshotRecoveryResult(Stream, subscriptionId, snapshot, dispositions);

    /// <summary>Creates a structurally valid recovered result for a stream.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="snapshot">The captured snapshot frontier.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <returns>The recovery result.</returns>
    private static RemoteSnapshotRecoveryResult SnapshotRecoveryResult(
        StreamId streamId,
        SubscriptionId subscriptionId,
        ServerCommitSnapshot snapshot,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) =>
        new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = new()
            {
                StreamId = streamId,
                SubscriptionId = subscriptionId,
                FrontierCursor = snapshot.LastCursor
                    ?? ServerReceiveGroupCursor.Create(snapshot.StreamKey, snapshot.LastGroupSequence),
                ServerVersion = snapshot.State?.Version ?? string.Empty,
                SnapshotFormatVersion = SingleEntryCount,
                ClientState = Payload("snapshot"),
                ObservedAtUtc = Start,
            },
            OperationDispositions = dispositions,
        };

    /// <summary>Creates the retained source view for a snapshot offer.</summary>
    /// <param name="snapshot">The stream snapshot.</param>
    /// <param name="state">The subscription state.</param>
    /// <returns>The retained view.</returns>
    private static ServerSnapshotRecoveryView SnapshotView(ServerCommitSnapshot snapshot, ServerSubscriptionState state) =>
        new()
        {
            Snapshot = snapshot,
            SubscriptionState = state,
            ExpiredCursorOffer = null,
            RequestedExpiredCursor = ServerReceiveGroupCursor.Create(StreamKey(), 0),
            OperationDispositions = [],
            OperationFingerprints = [],
        };

    /// <summary>Creates finite structural snapshot recovery limits for tests.</summary>
    /// <returns>The limits.</returns>
    private static SnapshotRecoveryLimits SnapshotLimits() => new() { MaximumLogicalBytes = DefaultMaximumLogicalBytes };
}
