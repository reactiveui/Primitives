// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ILocalStoreAdapter"/> implementations.</summary>
public sealed class ILocalStoreAdapterTests
{
    /// <summary>The current in-memory schema version.</summary>
    private const int InMemorySchemaVersion = 1;

    /// <summary>The current SQLite local store schema version.</summary>
    private const int SqliteSchemaVersion = 8;

    /// <summary>The in-memory adapter selector used by parameterized tests.</summary>
    private const int InMemoryAdapterKind = 0;

    /// <summary>The SQLite adapter selector used by parameterized tests.</summary>
    private const int SqliteAdapterKind = 1;

    /// <summary>The first client sequence.</summary>
    private const int FirstClientSequence = 1;

    /// <summary>The second client sequence.</summary>
    private const int SecondClientSequence = 2;

    /// <summary>The first snapshot revision.</summary>
    private const int FirstSnapshotRevision = 1;

    /// <summary>The second snapshot revision.</summary>
    private const int SecondSnapshotRevision = 2;

    /// <summary>The default maximum lease byte count.</summary>
    private const long DefaultLeaseBytes = 256;

    /// <summary>The number of bytes in one kibibyte.</summary>
    private const int BytesPerKibibyte = 1024;

    /// <summary>The maximum in-memory encoded byte count used by shared tests.</summary>
    private const int InMemoryMaximumEncodedBytes = 64 * BytesPerKibibyte;

    /// <summary>The SQLite worker capacity used by shared tests.</summary>
    private const int SqliteWorkers = 4;

    /// <summary>The SQLite worker byte capacity used by shared tests.</summary>
    private const int SqliteBytes = 256 * BytesPerKibibyte;

    /// <summary>The renewed lease extension in minutes.</summary>
    private const int RenewedLeaseExtensionMinutes = 2;

    /// <summary>The time to advance after renewal before the renewed lease expires.</summary>
    private const int RenewedLeasePreExpirySeconds = 90;

    /// <summary>The operation count expected after two committed recovery operations.</summary>
    private const int RecoveryOperationCount = 2;

    /// <summary>The store identity used by conformance tests.</summary>
    private const string StoreIdentity = "store-conformance";

    /// <summary>The client identity bound to conformance tests.</summary>
    private const string ClientId = "client-conformance";

    /// <summary>The conflicting client identity used by conformance tests.</summary>
    private const string OtherClientId = "client-other";

    /// <summary>The initial remote cursor.</summary>
    private const string FirstRemoteCursor = "remote-cursor-1";

    /// <summary>The second remote cursor.</summary>
    private const string SecondRemoteCursor = "remote-cursor-2";

    /// <summary>The server version used for upload acknowledgements.</summary>
    private const string ServerVersion = "server-version";

    /// <summary>The cursor applied by snapshot recovery tests.</summary>
    private const string SnapshotRecoveryCursor = "snapshot-recovery-cursor";

    /// <summary>The shared stream identifier used by conformance tests.</summary>
    private static readonly StreamId Stream = new("store/conformance");

    /// <summary>The policy used for shared adapter tests that do not require durable local commits.</summary>
    private static readonly OperationPolicy VolatileOperationPolicy =
        OperationPolicy.Default with { Durability = OperationDurability.Volatile };

    /// <summary>Verifies advertised local commit capability updates operation, sequence and snapshot together.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task AtomicLocalCommitCapabilityCommitsOperationSequenceAndSnapshotTogether(int kind)
    {
        await using var fixture = await CreateInitializedFixtureAsync(kind);
        var subscription = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var firstOperation = CreateOperation(FirstClientSequence, "first-operation");
        var firstReceipt = await fixture.Store.CommitLocalOperationAsync(
            firstOperation,
            CreateSnapshotMutation(0, "first-snapshot"),
            CancellationToken.None);
        var staleOperation = CreateOperation(SecondClientSequence, "stale-operation");

        Func<Task> staleCommit = () => fixture.Store.CommitLocalOperationAsync(
            staleOperation,
            CreateSnapshotMutation(0, "stale-snapshot"),
            CancellationToken.None).AsTask();

        await Assert.That(HasCapability(fixture.Store, LocalStoreCapabilities.AtomicLocalCommit)).IsTrue();
        await Assert.That(staleCommit).ThrowsExactly<InvalidOperationException>();
        var recovered = await fixture.Store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(firstReceipt.OperationId).IsEqualTo(firstOperation.OperationId);
        await Assert.That(firstReceipt.ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(firstReceipt.SnapshotRevision).IsEqualTo(FirstSnapshotRevision);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(firstOperation.OperationId);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstSnapshotRevision);
        await Assert.That(
            recovered.Snapshot?.State.Payload.ToArray().SequenceEqual(CreatePayload("first-snapshot").Payload.ToArray()))
            .IsTrue();
    }

    /// <summary>Verifies advertised remote apply capability updates inbox, cursor and snapshot together.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task AtomicRemoteApplyCapabilityCommitsInboxCursorAndSnapshotTogether(int kind)
    {
        await using var fixture = await CreateInitializedFixtureAsync(kind);
        var subscription = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var appliedEvent = CreateRemoteEvent(FirstRemoteCursor, "applied-event");
        var receipt = await fixture.Store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, FirstRemoteCursor, [appliedEvent]),
            CreateSnapshotMutation(0, "applied-snapshot"),
            CancellationToken.None);
        var rejectedEvent = CreateRemoteEvent(SecondRemoteCursor, "rejected-event");

        Func<Task> staleApply = () => fixture.Store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, SecondRemoteCursor, [rejectedEvent]),
            CreateSnapshotMutation(FirstSnapshotRevision, "rejected-snapshot"),
            CancellationToken.None).AsTask();

        await Assert.That(HasCapability(fixture.Store, LocalStoreCapabilities.AtomicRemoteApply)).IsTrue();
        await Assert.That(staleApply).ThrowsExactly<InvalidOperationException>();
        var recovered = await fixture.Store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var appliedLookup = await fixture.Store.GetUnappliedEventIdsAsync(Stream, [appliedEvent.EventId], CancellationToken.None);
        var rejectedLookup = await fixture.Store.GetUnappliedEventIdsAsync(Stream, [rejectedEvent.EventId], CancellationToken.None);
        await Assert.That(receipt.NextCursor).IsEqualTo(FirstRemoteCursor);
        await Assert.That(receipt.AppliedCount).IsEqualTo(1);
        await Assert.That(receipt.DuplicateCount).IsEqualTo(0);
        await Assert.That(receipt.SnapshotRevision).IsEqualTo(FirstSnapshotRevision);
        await Assert.That(recovered.ServerCursor).IsEqualTo(FirstRemoteCursor);
        await Assert.That(recovered.Snapshot?.ServerCursor).IsEqualTo(FirstRemoteCursor);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstSnapshotRevision);
        await Assert.That(
            recovered.Snapshot?.State.Payload.ToArray().SequenceEqual(CreatePayload("applied-snapshot").Payload.ToArray()))
            .IsTrue();
        await Assert.That(appliedLookup.Count).IsEqualTo(0);
        await Assert.That(rejectedLookup.Count).IsEqualTo(1);
        await Assert.That(rejectedLookup[0]).IsEqualTo(rejectedEvent.EventId);
    }

    /// <summary>Verifies advertised lease capability excludes, renews, expires and reclaims outbox ownership.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task LeasedOutboxCapabilityExcludesRenewsExpiresAndReclaimsOwnership(int kind)
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var fixture = await CreateInitializedFixtureAsync(kind, clock);
        var operation = await CommitOperationAsync(fixture.Store, FirstClientSequence, "leased-operation");
        var firstLease = RequireLease(await LeaseSingleBatchAsync(fixture.Store, TimeSpan.FromMinutes(1)));
        var excludedLease = await LeaseSingleBatchAsync(fixture.Store, TimeSpan.FromMinutes(1));

        await fixture.Store.RenewLeaseAsync(
            firstLease.LeaseId,
            TimeSpan.FromMinutes(RenewedLeaseExtensionMinutes),
            CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(RenewedLeasePreExpirySeconds));
        var renewedLeaseStillExcluded = await LeaseSingleBatchAsync(fixture.Store, TimeSpan.FromMinutes(1));
        clock.Advance(TimeSpan.FromMinutes(RenewedLeaseExtensionMinutes));
        var reclaimedLease = RequireLease(await LeaseSingleBatchAsync(fixture.Store, TimeSpan.FromMinutes(1)));
        Func<Task> staleRelease = () => fixture.Store.ReleaseLeaseAsync(firstLease.LeaseId, CancellationToken.None).AsTask();

        await Assert.That(HasCapability(fixture.Store, LocalStoreCapabilities.LeasedOutbox)).IsTrue();
        await Assert.That(firstLease.ExpiresAtUtc).IsEqualTo(DateTimeOffset.UnixEpoch.AddMinutes(1));
        await Assert.That(firstLease.Operations.Count).IsEqualTo(1);
        await Assert.That(firstLease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(excludedLease).IsNull();
        await Assert.That(renewedLeaseStillExcluded).IsNull();
        await Assert.That(reclaimedLease.LeaseId).IsNotEqualTo(firstLease.LeaseId);
        await Assert.That(reclaimedLease.ExpiresAtUtc).IsEqualTo(clock.GetUtcNow().AddMinutes(1));
        await Assert.That(reclaimedLease.Operations.Count).IsEqualTo(1);
        await Assert.That(reclaimedLease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(staleRelease).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await LeaseSingleBatchAsync(fixture.Store, TimeSpan.FromMinutes(1))).IsNull();
        await fixture.Store.ApplySyncResultAsync(
            reclaimedLease.LeaseId,
            new(reclaimedLease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var status = await fixture.Store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(status?.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(status?.StreamId).IsEqualTo(Stream);
        await Assert.That(status?.ReasonCode).IsNull();
    }

    /// <summary>Verifies duplicate remote event identifiers are reported once and skipped later.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task RemoteInboxDeduplicatesPreviouslyAppliedEvents(int kind)
    {
        await using var fixture = await CreateInitializedFixtureAsync(kind);
        _ = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var firstEvent = CreateRemoteEvent(FirstRemoteCursor, "first-event");
        _ = await fixture.Store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, FirstRemoteCursor, [firstEvent]),
            CreateSnapshotMutation(0, "first-remote-snapshot"),
            CancellationToken.None);
        var secondEvent = CreateRemoteEvent(SecondRemoteCursor, "second-event");
        var candidates = await fixture.Store.GetUnappliedEventIdsAsync(
            Stream,
            [firstEvent.EventId, secondEvent.EventId],
            CancellationToken.None);

        var receipt = await fixture.Store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(FirstRemoteCursor, SecondRemoteCursor, [firstEvent, secondEvent]),
            CreateSnapshotMutation(FirstSnapshotRevision, "second-remote-snapshot"),
            CancellationToken.None);
        var after = await fixture.Store.GetUnappliedEventIdsAsync(
            Stream,
            [firstEvent.EventId, secondEvent.EventId],
            CancellationToken.None);

        await Assert.That(candidates.Count).IsEqualTo(1);
        await Assert.That(candidates[0]).IsEqualTo(secondEvent.EventId);
        await Assert.That(receipt.AppliedCount).IsEqualTo(1);
        await Assert.That(receipt.DuplicateCount).IsEqualTo(1);
        await Assert.That(receipt.NextCursor).IsEqualTo(SecondRemoteCursor);
        await Assert.That(receipt.SnapshotRevision).IsEqualTo(SecondSnapshotRevision);
        await Assert.That(after.Count).IsEqualTo(0);
    }

    /// <summary>Verifies cancellation before commit and disposal after use leave concrete state boundaries.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task CancellationAndDisposalRejectWorkWithoutCommittingPartialState(int kind)
    {
        await using var fixture = await CreateInitializedFixtureAsync(kind);
        var subscription = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task> canceledCommit = () => fixture.Store.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence, "canceled-operation"),
            CreateSnapshotMutation(0, "canceled-snapshot"),
            cancellation.Token).AsTask();

        await Assert.That(canceledCommit).ThrowsExactly<OperationCanceledException>();
        var recovered = await fixture.Store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.Snapshot).IsNull();
        await fixture.Store.DisposeAsync();
        Func<Task> disposedStatus = () => fixture.Store.GetOperationStatusAsync(OperationId.New(), CancellationToken.None).AsTask();
        await Assert.That(disposedStatus).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies adapters advertise the precise capabilities covered by this suite.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task CapabilitiesDescribeSupportedLocalStoreBehavior(int kind)
    {
        await using var fixture = await CreateFixtureAsync(kind);
        var expected = kind == InMemoryAdapterKind
            ? LocalStoreCapabilities.AtomicLocalCommit
                | LocalStoreCapabilities.AtomicRemoteApply
                | LocalStoreCapabilities.AtomicSnapshotRecovery
                | LocalStoreCapabilities.ClientIdentityBinding
                | LocalStoreCapabilities.LeasedOutbox
            : LocalStoreCapabilities.AtomicLocalCommit
                | LocalStoreCapabilities.DurableLocalCommit
                | LocalStoreCapabilities.AtomicRemoteApply
                | LocalStoreCapabilities.DurableInbox
                | LocalStoreCapabilities.ClientIdentityBinding
                | LocalStoreCapabilities.LeasedOutbox
                | LocalStoreCapabilities.AtomicSnapshotRecovery;

        await Assert.That(fixture.Store.Capabilities).IsEqualTo(expected);
        await Assert.That(HasCapability(fixture.Store, LocalStoreCapabilities.MultiProcessCoordination)).IsFalse();
        await Assert.That(HasCapability(fixture.Store, LocalStoreCapabilities.AuthenticatedEncryptionAtRest)).IsFalse();
    }

    /// <summary>Verifies client identity binding is idempotent for the same client and rejects a different client.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task ClientIdentityBindingCapabilityAcceptsSameClientAndRejectsDifferentClientWithoutMutation(
        int kind)
    {
        await using var fixture = await CreateFixtureAsync(kind);
        await fixture.Store.InitializeAsync(
            new(StoreIdentity, fixture.RequiredSchemaVersion, false) { ClientId = ClientId },
            CancellationToken.None);
        var subscription = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await CommitOperationAsync(fixture.Store, FirstClientSequence, "client-bound-operation");

        await fixture.Store.InitializeAsync(
            new(StoreIdentity, fixture.RequiredSchemaVersion, false) { ClientId = ClientId },
            CancellationToken.None);
        Func<Task> differentClient = () => fixture.Store.InitializeAsync(
            new(StoreIdentity, fixture.RequiredSchemaVersion, false) { ClientId = OtherClientId },
            CancellationToken.None).AsTask();

        await Assert.That(HasCapability(fixture.Store, LocalStoreCapabilities.ClientIdentityBinding)).IsTrue();
        await Assert.That(differentClient).ThrowsExactly<InvalidOperationException>();
        await fixture.Store.InitializeAsync(
            new(StoreIdentity, fixture.RequiredSchemaVersion, false) { ClientId = ClientId },
            CancellationToken.None);
        await Assert.That(await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None))
            .IsEqualTo(subscription);
        var recovered = await fixture.Store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(FirstSnapshotRevision);
        await Assert.That(
            SamePayload(recovered.Snapshot?.State, CreatePayload("snapshot-client-bound-operation")))
            .IsTrue();
    }

    /// <summary>Verifies snapshot recovery accepts valid proof and rejects invalid fences without mutation.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task AtomicSnapshotRecoveryCapabilityAcceptsValidRecoveryAndRejectsInvalidMutations(
        int kind)
    {
        await using var fixture = await CreateInitializedFixtureAsync(kind);
        var recoveryStore = RequireSnapshotRecoveryStore(fixture.Store);
        var subscription = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var included = await CommitOperationAsync(fixture.Store, FirstClientSequence, "recovery-included");
        var preserved = await CommitOperationAsync(fixture.Store, SecondClientSequence, "recovery-preserved");
        var missingDisposition = CreateSnapshotRecoveryMutation(
            subscription,
            SecondSnapshotRevision,
            previousCursor: null,
            [UnknownSnapshotDisposition(included.OperationId)]);
        var staleRevision = CreateSnapshotRecoveryMutation(
            subscription,
            expectedRevision: 0,
            previousCursor: null,
            [
                IncludedSnapshotDisposition(included.OperationId),
                UnknownSnapshotDisposition(preserved.OperationId),
            ]);

        await Assert.That(HasCapability(fixture.Store, LocalStoreCapabilities.AtomicSnapshotRecovery)).IsTrue();
        Func<Task> applyMissing = () => recoveryStore.ApplySnapshotRecoveryAsync(
            missingDisposition,
            CancellationToken.None).AsTask();
        Func<Task> applyStale = () => recoveryStore.ApplySnapshotRecoveryAsync(
            staleRevision,
            CancellationToken.None).AsTask();
        await Assert.That(applyMissing).ThrowsExactly<ArgumentException>();
        await Assert.That(applyStale).ThrowsExactly<InvalidOperationException>();
        await AssertUnchangedRecoveryStateAsync(fixture.Store, subscription, included.OperationId, preserved.OperationId);

        var valid = CreateSnapshotRecoveryMutation(
            subscription,
            SecondSnapshotRevision,
            previousCursor: null,
            [
                IncludedSnapshotDisposition(included.OperationId),
                UnknownSnapshotDisposition(preserved.OperationId),
            ]);
        var result = await recoveryStore.ApplySnapshotRecoveryAsync(valid, CancellationToken.None);
        var recovered = await fixture.Store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        var includedStatus = await fixture.Store.GetOperationStatusAsync(included.OperationId, CancellationToken.None);

        await Assert.That(result.IncludedOperationCount).IsEqualTo(1);
        await Assert.That(result.TerminalOperationCount).IsEqualTo(0);
        await Assert.That(result.PreservedPendingOperationCount).IsEqualTo(1);
        await Assert.That(result.Snapshot.Revision).IsEqualTo(SecondSnapshotRevision + 1);
        await Assert.That(result.Snapshot.ServerCursor).IsEqualTo(SnapshotRecoveryCursor);
        await Assert.That(SamePayload(result.Snapshot.State, CreatePayload("recovery-optimistic"))).IsTrue();
        await Assert.That(SamePayload(result.Snapshot.AuthoritativeState, CreatePayload("recovery-authoritative"))).IsTrue();
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryCursor);
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(SecondSnapshotRevision + 1);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(preserved.OperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(preserved.OperationId);
        await Assert.That(includedStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
    }

    /// <summary>Verifies SQLite reopens durable operation, inbox, cursor, snapshot and status state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task SqliteDurableCapabilitiesReopenOperationInboxCursorSnapshotAndStatusState()
    {
        var directory = SqliteTestDirectory.Create("rxui-oc-conformance-");
        var databasePath = Path.Combine(directory.FullName, "local.db");
        try
        {
            SubscriptionId subscription;
            SyncOperation operation;
            Guid eventId;
            await using (var fixture = await CreateInitializedFixtureAsync(SqliteAdapterKind, databasePath: databasePath))
            {
                subscription = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
                operation = await CommitOperationAsync(
                    fixture.Store,
                    FirstClientSequence,
                    "durable-operation",
                    OperationPolicy.Default);
                var lease = RequireLease(await LeaseSingleBatchAsync(fixture.Store, TimeSpan.FromMinutes(1)));
                await fixture.Store.ApplySyncResultAsync(
                    lease.LeaseId,
                    new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
                    CancellationToken.None);
                var remoteEvent = CreateRemoteEvent(FirstRemoteCursor, "durable-remote");
                eventId = remoteEvent.EventId;
                _ = await fixture.Store.ApplyRemoteBatchAsync(
                    CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
                    CreateSnapshotMutation(FirstSnapshotRevision, "durable-remote-snapshot"),
                    CancellationToken.None);
            }

            await using var reopened = await CreateInitializedFixtureAsync(SqliteAdapterKind, databasePath: databasePath);
            var stableSubscription = await reopened.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var recovery = await reopened.Store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
            var unapplied = await reopened.Store.GetUnappliedEventIdsAsync(Stream, [eventId], CancellationToken.None);
            var status = await reopened.Store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

            await Assert.That(HasCapability(reopened.Store, LocalStoreCapabilities.DurableLocalCommit)).IsTrue();
            await Assert.That(HasCapability(reopened.Store, LocalStoreCapabilities.DurableInbox)).IsTrue();
            await AssertSqliteReopenedStateAsync(subscription, operation.OperationId, stableSubscription, recovery, unapplied, status);
        }
        finally
        {
            if (Directory.Exists(directory.FullName))
            {
                Directory.Delete(directory.FullName, recursive: true);
            }
        }
    }

    /// <summary>Creates and initializes an adapter fixture.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <param name="timeProvider">The time provider.</param>
    /// <param name="databasePath">The optional SQLite database path.</param>
    /// <returns>The initialized fixture.</returns>
    private static async ValueTask<LocalStoreAdapterFixture> CreateInitializedFixtureAsync(
        int kind,
        TimeProvider? timeProvider = null,
        string? databasePath = null)
    {
        var fixture = await CreateFixtureAsync(kind, timeProvider, databasePath);
        try
        {
            await fixture.Store.InitializeAsync(
                new(StoreIdentity, fixture.RequiredSchemaVersion, false) { ClientId = ClientId },
                CancellationToken.None);
            return fixture;
        }
        catch
        {
            await fixture.DisposeAsync();
            throw;
        }
    }

    /// <summary>Creates an adapter fixture.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <param name="timeProvider">The time provider.</param>
    /// <param name="databasePath">The optional SQLite database path.</param>
    /// <returns>The fixture.</returns>
    private static ValueTask<LocalStoreAdapterFixture> CreateFixtureAsync(
        int kind,
        TimeProvider? timeProvider = null,
        string? databasePath = null)
    {
        if (kind == InMemoryAdapterKind)
        {
            return ValueTask.FromResult(new LocalStoreAdapterFixture(
                new InMemoryLocalStoreAdapter(
                    timeProvider ?? TimeProvider.System,
                    maximumRecordCount: 128,
                    maximumEncodedBytes: InMemoryMaximumEncodedBytes,
                    new RetentionOptions()),
                InMemorySchemaVersion,
                OwnedDirectory: null));
        }

        var options = new SqliteLocalStoreAdapterOptions { TimeProvider = timeProvider ?? TimeProvider.System };
        options = options with { WorkerCapacity = SqliteWorkers };
        options = options with { WorkerCapacityBytes = SqliteBytes };
        if (databasePath is not null)
        {
            return ValueTask.FromResult(new LocalStoreAdapterFixture(
                new SqliteLocalStoreAdapter(databasePath, options),
                SqliteSchemaVersion,
                OwnedDirectory: null));
        }

        var ownedDirectory = SqliteTestDirectory.Create("rxui-oc-conformance-");
        var path = Path.Combine(ownedDirectory.FullName, "local.db");
        return ValueTask.FromResult(new LocalStoreAdapterFixture(
            new SqliteLocalStoreAdapter(path, options),
            SqliteSchemaVersion,
            ownedDirectory));
    }

    /// <summary>Commits one local operation.</summary>
    /// <param name="store">The store.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <param name="policy">The optional operation policy.</param>
    /// <returns>The committed operation.</returns>
    private static async ValueTask<SyncOperation> CommitOperationAsync(
        ILocalStoreAdapter store,
        long clientSequence,
        string payloadText,
        OperationPolicy? policy = null)
    {
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence, payloadText, policy);
        _ = await store.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(clientSequence - 1, $"snapshot-{payloadText}"),
            CancellationToken.None);
        return operation;
    }

    /// <summary>Reads one leased batch from a store.</summary>
    /// <param name="store">The store.</param>
    /// <param name="leaseDuration">The lease duration.</param>
    /// <returns>The lease, if any.</returns>
    private static async ValueTask<LeasedOperationBatch?> LeaseSingleBatchAsync(ILocalStoreAdapter store, TimeSpan leaseDuration)
    {
        LeasedOperationBatch? result = null;
        await foreach (var batch in store.LeasePendingOperationsAsync(
            new(Stream, MaximumOperations: 8, DefaultLeaseBytes, leaseDuration),
            CancellationToken.None))
        {
            result = batch;
        }

        return result;
    }

    /// <summary>Asserts failed recovery attempts left the stream unchanged.</summary>
    /// <param name="store">The store.</param>
    /// <param name="subscription">The subscription.</param>
    /// <param name="firstOperationId">The first operation identifier.</param>
    /// <param name="secondOperationId">The second operation identifier.</param>
    /// <returns>The asynchronous assertion.</returns>
    private static async Task AssertUnchangedRecoveryStateAsync(
        ILocalStoreAdapter store,
        SubscriptionId subscription,
        OperationId firstOperationId,
        OperationId secondOperationId)
    {
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsNull();
        await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(SecondSnapshotRevision);
        await Assert.That(SamePayload(recovered.Snapshot?.State, CreatePayload("snapshot-recovery-preserved"))).IsTrue();
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(RecoveryOperationCount);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(firstOperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(secondOperationId);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(RecoveryOperationCount);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(firstOperationId);
        await Assert.That(recovered.ReplayOperations[1].OperationId).IsEqualTo(secondOperationId);
    }

    /// <summary>Asserts durable SQLite state after reopening the same database.</summary>
    /// <param name="subscription">The original subscription.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="stableSubscription">The subscription resolved after reopening.</param>
    /// <param name="recovery">The recovered stream.</param>
    /// <param name="unapplied">The unopened inbox candidates after reopening.</param>
    /// <param name="status">The operation status after reopening.</param>
    /// <returns>The asynchronous assertion.</returns>
    private static async Task AssertSqliteReopenedStateAsync(
        SubscriptionId subscription,
        OperationId operationId,
        SubscriptionId stableSubscription,
        RecoveredStream recovery,
        IReadOnlyList<Guid> unapplied,
        SyncOperationStatus? status)
    {
        await Assert.That(stableSubscription).IsEqualTo(subscription);
        await Assert.That(recovery.SubscriptionId).IsEqualTo(subscription);
        await Assert.That(recovery.ServerCursor).IsEqualTo(FirstRemoteCursor);
        await Assert.That(recovery.Snapshot?.ServerCursor).IsEqualTo(FirstRemoteCursor);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(SecondSnapshotRevision);
        await Assert.That(SamePayload(recovery.Snapshot?.State, CreatePayload("durable-remote-snapshot"))).IsTrue();
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.ReplayOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.ReplayOperations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(recovery.ReplayOperations[0].ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(recovery.ReplayOperations[0].Policy.Durability).IsEqualTo(OperationDurability.Durable);
        await Assert.That(SamePayload(recovery.ReplayOperations[0].Payload, CreatePayload("durable-operation"))).IsTrue();
        await Assert.That(unapplied.Count).IsEqualTo(0);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(status?.OperationId).IsEqualTo(operationId);
        await Assert.That(status?.StreamId).IsEqualTo(Stream);
        await Assert.That(status?.Attempt).IsEqualTo(0);
        await Assert.That(status?.ReasonCode).IsNull();
    }

    /// <summary>Creates a representative local operation.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <param name="policy">The optional operation policy.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(
        long clientSequence,
        string payloadText,
        OperationPolicy? policy = null) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = clientSequence,
        TimestampUtc = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
        BaseVersion = "server-a",
        Type = SyncOperationType.Update,
        Payload = CreatePayload(payloadText),
        Policy = policy ?? VolatileOperationPolicy,
        Metadata = new Dictionary<string, string> { ["origin"] = "conformance" },
    };

    /// <summary>Creates a representative snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(long expectedRevision, string payloadText) =>
        new(Stream, CreatePayload(payloadText), FormatVersion: 1, expectedRevision);

    /// <summary>Creates a representative remote batch.</summary>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="events">The remote events.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateRemoteBatch(string? previousCursor, string nextCursor, IReadOnlyList<RemoteEvent> events) =>
        new(Guid.NewGuid(), Stream, previousCursor, nextCursor, events);

    /// <summary>Creates a representative remote event.</summary>
    /// <param name="serverCursor">The server cursor.</param>
    /// <param name="payloadText">The payload text.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(string serverCursor, string payloadText) =>
        new(
            Guid.NewGuid(),
            Stream,
            serverCursor,
            new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
            null,
            CreatePayload(payloadText),
            new Dictionary<string, string>());

    /// <summary>Creates a representative snapshot recovery mutation.</summary>
    /// <param name="subscription">The subscription identifier.</param>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <param name="previousCursor">The expected previous server cursor.</param>
    /// <param name="dispositions">The operation dispositions.</param>
    /// <returns>The mutation.</returns>
    private static LocalSnapshotRecoveryMutation CreateSnapshotRecoveryMutation(
        SubscriptionId subscription,
        long expectedRevision,
        string? previousCursor,
        IReadOnlyList<SnapshotOperationDisposition> dispositions) =>
        new()
        {
            StreamId = Stream,
            SubscriptionId = subscription,
            ExpectedRevision = expectedRevision,
            ExpectedPreviousCursor = previousCursor,
            Checkpoint = new()
            {
                StreamId = Stream,
                SubscriptionId = subscription,
                FrontierCursor = SnapshotRecoveryCursor,
                ServerVersion = ServerVersion,
                SnapshotFormatVersion = 1,
                ClientState = CreatePayload("recovery-authoritative"),
                ObservedAtUtc = DateTimeOffset.UnixEpoch,
            },
            OptimisticState = CreatePayload("recovery-optimistic"),
            SnapshotFormatVersion = 1,
            OperationDispositions = dispositions,
        };

    /// <summary>Creates an included snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition IncludedSnapshotDisposition(OperationId operationId)
    {
        var result = new OperationSyncResult(operationId, OperationResultKind.Accepted, null, ServerVersion);
        return new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.IncludedAccepted, Result = result };
    }

    /// <summary>Creates an unknown snapshot recovery disposition.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The disposition.</returns>
    private static SnapshotOperationDisposition UnknownSnapshotDisposition(OperationId operationId) =>
        new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown };

    /// <summary>Creates a representative payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string text)
    {
        var payload = System.Text.Encoding.UTF8.GetBytes($$"""{"value":"{{text}}"}""");
        return new("reading", 1, "application/json", payload, CreateSha256Hash(payload));
    }

    /// <summary>Creates the SHA-256 payload hash text.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The hash text.</returns>
    private static string CreateSha256Hash(byte[] payload) =>
        $"sha256:{Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant()}";

    /// <summary>Compares optional payload envelopes by content.</summary>
    /// <param name="actual">The actual payload.</param>
    /// <param name="expected">The expected payload.</param>
    /// <returns><see langword="true"/> when payload values match.</returns>
    private static bool SamePayload(PayloadEnvelope? actual, PayloadEnvelope expected) =>
        actual is not null
        && string.Equals(actual.ContractId, expected.ContractId, StringComparison.Ordinal)
        && actual.SchemaVersion == expected.SchemaVersion
        && string.Equals(actual.ContentType, expected.ContentType, StringComparison.Ordinal)
        && SameHash(actual.PayloadHash, expected.PayloadHash)
        && actual.Payload.ToArray().SequenceEqual(expected.Payload.ToArray());

    /// <summary>Compares payload hashes in fixed time.</summary>
    /// <param name="actual">The actual hash.</param>
    /// <param name="expected">The expected hash.</param>
    /// <returns><see langword="true"/> when hashes match.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static bool SameHash(string actual, string expected) =>
        CryptographicOperations.FixedTimeEquals(
            System.Text.Encoding.UTF8.GetBytes(actual),
            System.Text.Encoding.UTF8.GetBytes(expected));

    /// <summary>Checks whether a store advertises a capability.</summary>
    /// <param name="store">The store.</param>
    /// <param name="capability">The capability.</param>
    /// <returns><see langword="true"/> when the capability is present.</returns>
    private static bool HasCapability(ILocalStoreAdapter store, LocalStoreCapabilities capability) =>
        (store.Capabilities & capability) == capability;

    /// <summary>Requires a lease batch.</summary>
    /// <param name="lease">The lease.</param>
    /// <returns>The lease batch.</returns>
    /// <exception cref="InvalidOperationException">The lease is missing.</exception>
    private static LeasedOperationBatch RequireLease(LeasedOperationBatch? lease) =>
        lease ?? throw new InvalidOperationException("Expected a leased operation batch.");

    /// <summary>Requires the snapshot recovery store interface.</summary>
    /// <param name="candidate">The candidate store.</param>
    /// <returns>The snapshot recovery store.</returns>
    /// <exception cref="InvalidOperationException">The interface is missing.</exception>
    private static ILocalSnapshotRecoveryStore RequireSnapshotRecoveryStore(object candidate) =>
        candidate as ILocalSnapshotRecoveryStore
        ?? throw new InvalidOperationException("Expected advertised snapshot recovery store support.");

    /// <summary>A manually advanced clock for deterministic lease tests.</summary>
    /// <param name="timestamp">The starting timestamp.</param>
    private sealed class ManualTimeProvider(DateTimeOffset timestamp) : TimeProvider
    {
        /// <summary>The current timestamp.</summary>
        private DateTimeOffset _timestamp = timestamp;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _timestamp;

        /// <summary>Advances the timestamp.</summary>
        /// <param name="duration">The duration.</param>
        public void Advance(TimeSpan duration) => _timestamp = _timestamp.Add(duration);
    }

    /// <summary>Owns one local store adapter and any backing test directory.</summary>
    /// <param name="Store">The local store.</param>
    /// <param name="RequiredSchemaVersion">The required schema version.</param>
    /// <param name="OwnedDirectory">The optional owned directory.</param>
    private sealed record LocalStoreAdapterFixture(
        ILocalStoreAdapter Store,
        int RequiredSchemaVersion,
        DirectoryInfo? OwnedDirectory) : IAsyncDisposable
    {
        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            try
            {
                await Store.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                if (OwnedDirectory is not null && Directory.Exists(OwnedDirectory.FullName))
                {
                    Directory.Delete(OwnedDirectory.FullName, recursive: true);
                }
            }
        }
    }
}
