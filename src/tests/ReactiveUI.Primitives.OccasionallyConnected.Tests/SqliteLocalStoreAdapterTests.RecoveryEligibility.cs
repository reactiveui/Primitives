// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Recovery eligibility tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed class SqliteLocalStoreAdapterTests
{
    /// <summary>The default local store identity.</summary>
    private const string StoreIdentity = "sqlite-recovery-eligibility";

    /// <summary>The SQLite database file name.</summary>
    private const string DatabaseFileName = "store.db";

    /// <summary>The first client sequence.</summary>
    private const int FirstClientSequence = 1;

    /// <summary>The default payload text.</summary>
    private const string PayloadText = "operation";

    /// <summary>The default server base version.</summary>
    private const string BaseVersion = "server-a";

    /// <summary>The default lease byte budget.</summary>
    private const long LeaseBytes = 1024;

    /// <summary>The clock advance minutes used to expire retry and lease blockers.</summary>
    private const int ExpiredBlockerAdvanceMinutes = 2;

    /// <summary>The later retry due minutes used by recovery tests.</summary>
    private const int LaterRetryDueMinutes = 5;

    /// <summary>The past retry due seconds used by recovery tests.</summary>
    private const int PastRetryDueSeconds = 30;

    /// <summary>A test stream identifier.</summary>
    private static readonly StreamId Stream = new("sqlite/recovery");

    /// <summary>Verifies recovered SQLite pending heads report a future retry due time.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RecoveryReportsFutureRetryDueAsPendingUploadNotBefore()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await CommitOperationAsync(store, FirstClientSequence);
        var dueUtc = clock.GetUtcNow().AddMinutes(1);
        await store.SaveRetryStateAsync(operation.OperationId, RetryState.Start(clock.GetUtcNow()) with { DueUtc = dueUtc }, CancellationToken.None);

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsEqualTo(dueUtc);
    }

    /// <summary>Verifies recovered SQLite pending heads report an unexpired lease expiry.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RecoveryReportsUnexpiredLeaseAsPendingUploadNotBefore()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await CommitOperationAsync(store, FirstClientSequence);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, TimeSpan.FromMinutes(1)));

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsEqualTo(lease.ExpiresAtUtc);
    }

    /// <summary>Verifies recovered SQLite pending heads report the later retry due when lease and retry both block.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RecoveryReportsLaterRetryDueWhenLeaseAlsoBlocks()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await CommitOperationAsync(store, FirstClientSequence);
        _ = RequireBatch(await LeaseSingleBatchAsync(store, TimeSpan.FromMinutes(1)));
        var dueUtc = clock.GetUtcNow().AddMinutes(LaterRetryDueMinutes);
        await store.SaveRetryStateAsync(operation.OperationId, RetryState.Start(clock.GetUtcNow()) with { DueUtc = dueUtc }, CancellationToken.None);

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsEqualTo(dueUtc);
    }

    /// <summary>Verifies recovered SQLite streams without pending work have no upload not-before constraint.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RecoveryWithoutPendingWorkHasNoPendingUploadNotBefore()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsNull();
    }

    /// <summary>Verifies past SQLite retry and expired lease blockers do not constrain recovered scheduling.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RecoveryIgnoresPastRetryAndExpiredLeaseNotBefore()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await CommitOperationAsync(store, FirstClientSequence);
        _ = RequireBatch(await LeaseSingleBatchAsync(store, TimeSpan.FromMinutes(1)));
        await store.SaveRetryStateAsync(operation.OperationId, RetryState.Start(clock.GetUtcNow()) with { DueUtc = clock.GetUtcNow().AddSeconds(PastRetryDueSeconds) }, CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(ExpiredBlockerAdvanceMinutes));

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsNull();
    }

    /// <summary>Verifies irreversible SQLite pending heads do not create recovered upload not-before constraints.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RecoveryLeavesPermanentlyBlockedHeadWithoutPendingUploadNotBefore()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await CommitOperationAsync(store, FirstClientSequence, OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce });
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, TimeSpan.FromMinutes(1)));
        _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);

        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsNull();
    }

    /// <summary>Creates an initialized SQLite store.</summary>
    /// <param name="clock">The store clock.</param>
    /// <returns>The initialized store.</returns>
    private static async ValueTask<SqliteLocalStoreAdapter> CreateInitializedStoreAsync(TimeProvider clock)
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-sqlite-recovery-eligibility-").FullName, DatabaseFileName);
        var store = new SqliteLocalStoreAdapter(databasePath, new() { TimeProvider = clock });
        await store.InitializeAsync(new(StoreIdentity, 1, false), CancellationToken.None);
        return store;
    }

    /// <summary>Commits one operation.</summary>
    /// <param name="store">The store.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="policy">The optional operation policy.</param>
    /// <returns>The committed operation.</returns>
    private static async ValueTask<SyncOperation> CommitOperationAsync(
        SqliteLocalStoreAdapter store,
        long clientSequence,
        OperationPolicy? policy = null)
    {
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence, policy);
        _ = await store.CommitLocalOperationAsync(operation, CreateSnapshotMutation(clientSequence - 1), CancellationToken.None);
        return operation;
    }

    /// <summary>Leases one batch.</summary>
    /// <param name="store">The store.</param>
    /// <param name="duration">The lease duration.</param>
    /// <returns>The leased batch, if any.</returns>
    private static async ValueTask<LeasedOperationBatch?> LeaseSingleBatchAsync(SqliteLocalStoreAdapter store, TimeSpan duration)
    {
        LeasedOperationBatch? result = null;
        await foreach (var batch in store.LeasePendingOperationsAsync(new(Stream, 1, LeaseBytes, duration), CancellationToken.None))
        {
            result = batch;
        }

        return result;
    }

    /// <summary>Requires a leased batch.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The leased batch.</returns>
    /// <exception cref="InvalidOperationException">The batch is null.</exception>
    private static LeasedOperationBatch RequireBatch(LeasedOperationBatch? batch) =>
        batch ?? throw new InvalidOperationException("Expected a leased operation batch.");

    /// <summary>Creates a representative operation.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="policy">The optional operation policy.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(long clientSequence, OperationPolicy? policy = null) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = clientSequence,
        TimestampUtc = DateTimeOffset.UnixEpoch,
        BaseVersion = BaseVersion,
        Type = SyncOperationType.Update,
        Payload = CreatePayload(PayloadText),
        Policy = policy ?? OperationPolicy.Default,
        Metadata = new Dictionary<string, string>(),
    };

    /// <summary>Creates a representative snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected revision.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(long expectedRevision) =>
        new(Stream, CreatePayload($"snapshot-{expectedRevision}"), FormatVersion: 1, expectedRevision);

    /// <summary>Creates a representative payload.</summary>
    /// <param name="text">The text.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreatePayload(string text) =>
        new("reading", 1, "application/json", Encoding.UTF8.GetBytes(text), $"hash-{text}");
}
