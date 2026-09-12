// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The minimum compatible local store schema version.</summary>
    private const int MinimumRequiredSchemaVersion = 1;

    /// <summary>The current SQLite local commit schema version.</summary>
    private const int SchemaVersion = 6;

    /// <summary>An unsupported future local store schema version.</summary>
    private const int FutureRequiredSchemaVersion = SchemaVersion + 1;

    /// <summary>The first client sequence value.</summary>
    private const int FirstClientSequence = 1;

    /// <summary>The second client sequence value.</summary>
    private const int SecondClientSequence = 2;

    /// <summary>The first remote send attempt.</summary>
    private const int FirstAttempt = 1;

    /// <summary>A worker capacity that admits only one active or captured command.</summary>
    private const int SingleWorkerCommand = 1;

    /// <summary>A bounded worker capacity that admits the active command and one queued command.</summary>
    private const int TwoWorkerCommands = 2;

    /// <summary>The worker byte capacity used for normal adapter calls.</summary>
    private const long NormalWorkerBytes = 256L * 1024L;

    /// <summary>The worker byte capacity used to reject oversized caller input.</summary>
    private const long TinyWorkerBytes = 512;

    /// <summary>The oversized payload length.</summary>
    private const int OversizedPayloadLength = 1024;

    /// <summary>The oversized retry text length.</summary>
    private const int OversizedRetryTextLength = 4096;

    /// <summary>The worker byte capacity used for retry state input rejection.</summary>
    private const long RetryWorkerBytes = 2048;

    /// <summary>The store identity used by tests.</summary>
    private const string StoreIdentity = "client-alpha";

    /// <summary>The metadata key used for operation origin.</summary>
    private const string MetadataOriginKey = "origin";

    /// <summary>The metadata value used for unit-test-origin operations.</summary>
    private const string UnitTestOrigin = "unit-test";

    /// <summary>The remote cursor used by adapter pass-through tests.</summary>
    private const string RemoteCursor = "remote-cursor";

    /// <summary>The server version used by adapter pass-through tests.</summary>
    private const string ServerVersion = "server-version";

    /// <summary>The SQLite store identity parameter name.</summary>
    private const string StoreIdentityParameter = "$storeIdentity";

    /// <summary>The SQLite stream id parameter name.</summary>
    private const string StreamIdParameter = "$streamId";

    /// <summary>A representative stream identity.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>A short guard timeout for bounded asynchronous checks.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies construction validates public options and advertises truthful SQLite capabilities.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAdapterIsConstructed_ThenOptionsAreValidatedAndCapabilitiesAreTruthful()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = NormalWorkerBytes, Retention = new() };

        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        Action invalidCount = () => _ = new SqliteLocalStoreAdapter(
            database.Path,
            options with { WorkerCapacity = 0 });
        Action invalidBytes = () => _ = new SqliteLocalStoreAdapter(
            database.Path,
            options with { WorkerCapacityBytes = 0 });
        Action invalidRetention = () => _ = new SqliteLocalStoreAdapter(
            database.Path,
            options with { Retention = new() { InboxDeduplicationRetention = TimeSpan.Zero } });
        Func<Task> invalidRequiredSchema = () => adapter.InitializeAsync(new(StoreIdentity, 0, false), CancellationToken.None).AsTask();

        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.AtomicLocalCommit) != 0).IsTrue();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.AtomicRemoteApply) != 0).IsTrue();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.DurableInbox) != 0).IsTrue();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.LeasedOutbox) != 0).IsTrue();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.ClientIdentityBinding) != 0).IsTrue();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.MultiProcessCoordination) != 0).IsFalse();
        await Assert.That((adapter.Capabilities & LocalStoreCapabilities.AuthenticatedEncryptionAtRest) != 0).IsFalse();
        await Assert.That(invalidCount).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(invalidBytes).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(invalidRetention).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(invalidRequiredSchema).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies public schema requirements are interpreted as minimum compatible adapter requirements.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMinimumSchemaVersionIsRequired_ThenAdapterInitializesCurrentSqliteSchema()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);

        await adapter.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
    }

    /// <summary>Verifies public future schema requirements are rejected before SQLite creates a database.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenFutureSchemaVersionIsRequired_ThenAdapterRejectsBeforeSQLiteMutation()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        Func<Task> action = () => adapter.InitializeAsync(new(StoreIdentity, FutureRequiredSchemaVersion, false), CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<NotSupportedException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies adapter pass-through methods execute against real SQLite state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAdapterRoutesRemoteLeaseAndCompactionCalls_ThenReceiptsPersist()
    {
        using var database = TempDatabase.Create();
        await using var adapter = new SqliteLocalStoreAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(RemoteCursor);
        var remoteApply = await adapter.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, [remoteEvent]),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(remoteApply.SnapshotRevision), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        var attempt = await adapter.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, FirstAttempt, CancellationToken.None);
        var ambiguousStatus = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await adapter.RenewLeaseAsync(lease.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
        await adapter.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var renewedLease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await adapter.ApplySyncResultAsync(
            renewedLease.LeaseId,
            new(renewedLease.LeaseId, [new(operation.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
        var synchronizedStatus = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var compaction = await adapter.CompactAsync(new(Stream, DateTimeOffset.UnixEpoch.AddDays(1), TargetBytes: 0), CancellationToken.None);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(remoteApply.AppliedCount).IsEqualTo(FirstAttempt);
        await Assert.That(attempt.MaySend).IsTrue();
        await Assert.That(ambiguousStatus?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(synchronizedStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(compaction.RecordsRemoved).IsGreaterThanOrEqualTo(0);
        await Assert.That(recovery.ServerCursor).IsEqualTo(RemoteCursor);
    }

    /// <summary>Verifies oversized retry state input is rejected before it reaches SQLite.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRetryStateInputExceedsWorkerBytes_ThenAdmissionRejectsBeforeSQLiteMutation()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = RetryWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var retryState = RetryState.Start(DateTimeOffset.UnixEpoch) with
        {
            DueUtc = DateTimeOffset.UnixEpoch.AddMinutes(1),
            PreviousDelay = TimeSpan.FromSeconds(1),
            AuthenticationState = RetryAuthenticationState.RenewalRetryUsed,
            CredentialsVersion = new('r', OversizedRetryTextLength),
        };

        Func<Task> action = () => adapter.SaveRetryStateAsync(operation.OperationId, retryState, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(action);
        var persisted = await adapter.GetRetryStateAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(persisted).IsNull();
    }

    /// <summary>Verifies event identifier lookup rejects an oversized list before indexing or copying.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEventIdListCountExceedsWorkerBytes_ThenLookupRejectsBeforeIndexing()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = TinyWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var eventIds = new ThrowingEventIdList();

        Func<Task<IReadOnlyList<Guid>>> action = () => adapter.GetUnappliedEventIdsAsync(Stream, eventIds, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(action);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(eventIds.IndexerRead).IsFalse();
    }

    /// <summary>Verifies concurrent event identifier snapshots are bounded before indexing caller lists.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenConcurrentEventIdSnapshotsExceedCaptureBounds_ThenSecondLookupRejectsBeforeIndexing()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = SingleWorkerCommand, WorkerCapacityBytes = NormalWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var originalEventId = Guid.NewGuid();
        using var blockingEventIds = new BlockingEventIdList(originalEventId);
        var throwingEventIds = new ThrowingSingleEventIdList();

        var blockingLookup = Task.Run(async () => await adapter.GetUnappliedEventIdsAsync(Stream, blockingEventIds, CancellationToken.None));
        try
        {
            await Assert.That(blockingEventIds.WaitForIndexer()).IsTrue();
            Func<Task<IReadOnlyList<Guid>>> secondLookup = () => adapter.GetUnappliedEventIdsAsync(Stream, throwingEventIds, CancellationToken.None).AsTask();
            var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(secondLookup);
            await Assert.That(exception?.CanFitWhenEmpty).IsTrue();
        }
        finally
        {
            blockingEventIds.Release();
            _ = await blockingLookup.WaitAsync(GuardTimeout);
        }

        var unapplied = await blockingLookup.WaitAsync(GuardTimeout);

        await Assert.That(throwingEventIds.IndexerRead).IsFalse();
        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(unapplied[0]).IsEqualTo(originalEventId);
    }

    /// <summary>Verifies local commits and recovery go through the public adapter and persist to SQLite.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalOperationIsCommittedThroughAdapter_ThenReopenRecoversSnapshotAndOutbox()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        LocalCommitResult result;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            result = await adapter.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(result.SnapshotRevision).IsEqualTo(1);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies oversized caller input is rejected by byte bounds before the commit reaches SQLite.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCommitInputExceedsWorkerBytes_ThenAdmissionRejectsBeforeSQLiteMutation()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = TinyWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence) with { Payload = CreatePayload(new('x', OversizedPayloadLength)) };

        Func<Task<LocalCommitResult>> action = () => adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(action);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies authoritative snapshot bytes count toward commit admission before SQLite mutation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeCommitInputExceedsWorkerBytes_ThenAdmissionRejectsBeforeSQLiteMutation()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = TinyWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0) with
        {
            AuthoritativeState = CreatePayload(new('a', OversizedPayloadLength)),
        };

        Func<Task<LocalCommitResult>> action = () => adapter.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            snapshot,
            CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(action);
        var recovery = await adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies the adapter snapshots caller event identifiers before queueing SQLite work.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEventIdListIsMutatedAfterCall_ThenQueuedLookupUsesOriginalIdentifiers()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        await using var blocker = OpenRawConnection(database.Path);
        await using var transaction = blocker.BeginTransaction();
        InsertBlockingIdentity(blocker, transaction);
        var commitTask = adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None).AsTask();
        var originalEventId = Guid.NewGuid();
        List<Guid> eventIds = [originalEventId];

        var unappliedTask = adapter.GetUnappliedEventIdsAsync(Stream, eventIds, CancellationToken.None).AsTask();
        eventIds[0] = Guid.Empty;
        transaction.Rollback();
        _ = await commitTask.WaitAsync(GuardTimeout);
        var unapplied = await unappliedTask.WaitAsync(GuardTimeout);

        await Assert.That(unapplied.Count).IsEqualTo(1);
        await Assert.That(unapplied[0]).IsEqualTo(originalEventId);
    }

    /// <summary>Verifies disposal waits for admitted input capture before disposing the backend.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAdapterIsDisposedWithActiveCapture_ThenDisposeWaitsForCaptureToDrain()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var originalEventId = Guid.NewGuid();
        using var blockingEventIds = new BlockingEventIdList(originalEventId);

        var lookupTask = Task.Run(async () => await adapter.GetUnappliedEventIdsAsync(Stream, blockingEventIds, CancellationToken.None));
        try
        {
            await Assert.That(blockingEventIds.WaitForIndexer()).IsTrue();
            var disposeTask = adapter.DisposeAsync().AsTask();
            Func<Task> statusAfterDisposeStarted = () => adapter.GetOperationStatusAsync(OperationId.New(), CancellationToken.None).AsTask();
            await Assert.That(disposeTask.IsCompleted).IsFalse();
            await Assert.That(statusAfterDisposeStarted).ThrowsExactly<ObjectDisposedException>();
        }
        finally
        {
            blockingEventIds.Release();
            await adapter.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
        }

        await Assert.That(lookupTask).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposal rejects queued work while preserving the active committed receipt.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAdapterIsDisposedWithActiveCommand_ThenQueuedWorkIsRejectedAndActiveCommitPersists()
    {
        using var database = TempDatabase.Create();
        using var clock = new BlockingCommitClock();
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, new() { TimeProvider = clock });
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        clock.Block = true;
        var commitTask = adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None).AsTask();
        Task<RecoveredStream> queuedTask;
        LocalCommitResult receipt;
        try
        {
            await Assert.That(clock.Entered.Wait(GuardTimeout)).IsTrue();
            queuedTask = adapter.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();
            var disposeTask = adapter.DisposeAsync().AsTask();
            var concurrentDisposeTask = adapter.DisposeAsync().AsTask();
            clock.Release();
            receipt = await commitTask.WaitAsync(GuardTimeout);
            await Task.WhenAll(disposeTask, concurrentDisposeTask).WaitAsync(GuardTimeout);
        }
        finally
        {
            clock.Release();
            _ = await commitTask.WaitAsync(GuardTimeout);
        }

        await Assert.That(receipt.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(queuedTask).ThrowsExactly<ObjectDisposedException>();
        var postDisposeEventIds = new ThrowingSingleEventIdList();
        Func<Task> postDisposeLookup = () => adapter.GetUnappliedEventIdsAsync(Stream, postDisposeEventIds, CancellationToken.None).AsTask();
        await Assert.That(postDisposeLookup).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(postDisposeEventIds.IndexerRead).IsFalse();
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies required authenticated encryption is rejected until SQLite encryption support exists.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEncryptionAtRestIsRequired_ThenInitializeRejectsIt()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);

        Func<Task> action = () => adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, true), CancellationToken.None).AsTask();

        await Assert.That(action).ThrowsExactly<NotSupportedException>();
    }

    /// <summary>Creates a configured adapter.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The configured adapter.</returns>
    private static SqliteLocalStoreAdapter CreateAdapter(string path) =>
        new(
            path,
            new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = NormalWorkerBytes });

    /// <summary>Creates a representative operation.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(long clientSequence) => new()
    {
        OperationId = OperationId.New(),
        StreamId = Stream,
        ClientSequence = clientSequence,
        TimestampUtc = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
        BaseVersion = "server-a",
        Type = SyncOperationType.Update,
        Payload = CreatePayload("operation"),
        Policy = new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, Priority: 1, ConflictPolicy.Merge),
        Metadata = new Dictionary<string, string> { [MetadataOriginKey] = UnitTestOrigin },
    };

    /// <summary>Reads a single leased operation batch from the adapter.</summary>
    /// <param name="adapter">The local store adapter.</param>
    /// <param name="request">The lease request.</param>
    /// <returns>The leased operation batch.</returns>
    /// <exception cref="InvalidOperationException">No operation batch was leased.</exception>
    private static async ValueTask<LeasedOperationBatch> ReadSingleLeaseAsync(SqliteLocalStoreAdapter adapter, OutboxLeaseRequest request)
    {
        List<LeasedOperationBatch> batches = [];
        await foreach (var batch in adapter.LeasePendingOperationsAsync(request, CancellationToken.None))
        {
            batches.Add(batch);
        }

        return batches.Count == 0
            ? throw new InvalidOperationException("Expected one leased operation batch.")
            : batches[0];
    }

    /// <summary>Creates a representative remote batch.</summary>
    /// <param name="previousCursor">The previous remote cursor.</param>
    /// <param name="nextCursor">The next remote cursor.</param>
    /// <param name="events">The remote events.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateRemoteBatch(string? previousCursor, string nextCursor, IReadOnlyList<RemoteEvent> events) =>
        new(Guid.NewGuid(), Stream, previousCursor, nextCursor, events);

    /// <summary>Creates a representative remote event.</summary>
    /// <param name="serverCursor">The server cursor.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(string serverCursor) =>
        new(Guid.NewGuid(), Stream, serverCursor, DateTimeOffset.UnixEpoch, null, CreatePayload("remote"), new Dictionary<string, string>());

    /// <summary>Creates a representative snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <returns>The mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(long expectedRevision) =>
        new(Stream, CreatePayload("snapshot"), FormatVersion: 1, expectedRevision);

    /// <summary>Creates a representative payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreatePayload(string text) =>
        new("reading", 1, "application/json", System.Text.Encoding.UTF8.GetBytes(text), $"hash-{text.Length}");

    /// <summary>Inserts a row to hold a writer lock.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    private static void InsertBlockingIdentity(SqliteConnection connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_subscription_identities
                (store_identity, stream_id, subscription_id)
            VALUES
                ($storeIdentity, $streamId, $subscriptionId);
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, "sensor/held-lock");
        _ = command.Parameters.AddWithValue("$subscriptionId", SubscriptionId.New().Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Reads the SQLite user version from the database.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The SQLite user version.</returns>
    /// <exception cref="InvalidOperationException">SQLite returns an unexpected user version shape.</exception>
    private static long ReadUserVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA user_version;";
        return command.ExecuteScalar() is long value
            ? value
            : throw new InvalidOperationException("SQLite user_version returned an unexpected value.");
    }

    /// <summary>Opens a raw SQLite connection with pooling disabled.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The open connection.</returns>
    private static SqliteConnection OpenRawConnection(string path)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString();
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return connection;
    }

    /// <summary>Temporary database file helper.</summary>
    private sealed class TempDatabase : IDisposable
    {
        /// <summary>The temporary directory path.</summary>
        private readonly string _directory;

        /// <summary>Initializes a new instance of the <see cref="TempDatabase"/> class.</summary>
        /// <param name="directory">The temporary directory path.</param>
        private TempDatabase(string directory)
        {
            _directory = directory;
            Path = System.IO.Path.Combine(directory, "local.db");
        }

        /// <summary>Gets the SQLite database path.</summary>
        public string Path { get; }

        /// <summary>Creates a new temporary database helper.</summary>
        /// <returns>The temporary database helper.</returns>
        public static TempDatabase Create()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rxui-oc-sqlite-adapter", Guid.NewGuid().ToString("N"));
            _ = System.IO.Directory.CreateDirectory(directory);
            return new(directory);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (!System.IO.Directory.Exists(_directory))
            {
                return;
            }

            System.IO.Directory.Delete(_directory, true);
        }
    }

    /// <summary>A blocking event identifier list whose indexer proves the first snapshot is in progress.</summary>
    private sealed class BlockingEventIdList : IReadOnlyList<Guid>, IDisposable
    {
        /// <summary>The event identifier returned by the indexer.</summary>
        private readonly Guid _eventId;

        /// <summary>Signals that the indexer has been entered.</summary>
        private readonly ManualResetEventSlim _entered = new();

        /// <summary>Releases the blocked indexer.</summary>
        private readonly ManualResetEventSlim _release = new();

        /// <summary>Initializes a new instance of the <see cref="BlockingEventIdList"/> class.</summary>
        /// <param name="eventId">The event identifier returned by the indexer.</param>
        public BlockingEventIdList(Guid eventId) => _eventId = eventId;

        /// <inheritdoc/>
        public int Count => SingleWorkerCommand;

        /// <inheritdoc/>
        public Guid this[int index]
        {
            get
            {
                _entered.Set();
                _ = _release.Wait(GuardTimeout);
                return _eventId;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _entered.Dispose();
            _release.Dispose();
        }

        /// <summary>Releases the blocked indexer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release() => _release.Set();

        /// <summary>Waits until the indexer has been entered.</summary>
        /// <returns>A value indicating whether the indexer was entered.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool WaitForIndexer() => _entered.Wait(GuardTimeout);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<Guid> GetEnumerator() => throw new InvalidOperationException("The enumerator should not be read during capture.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A hostile single event identifier list whose indexer must not be touched.</summary>
    private sealed class ThrowingSingleEventIdList : IReadOnlyList<Guid>
    {
        /// <inheritdoc/>
        public int Count => SingleWorkerCommand;

        /// <summary>Gets a value indicating whether the indexer was read.</summary>
        public bool IndexerRead { get; private set; }

        /// <inheritdoc/>
        public Guid this[int index]
        {
            get
            {
                IndexerRead = true;
                throw new InvalidOperationException("The indexer should not be read when capture reservations are exhausted.");
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<Guid> GetEnumerator() => throw new InvalidOperationException("The enumerator should not be read during capture.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A hostile event identifier list whose count is available but indexer must not be touched.</summary>
    private sealed class ThrowingEventIdList : IReadOnlyList<Guid>
    {
        /// <inheritdoc/>
        public int Count => int.MaxValue;

        /// <summary>Gets a value indicating whether the indexer was read.</summary>
        public bool IndexerRead { get; private set; }

        /// <inheritdoc/>
        public Guid this[int index]
        {
            get
            {
                IndexerRead = true;
                throw new InvalidOperationException("The indexer should not be read during preflight.");
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<Guid> GetEnumerator() => throw new InvalidOperationException("The enumerator should not be read during preflight.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
