// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The current local commit schema version.</summary>
    private const int SchemaVersion = 3;

    /// <summary>The legacy local commit schema version without a remote inbox.</summary>
    private const int LegacyLocalCommitSchemaVersion = 2;

    /// <summary>The identity-only schema version.</summary>
    private const int IdentitySchemaVersion = 1;

    /// <summary>The second client sequence value.</summary>
    private const int SecondClientSequence = 2;

    /// <summary>The third client sequence value.</summary>
    private const int ThirdClientSequence = 3;

    /// <summary>The changed snapshot format version used by duplicate-intent tests.</summary>
    private const int ChangedSnapshotFormatVersion = 2;

    /// <summary>The changed operation priority used by duplicate-intent tests.</summary>
    private const int ChangedOperationPriority = 2;

    /// <summary>The number of pending operations after two local commits.</summary>
    private const int TwoPendingOperations = 2;

    /// <summary>Milliseconds to wait so Microsoft.Data.Sqlite observes a managed busy timeout attempt.</summary>
    private const int ManagedBusyRetryDelayMilliseconds = 1200;

    /// <summary>The primary store identity used by tests.</summary>
    private const string StoreIdentity = "client-alpha";

    /// <summary>The secondary store identity used by partition tests.</summary>
    private const string SecondaryStoreIdentity = "client-beta";

    /// <summary>The metadata key used for operation origin.</summary>
    private const string MetadataOriginKey = "origin";

    /// <summary>The metadata key used for path ordering checks.</summary>
    private const string MetadataPathKey = "path";

    /// <summary>The metadata value used for unit-test-origin operations.</summary>
    private const string UnitTestOrigin = "unit-test";

    /// <summary>The metadata value used for path ordering checks.</summary>
    private const string PrimaryPath = "primary";

    /// <summary>The default operation payload text.</summary>
    private const string OperationPayloadText = "operation";

    /// <summary>The default snapshot payload text.</summary>
    private const string SnapshotPayloadText = "snapshot";

    /// <summary>The SQLite store identity parameter name.</summary>
    private const string StoreIdentityParameter = "$storeIdentity";

    /// <summary>The SQLite stream id parameter name.</summary>
    private const string StreamIdParameter = "$streamId";

    /// <summary>The SQLite server cursor parameter name.</summary>
    private const string ServerCursorParameter = "$serverCursor";

    /// <summary>A representative stream identity.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>A representative reopened stream identity.</summary>
    private static readonly StreamId ReopenedStream = new("sensor/reopened");

    /// <summary>Verifies schema version two is created explicitly and keeps committed stream state after reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalOperationIsCommittedAndStoreReopens_ThenSnapshotSequenceAndOutboxRecover()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);

        var result = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.ClientSequence).IsEqualTo(1);
        await Assert.That(result.SnapshotRevision).IsEqualTo(1);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovery.PendingOperations[0].Metadata[MetadataOriginKey]).IsEqualTo(UnitTestOrigin);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.State.Payload.ToArray().SequenceEqual(snapshot.State.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies schema version one identity databases migrate without losing identities.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenIdentitySchemaMigratesToLocalCommitSchema_ThenExistingIdentityIsPreserved()
    {
        using var database = TempDatabase.Create();
        var subscriptionId = SubscriptionId.New();
        using (var identityStore = new SqliteSubscriptionIdentityStore(database.Path))
        {
            identityStore.Initialize(new(StoreIdentity, IdentitySchemaVersion, false), CancellationToken.None);
            _ = identityStore.GetOrCreateSubscriptionId(Stream, subscriptionId, CancellationToken.None);
        }

        using var store = CreateInitializedStore(database.Path);

        await Assert.That(store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(subscriptionId);
        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
    }

    /// <summary>Verifies reopening schema version two through the identity facade keeps schema version two valid.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenIdentityFacadeOpensLocalCommitSchema_ThenSchemaVersionTwoRemainsValid()
    {
        using var database = TempDatabase.Create();
        using (var store = CreateInitializedStore(database.Path))
        {
            _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        }

        using var identityStore = new SqliteSubscriptionIdentityStore(database.Path);
        identityStore.Initialize(new(StoreIdentity, IdentitySchemaVersion, false), CancellationToken.None);

        await Assert.That(identityStore.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None).Value).IsNotEqualTo(Guid.Empty);
        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(SchemaVersion);
    }

    /// <summary>Verifies duplicate operation ids return the first durable result without changing state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDuplicateOperationIsCommitted_ThenExistingResultIsReturnedWithoutSideEffects()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1) with
        {
            Metadata = new Dictionary<string, string> { [MetadataOriginKey] = UnitTestOrigin, [MetadataPathKey] = PrimaryPath },
        };
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        var first = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        var reorderedMetadata = operation with
        {
            Metadata = new Dictionary<string, string> { [MetadataPathKey] = PrimaryPath, [MetadataOriginKey] = UnitTestOrigin },
        };

        var second = store.CommitLocalOperation(reorderedMetadata, snapshot, CancellationToken.None);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(second).IsEqualTo(first);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies operation identifiers are scoped by store identity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSameOperationIdIsCommittedInDifferentPartitions_ThenBothPartitionsRecoverIndependently()
    {
        using var database = TempDatabase.Create();
        using var alpha = CreateInitializedStore(database.Path, StoreIdentity);
        using var beta = CreateInitializedStore(database.Path, SecondaryStoreIdentity);
        var operationId = OperationId.New();
        var alphaSubscription = alpha.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var betaSubscription = beta.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var alphaOperation = CreateOperation(clientSequence: 1) with { OperationId = operationId };
        var betaOperation = CreateOperation(clientSequence: 1) with { OperationId = operationId };

        var alphaResult = alpha.CommitLocalOperation(alphaOperation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var betaResult = beta.CommitLocalOperation(betaOperation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var alphaRecovery = alpha.RecoverStream(Stream, alphaSubscription, CancellationToken.None);
        var betaRecovery = beta.RecoverStream(Stream, betaSubscription, CancellationToken.None);

        await Assert.That(alphaResult.OperationId).IsEqualTo(operationId);
        await Assert.That(betaResult.OperationId).IsEqualTo(operationId);
        await Assert.That(alphaRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(betaRecovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(alphaRecovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(betaRecovery.NextClientSequence).IsEqualTo(SecondClientSequence);
    }

    /// <summary>Verifies replaying an older exact operation returns its original receipt after newer commits.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEarlierOperationIsReplayedAfterLaterCommit_ThenOriginalReceiptIsReturned()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var firstOperation = CreateOperation(clientSequence: 1);
        var firstSnapshot = CreateSnapshotMutation(expectedRevision: 0);
        var first = store.CommitLocalOperation(firstOperation, firstSnapshot, CancellationToken.None);
        var secondOperation = CreateOperation(clientSequence: SecondClientSequence);
        var secondSnapshot = new SnapshotMutation(Stream, CreatePayload("second-snapshot"), FormatVersion: 1, ExpectedRevision: 1);
        var second = store.CommitLocalOperation(secondOperation, secondSnapshot, CancellationToken.None);

        var replay = store.CommitLocalOperation(firstOperation, firstSnapshot, CancellationToken.None);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(replay).IsEqualTo(first);
        await Assert.That(replay.CommittedAtUtc).IsEqualTo(first.CommittedAtUtc);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(ThirdClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(TwoPendingOperations);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(firstOperation.OperationId);
        await Assert.That(recovery.PendingOperations[1].OperationId).IsEqualTo(secondOperation.OperationId);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(second.SnapshotRevision);
        await Assert.That(recovery.Snapshot?.State.Payload.ToArray().SequenceEqual(secondSnapshot.State.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies a reused operation id with different intent is rejected without state changes.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDuplicateOperationIdHasDifferentIntent_ThenStoreRejectsWithoutSideEffects()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        _ = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        var changedSequence = operation with { ClientSequence = SecondClientSequence };
        var changedTimestamp = operation with { TimestampUtc = operation.TimestampUtc.AddSeconds(1) };
        var changedBaseVersion = operation with { BaseVersion = "server-b" };
        var changedType = operation with { Type = SyncOperationType.Delete };
        var changedPayload = operation with { Payload = CreatePayload("changed-operation") };
        var changedPolicy = operation with { Policy = operation.Policy with { Priority = ChangedOperationPriority } };
        var changedMetadata = operation with { Metadata = new Dictionary<string, string> { [MetadataOriginKey] = "changed" } };
        var changedSnapshotPayload = new SnapshotMutation(Stream, CreatePayload("changed-snapshot"), FormatVersion: 1, ExpectedRevision: 0);
        var changedSnapshotFormat = snapshot with { FormatVersion = ChangedSnapshotFormatVersion };
        var changedSnapshotRevision = snapshot with { ExpectedRevision = 1 };

        Action sequenceAction = () => store.CommitLocalOperation(changedSequence, snapshot, CancellationToken.None);
        Action timestampAction = () => store.CommitLocalOperation(changedTimestamp, snapshot, CancellationToken.None);
        Action baseVersionAction = () => store.CommitLocalOperation(changedBaseVersion, snapshot, CancellationToken.None);
        Action typeAction = () => store.CommitLocalOperation(changedType, snapshot, CancellationToken.None);
        Action payloadAction = () => store.CommitLocalOperation(changedPayload, snapshot, CancellationToken.None);
        Action policyAction = () => store.CommitLocalOperation(changedPolicy, snapshot, CancellationToken.None);
        Action metadataAction = () => store.CommitLocalOperation(changedMetadata, snapshot, CancellationToken.None);
        Action snapshotPayloadAction = () => store.CommitLocalOperation(operation, changedSnapshotPayload, CancellationToken.None);
        Action snapshotFormatAction = () => store.CommitLocalOperation(operation, changedSnapshotFormat, CancellationToken.None);
        Action snapshotRevisionAction = () => store.CommitLocalOperation(operation, changedSnapshotRevision, CancellationToken.None);

        await Assert.That(sequenceAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(timestampAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(baseVersionAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(typeAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(payloadAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(policyAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(metadataAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(snapshotPayloadAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(snapshotFormatAction).ThrowsExactly<InvalidOperationException>();
        await Assert.That(snapshotRevisionAction).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies stale writers fail the sequence/revision compare-and-swap without side effects.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStaleWriterCommitsOldSequenceAndRevision_ThenStoreRejectsWithoutSideEffects()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var staleOperation = CreateOperation(clientSequence: 1);

        Action action = () => store.CommitLocalOperation(staleOperation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies competing writers use first-winner semantics for the exact next sequence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenTwoWritersCommitSameNextSequence_ThenOnlyOneCommitWins()
    {
        using var database = TempDatabase.Create();
        using var first = CreateInitializedStore(database.Path);
        using var second = CreateInitializedStore(database.Path);
        var subscriptionId = first.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var firstTask = Task.Run(() => TryCommit(first, CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0)));
        var secondTask = Task.Run(() => TryCommit(second, CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0)));

        var attempts = await Task.WhenAll(firstTask, secondTask);
        var recovery = first.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(attempts.Count(static attempt => attempt.Result is not null)).IsEqualTo(1);
        await Assert.That(attempts.Count(static attempt => attempt.Exception is InvalidOperationException)).IsEqualTo(1);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies cancellation while waiting for a writer lock does not commit the operation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCommitIsCancelledWhileWaitingForWriter_ThenNothingIsCommitted()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await using var blocker = OpenRawConnection(database.Path);
        await using var transaction = blocker.BeginTransaction();
        InsertBlockingIdentity(blocker, transaction);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockedCommit = Task.Run(() =>
        {
            started.SetResult();
            return store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), cancellation.Token);
        });

        await started.Task;
        await Task.Delay(TimeSpan.FromMilliseconds(ManagedBusyRetryDelayMilliseconds));
        await cancellation.CancelAsync();

        await Assert.That(async () => await blockedCommit).ThrowsExactly<OperationCanceledException>();
        transaction.Rollback();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies a failure after outbox insertion rolls the snapshot, sequence, and outbox back together.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSqlTriggerFailsAfterOutboxInsert_ThenTransactionRollsBackOutboxSnapshotAndSequence()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        CreateRollbackTrigger(database.Path);

        Action action = () => store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<SqliteException>();
        DropRollbackTrigger(database.Path);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies malformed stored payload metadata is rejected during recovery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoredPayloadIsMalformed_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = "UPDATE oc_outbox SET payload_schema_version = 0;";
            _ = command.ExecuteNonQuery();
        }

        Action action = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema migration is transactional when a real trigger aborts table backfill.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaMigrationFails_ThenIdentitySchemaRemainsUsable()
    {
        using var database = TempDatabase.Create();
        var subscriptionId = SubscriptionId.New();
        using (var identityStore = new SqliteSubscriptionIdentityStore(database.Path))
        {
            identityStore.Initialize(new(StoreIdentity, IdentitySchemaVersion, false), CancellationToken.None);
            _ = identityStore.GetOrCreateSubscriptionId(Stream, subscriptionId, CancellationToken.None);
        }

        var triggerConnection = CreateMigrationRollbackTrigger(database.Path);
        using var store = new SqliteLocalCommitStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<SqliteException>();
        triggerConnection.Dispose();
        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(IdentitySchemaVersion);
        using var identityReopen = new SqliteSubscriptionIdentityStore(database.Path);
        identityReopen.Initialize(new(StoreIdentity, IdentitySchemaVersion, false), CancellationToken.None);
        await Assert.That(identityReopen.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(subscriptionId);
    }

    /// <summary>Verifies identity and commit connections use foreign-key enforcement and FULL synchronous writes after reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreReopens_ThenOperationalConnectionsEnforceForeignKeysAndFullSynchronous()
    {
        using var database = TempDatabase.Create();
        using (var store = CreateInitializedStore(database.Path))
        {
            InstallConnectionSettingsProbes(database.Path);
            var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
            _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
            _ = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        }

        using var reopened = CreateInitializedStore(database.Path);
        var reopenedSubscriptionId = reopened.GetOrCreateSubscriptionId(ReopenedStream, SubscriptionId.New(), CancellationToken.None);
        var reopenedOperation = CreateOperation(clientSequence: 1) with { StreamId = ReopenedStream };
        var reopenedSnapshot = new SnapshotMutation(ReopenedStream, CreatePayload(SnapshotPayloadText), FormatVersion: 1, ExpectedRevision: 0);
        _ = reopened.CommitLocalOperation(reopenedOperation, reopenedSnapshot, CancellationToken.None);
        _ = reopened.RecoverStream(ReopenedStream, reopenedSubscriptionId, CancellationToken.None);

        await Assert.That(ReadConnectionSettingsProbe(database.Path, "probe_identity")).IsEqualTo("1:2");
        await Assert.That(ReadConnectionSettingsProbe(database.Path, "probe_commit")).IsEqualTo("1:2");
    }

    /// <summary>Verifies encryption requirements fail before a database file or directory is created.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEncryptionIsRequired_ThenInitializeFailsBeforeCreatingDatabaseFile()
    {
        using var database = TempDatabase.CreateWithoutDirectory();
        using var store = new SqliteLocalCommitStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, true), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<NotSupportedException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
        await Assert.That(Directory.Exists(database.DirectoryPath)).IsFalse();
    }

    /// <summary>Verifies initialization rejects unsupported schema and lifecycle inputs.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInitializationInputOrLifecycleIsInvalid_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteLocalCommitStore(database.Path);
        LocalStoreInitialization missingInitialization = null!;

        Action missing = () => store.Initialize(missingInitialization, CancellationToken.None);
        Action unsupported = () => store.Initialize(new(StoreIdentity, IdentitySchemaVersion, false), CancellationToken.None);
        Action blank = () => store.Initialize(new(" ", SchemaVersion, false), CancellationToken.None);

        await Assert.That(missing).ThrowsExactly<ArgumentNullException>();
        await Assert.That(unsupported).ThrowsExactly<InvalidOperationException>();
        await Assert.That(blank).ThrowsExactly<ArgumentException>();

        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Action conflicting = () => store.Initialize(new(SecondaryStoreIdentity, SchemaVersion, false), CancellationToken.None);
        await Assert.That(conflicting).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies unsupported database path forms are rejected before SQLite opens them.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabasePathIsUnsupported_ThenConstructorRejectsIt()
    {
        Action memory = static () => _ = new SqliteLocalCommitStore(":memory:");
        Action uri = static () => _ = new SqliteLocalCommitStore("file:local.db");
        Action blank = static () => _ = new SqliteLocalCommitStore(" ");
        Action missingPath = static () => _ = new SqliteLocalCommitStore(null!);

        await Assert.That(memory).ThrowsExactly<ArgumentException>();
        await Assert.That(uri).ThrowsExactly<ArgumentException>();
        await Assert.That(blank).ThrowsExactly<ArgumentException>();
        await Assert.That(missingPath).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies invalid commit identity inputs are rejected before durable state changes.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCommitIdentityInputIsInvalid_ThenCommitFailsBeforeWriting()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        SyncOperation missingOperation = null!;
        SnapshotMutation missingSnapshot = null!;
        var otherStream = new StreamId("sensor/humidity");

        Action missingOperationAction = () => store.CommitLocalOperation(
            missingOperation,
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action missingSnapshotAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1),
            missingSnapshot,
            CancellationToken.None);
        Action mismatchedStreamAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { StreamId = otherStream },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action nonPositiveSequenceAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 0),
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action emptyOperationIdAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { OperationId = new(Guid.Empty) },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action invalidTypeAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Type = (SyncOperationType)int.MaxValue },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);

        await Assert.That(missingOperationAction).ThrowsExactly<ArgumentNullException>();
        await Assert.That(missingSnapshotAction).ThrowsExactly<ArgumentNullException>();
        await Assert.That(mismatchedStreamAction).ThrowsExactly<ArgumentException>();
        await Assert.That(nonPositiveSequenceAction).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(emptyOperationIdAction).ThrowsExactly<ArgumentException>();
        await Assert.That(invalidTypeAction).ThrowsExactly<ArgumentException>();

        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies invalid commit payload inputs are rejected before durable state changes.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCommitPayloadInputIsInvalid_ThenCommitFailsBeforeWriting()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        Action invalidPayloadSchemaAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Payload = CreatePayload(OperationPayloadText) with { SchemaVersion = 0 } },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action blankContractAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Payload = CreatePayload(OperationPayloadText) with { ContractId = string.Empty } },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action blankContentTypeAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Payload = CreatePayload(OperationPayloadText) with { ContentType = string.Empty } },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action blankPayloadHashAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Payload = CreatePayload(OperationPayloadText) with { PayloadHash = string.Empty } },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action invalidPolicyAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Policy = new((DeliveryGuarantee)int.MaxValue, OperationDurability.Durable, 0, ConflictPolicy.Merge) },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action blankMetadataKeyAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Metadata = new Dictionary<string, string> { [string.Empty] = "value" } },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action nullMetadataValueAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1) with { Metadata = new Dictionary<string, string> { ["key"] = null! } },
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);
        Action negativeSnapshotRevisionAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1),
            CreateSnapshotMutation(expectedRevision: -1),
            CancellationToken.None);
        Action invalidSnapshotFormatAction = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1),
            new(Stream, CreatePayload(SnapshotPayloadText), FormatVersion: 0, ExpectedRevision: 0),
            CancellationToken.None);

        await Assert.That(invalidPayloadSchemaAction).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(blankContractAction).ThrowsExactly<ArgumentException>();
        await Assert.That(blankContentTypeAction).ThrowsExactly<ArgumentException>();
        await Assert.That(blankPayloadHashAction).ThrowsExactly<ArgumentException>();
        await Assert.That(invalidPolicyAction).ThrowsExactly<ArgumentException>();
        await Assert.That(blankMetadataKeyAction).ThrowsExactly<ArgumentException>();
        await Assert.That(nullMetadataValueAction).ThrowsExactly<ArgumentNullException>();
        await Assert.That(negativeSnapshotRevisionAction).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(invalidSnapshotFormatAction).ThrowsExactly<ArgumentOutOfRangeException>();

        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies uninitialized and disposed stores reject work before opening SQLite.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIsNotUsable_ThenOperationsFailBeforeSqliteWork()
    {
        using var database = TempDatabase.Create();
        using var uninitialized = new SqliteLocalCommitStore(database.Path);
        Action uninitializedCommit = () => uninitialized.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        Action uninitializedRecover = () => uninitialized.RecoverStream(Stream, SubscriptionId.New(), CancellationToken.None);

        await Assert.That(uninitializedCommit).ThrowsExactly<InvalidOperationException>();
        await Assert.That(uninitializedRecover).ThrowsExactly<InvalidOperationException>();

        var disposed = CreateInitializedStore(database.Path);
        disposed.Dispose();
        Action disposedGet = () => disposed.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        Action disposedCommit = () => disposed.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);

        await Assert.That(disposedGet).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(disposedCommit).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies invalid recovery inputs and subscription mismatches fail closed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRecoveryInputIsInvalid_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);

        Action missingStream = () => store.RecoverStream(default, subscriptionId, CancellationToken.None);
        Action missingSubscription = () => store.RecoverStream(Stream, new(Guid.Empty), CancellationToken.None);
        Action wrongSubscription = () => store.RecoverStream(Stream, SubscriptionId.New(), CancellationToken.None);

        await Assert.That(missingStream).ThrowsExactly<ArgumentException>();
        await Assert.That(missingSubscription).ThrowsExactly<ArgumentException>();
        await Assert.That(wrongSubscription).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies migrated identities recover with initial sequence when a stream row is missing.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenIdentityExistsWithoutStream_ThenRecoveryUsesInitialSequence()
    {
        using var database = TempDatabase.Create();
        var subscriptionId = SubscriptionId.New();
        using (var identityStore = new SqliteSubscriptionIdentityStore(database.Path))
        {
            identityStore.Initialize(new(StoreIdentity, IdentitySchemaVersion, false), CancellationToken.None);
            _ = identityStore.GetOrCreateSubscriptionId(Stream, subscriptionId, CancellationToken.None);
        }

        using var store = CreateInitializedStore(database.Path);
        DeleteStreams(database.Path);

        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies deleting a durable stream row under committed rows fails recovery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamRowIsMissingForCommittedRows_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        DeleteStreamWithoutCascade(database.Path);

        Action action = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies pending operations cannot equal the stored next client sequence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPendingSequenceReachesStoredNextSequence_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        SetStreamNextClientSequence(database.Path, 1);

        Action action = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies snapshot cursor drift from stream cursor is rejected during recovery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotCursorDiffersFromStreamCursor_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        SetSnapshotServerCursor(database.Path, "snapshot-cursor");

        Action action = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies stale snapshot revision compare-and-swap rejects without side effects.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRevisionIsStale_ThenCommitFailsWithoutSideEffects()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        var stale = CreateOperation(clientSequence: SecondClientSequence);

        Action action = () => store.CommitLocalOperation(stale, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies a maximum client sequence cannot overflow the next durable sequence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenClientSequenceWouldOverflowNextSequence_ThenCommitFailsWithoutSideEffects()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        SetStreamNextClientSequence(database.Path, long.MaxValue);
        var overflowingOperation = CreateOperation(clientSequence: long.MaxValue);

        Action action = () => store.CommitLocalOperation(
            overflowingOperation,
            CreateSnapshotMutation(expectedRevision: 0),
            CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(long.MaxValue);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies a maximum expected snapshot revision cannot overflow the next revision.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotRevisionWouldOverflowNextRevision_ThenCommitFailsWithoutSideEffects()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        SetSnapshotRevision(database.Path, long.MaxValue);

        Action action = () => store.CommitLocalOperation(
            CreateOperation(clientSequence: 1),
            CreateSnapshotMutation(expectedRevision: long.MaxValue),
            CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(long.MaxValue);
    }

    /// <summary>Verifies a writer held past the bounded wait times out without committing.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenWriterLockIsHeldPastBound_ThenCommitTimesOutWithoutSideEffects()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        await using var blocker = OpenRawConnection(database.Path);
        await using var transaction = blocker.BeginTransaction();
        InsertBlockingIdentity(blocker, transaction);

        Action action = () => store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<TimeoutException>();
        transaction.Rollback();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies corrupt durable rows are rejected during recovery or duplicate lookup.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoredRowsAreMalformed_ThenRecoveryFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1) with { Metadata = new Dictionary<string, string>() };
        _ = store.CommitLocalOperation(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);

        SetSnapshotSavedAtMalformed(database.Path);
        Action badSnapshotTimestamp = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(badSnapshotTimestamp).ThrowsExactly<InvalidOperationException>();

        SetSnapshotSavedAtValid(database.Path);
        SetOutboxOperationIdMalformed(database.Path);
        Action badOperationId = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(badOperationId).ThrowsExactly<InvalidOperationException>();

        SetOutboxOperationId(database.Path, operation.OperationId);
        SetOutboxClientSequenceZero(database.Path);
        Action badSequence = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(badSequence).ThrowsExactly<InvalidOperationException>();

        SetOutboxClientSequence(database.Path, 1);
        SetOutboxOperationTypeInvalid(database.Path);
        Action badOperationType = () => store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(badOperationType).ThrowsExactly<ArgumentException>();

        SetOutboxOperationType(database.Path, SyncOperationType.Update);
        SetOutboxSnapshotRevisionNegative(database.Path);
        Action badDuplicateResult = () => store.CommitLocalOperation(operation, CreateSnapshotMutation(expectedRevision: 1), CancellationToken.None);
        await Assert.That(badDuplicateResult).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies corrupt commit fingerprints reject duplicate replay.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoredCommitFingerprintIsMalformed_ThenDuplicateReplayFailsClosed()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(clientSequence: 1);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0);
        _ = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        SetOutboxCommitFingerprintMalformed(database.Path);

        Action replay = () => store.CommitLocalOperation(operation, snapshot, CancellationToken.None);

        await Assert.That(replay).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a trigger that removes the stream row makes the sequence update fail atomically.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStreamRowDisappearsBeforeSequenceUpdate_ThenCommitRollsBack()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        CreateSequenceUpdateRollbackTrigger(database.Path);

        Action action = () => store.CommitLocalOperation(CreateOperation(clientSequence: 1), CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        DropSequenceUpdateRollbackTrigger(database.Path);
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies local schema drift and newer schemas are rejected without repair.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalCommitSchemaDrifts_ThenReopenRejectsIt()
    {
        using var driftedDatabase = TempDatabase.Create();
        using (var store = CreateInitializedStore(driftedDatabase.Path))
        {
            _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        }

        CreateUnexpectedTable(driftedDatabase.Path);
        using var drifted = new SqliteLocalCommitStore(driftedDatabase.Path);
        Action driftedInitialize = () => drifted.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await Assert.That(driftedInitialize).ThrowsExactly<InvalidOperationException>();

        using var newerDatabase = TempDatabase.Create();
        using (var store = CreateInitializedStore(newerDatabase.Path))
        {
            _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        }

        SetUserVersionToNewer(newerDatabase.Path);
        using var newer = new SqliteLocalCommitStore(newerDatabase.Path);
        Action newerInitialize = () => newer.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await Assert.That(newerInitialize).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies low-level SQLite row readers reject null and malformed scalar values.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoredScalarValuesAreMalformed_ThenReadersFailClosed()
    {
        using var database = TempDatabase.Create();
        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT NULL, X'01', 'not-a-date', 0, -1;";
        await using var reader = await command.ExecuteReaderAsync();
        _ = await reader.ReadAsync();

        const int NullColumnIndex = 0;
        const int BytesColumnIndex = 1;
        const int DateColumnIndex = 2;
        const int ZeroColumnIndex = 3;
        const int NegativeColumnIndex = 4;

        Action nullString = () => SqliteLocalCommitSql.ReadString(reader, NullColumnIndex, "string");
        Action nullBytes = () => SqliteLocalCommitSql.ReadBytes(reader, NullColumnIndex, "bytes");
        Action nullInt = () => SqliteLocalCommitSql.ReadInt(reader, NullColumnIndex, "int");
        Action zeroPositiveInt = () => SqliteLocalCommitSql.ReadPositiveInt(reader, ZeroColumnIndex, "positive-int");
        Action negativePositiveLong = () => SqliteLocalCommitSql.ReadPositiveLong(reader, NegativeColumnIndex, "positive-long");
        Action nullNonNegativeLong = () => SqliteLocalCommitSql.ReadNonNegativeLong(reader, NullColumnIndex, "non-negative-long");
        Action negativeNonNegativeScalar = static () => SqliteLocalCommitSql.ReadNonNegativeLong(-1L, "non-negative-scalar");
        Action nonLongNonNegativeScalar = static () => SqliteLocalCommitSql.ReadNonNegativeLong("0", "non-negative-scalar");
        Action malformedDate = () => SqliteLocalCommitSql.ReadDateTimeOffset(reader, DateColumnIndex, "date");

        await Assert.That(nullString).ThrowsExactly<InvalidOperationException>();
        await Assert.That(nullBytes).ThrowsExactly<InvalidOperationException>();
        await Assert.That(nullInt).ThrowsExactly<InvalidOperationException>();
        await Assert.That(zeroPositiveInt).ThrowsExactly<InvalidOperationException>();
        await Assert.That(negativePositiveLong).ThrowsExactly<InvalidOperationException>();
        await Assert.That(nullNonNegativeLong).ThrowsExactly<InvalidOperationException>();
        await Assert.That(negativeNonNegativeScalar).ThrowsExactly<InvalidOperationException>();
        await Assert.That(nonLongNonNegativeScalar).ThrowsExactly<InvalidOperationException>();
        await Assert.That(malformedDate).ThrowsExactly<InvalidOperationException>();
        await Assert.That(SqliteLocalCommitSql.ReadBytes(reader, BytesColumnIndex, "bytes").Length).IsEqualTo(1);
    }
}
