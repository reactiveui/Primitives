// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Crash matrix tests that stop a child writer process at named <see cref="SqliteCommitCheckpoint"/> values.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The child crash matrix environment variable that carries the encoded case.</summary>
    private const string CrashMatrixChildCaseVariable = "RXUI_SQLITE_CRASH_MATRIX_CASE";

    /// <summary>The parallel constraint key shared by crash matrix parent tests.</summary>
    private const string CrashMatrixParallelKey = "sqlite-crash-matrix";

    /// <summary>The number of encoded crash matrix case fields.</summary>
    private const int CrashMatrixCaseFieldCount = 9;

    /// <summary>The optimistic payload committed by compaction crash matrix preparation.</summary>
    private const string CrashMatrixCompactionPayloadText = "crash-compaction";

    /// <summary>The second receive cursor used by crash matrix redelivery checks.</summary>
    private const string CrashMatrixRedeliveryCursor = "crash-cursor-redelivery";

    /// <summary>The snapshot revision after two local commits.</summary>
    private const int CrashMatrixTwoCommitRevision = 2;

    /// <summary>The next client sequence after two local commits.</summary>
    private const int CrashMatrixThirdClientSequence = 3;

    /// <summary>The child crash matrix test tree node filter.</summary>
    private const string CrashMatrixChildTestTreeNodeFilter = $"/*/*/*/{nameof(WhenCrashMatrixChildReachesCheckpoint_ThenSignalIsPublished)}";

    /// <summary>The short retention used so compaction finds eligible terminal rows.</summary>
    private static readonly RetentionOptions CrashMatrixRetention = new()
    {
        OutboxTerminalRetention = TimeSpan.FromSeconds(1),
        InboxDeduplicationRetention = TimeSpan.FromSeconds(1),
        DeadLetterRetention = TimeSpan.FromSeconds(1),
        ServerIdempotencyRetention = TimeSpan.FromSeconds(1),
    };

    /// <summary>Verifies a writer process killed at a named SQLite checkpoint reopens with all-or-nothing durable state.</summary>
    /// <param name="checkpoint">The <see cref="SqliteCommitCheckpoint"/> name where the child blocks.</param>
    /// <param name="deliveryGuarantee">The delivery guarantee of the committed operations.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    [Test]
    [NotInParallel(CrashMatrixParallelKey)]
    [Arguments(nameof(SqliteCommitCheckpoint.LocalCommitBeforeCommit), DeliveryGuarantee.AtMostOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.LocalCommitBeforeCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.LocalCommitBeforeCommit), DeliveryGuarantee.ExactlyOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.LocalCommitAfterCommit), DeliveryGuarantee.AtMostOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.LocalCommitAfterCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.LocalCommitAfterCommit), DeliveryGuarantee.ExactlyOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.AttemptBarrierBeforeCommit), DeliveryGuarantee.AtMostOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.AttemptBarrierBeforeCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.AttemptBarrierBeforeCommit), DeliveryGuarantee.ExactlyOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.AttemptBarrierAfterCommit), DeliveryGuarantee.AtMostOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.AttemptBarrierAfterCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.AttemptBarrierAfterCommit), DeliveryGuarantee.ExactlyOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.SyncResultBeforeCommit), DeliveryGuarantee.AtMostOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.SyncResultBeforeCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.SyncResultBeforeCommit), DeliveryGuarantee.ExactlyOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.SyncResultAfterCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.SyncResultAfterCommit), DeliveryGuarantee.ExactlyOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.RemoteApplyBeforeCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.RemoteApplyAfterCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.DeadLetterBeforeCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.DeadLetterAfterCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.CompactionBeforeCommit), DeliveryGuarantee.AtLeastOnce)]
    [Arguments(nameof(SqliteCommitCheckpoint.CompactionAfterCommit), DeliveryGuarantee.AtLeastOnce)]
    public async Task WhenWriterProcessDiesAtCommitCheckpoint_ThenReopenHonorsTransactionBoundary(string checkpoint, DeliveryGuarantee deliveryGuarantee)
    {
        using var database = TempDatabase.Create();
        var matrixCase = CrashMatrixCase.Create(database.Path, ParseCrashMatrixCheckpoint(checkpoint), deliveryGuarantee);
        await PrepareCrashMatrixDatabaseAsync(matrixCase);

        await RunCrashMatrixChildUntilSignalAsync(matrixCase);

        await Assert.That(await File.ReadAllTextAsync(matrixCase.SignalPath)).IsEqualTo(checkpoint);
        await AssertCrashMatrixRecoveryAsync(matrixCase);
    }

    /// <summary>Child workflow used by crash matrix parent process tests.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child crash matrix environment is malformed or the checkpoint was not reached.</exception>
    [Test]
    public async Task WhenCrashMatrixChildReachesCheckpoint_ThenSignalIsPublished()
    {
        var encoded = Environment.GetEnvironmentVariable(CrashMatrixChildCaseVariable);
        if (encoded is null)
        {
            await Assert.That(encoded).IsNull();
            return;
        }

        var matrixCase = CrashMatrixCase.Parse(encoded);
        var faultPoint = new BlockingCommitFaultPoint(matrixCase.Checkpoint, matrixCase.SignalPath);
        await using var adapter = CreateCrashMatrixAdapter(matrixCase, faultPoint);
        await adapter.InitializeAsync(CreateCrashMatrixInitialization(matrixCase.Checkpoint), CancellationToken.None);
        await RunCrashMatrixChildActionAsync(adapter, matrixCase);
        throw new InvalidOperationException($"The crash matrix child finished without reaching {matrixCase.Checkpoint}.");
    }

    /// <summary>Parses a checkpoint name supplied by a test argument.</summary>
    /// <param name="checkpoint">The checkpoint name.</param>
    /// <returns>The parsed checkpoint.</returns>
    /// <exception cref="ArgumentException">The name is not a checkpoint.</exception>
    private static SqliteCommitCheckpoint ParseCrashMatrixCheckpoint(string checkpoint) =>
        Enum.TryParse<SqliteCommitCheckpoint>(checkpoint, ignoreCase: false, out var parsed)
            ? parsed
            : throw new ArgumentException("The crash matrix checkpoint name is unknown.", nameof(checkpoint));

    /// <summary>Creates the child adapter with the blocking checkpoint observer.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <param name="faultPoint">The checkpoint observer.</param>
    /// <returns>The adapter.</returns>
    private static SqliteLocalStoreAdapter CreateCrashMatrixAdapter(CrashMatrixCase matrixCase, ISqliteCommitFaultPoint faultPoint)
    {
        var timestamp = matrixCase.Checkpoint is SqliteCommitCheckpoint.CompactionBeforeCommit or SqliteCommitCheckpoint.CompactionAfterCommit
            ? CrashRecoveryExpiredLeaseTimestamp
            : CrashRecoveryTimestamp;
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = NormalWorkerBytes, Retention = CrashMatrixRetention };
        return new(matrixCase.DatabasePath, options with { TimeProvider = new FixedTimeProvider(timestamp) }, faultPoint);
    }

    /// <summary>Runs the child store call that reaches the requested checkpoint.</summary>
    /// <param name="adapter">The child adapter.</param>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that completes only if the checkpoint is not reached.</returns>
    private static Task RunCrashMatrixChildActionAsync(SqliteLocalStoreAdapter adapter, CrashMatrixCase matrixCase) => matrixCase.Checkpoint switch
    {
        SqliteCommitCheckpoint.LocalCommitBeforeCommit or SqliteCommitCheckpoint.LocalCommitAfterCommit => CommitCrashMatrixOperationAsync(adapter, matrixCase),
        SqliteCommitCheckpoint.AttemptBarrierBeforeCommit or SqliteCommitCheckpoint.AttemptBarrierAfterCommit => BeginCrashMatrixAttemptAsync(adapter, matrixCase),
        SqliteCommitCheckpoint.SyncResultBeforeCommit or SqliteCommitCheckpoint.SyncResultAfterCommit => AcknowledgeCrashMatrixOperationAsync(adapter, matrixCase),
        SqliteCommitCheckpoint.RemoteApplyBeforeCommit or SqliteCommitCheckpoint.RemoteApplyAfterCommit => ApplyCrashMatrixRemoteBatchAsync(adapter, matrixCase),
        SqliteCommitCheckpoint.DeadLetterBeforeCommit or SqliteCommitCheckpoint.DeadLetterAfterCommit => DeadLetterCrashMatrixSecondOperationAsync(adapter, matrixCase),
        _ => adapter.CompactAsync(new(Stream, CrashRecoveryExpiredLeaseTimestamp, TargetBytes: 0), CancellationToken.None).AsTask(),
    };

    /// <summary>Commits the case operation as the first local operation.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task CommitCrashMatrixOperationAsync(SqliteLocalStoreAdapter adapter, CrashMatrixCase matrixCase) =>
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(matrixCase.OperationId, FirstClientSequence, matrixCase.DeliveryGuarantee, "matrix"),
            CreateCrashMatrixInitialMutation(),
            CancellationToken.None);

    /// <summary>Leases the case operation and records its first pre-send attempt barrier.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>The lease that owns the attempt.</returns>
    private static async Task<LeasedOperationBatch> BeginCrashMatrixAttemptAsync(SqliteLocalStoreAdapter adapter, CrashMatrixCase matrixCase)
    {
        var lease = await LeaseFirstCrashMatrixBatchAsync(adapter, FirstAttempt);
        _ = await adapter.TryBeginRemoteAttemptAsync(lease.LeaseId, matrixCase.OperationId, FirstAttempt, CancellationToken.None);
        return lease;
    }

    /// <summary>Sends the case operation and records the server acknowledgement.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task AcknowledgeCrashMatrixOperationAsync(SqliteLocalStoreAdapter adapter, CrashMatrixCase matrixCase)
    {
        var lease = await BeginCrashMatrixAttemptAsync(adapter, matrixCase);
        await adapter.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(matrixCase.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
    }

    /// <summary>Applies the receive batch that completes the case operation.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task ApplyCrashMatrixRemoteBatchAsync(SqliteLocalStoreAdapter adapter, CrashMatrixCase matrixCase) =>
        _ = await adapter.ApplyRemoteBatchAsync(
            CreateCrashMatrixRemoteBatch(matrixCase, matrixCase.BatchId, null, CrashRecoveryRemoteCursor),
            CreateCrashMatrixRemoteMutation(FirstClientSequence),
            CancellationToken.None);

    /// <summary>Leases both committed operations and dead-letters the second one.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task DeadLetterCrashMatrixSecondOperationAsync(SqliteLocalStoreAdapter adapter, CrashMatrixCase matrixCase)
    {
        var lease = await LeaseFirstCrashMatrixBatchAsync(adapter, TwoWorkerCommands);
        _ = await adapter.DeadLetterOperationAsync(
            lease.LeaseId,
            matrixCase.SecondOperationId,
            CrashRecoveryDeadLetterReason,
            CreateSnapshotMutation(SecondClientSequence, CrashRecoveryDeadLetterPayloadText),
            CancellationToken.None);
    }

    /// <summary>Prepares the committed database state that the child process starts from.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static Task PrepareCrashMatrixDatabaseAsync(CrashMatrixCase matrixCase) => matrixCase.Checkpoint switch
    {
        SqliteCommitCheckpoint.LocalCommitBeforeCommit or SqliteCommitCheckpoint.LocalCommitAfterCommit => PrepareCrashMatrixEmptyStreamAsync(matrixCase),
        SqliteCommitCheckpoint.DeadLetterBeforeCommit or SqliteCommitCheckpoint.DeadLetterAfterCommit => PrepareCrashMatrixTwoOperationDatabaseAsync(matrixCase),
        SqliteCommitCheckpoint.CompactionBeforeCommit or SqliteCommitCheckpoint.CompactionAfterCommit => PrepareCrashMatrixCompactionDatabaseAsync(matrixCase),
        SqliteCommitCheckpoint.RemoteApplyBeforeCommit or SqliteCommitCheckpoint.RemoteApplyAfterCommit =>
            PrepareSingleOperationDatabaseAsync(matrixCase.DatabasePath, matrixCase.OperationId, matrixCase.SubscriptionId, matrixCase.DeliveryGuarantee),
        _ => PrepareAttemptBoundaryDatabaseAsync(matrixCase.DatabasePath, matrixCase.OperationId, matrixCase.SubscriptionId, matrixCase.DeliveryGuarantee),
    };

    /// <summary>Creates store initialization for a checkpoint.</summary>
    /// <param name="checkpoint">The checkpoint.</param>
    /// <returns>The initialization; upload checkpoints use an unbound client identity like the attempt barrier fixtures.</returns>
    private static LocalStoreInitialization CreateCrashMatrixInitialization(SqliteCommitCheckpoint checkpoint) =>
        checkpoint is SqliteCommitCheckpoint.AttemptBarrierBeforeCommit
            or SqliteCommitCheckpoint.AttemptBarrierAfterCommit
            or SqliteCommitCheckpoint.SyncResultBeforeCommit
            or SqliteCommitCheckpoint.SyncResultAfterCommit
            ? new(StoreIdentity, SchemaVersion, false)
            : new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId };

    /// <summary>Prepares an initialized store with a subscription and no committed operations.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PrepareCrashMatrixEmptyStreamAsync(CrashMatrixCase matrixCase)
    {
        await using var adapter = CreateAdapter(matrixCase.DatabasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
    }

    /// <summary>Prepares two committed operations while the store is bound to the client identity.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PrepareCrashMatrixTwoOperationDatabaseAsync(CrashMatrixCase matrixCase)
    {
        await using var adapter = CreateAdapter(matrixCase.DatabasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(matrixCase.OperationId, FirstClientSequence, matrixCase.DeliveryGuarantee, "first"),
            CreateCrashMatrixInitialMutation(),
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(matrixCase.SecondOperationId, SecondClientSequence, matrixCase.DeliveryGuarantee, "second"),
            CreateSnapshotMutation(FirstClientSequence, ResultOptimisticLocalText),
            CancellationToken.None);
    }

    /// <summary>Prepares one synchronized terminal operation and one pending operation for compaction.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PrepareCrashMatrixCompactionDatabaseAsync(CrashMatrixCase matrixCase)
    {
        await using var adapter = CreateAdapter(matrixCase.DatabasePath, new FixedTimeProvider(CrashRecoveryTimestamp));
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, matrixCase.SubscriptionId, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(matrixCase.OperationId, FirstClientSequence, matrixCase.DeliveryGuarantee, "first"),
            CreateSnapshotMutation(0, ResultOptimisticInitialText),
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateCrashRecoveryOperation(matrixCase.SecondOperationId, SecondClientSequence, matrixCase.DeliveryGuarantee, "second"),
            CreateSnapshotMutation(FirstClientSequence, CrashMatrixCompactionPayloadText),
            CancellationToken.None);
        var lease = await LeaseFirstCrashMatrixBatchAsync(adapter, FirstAttempt);
        await adapter.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(matrixCase.OperationId, OperationResultKind.Accepted, null, ServerVersion)], null, null),
            CancellationToken.None);
    }

    /// <summary>Leases the first pending batch without leasing later batches.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <param name="maximumOperations">The maximum operations in the batch.</param>
    /// <returns>The first leased batch.</returns>
    /// <exception cref="InvalidOperationException">No batch was leased.</exception>
    private static async Task<LeasedOperationBatch> LeaseFirstCrashMatrixBatchAsync(SqliteLocalStoreAdapter adapter, int maximumOperations)
    {
        var batches = adapter.LeasePendingOperationsAsync(new(Stream, maximumOperations, NormalWorkerBytes, TimeSpan.FromMinutes(1)), CancellationToken.None);
        await using var enumerator = batches.GetAsyncEnumerator(CancellationToken.None);
        return await enumerator.MoveNextAsync()
            ? enumerator.Current
            : throw new InvalidOperationException("Expected one leased crash matrix batch.");
    }

    /// <summary>Creates the first local commit mutation with an authoritative baseline.</summary>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateCrashMatrixInitialMutation() =>
        CreateSnapshotMutation(0, ResultOptimisticInitialText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeInitialText) };

    /// <summary>Creates the remote apply mutation used by receive checkpoints.</summary>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateCrashMatrixRemoteMutation(long expectedRevision) =>
        CreateSnapshotMutation(expectedRevision, CrashRecoveryRemotePayloadText) with { AuthoritativeState = CreatePayload(CrashRecoveryAuthoritativeRemoteText) };

    /// <summary>Creates the receive batch that completes the case operation.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <returns>The receive batch.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemoteEventBatch CreateCrashMatrixRemoteBatch(CrashMatrixCase matrixCase, Guid batchId, string? previousCursor, string nextCursor) =>
        CreateCrashRecoveryRemoteBatch(
            batchId,
            previousCursor,
            nextCursor,
            CreateCrashRecoveryRemoteEvent(matrixCase.EventId, CrashRecoveryRemoteCursor, matrixCase.OperationId, ClientId),
            matrixCase.OperationId);

    /// <summary>Starts and kills a crash matrix child after it signals from inside the store call.</summary>
    /// <param name="matrixCase">The crash matrix case.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    private static async Task RunCrashMatrixChildUntilSignalAsync(CrashMatrixCase matrixCase)
    {
        var testAssembly = Path.Combine(AppContext.BaseDirectory, TestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(CrashMatrixChildTestTreeNodeFilter);
        startInfo.Environment[CrashMatrixChildCaseVariable] = matrixCase.Encode();
        using var child = Process.Start(startInfo) ?? throw new InvalidOperationException("The crash matrix child process did not start.");
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        var signaled = await WaitForSignalAsync(matrixCase.SignalPath, child, SignalWaitTimeout);
        var output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
        if (!signaled)
        {
            throw new InvalidOperationException(CreateCrashRecoverySignalTimeoutMessage(output));
        }
    }

    /// <summary>Blocks the SQLite worker at one named checkpoint after publishing an atomic signal file.</summary>
    /// <param name="target">The checkpoint that blocks.</param>
    /// <param name="signalPath">The signal file path.</param>
    private sealed class BlockingCommitFaultPoint(SqliteCommitCheckpoint target, string signalPath) : ISqliteCommitFaultPoint
    {
        /// <inheritdoc/>
        public void BeforeLocalCommitTransaction(Microsoft.Data.Sqlite.SqliteConnection connection)
        {
        }

        /// <inheritdoc/>
        public void Reached(SqliteCommitCheckpoint checkpoint)
        {
            if (checkpoint != target)
            {
                return;
            }

            var temporaryPath = $"{signalPath}.{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}.tmp";
            File.WriteAllText(temporaryPath, checkpoint.ToString());
            File.Move(temporaryPath, signalPath);
            using var never = new ManualResetEventSlim(false);
            never.Wait();
        }
    }

    /// <summary>One crash matrix case shared by the parent and child process.</summary>
    /// <param name="Checkpoint">The checkpoint where the child blocks.</param>
    /// <param name="DeliveryGuarantee">The delivery guarantee of committed operations.</param>
    /// <param name="DatabasePath">The SQLite database path.</param>
    /// <param name="SignalPath">The atomic signal path.</param>
    /// <param name="OperationId">The primary operation identifier.</param>
    /// <param name="SecondOperationId">The second operation identifier.</param>
    /// <param name="SubscriptionId">The subscription identifier.</param>
    /// <param name="EventId">The remote event identifier.</param>
    /// <param name="BatchId">The remote batch identifier.</param>
    private sealed record CrashMatrixCase(
        SqliteCommitCheckpoint Checkpoint,
        DeliveryGuarantee DeliveryGuarantee,
        string DatabasePath,
        string SignalPath,
        OperationId OperationId,
        OperationId SecondOperationId,
        SubscriptionId SubscriptionId,
        Guid EventId,
        Guid BatchId)
    {
        /// <summary>Creates a case with fresh identifiers beside the database.</summary>
        /// <param name="databasePath">The database path.</param>
        /// <param name="checkpoint">The checkpoint.</param>
        /// <param name="deliveryGuarantee">The delivery guarantee.</param>
        /// <returns>The case.</returns>
        public static CrashMatrixCase Create(string databasePath, SqliteCommitCheckpoint checkpoint, DeliveryGuarantee deliveryGuarantee) =>
            new(
                checkpoint,
                deliveryGuarantee,
                databasePath,
                CreateCrashRecoverySignalPath(databasePath, checkpoint.ToString()),
                OperationId.New(),
                OperationId.New(),
                SubscriptionId.New(),
                Guid.NewGuid(),
                Guid.NewGuid());

        /// <summary>Parses an encoded case.</summary>
        /// <param name="encoded">The encoded case.</param>
        /// <returns>The case.</returns>
        /// <exception cref="InvalidOperationException">The encoded case is malformed.</exception>
        public static CrashMatrixCase Parse(string encoded)
        {
            var fields = encoded.Split('\n');
            if (fields.Length != CrashMatrixCaseFieldCount
                || !Enum.TryParse<SqliteCommitCheckpoint>(fields[0], ignoreCase: false, out var checkpoint)
                || !int.TryParse(fields[1], NumberStyles.None, CultureInfo.InvariantCulture, out var guarantee))
            {
                throw new InvalidOperationException(CrashRecoveryEnvironmentIncompleteMessage);
            }

            return new(
                checkpoint,
                (DeliveryGuarantee)guarantee,
                fields[2],
                fields[3],
                new(ParseField(fields[4])),
                new(ParseField(fields[5])),
                new(ParseField(fields[6])),
                ParseField(fields[7]),
                ParseField(fields[8]));
        }

        /// <summary>Encodes the case for the child environment.</summary>
        /// <returns>The encoded case.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string Encode() =>
            string.Join(
                '\n',
                Checkpoint.ToString(),
                ((int)DeliveryGuarantee).ToString(CultureInfo.InvariantCulture),
                DatabasePath,
                SignalPath,
                OperationId.Value.ToString("D"),
                SecondOperationId.Value.ToString("D"),
                SubscriptionId.Value.ToString("D"),
                EventId.ToString("D"),
                BatchId.ToString("D"));

        /// <summary>Parses one GUID field.</summary>
        /// <param name="value">The field text.</param>
        /// <returns>The GUID.</returns>
        /// <exception cref="InvalidOperationException">The field is malformed.</exception>
        private static Guid ParseField(string value) =>
            Guid.TryParse(value, out var parsed) ? parsed : throw new InvalidOperationException(CrashRecoveryEnvironmentIncompleteMessage);
    }
}
