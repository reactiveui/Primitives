// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests SQLite storage failures through <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Disk capacity failures through the public adapter.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The payload size that exceeds the initialized database page limit.</summary>
    private const int StorageFullPayloadLength = 131_072;

    /// <summary>The worker capacity for large SQLite payloads.</summary>
    private const long StorageFullWorkerCapacityBytes = 1_048_576;

    /// <summary>The SQLite page size used by the controlled database.</summary>
    private const int StorageFullPageSize = 512;

    /// <summary>The SQLite page limit used to force a bounded on-disk database.</summary>
    private const long StorageFullMaximumPageCount = 256;

    /// <summary>Verifies a page limit failure rolls back a local commit through the public adapter.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabasePageLimitIsReached_ThenLocalCommitRollsBackAndReportsStorageFull()
    {
        using var database = TempDatabase.Create();
        await SetStorageFullPageSizeAsync(database.Path);

        SubscriptionId subscriptionId;
        OperationId operationId;
        await using (var initialized = CreateAdapter(database.Path))
        {
            await initialized.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
            subscriptionId = await initialized.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        }

        var limit = new PageLimitFaultPoint();
        await using (var adapter = new SqliteLocalStoreAdapter(
            database.Path,
            new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = StorageFullWorkerCapacityBytes },
            limit))
        {
            await adapter.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
            var operation = CreateOperation(FirstClientSequence) with
            {
                Payload = CreatePayload(new('x', StorageFullPayloadLength)),
            };
            operationId = operation.OperationId;
            var mutation = CreateSnapshotMutation(expectedRevision: 0) with
            {
                State = CreatePayload(new('s', StorageFullPayloadLength)),
            };
            DurableStorageException? storageFailure = null;
            try
            {
                _ = await adapter.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
            }
            catch (DurableStorageException exception)
            {
                storageFailure = exception;
            }

            await Assert.That(limit.PageSize).IsEqualTo(StorageFullPageSize);
            await Assert.That(limit.PageCount).IsLessThan(StorageFullMaximumPageCount);
            await Assert.That(limit.PageLimit).IsEqualTo(StorageFullMaximumPageCount);
            await Assert.That(storageFailure).IsNotNull();
            await Assert.That(storageFailure!.Failure).IsEqualTo(DurableStorageFailure.StorageFull);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var operationStatus = await reopened.GetOperationStatusAsync(operationId, CancellationToken.None);

        await Assert.That(limit.PageLimit).IsEqualTo(StorageFullMaximumPageCount);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(recovery.Snapshot).IsNull();
        await Assert.That(operationStatus).IsNull();
    }

    /// <summary>Sets the SQLite page size before the database creates its first table.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>A task that represents the asynchronous setup.</returns>
    private static async Task SetStorageFullPageSizeAsync(string path)
    {
        using var connection = OpenRawConnection(path);
        using var setPageSize = connection.CreateStatement();
        setPageSize.SetSql("PRAGMA page_size = 512;");
        _ = setPageSize.Execute();
        using var persistPageSize = connection.CreateStatement();
        persistPageSize.SetSql("VACUUM;");
        _ = persistPageSize.Execute();
        using var readPageSize = connection.CreateStatement();
        readPageSize.SetSql("PRAGMA page_size;");
        var pageSize = Convert.ToInt64(readPageSize.Scalar(), System.Globalization.CultureInfo.InvariantCulture);
        await Assert.That(pageSize).IsEqualTo(StorageFullPageSize);
    }

    /// <summary>Sets SQLite's page limit on the same connection that performs the local commit.</summary>
    private sealed class PageLimitFaultPoint : ISqliteCommitFaultPoint
    {
        /// <summary>Gets the page size observed on the commit connection.</summary>
        public long PageSize { get; private set; }

        /// <summary>Gets the page count observed on the commit connection.</summary>
        public long PageCount { get; private set; }

        /// <summary>Gets the page limit applied to the commit connection.</summary>
        public long PageLimit { get; private set; }

        /// <inheritdoc/>
        public void BeforeLocalCommitTransaction(SqliteDatabase connection)
        {
            using var pageSizeCommand = connection.CreateStatement();
            pageSizeCommand.SetSql("PRAGMA page_size;");
            PageSize = Convert.ToInt64(pageSizeCommand.Scalar(), System.Globalization.CultureInfo.InvariantCulture);

            using var pageCountCommand = connection.CreateStatement();
            pageCountCommand.SetSql("PRAGMA page_count;");
            PageCount = Convert.ToInt64(pageCountCommand.Scalar(), System.Globalization.CultureInfo.InvariantCulture);

            using var limitCommand = connection.CreateStatement();
            limitCommand.SetSql("PRAGMA max_page_count = 256;");
            PageLimit = Convert.ToInt64(limitCommand.Scalar(), System.Globalization.CultureInfo.InvariantCulture);
            if (PageLimit != StorageFullMaximumPageCount || PageCount >= StorageFullMaximumPageCount)
            {
                throw new InvalidOperationException("The SQLite schema does not fit below the controlled page limit.");
            }
        }

        /// <inheritdoc/>
        public void Reached(SqliteCommitCheckpoint checkpoint)
        {
        }
    }
}
