// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>SQL metacharacter round-trip tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>A store identity made of SQL metacharacters.</summary>
    private const string SqlStoreIdentity = "client'); DROP TABLE oc_outbox; --";

    /// <summary>A contract identifier made of SQL metacharacters.</summary>
    private const string SqlContract = "reading' OR '1'='1";

    /// <summary>A base version made of SQL metacharacters.</summary>
    private const string SqlBaseVersion = "v1\"; DELETE FROM oc_snapshots; /*";

    /// <summary>A metadata key made of SQL metacharacters.</summary>
    private const string SqlMetadataKey = "key%_[]";

    /// <summary>A metadata value made of SQL metacharacters.</summary>
    private const string SqlMetadataValue = "value'); DROP TABLE oc_inbox; --";

    /// <summary>A remote cursor made of SQL metacharacters.</summary>
    private const string SqlRemoteCursor = "cursor' UNION SELECT * FROM oc_outbox; --";

    /// <summary>Verifies SQL metacharacters in identities, payload fields, metadata, and cursors round-trip as data.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenFieldsContainSqlMetacharacters_ThenReopenRecoversThemUnchanged()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence) with
        {
            BaseVersion = SqlBaseVersion,
            Payload = new(SqlContract, 1, "application/json", "'; DROP TABLE oc_outbox; --"u8.ToArray(), "hash-'--"),
            Metadata = new Dictionary<string, string> { [SqlMetadataKey] = SqlMetadataValue },
        };
        var remoteEvent = new RemoteEvent(
            Guid.NewGuid(),
            Stream,
            SqlRemoteCursor,
            DateTimeOffset.UnixEpoch,
            null,
            CreatePayload("remote"),
            new Dictionary<string, string> { [SqlMetadataKey] = SqlMetadataValue });
        SubscriptionId subscriptionId;
        IReadOnlyList<string> tablesBefore;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(SqlStoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            var remoteApply = await adapter.ApplyRemoteBatchAsync(
                CreateRemoteBatch(null, SqlRemoteCursor, [remoteEvent]),
                CreateSnapshotMutation(expectedRevision: 0),
                CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(remoteApply.SnapshotRevision), CancellationToken.None);
            tablesBefore = ReadTableNames(database.Path);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(SqlStoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await reopened.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);

        await Assert.That(recovery.ServerCursor).IsEqualTo(SqlRemoteCursor);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        var recovered = recovery.PendingOperations[0];
        await Assert.That(recovered.OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovered.BaseVersion).IsEqualTo(SqlBaseVersion);
        await Assert.That(recovered.Payload.ContractId).IsEqualTo(SqlContract);
        await Assert.That(recovered.Payload.PayloadHash).IsEqualTo(operation.Payload.PayloadHash);
        await Assert.That(recovered.Payload.Payload.ToArray().SequenceEqual(operation.Payload.Payload.ToArray())).IsTrue();
        await Assert.That(recovered.Metadata[SqlMetadataKey]).IsEqualTo(SqlMetadataValue);
        await Assert.That(unapplied.Count).IsEqualTo(0);
        await Assert.That(string.Join(",", ReadTableNames(database.Path))).IsEqualTo(string.Join(",", tablesBefore));
    }

    /// <summary>Reads the table names in a SQLite database.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The sorted table names.</returns>
    private static List<string> ReadTableNames(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name;";
        using var reader = command.ExecuteReader();
        var names = new List<string>();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }
}
