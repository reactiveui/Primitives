// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests <see cref="SqliteLocalStoreAdapter"/> against the retained store schema v1 golden fixtures.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The retained schema DDL fixture for the frozen store layout.</summary>
    private const string GoldenSchemaFile = "store-schema.sql";

    /// <summary>The retained populated store dump, kept for review and writer comparison.</summary>
    private const string GoldenPopulatedStoreFile = "store-populated.sql";

    /// <summary>The retained populated store database written by the frozen release.</summary>
    private const string GoldenPopulatedDatabaseFile = "store-populated.db";

    /// <summary>The number of characters in a SQL list separator.</summary>
    private const int SqlListSeparatorLength = 2;

    /// <summary>The constant query that reads every row of every frozen store table in a stable order.</summary>
    private const string GoldenRowsQuery = """
        SELECT 'oc_inbox', * FROM oc_inbox ORDER BY rowid;
        SELECT 'oc_metadata', * FROM oc_metadata ORDER BY rowid;
        SELECT 'oc_outbox', * FROM oc_outbox ORDER BY rowid;
        SELECT 'oc_outbox_authoritative_mutations', * FROM oc_outbox_authoritative_mutations ORDER BY rowid;
        SELECT 'oc_outbox_leases', * FROM oc_outbox_leases ORDER BY rowid;
        SELECT 'oc_outbox_metadata', * FROM oc_outbox_metadata ORDER BY rowid;
        SELECT 'oc_outbox_operation_states', * FROM oc_outbox_operation_states ORDER BY rowid;
        SELECT 'oc_outbox_receive_inclusions', * FROM oc_outbox_receive_inclusions ORDER BY rowid;
        SELECT 'oc_payload_quarantine', * FROM oc_payload_quarantine ORDER BY rowid;
        SELECT 'oc_snapshot_authoritative_states', * FROM oc_snapshot_authoritative_states ORDER BY rowid;
        SELECT 'oc_snapshots', * FROM oc_snapshots ORDER BY rowid;
        SELECT 'oc_streams', * FROM oc_streams ORDER BY rowid;
        SELECT 'oc_subscription_identities', * FROM oc_subscription_identities ORDER BY rowid;
        """;

    /// <summary>The column index of the object SQL in the schema query.</summary>
    private const int SchemaSqlColumn = 2;

    /// <summary>The query that reads the SQLite user version.</summary>
    private const string UserVersionQuery = "PRAGMA user_version;";

    /// <summary>The store identity written into the populated fixture.</summary>
    private const string GoldenStoreIdentity = "golden-client";

    /// <summary>The golden subscription identifier.</summary>
    private const string GoldenSubscriptionIdText = "00000000-0000-0000-0000-000000000301";

    /// <summary>The golden remote batch identifier.</summary>
    private const string GoldenRemoteBatchIdText = "00000000-0000-0000-0000-000000000100";

    /// <summary>The golden remote event identifier.</summary>
    private const string GoldenRemoteEventIdText = "00000000-0000-0000-0000-000000000201";

    /// <summary>The golden first operation identifier.</summary>
    private const string GoldenFirstOperationIdText = "00000000-0000-0000-0000-000000000001";

    /// <summary>The golden second operation identifier.</summary>
    private const string GoldenSecondOperationIdText = "00000000-0000-0000-0000-000000000002";

    /// <summary>The golden third operation identifier, written only by the compatibility test.</summary>
    private const string GoldenThirdOperationIdText = "00000000-0000-0000-0000-000000000003";

    /// <summary>The golden remote cursor.</summary>
    private const string GoldenRemoteCursor = "cursor-1";

    /// <summary>The golden JSON content type.</summary>
    private const string GoldenContentType = "application/json";

    /// <summary>The golden reading contract identifier.</summary>
    private const string GoldenReadingContract = "temperature-reading";

    /// <summary>The golden projection contract identifier.</summary>
    private const string GoldenStateContract = "temperature-state";

    /// <summary>The golden snapshot format version.</summary>
    private const int GoldenSnapshotFormatVersion = 1;

    /// <summary>The SQLite user version written by a future unsupported release.</summary>
    private const int GoldenFutureUserVersion = 99;

    /// <summary>The statement that marks a store as written by a future unsupported release.</summary>
    private const string GoldenFutureUserVersionStatement = "PRAGMA user_version = 99;";

    /// <summary>Gets the fixed store clock value.</summary>
    private static DateTimeOffset GoldenStoreTimestamp => new(2026, 9, 13, 0, 0, 10, TimeSpan.Zero);

    /// <summary>Gets the golden stream.</summary>
    private static StreamId GoldenStoreStream => new("sensor/temperature");

    /// <summary>Verifies a freshly initialized database matches the retained schema DDL exactly.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIsInitialized_ThenSchemaMatchesGoldenDdl()
    {
        using var database = TempDatabase.Create();
        await using (var adapter = new SqliteLocalStoreAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(GoldenStoreIdentity, SchemaVersion, false), CancellationToken.None);
        }

        await Assert.That(DescribeGoldenSchema(database.Path)).IsEqualTo(ReadGoldenFixture(GoldenSchemaFile));
    }

    /// <summary>Verifies the current writer produces the retained populated store row for row.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenGoldenStoreIsPopulated_ThenDumpMatchesGoldenScript()
    {
        using var database = TempDatabase.Create();
        await PopulateGoldenStoreAsync(database.Path);

        await Assert.That(DumpGoldenStore(database.Path)).IsEqualTo(ReadGoldenFixture(GoldenPopulatedStoreFile));
    }

    /// <summary>Verifies the retained database and its reviewable dump describe the same rows.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenGoldenDatabaseIsDumped_ThenItMatchesGoldenScript()
    {
        using var database = TempDatabase.Create();
        CopyGoldenDatabase(database.Path);

        await Assert.That(DumpGoldenStore(database.Path)).IsEqualTo(ReadGoldenFixture(GoldenPopulatedStoreFile));
    }

    /// <summary>Verifies a store written by the frozen release opens and recovers with the current code.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The retained store has no snapshot.</exception>
    [Test]
    public async Task WhenGoldenStoreIsOpened_ThenCurrentCodeRecoversIt()
    {
        using var database = TempDatabase.Create();
        CopyGoldenDatabase(database.Path);
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, new() { TimeProvider = new FixedTimeProvider(GoldenStoreTimestamp) });
        await adapter.InitializeAsync(new(GoldenStoreIdentity, SchemaVersion, false), CancellationToken.None);

        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(GoldenStoreStream, null, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(GoldenStoreStream, subscriptionId, CancellationToken.None);
        var snapshot = recovered.Snapshot ?? throw new InvalidOperationException("Expected the retained snapshot.");
        var commit = await adapter.CommitLocalOperationAsync(
            CreateGoldenStoreOperation(GoldenThirdOperationIdText, recovered.NextClientSequence, """{"value":23.4,"unit":"C"}"""u8.ToArray()),
            new(GoldenStoreStream, CreateGoldenState("""{"readings":4,"last":23.4}"""u8.ToArray()), GoldenSnapshotFormatVersion, snapshot.Revision),
            CancellationToken.None);

        await Assert.That(subscriptionId.Value).IsEqualTo(Guid.Parse(GoldenSubscriptionIdText));
        await Assert.That(recovered.ServerCursor).IsEqualTo(GoldenRemoteCursor);
        await Assert.That(snapshot.FormatVersion).IsEqualTo(GoldenSnapshotFormatVersion);
        await Assert.That(snapshot.ServerCursor).IsEqualTo(GoldenRemoteCursor);
        await Assert.That(snapshot.SavedAtUtc).IsEqualTo(GoldenStoreTimestamp);
        await Assert.That(snapshot.State.ContractId).IsEqualTo(GoldenStateContract);
        await Assert.That(Encoding.UTF8.GetString(snapshot.State.Payload.Span)).IsEqualTo("""{"readings":3,"last":22.1}""");
        await Assert.That(snapshot.State.PayloadHash).IsEqualTo(CreateGoldenThirdState().PayloadHash);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(SecondClientSequence);
        await Assert.That(recovered.PendingOperations[0].OperationId.Value).IsEqualTo(Guid.Parse(GoldenFirstOperationIdText));
        await Assert.That(recovered.PendingOperations[1].OperationId.Value).IsEqualTo(Guid.Parse(GoldenSecondOperationIdText));
        await Assert.That(recovered.PendingOperations[1].ClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovered.PendingOperations[1].Metadata[MetadataOriginKey]).IsEqualTo(UnitTestOrigin);
        await Assert.That(recovered.PendingOperations[0].Payload.Payload.Span.SequenceEqual(CreateGoldenSecondReading())).IsTrue();
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence + 1);
        await Assert.That(commit.ClientSequence).IsEqualTo(SecondClientSequence + 1);
    }

    /// <summary>Verifies a newer unsupported store version fails closed and is not overwritten.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreVersionIsFromTheFuture_ThenInitializationFailsClosedWithoutOverwrite()
    {
        using var database = TempDatabase.Create();
        CopyGoldenDatabase(database.Path);
        SetGoldenFutureUserVersion(database.Path);
        var before = DumpGoldenStore(database.Path);

        await using (var adapter = new SqliteLocalStoreAdapter(database.Path))
        {
            Func<Task> initialize = () => adapter.InitializeAsync(new(GoldenStoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();
            await Assert.That(initialize).ThrowsExactly<InvalidOperationException>();
        }

        await Assert.That(ReadUserVersion(database.Path)).IsEqualTo(GoldenFutureUserVersion);
        await Assert.That(DumpGoldenStore(database.Path)).IsEqualTo(before);
    }

    /// <summary>Populates a store with the fixed golden rows through the public adapter.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PopulateGoldenStoreAsync(string path)
    {
        await using var adapter = new SqliteLocalStoreAdapter(path, new() { TimeProvider = new FixedTimeProvider(GoldenStoreTimestamp) });
        await adapter.InitializeAsync(new(GoldenStoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(GoldenStoreStream, new(Guid.Parse(GoldenSubscriptionIdText)), CancellationToken.None);
        RemoteEvent remoteEvent = new(
            Guid.Parse(GoldenRemoteEventIdText),
            GoldenStoreStream,
            GoldenRemoteCursor,
            GoldenStoreTimestamp,
            null,
            new(GoldenReadingContract, 1, GoldenContentType, """{"value":21.3,"unit":"C"}"""u8.ToArray(), "sha256-QMm8O6gF7xuFhW8fMH5aq829cNZL3UygF9155r+sJBw="),
            new Dictionary<string, string>());
        var remote = await adapter.ApplyRemoteBatchAsync(
            new(Guid.Parse(GoldenRemoteBatchIdText), GoldenStoreStream, null, GoldenRemoteCursor, [remoteEvent]),
            new(GoldenStoreStream, CreateGoldenState("""{"readings":1,"last":21.3}"""u8.ToArray()), GoldenSnapshotFormatVersion, 0),
            CancellationToken.None);
        var first = await adapter.CommitLocalOperationAsync(
            CreateGoldenStoreOperation(GoldenFirstOperationIdText, FirstClientSequence, CreateGoldenSecondReading()),
            new(GoldenStoreStream, CreateGoldenState("""{"readings":2,"last":22.1}"""u8.ToArray()), GoldenSnapshotFormatVersion, remote.SnapshotRevision),
            CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(
            CreateGoldenStoreOperation(GoldenSecondOperationIdText, SecondClientSequence, CreateGoldenSecondReading()),
            new(GoldenStoreStream, CreateGoldenThirdState(), GoldenSnapshotFormatVersion, first.SnapshotRevision),
            CancellationToken.None);
    }

    /// <summary>Creates one golden local operation.</summary>
    /// <param name="operationId">The operation identifier text.</param>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateGoldenStoreOperation(string operationId, long clientSequence, byte[] payload) => new()
    {
        OperationId = new(Guid.Parse(operationId)),
        StreamId = GoldenStoreStream,
        ClientSequence = clientSequence,
        TimestampUtc = GoldenStoreTimestamp,
        BaseVersion = GoldenRemoteCursor,
        Type = SyncOperationType.Append,
        Payload = new(GoldenReadingContract, 1, GoldenContentType, payload, ComputeGoldenHash(payload)),
        Metadata = new Dictionary<string, string> { [MetadataOriginKey] = UnitTestOrigin },
    };

    /// <summary>Creates the second golden reading payload bytes.</summary>
    /// <returns>The payload bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] CreateGoldenSecondReading() => """{"value":22.1,"unit":"C"}"""u8.ToArray();

    /// <summary>Creates a golden projection state envelope.</summary>
    /// <param name="payload">The state bytes.</param>
    /// <returns>The state envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreateGoldenState(byte[] payload) => new(GoldenStateContract, 1, GoldenContentType, payload, ComputeGoldenHash(payload));

    /// <summary>Creates the projection state held by the retained store.</summary>
    /// <returns>The state envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreateGoldenThirdState() => CreateGoldenState("""{"readings":3,"last":22.1}"""u8.ToArray());

    /// <summary>Computes the SHA-256 payload hash in the serializer's text form.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The hash text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string ComputeGoldenHash(byte[] payload) =>
        $"sha256-{Convert.ToBase64String(System.Security.Cryptography.SHA256.HashData(payload))}";

    /// <summary>Reads a retained fixture with line endings normalized and the final line break removed.</summary>
    /// <param name="name">The fixture name.</param>
    /// <returns>The fixture text.</returns>
    private static string ReadGoldenFixture(string name)
    {
        var text = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "GoldenFixtures", "protocol-v1", name), Encoding.UTF8);
        return text.Replace("\r\n", "\n", StringComparison.Ordinal).TrimEnd('\n');
    }

    /// <summary>Describes the schema objects, user version, and schema metadata of a database.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The normalized schema description.</returns>
    private static string DescribeGoldenSchema(string path)
    {
        using var connection = OpenRawConnection(path);
        StringBuilder builder = new();
        _ = builder.Append("-- user_version: ").Append(ReadGoldenUserVersion(connection)).Append('\n');
        _ = builder.Append("-- schema_version: ")
            .Append(ReadGoldenSchemaVersion(connection))
            .Append('\n');
        AppendGoldenSchemaObjects(connection, builder);
        return builder.ToString().TrimEnd('\n');
    }

    /// <summary>Dumps a database as a replayable SQL script with deterministic row order.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The SQL script.</returns>
    private static string DumpGoldenStore(string path)
    {
        using var connection = OpenRawConnection(path);
        StringBuilder builder = new();
        _ = builder.Append("-- Store written by the frozen release. Replay it into an empty database file.\n");
        AppendGoldenSchemaObjects(connection, builder);
        AppendGoldenRows(connection, builder);

        _ = builder.Append("PRAGMA user_version = ").Append(ReadGoldenUserVersion(connection)).Append(';').Append('\n');
        return builder.ToString().TrimEnd('\n');
    }

    /// <summary>Appends every schema object in a stable order.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="builder">The destination.</param>
    private static void AppendGoldenSchemaObjects(SqliteConnection connection, StringBuilder builder)
    {
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT type, name, sql FROM sqlite_master
            WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
            ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, name;
            """;
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var sql = reader.GetString(SchemaSqlColumn).Replace("\r\n", "\n", StringComparison.Ordinal).Trim();
            _ = builder.Append("-- ").Append(reader.GetString(0)).Append(' ').Append(reader.GetString(1)).Append('\n');
            _ = builder.Append(sql).Append(';').Append('\n');
        }
    }

    /// <summary>Appends every table row as an insert statement.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="builder">The destination.</param>
    private static void AppendGoldenRows(SqliteConnection connection, StringBuilder builder)
    {
        using var command = connection.CreateCommand();
        command.CommandText = GoldenRowsQuery;
        using var reader = command.ExecuteReader();
        do
        {
            while (reader.Read())
            {
                _ = builder.Append("INSERT INTO ").Append(reader.GetString(0)).Append(" VALUES (");
                for (var index = 1; index < reader.FieldCount; index++)
                {
                    _ = builder.Append(FormatGoldenValue(reader.GetValue(index))).Append(", ");
                }

                builder.Length -= SqlListSeparatorLength;
                _ = builder.Append(')').Append(';').Append('\n');
            }
        }
        while (reader.NextResult());
    }

    /// <summary>Formats one SQLite value as a SQL literal.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The literal.</returns>
    /// <exception cref="InvalidOperationException">The value has an unexpected storage class.</exception>
    private static string FormatGoldenValue(object value) => value switch
    {
        DBNull => "NULL",
        long number => number.ToString(CultureInfo.InvariantCulture),
        double real => real.ToString("R", CultureInfo.InvariantCulture),
        string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
        byte[] blob => $"X'{Convert.ToHexString(blob)}'",
        _ => throw new InvalidOperationException("Unexpected SQLite storage class."),
    };

    /// <summary>Reads the SQLite user version as invariant text.</summary>
    /// <param name="connection">The open connection.</param>
    /// <returns>The user version text.</returns>
    private static string ReadGoldenUserVersion(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = UserVersionQuery;
        return Convert.ToString(command.ExecuteScalar(), CultureInfo.InvariantCulture) ?? string.Empty;
    }

    /// <summary>Reads the store schema version metadata.</summary>
    /// <param name="connection">The open connection.</param>
    /// <returns>The schema version text.</returns>
    private static string ReadGoldenSchemaVersion(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM oc_metadata WHERE key = 'schema_version';";
        return Convert.ToString(command.ExecuteScalar(), CultureInfo.InvariantCulture) ?? string.Empty;
    }

    /// <summary>Copies the retained database into a test database path.</summary>
    /// <param name="path">The destination database path.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopyGoldenDatabase(string path) =>
        File.Copy(Path.Combine(AppContext.BaseDirectory, "GoldenFixtures", "protocol-v1", GoldenPopulatedDatabaseFile), path);

    /// <summary>Marks a database file as written by a future unsupported release.</summary>
    /// <param name="path">The database path.</param>
    private static void SetGoldenFutureUserVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = GoldenFutureUserVersionStatement;
        _ = command.ExecuteNonQuery();
    }
}
