// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Shared helpers for <see cref="SqliteLocalCommitStoreTests"/>.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>Creates an initialized store instance.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The initialized store.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SqliteLocalCommitStore CreateInitializedStore(string path) => CreateInitializedStore(path, StoreIdentity);

    /// <summary>Creates an initialized local commit store for a specific partition.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The initialized store.</returns>
    private static SqliteLocalCommitStore CreateInitializedStore(string path, string storeIdentity)
    {
        var store = new SqliteLocalCommitStore(path);
        store.Initialize(new(storeIdentity, SchemaVersion, false), CancellationToken.None);
        return store;
    }

    /// <summary>Creates an initialized local commit store for a specific clock.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="timeProvider">The clock used by the store.</param>
    /// <returns>The initialized store.</returns>
    private static SqliteLocalCommitStore CreateInitializedStore(string path, TimeProvider timeProvider)
    {
        var store = new SqliteLocalCommitStore(path, timeProvider);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        return store;
    }

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
        Payload = CreatePayload(OperationPayloadText),
        Policy = new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, Priority: 1, ConflictPolicy.Merge),
        Metadata = new Dictionary<string, string> { [MetadataOriginKey] = UnitTestOrigin },
    };

    /// <summary>Creates a representative snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <returns>The mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(long expectedRevision) => new(Stream, CreatePayload(SnapshotPayloadText), FormatVersion: 1, expectedRevision);

    /// <summary>Creates a representative payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreatePayload(string text) => new("reading", 1, "application/json", System.Text.Encoding.UTF8.GetBytes(text), $"hash-{text}");

    /// <summary>Attempts to commit and captures success or failure.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="snapshot">The snapshot mutation.</param>
    /// <returns>The attempt.</returns>
    private static CommitAttempt TryCommit(SqliteLocalCommitStore store, SyncOperation operation, SnapshotMutation snapshot)
    {
        try
        {
            return new(store.CommitLocalOperation(operation, snapshot, CancellationToken.None), null);
        }
        catch (Exception exception)
        {
            return new(null, exception);
        }
    }

    /// <summary>Records settings from the actual store connections while their writes execute.</summary>
    /// <param name="path">The database path.</param>
    private static void InstallConnectionSettingsProbes(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER probe_identity_settings AFTER INSERT ON oc_subscription_identities
            BEGIN
                INSERT OR REPLACE INTO oc_metadata (key, value)
                SELECT 'probe_identity', CAST(foreign_keys AS TEXT) || ':' || CAST(synchronous AS TEXT)
                FROM pragma_foreign_keys, pragma_synchronous;
            END;
            CREATE TRIGGER probe_commit_settings AFTER INSERT ON oc_outbox
            BEGIN
                INSERT OR REPLACE INTO oc_metadata (key, value)
                SELECT 'probe_commit', CAST(foreign_keys AS TEXT) || ':' || CAST(synchronous AS TEXT)
                FROM pragma_foreign_keys, pragma_synchronous;
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Reads settings captured during an actual store write.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="key">The probe metadata key.</param>
    /// <returns>The foreign key and synchronous values captured by the probe.</returns>
    /// <exception cref="InvalidOperationException">The store write did not record its connection settings.</exception>
    private static string ReadConnectionSettingsProbe(string path, string key)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value FROM oc_metadata WHERE key = $key;";
        _ = command.Parameters.AddWithValue("$key", key);
        return command.ExecuteScalar() is string settings ? settings : throw new InvalidOperationException("The store connection probe did not execute.");
    }

    /// <summary>Reads the SQLite user version.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The user version.</returns>
    /// <exception cref="InvalidOperationException">The user version could not be read.</exception>
    private static long ReadUserVersion(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA user_version;";
        return command.ExecuteScalar() is long version ? version : throw new InvalidOperationException("The user version could not be read.");
    }

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

    /// <summary>Inserts schema version two local commit rows for migration tests.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="operation">The committed operation.</param>
    /// <param name="snapshot">The committed snapshot mutation.</param>
    private static void InsertLegacyLocalCommitRows(
        SqliteConnection connection,
        SqliteTransaction transaction,
        SubscriptionId subscriptionId,
        SyncOperation operation,
        SnapshotMutation snapshot)
    {
        SqliteSubscriptionIdentitySql.InsertSubscriptionIdentityIfMissing(connection, transaction, StoreIdentity, Stream, subscriptionId);
        SqliteLocalCommitSql.EnsureStreamRow(connection, transaction, StoreIdentity, Stream, subscriptionId);
        SqliteLocalCommitSql.InsertOutboxOperation(
            connection,
            transaction,
            StoreIdentity,
            operation,
            snapshot.ExpectedRevision + 1,
            SqliteCommitFingerprint.Compute(operation, snapshot),
            operation.TimestampUtc);
        SqliteLocalCommitSql.InsertOperationMetadata(connection, transaction, StoreIdentity, operation);
        SqliteLocalCommitSql.UpsertSnapshot(connection, transaction, StoreIdentity, snapshot, snapshot.ExpectedRevision + 1, null, operation.TimestampUtc);
        SqliteLocalCommitSql.UpdateNextClientSequence(connection, transaction, StoreIdentity, Stream, operation.ClientSequence + 1);
    }

    /// <summary>Sets the persisted lifecycle state for a historical fixture operation.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The lifecycle state.</param>
    private static void SetOperationState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        OperationId operationId,
        SyncOperationState state)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE oc_outbox_operation_states
            SET operation_state = $operationState
            WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue("$operationState", (int)state);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue("$operationId", operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that aborts commits after outbox insertion.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_outbox_commit_abort
            AFTER INSERT ON oc_outbox
            BEGIN
                SELECT RAISE(ABORT, 'rollback outbox insert');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that aborts remote apply after inbox insertion.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateRemoteApplyRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_inbox_commit_abort
            AFTER INSERT ON oc_inbox
            BEGIN
                SELECT RAISE(ABORT, 'rollback inbox insert');
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the remote apply rollback trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropRemoteApplyRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_inbox_commit_abort;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Inserts a remote inbox event directly.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="remoteEvent">The remote event.</param>
    private static void InsertInboxEvent(string path, RemoteEvent remoteEvent)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO oc_inbox
                (store_identity, stream_id, event_id, server_cursor, committed_at_utc)
            VALUES
                ($storeIdentity, $streamId, $eventId, $serverCursor, $committedAtUtc);
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, remoteEvent.StreamId.Value);
        _ = command.Parameters.AddWithValue("$eventId", remoteEvent.EventId.ToString("D"));
        _ = command.Parameters.AddWithValue(ServerCursorParameter, remoteEvent.ServerCursor);
        _ = command.Parameters.AddWithValue("$committedAtUtc", SqliteLocalCommitSql.FormatDateTimeOffset(remoteEvent.CommittedAtUtc));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Inserts a malformed remote inbox event directly.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="eventId">The remote event identifier.</param>
    private static void InsertMalformedInboxEvent(string path, Guid eventId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO oc_inbox
                (store_identity, stream_id, event_id, server_cursor, committed_at_utc)
            VALUES
                ($storeIdentity, $streamId, $eventId, $serverCursor, 'not-a-date');
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.Parameters.AddWithValue("$eventId", eventId.ToString("D"));
        _ = command.Parameters.AddWithValue(ServerCursorParameter, FirstRemoteCursor);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the commit rollback trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_outbox_commit_abort;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that aborts schema migration after schema two tables are created.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The connection used to create the trigger.</returns>
    private static SqliteConnection CreateMigrationRollbackTrigger(string path)
    {
        var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_metadata_migration_abort
            BEFORE UPDATE OF value ON oc_metadata
            WHEN OLD.key = 'schema_version'
            BEGIN
                SELECT RAISE(ABORT, 'rollback schema migration');
            END;
            """;
        _ = command.ExecuteNonQuery();
        return connection;
    }

    /// <summary>Deletes local stream rows to simulate an interrupted schema backfill.</summary>
    /// <param name="path">The database path.</param>
    private static void DeleteStreams(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM oc_streams;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Deletes the stream row without cascading dependent rows.</summary>
    /// <param name="path">The database path.</param>
    private static void DeleteStreamWithoutCascade(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA foreign_keys = OFF;
            DELETE FROM oc_streams WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the stream next client sequence directly.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="nextClientSequence">The next client sequence.</param>
    private static void SetStreamNextClientSequence(string path, long nextClientSequence)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_streams
            SET next_client_sequence = $nextClientSequence
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$nextClientSequence", nextClientSequence);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the stored snapshot server cursor directly.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="serverCursor">The server cursor.</param>
    private static void SetSnapshotServerCursor(string path, string serverCursor)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET server_cursor = $serverCursor
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(ServerCursorParameter, serverCursor);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates or updates a snapshot with the supplied revision.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="revision">The snapshot revision.</param>
    private static void SetSnapshotRevision(string path, long revision)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        var payload = CreatePayload(SnapshotPayloadText);
        command.CommandText = """
            INSERT INTO oc_snapshots
                (store_identity, stream_id, format_version, server_cursor, payload_contract_id, payload_schema_version,
                 payload_content_type, payload, payload_hash, revision, saved_at_utc)
            VALUES
                ($storeIdentity, $streamId, 1, NULL, $payloadContractId, $payloadSchemaVersion,
                 $payloadContentType, $payload, $payloadHash, $revision, '2026-01-02T03:04:05.0000000+00:00')
            ON CONFLICT (store_identity, stream_id) DO UPDATE SET revision = excluded.revision;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        _ = command.Parameters.AddWithValue("$payloadContractId", payload.ContractId);
        _ = command.Parameters.AddWithValue("$payloadSchemaVersion", payload.SchemaVersion);
        _ = command.Parameters.AddWithValue("$payloadContentType", payload.ContentType);
        _ = command.Parameters.Add("$payload", SqliteType.Blob);
        command.Parameters["$payload"].Value = payload.Payload.ToArray();
        _ = command.Parameters.AddWithValue("$payloadHash", payload.PayloadHash);
        _ = command.Parameters.AddWithValue("$revision", revision);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Marks the snapshot timestamp malformed.</summary>
    /// <param name="path">The database path.</param>
    private static void SetSnapshotSavedAtMalformed(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_snapshots SET saved_at_utc = 'not-a-date';";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Restores the snapshot timestamp to a valid ISO value.</summary>
    /// <param name="path">The database path.</param>
    private static void SetSnapshotSavedAtValid(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_snapshots SET saved_at_utc = '2026-01-02T03:04:05.0000000+00:00';";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Marks the outbox operation id malformed.</summary>
    /// <param name="path">The database path.</param>
    private static void SetOutboxOperationIdMalformed(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET operation_id = 'not-a-guid';";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Restores the outbox operation id.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    private static void SetOutboxOperationId(string path, OperationId operationId)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET operation_id = $operationId;";
        _ = command.Parameters.AddWithValue("$operationId", operationId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the outbox client sequence to zero.</summary>
    /// <param name="path">The database path.</param>
    private static void SetOutboxClientSequenceZero(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET client_sequence = 0;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the outbox client sequence.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="clientSequence">The client sequence.</param>
    private static void SetOutboxClientSequence(string path, long clientSequence)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET client_sequence = $clientSequence;";
        _ = command.Parameters.AddWithValue("$clientSequence", clientSequence);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the outbox operation type to an invalid enum value.</summary>
    /// <param name="path">The database path.</param>
    private static void SetOutboxOperationTypeInvalid(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET operation_type = 2147483647;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the outbox operation type.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="operationType">The operation type.</param>
    private static void SetOutboxOperationType(string path, SyncOperationType operationType)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET operation_type = $operationType;";
        _ = command.Parameters.AddWithValue("$operationType", (int)operationType);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the outbox snapshot revision to a corrupt negative value.</summary>
    /// <param name="path">The database path.</param>
    private static void SetOutboxSnapshotRevisionNegative(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET snapshot_revision = -1;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets the stored commit fingerprint to an invalid value.</summary>
    /// <param name="path">The database path.</param>
    private static void SetOutboxCommitFingerprintMalformed(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE oc_outbox SET commit_fingerprint = X'00';";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a trigger that removes the stream row during sequence update.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateSequenceUpdateRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TRIGGER oc_stream_update_abort
            BEFORE UPDATE OF next_client_sequence ON oc_streams
            BEGIN
                DELETE FROM oc_streams WHERE store_identity = NEW.store_identity AND stream_id = NEW.stream_id;
                SELECT RAISE(IGNORE);
            END;
            """;
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Drops the sequence update rollback trigger.</summary>
    /// <param name="path">The database path.</param>
    private static void DropSequenceUpdateRollbackTrigger(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "DROP TRIGGER oc_stream_update_abort;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates an unexpected user table.</summary>
    /// <param name="path">The database path.</param>
    private static void CreateUnexpectedTable(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE unexpected_table (id INTEGER NOT NULL);";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates one supported historical local commit schema.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <exception cref="ArgumentOutOfRangeException">The schema version is not a supported historical version.</exception>
    private static void CreateHistoricalLocalCommitSchema(string path, int schemaVersion)
    {
        using var connection = OpenRawConnection(path);
        using var transaction = connection.BeginTransaction();
        switch (schemaVersion)
        {
            case SqliteStoreSchema.IdentitySchemaVersion:
            {
                SqliteStoreSchema.CreateIdentitySchema(connection, transaction);
                break;
            }

            case SqliteStoreSchema.LegacyLocalCommitSchemaVersion:
            {
                SqliteStoreSchemaTests.CreateLegacyLocalCommitSchema(connection, transaction);
                break;
            }

            case SqliteStoreSchema.RemoteApplySchemaVersion:
            {
                SqliteStoreSchemaTests.CreateRemoteApplySchema(connection, transaction);
                break;
            }

            case SqliteStoreSchema.LeaseSchemaVersion:
            {
                SqliteStoreSchemaTests.CreateLeaseSchema(connection, transaction);
                break;
            }

            case SqliteStoreSchema.PreAuthoritativeLocalCommitSchemaVersion:
            {
                CreatePreAuthoritativeLocalCommitSchema(connection, transaction);
                break;
            }

            case SqliteStoreSchema.AuthoritativeLocalCommitSchemaVersion:
            {
                SchemaSixFixture.Create(connection, transaction);
                break;
            }

            default:
            {
                throw new ArgumentOutOfRangeException(nameof(schemaVersion), schemaVersion, "The schema version is not supported by this fixture.");
            }
        }

        transaction.Commit();
    }

    /// <summary>Sets the user version to a newer unsupported schema value.</summary>
    /// <param name="path">The database path.</param>
    private static void SetUserVersionToNewer(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA user_version = 9;";
        _ = command.ExecuteNonQuery();
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

        /// <summary>Gets the temporary directory path.</summary>
        public string DirectoryPath => _directory;

        /// <summary>Creates a new temporary database helper.</summary>
        /// <returns>The temporary database helper.</returns>
        public static TempDatabase Create()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rxui-oc-sqlite-local", Guid.NewGuid().ToString("N"));
            _ = System.IO.Directory.CreateDirectory(directory);
            return new(directory);
        }

        /// <summary>Creates a temporary database helper without creating the directory.</summary>
        /// <returns>The temporary database helper.</returns>
        public static TempDatabase CreateWithoutDirectory()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rxui-oc-sqlite-local", Guid.NewGuid().ToString("N"));
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

    /// <summary>The result of one commit attempt.</summary>
    /// <param name="Result">The committed result.</param>
    /// <param name="Exception">The thrown exception.</param>
    private sealed record CommitAttempt(LocalCommitResult? Result, Exception? Exception);
}
