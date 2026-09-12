// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteSubscriptionIdentityStore"/>.</summary>
public sealed class SqliteSubscriptionIdentityStoreTests
{
    /// <summary>The supported store schema version.</summary>
    private const int SchemaVersion = 1;

    /// <summary>The primary store identity used by tests.</summary>
    private const string StoreIdentity = "client-alpha";

    /// <summary>The secondary store identity used by partitioning tests.</summary>
    private const string SecondaryStoreIdentity = "client-beta";

    /// <summary>The SQL statement used to stamp schema version one.</summary>
    private const string SetUserVersionSql = "PRAGMA user_version = 1;";

    /// <summary>The expected row count when two different streams share one explicit subscription identifier.</summary>
    private const int TwoRows = 2;

    /// <summary>A representative stream identity.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>The delay that allows a lookup task to reach the SQLite writer lock.</summary>
    private static readonly TimeSpan WriterBlockDelay = TimeSpan.FromMilliseconds(250);

    /// <summary>Verifies a preferred subscription survives closing and reopening the database.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExplicitIdentityIsCreatedAndStoreReopens_ThenOmittedLookupReusesIt()
    {
        using var database = TempDatabase.Create();
        var preferred = SubscriptionId.New();

        using (var first = CreateInitializedStore(database.Path))
        {
            var created = first.GetOrCreateSubscriptionId(Stream, preferred, CancellationToken.None);

            await Assert.That(created).IsEqualTo(preferred);
        }

        using var second = CreateInitializedStore(database.Path);
        var recovered = second.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);

        await Assert.That(recovered).IsEqualTo(preferred);
    }

    /// <summary>Verifies omitted concurrent lookups from separate instances return one committed identity.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenTwoInstancesCreateTheSameStreamConcurrently_ThenBothReturnTheCommittedIdentity()
    {
        using var database = TempDatabase.Create();
        using var first = CreateInitializedStore(database.Path);
        using var second = CreateInitializedStore(database.Path);

        var firstTask = Task.Run(() => first.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None));
        var secondTask = Task.Run(() => second.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None));
        var identities = await Task.WhenAll(firstTask, secondTask);

        await Assert.That(identities[0].Value).IsNotEqualTo(Guid.Empty);
        await Assert.That(identities[1]).IsEqualTo(identities[0]);
    }

    /// <summary>Verifies the same explicit subscription can identify different streams in one store partition.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExplicitIdentityIsUsedForDifferentStreams_ThenEachStreamCanStoreIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var explicitId = SubscriptionId.New();
        var secondStream = new StreamId("sensor/humidity");

        var first = store.GetOrCreateSubscriptionId(Stream, explicitId, CancellationToken.None);
        var second = store.GetOrCreateSubscriptionId(secondStream, explicitId, CancellationToken.None);

        await Assert.That(first).IsEqualTo(explicitId);
        await Assert.That(second).IsEqualTo(explicitId);
        await Assert.That(CountSubscriptionRows(database.Path)).IsEqualTo(TwoRows);
    }

    /// <summary>Verifies competing explicit first writes converge on the winner and reject the loser after reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDifferentExplicitIdentitiesRaceForANewStream_ThenWinnerIsStableAndLoserIsRejected()
    {
        using var database = TempDatabase.Create();
        using var first = CreateInitializedStore(database.Path);
        using var second = CreateInitializedStore(database.Path);
        var firstPreferred = SubscriptionId.New();
        var secondPreferred = SubscriptionId.New();

        var firstTask = Task.Run(() => TryGetOrCreate(first, Stream, firstPreferred));
        var secondTask = Task.Run(() => TryGetOrCreate(second, Stream, secondPreferred));
        var results = await Task.WhenAll(firstTask, secondTask);

        var winner = results.Single(static result => result.Identity.HasValue).Identity.GetValueOrDefault();
        var loser = results.Single(static result => result.Exception is not null).Preferred;

        await Assert.That(results.Count(static result => result.Identity.HasValue)).IsEqualTo(1);
        await Assert.That(results.Count(static result => result.Exception is InvalidOperationException)).IsEqualTo(1);

        using var reopened = CreateInitializedStore(database.Path);
        await Assert.That(reopened.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(winner);
        await Assert.That(() => reopened.GetOrCreateSubscriptionId(Stream, loser, CancellationToken.None)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies an explicit mismatch leaves the stored mapping unchanged.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExistingMappingDiffersFromExplicitIdentity_ThenThrowsAndKeepsOriginalValue()
    {
        using var database = TempDatabase.Create();
        var original = SubscriptionId.New();
        var mismatched = SubscriptionId.New();
        using var store = CreateInitializedStore(database.Path);

        _ = store.GetOrCreateSubscriptionId(Stream, original, CancellationToken.None);
        Action action = () => store.GetOrCreateSubscriptionId(Stream, mismatched, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(original);
    }

    /// <summary>Verifies different store identities are independent partitions in the same database.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDifferentStoreIdentitiesUseSameDatabase_ThenStreamMappingsArePartitioned()
    {
        using var database = TempDatabase.Create();
        var alpha = SubscriptionId.New();
        var beta = SubscriptionId.New();

        using (var first = CreateInitializedStore(database.Path))
        {
            _ = first.GetOrCreateSubscriptionId(Stream, alpha, CancellationToken.None);
        }

        using (var second = new SqliteSubscriptionIdentityStore(database.Path))
        {
            second.Initialize(new(SecondaryStoreIdentity, SchemaVersion, false), CancellationToken.None);
            _ = second.GetOrCreateSubscriptionId(Stream, beta, CancellationToken.None);
        }

        using var alphaStore = CreateInitializedStore(database.Path);
        using var betaStore = new SqliteSubscriptionIdentityStore(database.Path);
        betaStore.Initialize(new(SecondaryStoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(alphaStore.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(alpha);
        await Assert.That(betaStore.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(beta);
    }

    /// <summary>Verifies repeated initialization is stable for one identity and cannot switch the instance partition.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInitializedInstanceIsReinitialized_ThenSameIdentitySucceedsAndDifferentIdentityIsRejected()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var alpha = SubscriptionId.New();
        var beta = SubscriptionId.New();

        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = store.GetOrCreateSubscriptionId(Stream, alpha, CancellationToken.None);
        Action switchPartition = () => store.Initialize(new(SecondaryStoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(switchPartition).ThrowsExactly<InvalidOperationException>();

        using (var betaStore = new SqliteSubscriptionIdentityStore(database.Path))
        {
            betaStore.Initialize(new(SecondaryStoreIdentity, SchemaVersion, false), CancellationToken.None);
            _ = betaStore.GetOrCreateSubscriptionId(Stream, beta, CancellationToken.None);
        }

        await Assert.That(store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(alpha);
    }

    /// <summary>Verifies a wrong schema version is rejected without migrating or writing mappings.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSchemaVersionIsWrong_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = "PRAGMA user_version = 2;";
            _ = command.ExecuteNonQuery();
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountSubscriptionRows(database.Path)).IsEqualTo(0);
    }

    /// <summary>Verifies an unsupported requested schema version is rejected before creating a database file.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRequestedSchemaVersionIsUnsupported_ThenInitializeFailsBeforeFileCreation()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteSubscriptionIdentityStore(database.Path);

        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion + 1, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies an unversioned database with user tables is rejected as an unknown schema.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExistingDatabaseHasUserTablesWithoutVersion_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE unexpected_identity_table (value TEXT NOT NULL);";
            _ = command.ExecuteNonQuery();
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(CountSubscriptionRows(database.Path)).IsEqualTo(0);
    }

    /// <summary>Verifies malformed schema objects are rejected instead of being repaired silently.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMetadataSchemaIsCorrupt_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using (var versionCommand = connection.CreateCommand())
            {
                versionCommand.CommandText = SetUserVersionSql;
                _ = versionCommand.ExecuteNonQuery();
            }

            await using var metadataCommand = connection.CreateCommand();
            metadataCommand.CommandText = "CREATE TABLE oc_metadata (name TEXT NOT NULL PRIMARY KEY);";
            _ = metadataCommand.ExecuteNonQuery();
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a versioned database without the metadata table is rejected as malformed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMetadataTableIsMissing_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = SetUserVersionSql;
            _ = command.ExecuteNonQuery();
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies schema validation wraps SQLite errors after metadata tampering.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMetadataDefinitionIsTampered_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using (var versionCommand = connection.CreateCommand())
            {
                versionCommand.CommandText = SetUserVersionSql;
                _ = versionCommand.ExecuteNonQuery();
            }

            await using (var metadataCommand = connection.CreateCommand())
            {
                metadataCommand.CommandText = "CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY);";
                _ = metadataCommand.ExecuteNonQuery();
            }

            await using (var writableCommand = connection.CreateCommand())
            {
                writableCommand.CommandText = "PRAGMA writable_schema = ON;";
                _ = writableCommand.ExecuteNonQuery();
            }

            await using (var tamperCommand = connection.CreateCommand())
            {
                tamperCommand.CommandText = """
                    UPDATE sqlite_master
                    SET sql = 'CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)'
                    WHERE type = 'table' AND name = 'oc_metadata';
                    """;
                _ = tamperCommand.ExecuteNonQuery();
            }

            await using var readOnlyCommand = connection.CreateCommand();
            readOnlyCommand.CommandText = "PRAGMA writable_schema = OFF;";
            _ = readOnlyCommand.ExecuteNonQuery();
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing subscription table constraints are rejected before enabling persistent WAL mode.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSubscriptionSchemaIsMissingConstraints_ThenInitializeFailsBeforeDurabilityPragmas()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using (var versionCommand = connection.CreateCommand())
            {
                versionCommand.CommandText = SetUserVersionSql;
                _ = versionCommand.ExecuteNonQuery();
            }

            await using (var metadataCommand = connection.CreateCommand())
            {
                metadataCommand.CommandText = "CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);";
                _ = metadataCommand.ExecuteNonQuery();
            }

            await using (var schemaCommand = connection.CreateCommand())
            {
                schemaCommand.CommandText = """
                    CREATE TABLE oc_subscription_identities (
                        store_identity TEXT,
                        stream_id TEXT NOT NULL,
                        subscription_id TEXT NOT NULL);
                    """;
                _ = schemaCommand.ExecuteNonQuery();
            }

            await using var metadataInsert = connection.CreateCommand();
            metadataInsert.CommandText = "INSERT INTO oc_metadata (key, value) VALUES ('schema_version', '1');";
            _ = metadataInsert.ExecuteNonQuery();
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ReadJournalMode(database.Path)).IsEqualTo("delete");
    }

    /// <summary>Verifies a mismatched metadata schema version is rejected without migrating the database.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMetadataVersionDoesNotMatch_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            CreateSchemaShell(connection);
            await using var metadataCommand = connection.CreateCommand();
            metadataCommand.CommandText = "INSERT INTO oc_metadata (key, value) VALUES ('schema_version', '2');";
            _ = metadataCommand.ExecuteNonQuery();
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing metadata is rejected as an incomplete schema.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMetadataValueIsMissing_ThenInitializeFailsClosed()
    {
        using var database = TempDatabase.Create();
        await using (var connection = OpenRawConnection(database.Path))
        {
            CreateSchemaShell(connection);
        }

        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies requiring encryption is refused before creating a plaintext database.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthenticatedEncryptionAtRestIsRequired_ThenInitializeFailsBeforePlaintextWrites()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteSubscriptionIdentityStore(database.Path);

        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, true), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<NotSupportedException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies a null store identity is rejected by validation instead of leaking a null reference failure.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIdentityIsNull_ThenInitializeThrowsArgumentNullException()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteSubscriptionIdentityStore(database.Path);

        Action action = () => store.Initialize(new(null!, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<ArgumentNullException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies invalid database paths are rejected so identities persist across real reopen.</summary>
    /// <param name="path">The invalid SQLite path.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments("")]
    [Arguments("   ")]
    [Arguments(":memory:")]
    [Arguments("file:identity.db?mode=memory&cache=shared")]
    public async Task WhenDatabasePathIsInvalid_ThenConstructionThrows(string path)
    {
        Action action = () => _ = new SqliteSubscriptionIdentityStore(path);

        await Assert.That(action).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies a connection that fails to open is surfaced during initialization.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDatabasePathIsDirectory_ThenInitializeThrowsSqliteException()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteSubscriptionIdentityStore(database.Directory);

        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<SqliteException>();
    }

    /// <summary>Verifies cancellation before initialization prevents creating a database file.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInitializationIsAlreadyCancelled_ThenNoDatabaseIsCreated()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteSubscriptionIdentityStore(database.Path);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), cancellation.Token);

        await Assert.That(action).ThrowsExactly<OperationCanceledException>();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies disposed initialization is rejected before creating parent directories.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDisposedStoreInitializes_ThenNoDirectoryIsCreated()
    {
        using var database = TempDatabase.ReservePath();
        var store = new SqliteSubscriptionIdentityStore(database.Path);
        store.Dispose();

        Action action = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);

        await Assert.That(action).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(System.IO.Directory.Exists(database.Directory)).IsFalse();
    }

    /// <summary>Verifies cancellation before lookup leaves no stream mapping behind.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLookupIsAlreadyCancelled_ThenNoMappingIsWritten()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Action action = () => store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), cancellation.Token);

        await Assert.That(action).ThrowsExactly<OperationCanceledException>();
        await Assert.That(CountSubscriptionRows(database.Path)).IsEqualTo(0);
    }

    /// <summary>Verifies cancellation while SQLite is blocked by a real writer rolls back the pending mapping only.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLookupIsCancelledWhileBlockedByWriter_ThenPendingMappingIsNotCommitted()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var existing = SubscriptionId.New();
        var pending = SubscriptionId.New();
        var blockedStream = new StreamId("sensor/blocked");
        _ = store.GetOrCreateSubscriptionId(Stream, existing, CancellationToken.None);

        await using var blocker = OpenRawConnection(database.Path);
        await using var transaction = blocker.BeginTransaction();
        await using (var command = blocker.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO oc_subscription_identities
                    (store_identity, stream_id, subscription_id)
                VALUES
                    ($storeIdentity, $streamId, $subscriptionId);
                """;
            _ = command.Parameters.AddWithValue("$storeIdentity", StoreIdentity);
            _ = command.Parameters.AddWithValue("$streamId", "sensor/held-lock");
            _ = command.Parameters.AddWithValue("$subscriptionId", SubscriptionId.New().Value.ToString("D"));
            _ = command.ExecuteNonQuery();
        }

        using var cancellation = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockedLookup = Task.Run(() =>
        {
            started.SetResult();
            return store.GetOrCreateSubscriptionId(blockedStream, pending, cancellation.Token);
        });
        await started.Task;
        try
        {
            await Task.Delay(WriterBlockDelay);
            await cancellation.CancelAsync();
            await Assert.That(blockedLookup.IsCompleted).IsFalse();
        }
        finally
        {
            await cancellation.CancelAsync();
            transaction.Rollback();
            await Assert.That(async () => await blockedLookup).ThrowsExactly<OperationCanceledException>();
        }

        await Assert.That(CountSubscriptionRows(database.Path)).IsEqualTo(1);
        await Assert.That(store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None)).IsEqualTo(existing);
    }

    /// <summary>Verifies invalid lookup inputs are rejected before persistence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLookupInputsAreInvalid_ThenGetOrCreateThrows()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);

        Action defaultStream = () => store.GetOrCreateSubscriptionId(default, null, CancellationToken.None);
        Action emptyPreferred = () => store.GetOrCreateSubscriptionId(Stream, new(Guid.Empty), CancellationToken.None);

        await Assert.That(defaultStream).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyPreferred).ThrowsExactly<ArgumentException>();
        await Assert.That(CountSubscriptionRows(database.Path)).IsEqualTo(0);
    }

    /// <summary>Verifies lookup requires prior initialization.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreHasNotBeenInitialized_ThenLookupThrows()
    {
        using var database = TempDatabase.Create();
        using var store = new SqliteSubscriptionIdentityStore(database.Path);

        Action action = () => store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies disposed instances reject initialization and lookup.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreIsDisposed_ThenOperationsThrow()
    {
        using var database = TempDatabase.Create();
        var store = new SqliteSubscriptionIdentityStore(database.Path);
        store.Dispose();

        Action initialize = () => store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Action lookup = () => store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);

        await Assert.That(initialize).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(lookup).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies initialization applies durable SQLite safety pragmas.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStoreInitializes_ThenJournalModeIsWal()
    {
        using var database = TempDatabase.Create();
        using var initializedStore = CreateInitializedStore(database.Path);

        await using var connection = OpenRawConnection(database.Path);
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA journal_mode;";
        var journalMode = command.ExecuteScalar();

        await Assert.That(journalMode).IsEqualTo("wal");
    }

    /// <summary>Verifies malformed persisted subscription rows are rejected.</summary>
    /// <param name="subscriptionId">The malformed stored subscription value.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments("not-a-guid")]
    [Arguments("00000000-0000-0000-0000-000000000000")]
    public async Task WhenStoredSubscriptionIdentityIsMalformed_ThenLookupFailsClosed(string subscriptionId)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        await using (var connection = OpenRawConnection(database.Path))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = "UPDATE oc_subscription_identities SET subscription_id = $subscriptionId;";
            _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId);
            _ = command.ExecuteNonQuery();
        }

        Action action = () => store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);

        await Assert.That(action).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates an initialized store instance.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The initialized store.</returns>
    private static SqliteSubscriptionIdentityStore CreateInitializedStore(string path)
    {
        var store = new SqliteSubscriptionIdentityStore(path);
        store.Initialize(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        return store;
    }

    /// <summary>Attempts to resolve a subscription identity and captures success or failure.</summary>
    /// <param name="store">The identity store.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="preferred">The preferred subscription identity.</param>
    /// <returns>The preferred value with either the stored identity or thrown exception.</returns>
    private static IdentityAttempt TryGetOrCreate(
        SqliteSubscriptionIdentityStore store,
        StreamId streamId,
        SubscriptionId preferred)
    {
        try
        {
            return new(preferred, store.GetOrCreateSubscriptionId(streamId, preferred, CancellationToken.None), null);
        }
        catch (Exception exception)
        {
            return new(preferred, null, exception);
        }
    }

    /// <summary>Creates the schema objects without metadata contents.</summary>
    /// <param name="connection">The open connection.</param>
    private static void CreateSchemaShell(SqliteConnection connection)
    {
        using (var versionCommand = connection.CreateCommand())
        {
            versionCommand.CommandText = SetUserVersionSql;
            _ = versionCommand.ExecuteNonQuery();
        }

        using (var metadataCommand = connection.CreateCommand())
        {
            metadataCommand.CommandText = "CREATE TABLE oc_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);";
            _ = metadataCommand.ExecuteNonQuery();
        }

        using var subscriptionCommand = connection.CreateCommand();
        subscriptionCommand.CommandText = """
            CREATE TABLE oc_subscription_identities (
                store_identity TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                subscription_id TEXT NOT NULL,
                PRIMARY KEY (store_identity, stream_id));
            """;
        _ = subscriptionCommand.ExecuteNonQuery();
    }

    /// <summary>Counts stored subscription identity rows.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The number of rows in the identity table.</returns>
    /// <exception cref="InvalidOperationException">The subscription row count could not be read.</exception>
    private static long CountSubscriptionRows(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'oc_subscription_identities';";
        if (command.ExecuteScalar() is not long tableCount || tableCount == 0)
        {
            return 0;
        }

        command.CommandText = "SELECT COUNT(*) FROM oc_subscription_identities;";
        if (command.ExecuteScalar() is long rowCount)
        {
            return rowCount;
        }

        throw new InvalidOperationException("The subscription row count could not be read.");
    }

    /// <summary>Reads the database journal mode.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <returns>The journal mode.</returns>
    /// <exception cref="InvalidOperationException">The journal mode could not be read.</exception>
    private static string ReadJournalMode(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA journal_mode;";
        if (command.ExecuteScalar() is string journalMode)
        {
            return journalMode;
        }

        throw new InvalidOperationException("The journal mode could not be read.");
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
            Path = System.IO.Path.Combine(directory, "identity.db");
        }

        /// <summary>Gets the SQLite database path.</summary>
        public string Path { get; }

        /// <summary>Gets the temporary directory path.</summary>
        public string Directory => _directory;

        /// <summary>Creates a new temporary database helper.</summary>
        /// <returns>The temporary database helper.</returns>
        public static TempDatabase Create()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rxui-oc-sqlite", Guid.NewGuid().ToString("N"));
            _ = System.IO.Directory.CreateDirectory(directory);
            return new(directory);
        }

        /// <summary>Reserves a new temporary database path without creating its directory.</summary>
        /// <returns>The temporary database helper.</returns>
        public static TempDatabase ReservePath()
        {
            var directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "rxui-oc-sqlite", Guid.NewGuid().ToString("N"));
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

    /// <summary>The result of one explicit identity creation attempt.</summary>
    /// <param name="Preferred">The preferred identity supplied to the store.</param>
    /// <param name="Identity">The identity returned by the store.</param>
    /// <param name="Exception">The exception thrown by the store.</param>
    private sealed record IdentityAttempt(SubscriptionId Preferred, SubscriptionId? Identity, Exception? Exception);
}
