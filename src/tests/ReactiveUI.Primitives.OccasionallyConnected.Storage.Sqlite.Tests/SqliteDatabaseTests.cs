// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests native database ownership, locking, cancellation and page encryption.</summary>
public sealed class SqliteDatabaseTests
{
    /// <summary>The native in-memory database path.</summary>
    private const string MemoryPath = ":memory:";

    /// <summary>The query shared by native data round trips.</summary>
    private const string ReadDataSql = "SELECT value FROM data;";

    /// <summary>The native integrity check.</summary>
    private const string IntegrityCheckSql = "PRAGMA integrity_check;";

    /// <summary>The short busy timeout used by bounded lock tests.</summary>
    private const int BusyMilliseconds = 100;

    /// <summary>The cancellation delay used to interrupt native work.</summary>
    private const int CancellationMilliseconds = 50;

    /// <summary>The maximum allowed cancellation or lock timeout latency.</summary>
    private const int MaximumWaitSeconds = 5;

    /// <summary>The first page encryption passphrase.</summary>
    private const string FirstPassphrase = "native-key-one';not-sql";

    /// <summary>The replacement page encryption passphrase.</summary>
    private const string SecondPassphrase = "native-key-two";

    /// <summary>The protected data query reused across key checks.</summary>
    private const string ReadSecretsSql = "SELECT value FROM secrets;";

    /// <summary>The value copied by the native backup.</summary>
    private const long BackupValue = 42;

    /// <summary>The guard timeout that releases contention even when the native backup fails to stop.</summary>
    private const int BackupGuardSeconds = 2;

    /// <summary>The number of rows that require more than one native backup step.</summary>
    private const int BackupRows = 160;

    /// <summary>The number of pages copied by one native backup step.</summary>
    private const int NativeBackupPageCount = 64;

    /// <summary>Verifies database disposal closes outstanding native statements and BLOBs.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeClosesEveryOwnedNativeHandle()
    {
        var database = new SqliteDatabase(MemoryPath);
        try
        {
            database.Execute("CREATE TABLE data (value BLOB); INSERT INTO data VALUES (X'010203');");
            using var statement = database.CreateStatement();
            statement.SetSql(ReadDataSql);
            using var rows = statement.Query();
            _ = rows.Read();
            await using var blob = database.OpenBlob("data", "value", 1);
            database.Dispose();

            await Assert.That(database.IsDisposed).IsTrue();
            await Assert.That(() => database.CreateStatement()).ThrowsExactly<ObjectDisposedException>();
            await Assert.That(() => blob.ReadByte()).ThrowsExactly<ObjectDisposedException>();
        }
        finally
        {
            if (!database.IsDisposed)
            {
                database.Dispose();
            }
        }
    }

    /// <summary>Verifies writer transactions take the lock before their first mutation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ImmediateTransactionAcquiresWriterLockWithBoundedBusyTimeout()
    {
        using var lease = new DatabaseLease();
        using var first = new SqliteDatabase(lease.Path);
        using var writer = first.BeginTransaction();
        using var second = new SqliteDatabase(lease.Path);
        second.SetBusyTimeout(BusyMilliseconds);
        var start = Stopwatch.GetTimestamp();

        var exception = await Assert.ThrowsExactlyAsync<SqliteDatabaseException>(
            () => Task.Run(() => { using var blocked = second.BeginTransaction(); }));

        await Assert.That(exception!.SqliteErrorCode).IsEqualTo(raw.SQLITE_BUSY);
        await Assert.That(Stopwatch.GetElapsedTime(start)).IsLessThan(TimeSpan.FromSeconds(MaximumWaitSeconds));
        writer.Rollback();
        using var acquired = second.BeginTransaction();
        acquired.Commit();
    }

    /// <summary>Verifies cancellation interrupts long native virtual machine work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationInterruptsNativeProgressAndLeavesDatabaseUsable()
    {
        using var cancellation = new CancellationTokenSource();
        using var database = new SqliteDatabase(MemoryPath, cancellationToken: cancellation.Token);
        using var statement = database.CreateStatement();
        statement.SetSql("""
            WITH RECURSIVE numbers(value) AS (
                SELECT 1 UNION ALL SELECT value + 1 FROM numbers WHERE value < 1000000000)
            SELECT sum(value) FROM numbers;
            """);
        cancellation.CancelAfter(CancellationMilliseconds);
        var start = Stopwatch.GetTimestamp();

        _ = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => Task.Run(() => statement.Scalar()));

        await Assert.That(Stopwatch.GetElapsedTime(start)).IsLessThan(TimeSpan.FromSeconds(MaximumWaitSeconds));
        database.SetCancellation(CancellationToken.None);
        statement.SetSql("SELECT 1;");
        await Assert.That(statement.Scalar()).IsEqualTo(1L);
    }

    /// <summary>Verifies cancellation does not wait for the full busy timeout.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationInterruptsWriterLockPolling()
    {
        using var lease = new DatabaseLease();
        using var first = new SqliteDatabase(lease.Path);
        using var writer = first.BeginTransaction();
        using var cancellation = new CancellationTokenSource();
        using var second = new SqliteDatabase(lease.Path, cancellationToken: cancellation.Token);
        cancellation.CancelAfter(CancellationMilliseconds);
        var start = Stopwatch.GetTimestamp();

        _ = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => Task.Run(() => { using var blocked = second.BeginTransaction(); }));

        await Assert.That(Stopwatch.GetElapsedTime(start)).IsLessThan(TimeSpan.FromSeconds(MaximumWaitSeconds));
    }

    /// <summary>Verifies encryption and WAL key rotation reject old or missing passphrases.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PageEncryptionAndWalRekeyPreserveDataAndRejectOldKey()
    {
        using var lease = new DatabaseLease();
        using (var database = new SqliteDatabase(lease.Path, password: FirstPassphrase))
        {
            database.Execute("PRAGMA journal_mode=WAL; CREATE TABLE secrets (value TEXT); INSERT INTO secrets VALUES ('confidential-native-value');");
            database.Rekey(SecondPassphrase);
            database.Execute("PRAGMA wal_checkpoint(TRUNCATE);");
        }

        using (var database = new SqliteDatabase(lease.Path, password: SecondPassphrase))
        {
            using var statement = database.CreateStatement();
            statement.SetSql(ReadSecretsSql);
            await Assert.That(statement.Scalar()).IsEqualTo("confidential-native-value");
            statement.SetSql(IntegrityCheckSql);
            await Assert.That(statement.Scalar()).IsEqualTo("ok");
        }

        await Assert.That(System.Text.Encoding.UTF8.GetString(await File.ReadAllBytesAsync(lease.Path)))
            .DoesNotContain("confidential-native-value");
        using var oldKey = new SqliteDatabase(lease.Path, password: FirstPassphrase);
        using var oldRead = oldKey.CreateStatement();
        oldRead.SetSql(ReadSecretsSql);
        await Assert.That(() => oldRead.Scalar()).ThrowsExactly<SqliteDatabaseException>();
        using var noKey = new SqliteDatabase(lease.Path);
        using var noRead = noKey.CreateStatement();
        noRead.SetSql(ReadSecretsSql);
        await Assert.That(() => noRead.Scalar()).ThrowsExactly<SqliteDatabaseException>();
    }

    /// <summary>Verifies a native backup includes committed WAL data and preserves destination encryption.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BackupCopiesCommittedWalSnapshotIntoEncryptedDestination()
    {
        using var sourceLease = new DatabaseLease();
        using var destinationLease = new DatabaseLease();
        using var source = new SqliteDatabase(sourceLease.Path, password: FirstPassphrase);
        source.Execute("PRAGMA journal_mode=WAL; CREATE TABLE data (value INTEGER); INSERT INTO data VALUES (42);");
        using (var destination = new SqliteDatabase(destinationLease.Path, password: SecondPassphrase))
        {
            source.BackupTo(destination);
            using var statement = destination.CreateStatement();
            statement.SetSql(ReadDataSql);
            await Assert.That(statement.Scalar()).IsEqualTo(BackupValue);
        }

        using var reopened = new SqliteDatabase(destinationLease.Path, password: SecondPassphrase);
        using var check = reopened.CreateStatement();
        check.SetSql(IntegrityCheckSql);
        await Assert.That(check.Scalar()).IsEqualTo("ok");
    }

    /// <summary>Verifies an exhausted destination busy timeout stops backup while the writer remains locked.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NativeCompletionBoundaryBoundsBackupContention()
    {
        using var lease = new DatabaseLease();
        using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(MaximumWaitSeconds));
        using var source = new SqliteDatabase(MemoryPath, cancellationToken: guard.Token);
        source.Execute("CREATE TABLE data (value INTEGER); INSERT INTO data VALUES (42);");
        using var blocker = new SqliteDatabase(lease.Path);
        using var writer = blocker.BeginTransaction();
        using var destination = new SqliteDatabase(lease.Path);
        destination.SetBusyTimeout(BusyMilliseconds);
        var start = Stopwatch.GetTimestamp();
        var backup = Task.Run(() =>
        {
            try
            {
                source.BackupTo(destination);
                return (SqliteDatabaseException?)null;
            }
            catch (SqliteDatabaseException exception)
            {
                return exception;
            }
        });
        bool completedWhileLocked;
        SqliteDatabaseException? failure;
        try
        {
            var completed = await Task.WhenAny(backup, Task.Delay(TimeSpan.FromSeconds(BackupGuardSeconds)));
            completedWhileLocked = completed == backup;
            failure = completedWhileLocked ? await backup : null;
        }
        finally
        {
            writer.Rollback();
            _ = await backup;
        }

        await Assert.That(completedWhileLocked).IsTrue();
        await Assert.That(failure).IsNotNull();
        await Assert.That(failure!.SqliteErrorCode).IsEqualTo(raw.SQLITE_BUSY);
        await Assert.That(Stopwatch.GetElapsedTime(start)).IsLessThan(TimeSpan.FromSeconds(BackupGuardSeconds));
        source.BackupTo(destination);
        using var read = destination.CreateStatement();
        read.SetSql(ReadDataSql);
        await Assert.That(read.Scalar()).IsEqualTo(BackupValue);
    }

    /// <summary>Verifies successful page-copy steps continue until a large backup completes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NativeCompletionBoundaryPreservesMultiStepBackup()
    {
        using var source = new SqliteDatabase(MemoryPath);
        using var destination = new SqliteDatabase(MemoryPath);
        using var statement = source.CreateStatement();
        statement.SetSql("""
            PRAGMA page_size = 512;
            CREATE TABLE data (value BLOB);
            WITH RECURSIVE numbers(value) AS (
                SELECT 1 UNION ALL SELECT value + 1 FROM numbers WHERE value < $rows)
            INSERT INTO data SELECT zeroblob(1024) FROM numbers;
            """);
        _ = statement.Bind("$rows", BackupRows);
        _ = statement.Execute();
        statement.SetSql("PRAGMA page_count;");
        await Assert.That(Convert.ToInt64(statement.Scalar(), CultureInfo.InvariantCulture)).IsGreaterThan(NativeBackupPageCount);

        source.BackupTo(destination);

        using var read = destination.CreateStatement();
        read.SetSql("SELECT count(*) FROM data;");
        await Assert.That(read.Scalar()).IsEqualTo((long)BackupRows);
        read.SetSql(IntegrityCheckSql);
        await Assert.That(read.Scalar()).IsEqualTo("ok");
    }

    /// <summary>Verifies read-only opens cannot create or modify database files.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReadOnlyOpenPreservesNativeReadOnlyAndCannotOpenErrors()
    {
        using var lease = new DatabaseLease();
        await Assert.That(() => new SqliteDatabase(lease.Path, readOnly: true)).ThrowsExactly<SqliteDatabaseException>();
        using (var writer = new SqliteDatabase(lease.Path))
        {
            writer.Execute("CREATE TABLE data (value INTEGER);");
        }

        using var readOnly = new SqliteDatabase(lease.Path, readOnly: true);
        var exception = await Assert.ThrowsExactlyAsync<SqliteDatabaseException>(
            () => Task.Run(() => readOnly.Execute("INSERT INTO data VALUES (1);")));
        await Assert.That(exception!.SqliteErrorCode).IsEqualTo(raw.SQLITE_READONLY);
    }

    /// <summary>Owns a file path with connection-string metacharacters to verify native path handling.</summary>
    private sealed class DatabaseLease : IDisposable
    {
        /// <summary>The owned temporary directory.</summary>
        private readonly string _directory = System.IO.Path.Combine(PhysicalTempDirectory.GetRoot(), $"rxui-native-sqlite-{Guid.NewGuid():N}");

        /// <summary>Initializes a new instance of the <see cref="DatabaseLease"/> class.</summary>
        internal DatabaseLease()
        {
            _ = Directory.CreateDirectory(_directory);
            Path = System.IO.Path.Combine(_directory, "native;data=quoted'.db");
        }

        /// <summary>Gets the database file path.</summary>
        internal string Path { get; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(_directory, recursive: true);
    }
}
