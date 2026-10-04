// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests native preparation, parameter binding and statement lifetime.</summary>
public sealed class SqliteStatementTests
{
    /// <summary>The native in-memory database path.</summary>
    private const string MemoryPath = ":memory:";

    /// <summary>The parameter reused by statement lifetime tests.</summary>
    private const string ValueParameter = "$value";

    /// <summary>The parameterized scalar query.</summary>
    private const string SelectValueSql = "SELECT $value;";

    /// <summary>The number of preparations for two uncached queries.</summary>
    private const long UncachedQueryPreparations = 2;

    /// <summary>The second script result.</summary>
    private const long SecondResult = 2;

    /// <summary>The number of executions used to check a warm native handle.</summary>
    private const int RepeatedPreparedExecutions = 16;

    /// <summary>The number of distinct queries used to fill the cache.</summary>
    private const int CachePressureQueryCount = 256;

    /// <summary>The maximum retained idle native statement count.</summary>
    private const int MaximumIdleNativeStatements = 128;

    /// <summary>The maximum SQL characters retained with one cached handle.</summary>
    private const int MaximumRetainedSqlCharacters = 16 * 1024;

    /// <summary>The inserted key after a failed primary-key write.</summary>
    private const int ValueAfterConstraintFailure = 3;

    /// <summary>The BLOB bytes that include embedded zeros.</summary>
    private static readonly byte[] BlobBytes = [0, 1, 0];

    /// <summary>Verifies scripts prepare schema-dependent statements only after preceding statements execute.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExecutePreparesScriptsInOrderAndPreservesExtendedConstraintCodes()
    {
        using var database = new SqliteDatabase(MemoryPath);
        using var statement = database.CreateStatement();
        statement.SetSql("CREATE TABLE data (value INTEGER PRIMARY KEY); INSERT INTO data VALUES ($value); SELECT value FROM data;");
        _ = statement.Bind(ValueParameter, 1);
        await Assert.That(statement.Execute()).IsEqualTo(1);
        statement.SetSql("CREATE TABLE another (value INTEGER);");
        await Assert.That(statement.Execute()).IsEqualTo(0);
        statement.SetSql("INSERT INTO data VALUES ($value);");
        var exception = await Assert.ThrowsExactlyAsync<SqliteDatabaseException>(() => Task.Run(() => statement.Execute()));
        await Assert.That(exception!.SqliteErrorCode).IsEqualTo(raw.SQLITE_CONSTRAINT);
        await Assert.That(exception.SqliteExtendedErrorCode).IsEqualTo(raw.SQLITE_CONSTRAINT_PRIMARYKEY);
    }

    /// <summary>Verifies strings, nulls, numeric primitives and empty BLOBs retain their exact storage classes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BindPreservesStorageClassesAndTextIncludingEmbeddedNulls()
    {
        using var database = new SqliteDatabase(MemoryPath);
        using var statement = database.CreateStatement();
        statement.SetSql("""
            SELECT $text AS text, $emptyText AS emptyText, $blob AS blob, $emptyBlob AS emptyBlob,
                   $null AS nullValue, $bool AS boolean, $integer AS integer, $real AS real;
            """);
        const string Text = "before\0after';DROP TABLE data;--";
        _ = statement.Bind("$text", Text);
        _ = statement.Bind("$emptyText", string.Empty);
        _ = statement.Bind("$blob", BlobBytes);
        _ = statement.Bind("$emptyBlob", Array.Empty<byte>());
        _ = statement.Bind("$null", DBNull.Value);
        _ = statement.Bind("$bool", true);
        _ = statement.Bind("$integer", uint.MaxValue);
        _ = statement.Bind("$real", double.MaxValue);
        using var rows = statement.Query();
        await Assert.That(rows.Read()).IsTrue();
        await Assert.That(rows.GetString(0)).IsEqualTo(Text);
        await Assert.That(rows.GetString(0)).IsEqualTo(Text);
        await Assert.That(rows.GetString(1)).IsEqualTo(string.Empty);
        await Assert.That(Convert.ToHexString(rows.GetFieldValue<byte[]>(rows.GetOrdinal("blob")))).IsEqualTo("000100");
        await Assert.That(rows.GetFieldValue<byte[]>(rows.GetOrdinal("emptyBlob"))).IsEmpty();
        await Assert.That(rows.GetFieldType(rows.GetOrdinal("emptyBlob"))).IsEqualTo(typeof(byte[]));
        await Assert.That(rows.IsDBNull(rows.GetOrdinal("nullValue"))).IsTrue();
        await Assert.That(rows.GetInt64(rows.GetOrdinal("boolean"))).IsEqualTo(1L);
        await Assert.That(rows.GetInt64(rows.GetOrdinal("integer"))).IsEqualTo((long)uint.MaxValue);
        await Assert.That(rows.GetDouble(rows.GetOrdinal("real"))).IsEqualTo(double.MaxValue);
    }

    /// <summary>Verifies missing or unsupported parameter values cannot execute as implicit SQL NULL.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MissingAndUnsupportedBindingsFailExplicitlyAndStatementCanBeReused()
    {
        using var database = new SqliteDatabase(MemoryPath);
        using var statement = database.CreateStatement();
        statement.SetSql(SelectValueSql);
        await Assert.That(() => statement.Scalar()).ThrowsExactly<InvalidOperationException>();
        _ = statement.Bind(ValueParameter, new());
        await Assert.That(() => statement.Scalar()).ThrowsExactly<ArgumentException>();
        _ = statement.Bind(ValueParameter, DateTime.MinValue);
        await Assert.That(() => statement.Scalar()).ThrowsExactly<ArgumentException>();
        _ = statement.Bind(ValueParameter, ulong.MaxValue);
        await Assert.That(() => statement.Scalar()).ThrowsExactly<OverflowException>();
        _ = statement.Bind(ValueParameter, (short)1);
        await Assert.That(statement.Scalar()).IsEqualTo(1L);
        statement.ClearBindings();
        await Assert.That(() => statement.Scalar()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies active cursors prevent rebinding or starting another execution on the same statement.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActiveCursorRejectsStatementMutationAndDisposalReleasesNativeHandle()
    {
        using var database = new SqliteDatabase(MemoryPath);
        database.Handle.enable_sqlite3_next_stmt(true);
        using var statement = database.CreateStatement();
        statement.SetSql("SELECT 1;");
        using (var rows = statement.Query())
        {
            await Assert.That(() => statement.Bind(ValueParameter, 1)).ThrowsExactly<InvalidOperationException>();
            await Assert.That(() => statement.SetSql("SELECT 2;")).ThrowsExactly<InvalidOperationException>();
            await Assert.That(() => statement.Execute()).ThrowsExactly<InvalidOperationException>();
        }

        var idle = raw.sqlite3_next_stmt(database.Handle, null);
        await Assert.That(idle).IsNotNull();
        await Assert.That(raw.sqlite3_stmt_busy(idle!)).IsEqualTo(0);
        statement.SetSql("SELECT 2 WHERE 0;");
        await Assert.That(statement.Scalar()).IsNull();
        statement.SetSql("-- comment only");
        await Assert.That(() => statement.Query()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(statement.Execute()).IsEqualTo(0);
    }

    /// <summary>Verifies each result in a script has an independently stepped native cursor.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MultipleResultsKeepStatementOrderAndAllowInterleavedWrites()
    {
        using var database = new SqliteDatabase(MemoryPath);
        using var statement = database.CreateStatement();
        statement.SetSql("CREATE TABLE data (value INTEGER); SELECT 1; INSERT INTO data VALUES (2); SELECT value FROM data;");
        using var rows = statement.Query();
        _ = rows.Read();
        await Assert.That(rows.GetInt64(0)).IsEqualTo(1L);
        await Assert.That(rows.MoveNextResult()).IsTrue();
        _ = rows.Read();
        await Assert.That(rows.GetInt64(0)).IsEqualTo(SecondResult);
        await Assert.That(rows.MoveNextResult()).IsFalse();
    }

    /// <summary>Verifies independently owned commands reuse one reset handle without retaining parameters.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedHandlesAreReusedAndMissingBindingsNeverReusePreviousValues()
    {
        using var database = new SqliteDatabase(MemoryPath);
        for (var value = 0; value < RepeatedPreparedExecutions; value++)
        {
            using var command = database.CreateStatement();
            command.SetSql(SelectValueSql);
            _ = command.Bind(ValueParameter, value);
            await Assert.That(command.Scalar()).IsEqualTo((long)value);
        }

        await Assert.That(database.PreparationCount).IsEqualTo(1L);
        using var missing = database.CreateStatement();
        missing.SetSql(SelectValueSql);
        await Assert.That(() => missing.Scalar()).ThrowsExactly<InvalidOperationException>();
        _ = missing.Bind(ValueParameter, null);
        await Assert.That(missing.Scalar()).IsEqualTo(DBNull.Value);
        await Assert.That(database.PreparationCount).IsEqualTo(1L);
    }

    /// <summary>Verifies nested cursors own separate native handles and cache growth stays bounded.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedCacheIsBoundedAndConcurrentCursorsDoNotShareHandles()
    {
        using var database = new SqliteDatabase(MemoryPath);
        database.Handle.enable_sqlite3_next_stmt(true);
        using var first = database.CreateStatement();
        first.SetSql(SelectValueSql);
        _ = first.Bind(ValueParameter, 1);
        using (var rows = first.Query())
        {
            using var second = database.CreateStatement();
            second.SetSql(SelectValueSql);
            _ = second.Bind(ValueParameter, SecondResult);
            await Assert.That(second.Scalar()).IsEqualTo(SecondResult);
            await Assert.That(rows.Read()).IsTrue();
            await Assert.That(rows.GetInt64(0)).IsEqualTo(1L);
        }

        for (var value = 0; value < CachePressureQueryCount; value++)
        {
            database.Execute($"SELECT {value};");
        }

        var count = 0;
        var handle = raw.sqlite3_next_stmt(database.Handle, null);
        while (handle is not null)
        {
            count++;
            await Assert.That(raw.sqlite3_stmt_busy(handle)).IsEqualTo(0);
            handle = raw.sqlite3_next_stmt(database.Handle, handle);
        }

        await Assert.That(count).IsLessThanOrEqualTo(MaximumIdleNativeStatements);
    }

    /// <summary>Verifies SQLite automatically recompiles cached SQL when another command changes the schema.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CachedSelectSurvivesSchemaChangesAndConstraintFailures()
    {
        using var database = new SqliteDatabase(MemoryPath);
        database.Execute("CREATE TABLE data (value INTEGER PRIMARY KEY); INSERT INTO data VALUES (1);");
        using var command = database.CreateStatement();
        command.SetSql("SELECT value FROM data;");
        await Assert.That(command.Scalar()).IsEqualTo(1L);
        database.Execute("DROP TABLE data; CREATE TABLE data (value INTEGER PRIMARY KEY, other TEXT); INSERT INTO data VALUES (2, 'new');");
        await Assert.That(command.Scalar()).IsEqualTo(SecondResult);
        command.SetSql("INSERT INTO data (value) VALUES ($value);");
        _ = command.Bind(ValueParameter, SecondResult);
        await Assert.That(() => command.Execute()).ThrowsExactly<SqliteDatabaseException>();
        _ = command.Bind(ValueParameter, ValueAfterConstraintFailure);
        await Assert.That(command.Execute()).IsEqualTo(1);
    }

    /// <summary>Verifies a large script cannot pin unbounded SQL text in the connection cache.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LargeSqlScriptsAreNotRetainedByPreparedCache()
    {
        using var database = new SqliteDatabase(MemoryPath);
        database.Handle.enable_sqlite3_next_stmt(true);
        var sql = $"SELECT 1; --{new string('x', MaximumRetainedSqlCharacters)}";
        using var command = database.CreateStatement();
        command.SetSql(sql);
        await Assert.That(command.Scalar()).IsEqualTo(1L);
        await Assert.That(command.Scalar()).IsEqualTo(1L);
        await Assert.That(database.PreparationCount).IsEqualTo(UncachedQueryPreparations);
        await Assert.That(raw.sqlite3_next_stmt(database.Handle, null)).IsNull();
    }
}
