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

    /// <summary>The second script result.</summary>
    private const long SecondResult = 2;

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
        statement.SetSql("SELECT $value;");
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
        using var statement = database.CreateStatement();
        statement.SetSql("SELECT 1;");
        using (var rows = statement.Query())
        {
            await Assert.That(() => statement.Bind(ValueParameter, 1)).ThrowsExactly<InvalidOperationException>();
            await Assert.That(() => statement.SetSql("SELECT 2;")).ThrowsExactly<InvalidOperationException>();
            await Assert.That(() => statement.Execute()).ThrowsExactly<InvalidOperationException>();
        }

        await Assert.That(raw.sqlite3_next_stmt(database.Handle, null)).IsNull();
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
}
