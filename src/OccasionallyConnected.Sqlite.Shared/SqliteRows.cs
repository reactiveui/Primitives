// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

/// <summary>Reads native column storage classes and copies data before the next step.</summary>
internal sealed class SqliteRows : IDisposable
{
    /// <summary>The statement whose cursor this instance owns.</summary>
    private readonly SqliteStatement _statement;

    /// <summary>Whether the cursor currently has a row.</summary>
    private bool _hasRow;

    /// <summary>Whether the cursor has reached its end or been disposed.</summary>
    private bool _finished;

    /// <summary>Initializes a new instance of the <see cref="SqliteRows"/> class.</summary>
    /// <param name="statement">The prepared statement.</param>
    internal SqliteRows(SqliteStatement statement) => _statement = statement;

    /// <summary>Gets the number of result columns.</summary>
    internal int FieldCount => raw.sqlite3_column_count(_statement.Handle);

    /// <inheritdoc/>
    public void Dispose()
    {
        _statement.Finish();
        _finished = true;
        _hasRow = false;
    }

    /// <summary>Advances to the next native row.</summary>
    /// <returns>Whether a row exists.</returns>
    internal bool Read()
    {
        if (_finished)
        {
            return false;
        }

        _hasRow = _statement.Step() == raw.SQLITE_ROW;
        _finished = !_hasRow;
        return _hasRow;
    }

    /// <summary>Advances to the next native result in the script.</summary>
    /// <returns>Whether another result exists.</returns>
    internal bool MoveNextResult()
    {
        _hasRow = false;
        _finished = !_statement.MoveNextResult();
        return !_finished;
    }

    /// <summary>Reads a column while preserving its actual SQLite storage class.</summary>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The copied value, or <see cref="DBNull.Value"/> for SQL NULL.</returns>
    /// <exception cref="InvalidOperationException">The cursor is not on a row or SQLite returns an unknown storage class.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The column index is outside the result.</exception>
    internal object GetValue(int index) =>
        StorageClass(index) switch
        {
            raw.SQLITE_INTEGER => raw.sqlite3_column_int64(_statement.Handle, index),
            raw.SQLITE_FLOAT => raw.sqlite3_column_double(_statement.Handle, index),
            raw.SQLITE_TEXT => raw.sqlite3_column_text(_statement.Handle, index).utf8_to_string(),
            raw.SQLITE_BLOB => raw.sqlite3_column_blob(_statement.Handle, index).ToArray(),
            raw.SQLITE_NULL => DBNull.Value,
            _ => throw new InvalidOperationException("SQLite returned an unknown column storage class."),
        };

    /// <summary>Reads a string column.</summary>
    /// <param name="index">The column index.</param>
    /// <returns>The string value.</returns>
    internal string GetString(int index)
    {
        EnsureNotNull(index);
        return raw.sqlite3_column_text(_statement.Handle, index).utf8_to_string();
    }

    /// <summary>Reads an integer column.</summary>
    /// <param name="index">The column index.</param>
    /// <returns>The integer value.</returns>
    internal long GetInt64(int index)
    {
        EnsureNotNull(index);
        return raw.sqlite3_column_int64(_statement.Handle, index);
    }

    /// <summary>Reads a floating-point column.</summary>
    /// <param name="index">The column index.</param>
    /// <returns>The real value.</returns>
    internal double GetDouble(int index)
    {
        EnsureNotNull(index);
        return raw.sqlite3_column_double(_statement.Handle, index);
    }

    /// <summary>Finds a result column by its exact name.</summary>
    /// <param name="name">The column name.</param>
    /// <returns>The column index.</returns>
    /// <exception cref="ArgumentException">The result has no matching column.</exception>
    internal int GetOrdinal(string name)
    {
        for (var index = 0; index < FieldCount; index++)
        {
            if (string.Equals(raw.sqlite3_column_name(_statement.Handle, index).utf8_to_string(), name, StringComparison.Ordinal))
            {
                return index;
            }
        }

        throw new ArgumentException("The SQLite result has no matching column.", nameof(name));
    }

    /// <summary>Reads the CLR type of the native storage class without allocating the column value.</summary>
    /// <param name="index">The column index.</param>
    /// <returns>The storage type.</returns>
    /// <exception cref="InvalidOperationException">SQLite returned an unknown storage class.</exception>
    internal Type GetFieldType(int index) => StorageClass(index) switch
    {
        raw.SQLITE_INTEGER => typeof(long),
        raw.SQLITE_FLOAT => typeof(double),
        raw.SQLITE_TEXT => typeof(string),
        raw.SQLITE_BLOB => typeof(byte[]),
        raw.SQLITE_NULL => typeof(DBNull),
        _ => throw new InvalidOperationException("SQLite returned an unknown column storage class."),
    };

    /// <summary>Reads a checked 32-bit integer column.</summary>
    /// <param name="index">The column index.</param>
    /// <returns>The integer value.</returns>
    internal int GetInt32(int index) => checked((int)GetInt64(index));

    /// <summary>Reads a typed column value.</summary>
    /// <typeparam name="T">The expected CLR storage type.</typeparam>
    /// <param name="index">The column index.</param>
    /// <returns>The typed value.</returns>
    internal T GetFieldValue<T>(int index) => (T)GetValue(index);

    /// <summary>Reports whether a column contains SQL NULL.</summary>
    /// <param name="index">The column index.</param>
    /// <returns>Whether the column is null.</returns>
    internal bool IsDBNull(int index) => StorageClass(index) == raw.SQLITE_NULL;

    /// <summary>Checks the row and column index before accessing native column memory.</summary>
    /// <param name="index">The result column index.</param>
    /// <returns>The native storage class.</returns>
    /// <exception cref="InvalidOperationException">The cursor is not positioned on a row.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The index is outside the result.</exception>
    private int StorageClass(int index)
    {
        if (!_hasRow)
        {
            throw new InvalidOperationException("The SQLite cursor is not positioned on a row.");
        }

        if (index < 0 || index >= FieldCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index), "The SQLite column index is outside the result.");
        }

        return raw.sqlite3_column_type(_statement.Handle, index);
    }

    /// <summary>Rejects SQL NULL before a numeric coercion.</summary>
    /// <param name="index">The result column index.</param>
    /// <exception cref="InvalidOperationException">The column contains SQL NULL.</exception>
    private void EnsureNotNull(int index)
    {
        if (StorageClass(index) == raw.SQLITE_NULL)
        {
            throw new InvalidOperationException("The SQLite column contains SQL NULL.");
        }
    }
}
