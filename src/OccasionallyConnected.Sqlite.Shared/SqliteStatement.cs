// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

/// <summary>Prepares, binds, steps and finalizes native SQLite statements.</summary>
internal sealed class SqliteStatement : IDisposable
{
    /// <summary>The named parameter values retained for each execution.</summary>
    private readonly Dictionary<string, object?> _bindings = [with(StringComparer.Ordinal)];

    /// <summary>The SQL script to prepare lazily, after preceding schema statements execute.</summary>
    private string _sql = string.Empty;

    /// <summary>The current prepared native statement.</summary>
    private sqlite3_stmt? _handle;

    /// <summary>The remaining script after the current statement.</summary>
    private string _tail = string.Empty;

    /// <summary>Whether a row cursor is active.</summary>
    private bool _reading;

    /// <summary>Whether this owner has been disposed.</summary>
    private bool _disposed;

    /// <summary>Prevents entering native cleanup twice.</summary>
    private int _disposeStarted;

    /// <summary>Initializes a new instance of the <see cref="SqliteStatement"/> class.</summary>
    /// <param name="database">The owning database.</param>
    internal SqliteStatement(SqliteDatabase database) => Database = database;

    /// <summary>Gets the owning database.</summary>
    internal SqliteDatabase Database { get; }

    /// <summary>Gets the currently prepared native statement.</summary>
    internal sqlite3_stmt Handle => _handle ?? throw new InvalidOperationException("The SQLite statement has not been prepared.");

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
        {
            return;
        }

        Finish();
        Database.Forget(this);
        _disposed = true;
    }

    /// <summary>Verifies that a statement uses its database's active transaction.</summary>
    /// <param name="transaction">The required transaction.</param>
    /// <exception cref="InvalidOperationException">The transaction belongs to another database or has ended.</exception>
    internal void UseTransaction(SqliteTransaction? transaction)
    {
        if (transaction is not null && (transaction.Connection != Database || Database.Transaction != transaction))
        {
            throw new InvalidOperationException("The SQLite statement's transaction is not active on this database.");
        }
    }

    /// <summary>Selects SQL for the next execution without preparing later statements ahead of schema changes.</summary>
    /// <param name="sql">The statement or script.</param>
    internal void SetSql(string sql)
    {
        ThrowIfUnavailable();
        ArgumentExceptionHelper.ThrowIfNull(sql);
        Finish();
        _sql = sql;
    }

    /// <summary>Binds or replaces one named parameter value.</summary>
    /// <param name="name">The exact SQLite parameter name.</param>
    /// <param name="value">The value, or null for SQL NULL.</param>
    /// <returns>This statement owner.</returns>
    internal SqliteStatement Bind(string name, object? value)
    {
        ThrowIfUnavailable();
        _bindings[name] = value;
        return this;
    }

    /// <summary>Clears the retained named values before reusing the statement for another script.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void ClearBindings() => _bindings.Clear();

    /// <summary>Executes every statement in the script and returns affected rows.</summary>
    /// <returns>The number of directly changed rows.</returns>
    internal int Execute()
    {
        Start();
        var changes = 0;
        try
        {
            while (PrepareNext())
            {
                var previousChanges = raw.sqlite3_total_changes(Database.Handle);
                while (Step() == raw.SQLITE_ROW)
                {
                    // Consume result rows so PRAGMA and RETURNING statements finish before their handles close.
                }

                if (raw.sqlite3_total_changes(Database.Handle) != previousChanges)
                {
                    changes = checked(changes + raw.sqlite3_changes(Database.Handle));
                }

                Finish();
            }

            return changes;
        }
        finally
        {
            Finish();
        }
    }

    /// <summary>Reads the first column of the first result row.</summary>
    /// <returns>The value, SQL NULL as <see cref="DBNull"/>, or null when no row exists.</returns>
    internal object? Scalar()
    {
        using var rows = Query();
        return rows.Read() ? rows.GetValue(0) : null;
    }

    /// <summary>Opens a native row cursor for the first result-producing statement.</summary>
    /// <returns>The row cursor.</returns>
    /// <exception cref="InvalidOperationException">The script does not return columns.</exception>
    internal SqliteRows Query()
    {
        Start();
        try
        {
            while (PrepareNext())
            {
                if (raw.sqlite3_column_count(Handle) > 0)
                {
                    _reading = true;
                    return new(this);
                }

                _ = Step();
                Finish();
            }

            throw new InvalidOperationException("The SQLite script does not return columns.");
        }
        catch
        {
            Finish();
            throw;
        }
    }

    /// <summary>Prepares the next result-producing statement in the same SQL script.</summary>
    /// <returns>Whether another result remains.</returns>
    internal bool MoveNextResult()
    {
        Finish();
        while (PrepareNext())
        {
            if (raw.sqlite3_column_count(Handle) > 0)
            {
                _reading = true;
                return true;
            }

            _ = Step();
            Finish();
        }

        return false;
    }

    /// <summary>Steps the native virtual machine and reports any failure.</summary>
    /// <returns>The row or done result.</returns>
    internal int Step()
    {
        var result = Database.Run(() => raw.sqlite3_step(Handle));
        if (result is not raw.SQLITE_ROW and not raw.SQLITE_DONE)
        {
            Database.Check(result);
        }

        return result;
    }

    /// <summary>Finalizes the current native statement, including a partially consumed cursor.</summary>
    internal void Finish()
    {
        _handle?.Dispose();
        _handle = null;
        _reading = false;
    }

    /// <summary>Binds a CLR value to its SQLite storage class without interpolating SQL.</summary>
    /// <param name="handle">The native statement.</param>
    /// <param name="index">The one-based native parameter index.</param>
    /// <param name="value">The CLR value.</param>
    /// <returns>The native result code.</returns>
    /// <exception cref="ArgumentException">The value has an unsupported storage type.</exception>
    private static int BindValue(sqlite3_stmt handle, int index, object? value) =>
        value switch
        {
            null or DBNull => raw.sqlite3_bind_null(handle, index),
            byte[] bytes => bytes.Length == 0
                ? raw.sqlite3_bind_zeroblob(handle, index, 0)
                : raw.sqlite3_bind_blob(handle, index, bytes),
            string text => raw.sqlite3_bind_text(handle, index, text),
            bool flag => raw.sqlite3_bind_int(handle, index, flag ? 1 : 0),
            IConvertible number => BindNumber(handle, index, number),
            _ => throw new ArgumentException($"The SQLite parameter type '{value.GetType().FullName}' is not supported.", nameof(value)),
        };

    /// <summary>Binds a supported numeric primitive without changing its storage class.</summary>
    /// <param name="handle">The native statement.</param>
    /// <param name="index">The native parameter index.</param>
    /// <param name="value">The numeric value.</param>
    /// <returns>The native result code.</returns>
    /// <exception cref="ArgumentException">The value is not a supported numeric primitive.</exception>
    private static int BindNumber(sqlite3_stmt handle, int index, IConvertible value)
    {
        var type = value.GetTypeCode();
        if (type is >= TypeCode.SByte and <= TypeCode.UInt64)
        {
            return raw.sqlite3_bind_int64(handle, index, value.ToInt64(CultureInfo.InvariantCulture));
        }

        if (type is TypeCode.Single or TypeCode.Double)
        {
            return raw.sqlite3_bind_double(handle, index, value.ToDouble(CultureInfo.InvariantCulture));
        }

        throw new ArgumentException("The SQLite numeric parameter type is not supported.", nameof(value));
    }

    /// <summary>Starts a new execution using the current bindings.</summary>
    private void Start()
    {
        ThrowIfUnavailable();
        Finish();
        _tail = _sql;
    }

    /// <summary>Prepares the next native statement and binds all required values.</summary>
    /// <returns>Whether a statement remains.</returns>
    /// <exception cref="InvalidOperationException">A required named parameter has no bound value.</exception>
    private bool PrepareNext()
    {
        while (_tail.Length > 0)
        {
            var tail = string.Empty;
            var result = Database.Run(() => raw.sqlite3_prepare_v2(Database.Handle, _tail, out _handle, out tail));
            _tail = tail;
            Database.Check(result);
            if (_handle is null || _handle.IsInvalid)
            {
                Finish();
                continue;
            }

            var count = raw.sqlite3_bind_parameter_count(_handle);
            for (var index = 1; index <= count; index++)
            {
                var name = raw.sqlite3_bind_parameter_name(_handle, index).utf8_to_string();
                if (name is null || !_bindings.TryGetValue(name, out var value))
                {
                    throw new InvalidOperationException($"The SQLite parameter '{name}' has no bound value.");
                }

                Database.Check(BindValue(_handle, index, value));
            }

            return true;
        }

        return false;
    }

    /// <summary>Prevents using a disposed statement or changing an active cursor.</summary>
    /// <exception cref="InvalidOperationException">A row cursor is still active.</exception>
    private void ThrowIfUnavailable()
    {
        ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
        _ = Database.Handle;
        if (_reading)
        {
            throw new InvalidOperationException("The SQLite statement already has an active row cursor.");
        }
    }
}
