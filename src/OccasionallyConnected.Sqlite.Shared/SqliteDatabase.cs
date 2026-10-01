// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

/// <summary>Owns one native database and its statements, without connection pooling.</summary>
internal sealed class SqliteDatabase : IDisposable
{
    /// <summary>The default maximum lock wait.</summary>
    internal const int DefaultBusyTimeoutMilliseconds = 30_000;

    /// <summary>The maximum duration of one native lock wait.</summary>
    private const int NativeBusySliceMilliseconds = 10;

    /// <summary>The number of virtual machine instructions between cancellation checks.</summary>
    private const int ProgressInstructions = 1000;

    /// <summary>The number of database pages copied per backup step.</summary>
    private const int BackupPageCount = 64;

    /// <summary>The milliseconds per stopwatch second.</summary>
    private const double MillisecondsPerSecond = 1000;

    /// <summary>Initializes the single native provider before the first database opens.</summary>
    private static readonly Lazy<bool> Provider = new(static () =>
    {
        Batteries_V2.Init();
        return true;
    });

    /// <summary>The statements whose native handles belong to this database.</summary>
    private readonly HashSet<SqliteStatement> _statements = [];

    /// <summary>The incremental BLOB handles owned by this database.</summary>
    private readonly HashSet<SqliteBlobStream> _blobs = [];

    /// <summary>The native database handle.</summary>
    private readonly sqlite3 _handle;

    /// <summary>The registration that interrupts an active native operation.</summary>
    private CancellationTokenRegistration _cancellationRegistration;

    /// <summary>The current operation's cancellation token.</summary>
    private CancellationToken _cancellationToken;

    /// <summary>The maximum native busy wait in milliseconds.</summary>
    private int _busyTimeoutMilliseconds = DefaultBusyTimeoutMilliseconds;

    /// <summary>Prevents repeated disposal from entering native cleanup twice.</summary>
    private int _disposeStarted;

    /// <summary>Whether the native database has been released.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SqliteDatabase"/> class.</summary>
    /// <param name="path">The database file path or SQLite URI.</param>
    /// <param name="readOnly">Whether writes are forbidden.</param>
    /// <param name="create">Whether a missing file may be created.</param>
    /// <param name="password">The optional database passphrase.</param>
    /// <param name="cancellationToken">The native operation cancellation token.</param>
    internal SqliteDatabase(
        string path,
        bool readOnly = false,
        bool create = true,
        string? password = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentExceptionHelper.ThrowIfNull(path);
        _ = Provider.Value;
        var flags = raw.SQLITE_OPEN_FULLMUTEX | raw.SQLITE_OPEN_URI
            | (readOnly ? raw.SQLITE_OPEN_READONLY : raw.SQLITE_OPEN_READWRITE)
            | (!readOnly && create ? raw.SQLITE_OPEN_CREATE : 0);
        var result = raw.sqlite3_open_v2(path, out _handle, flags, null);
        try
        {
            Check(result);
            Check(raw.sqlite3_extended_result_codes(_handle, 1));
            SetCancellation(cancellationToken);
            SetBusyTimeout(_busyTimeoutMilliseconds);
            if (password is not null)
            {
                SetKey(password, rekey: false);
            }
        }
        catch
        {
            _cancellationRegistration.Dispose();
            _handle.Dispose();
            throw;
        }
    }

    /// <summary>Gets or sets adapter-owned state composed with this connection.</summary>
    internal object? Context { get; set; }

    /// <summary>Gets or sets the active transaction, or null outside a transaction.</summary>
    internal SqliteTransaction? Transaction { get; set; }

    /// <summary>Gets whether this database's native handle has been released.</summary>
    internal bool IsDisposed => _disposed;

    /// <summary>Gets the native handle after verifying its lifetime.</summary>
    internal sqlite3 Handle
    {
        get
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
            return _handle;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
        {
            return;
        }

        try
        {
            _cancellationRegistration.Dispose();
            SetCancellation(CancellationToken.None);
            foreach (var blob in new List<SqliteBlobStream>(_blobs))
            {
                blob.Dispose();
            }

            foreach (var statement in new List<SqliteStatement>(_statements))
            {
                statement.Dispose();
            }

            Transaction?.Dispose();
            Check(raw.sqlite3_close(_handle));
        }
        finally
        {
            _handle.Dispose();
            _disposed = true;
        }
    }

    /// <summary>Creates a statement owner for SQL prepared on this connection.</summary>
    /// <returns>The statement owner.</returns>
    internal SqliteStatement CreateStatement()
    {
        var statement = new SqliteStatement(this);
        _ = Handle;
        _ = _statements.Add(statement);
        return statement;
    }

    /// <summary>Starts a read snapshot or acquires the writer lock immediately.</summary>
    /// <param name="deferred">Whether to defer acquiring the writer lock.</param>
    /// <returns>The transaction owner.</returns>
    /// <exception cref="InvalidOperationException">A transaction is already active.</exception>
    internal SqliteTransaction BeginTransaction(bool deferred = false)
    {
        if (Transaction is not null)
        {
            throw new InvalidOperationException("The SQLite database already has an active transaction.");
        }

        Execute(deferred ? "BEGIN;" : "BEGIN IMMEDIATE;");
        Transaction = new(this);
        return Transaction;
    }

    /// <summary>Runs an unparameterized SQL script.</summary>
    /// <param name="sql">The SQL script.</param>
    internal void Execute(string sql)
    {
        using var statement = CreateStatement();
        statement.SetSql(sql);
        _ = statement.Execute();
    }

    /// <summary>Sets the maximum wait for native lock contention.</summary>
    /// <param name="milliseconds">The busy timeout in milliseconds.</param>
    internal void SetBusyTimeout(int milliseconds)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(milliseconds);
        _busyTimeoutMilliseconds = milliseconds;
        Check(raw.sqlite3_busy_timeout(Handle, Math.Min(milliseconds, NativeBusySliceMilliseconds)));
    }

    /// <summary>Installs cancellation and progress interruption for this database.</summary>
    /// <param name="cancellationToken">The operation cancellation token.</param>
    internal void SetCancellation(CancellationToken cancellationToken)
    {
        _ = Handle;
        _cancellationRegistration.Dispose();
        _cancellationToken = cancellationToken;
        raw.sqlite3_progress_handler(
            _handle,
            ProgressInstructions,
            static state => ((SqliteDatabase)state)._cancellationToken.IsCancellationRequested ? 1 : 0,
            this);
#if NET8_0_OR_GREATER
        _cancellationRegistration = cancellationToken.UnsafeRegister(static state => raw.sqlite3_interrupt(((SqliteDatabase)state!)._handle), this);
#else
        _cancellationRegistration = cancellationToken.Register(static state => raw.sqlite3_interrupt(((SqliteDatabase)state!)._handle), this);
#endif
    }

    /// <summary>Rotates the database passphrase outside a transaction.</summary>
    /// <param name="password">The new passphrase.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Rekey(string password) => SetKey(password, rekey: true);

    /// <summary>Copies a consistent database snapshot using SQLite's native backup API.</summary>
    /// <param name="destination">The destination database whose busy timeout bounds lock waits.</param>
    /// <exception cref="InvalidOperationException">SQLite cannot create a backup handle.</exception>
    internal void BackupTo(SqliteDatabase destination)
    {
        using var backup = raw.sqlite3_backup_init(destination.Handle, "main", Handle, "main");
        if (backup is null)
        {
            destination.Check(raw.sqlite3_errcode(destination.Handle));
            throw new InvalidOperationException("SQLite did not create a backup handle.");
        }

        int result;
        do
        {
            _cancellationToken.ThrowIfCancellationRequested();
            result = destination.Run(() =>
            {
                _cancellationToken.ThrowIfCancellationRequested();
                return raw.sqlite3_backup_step(backup, BackupPageCount);
            });
        }
        while (result == raw.SQLITE_OK);

        if (result != raw.SQLITE_DONE)
        {
            destination.Check(result);
        }

        destination.Check(raw.sqlite3_backup_finish(backup));
    }

    /// <summary>Checks a native result and preserves primary and extended SQLite error codes.</summary>
    /// <param name="result">The native result code.</param>
    /// <exception cref="SqliteDatabaseException">SQLite reports a native failure.</exception>
    internal void Check(int result)
    {
        if (result == raw.SQLITE_OK)
        {
            return;
        }

        if ((result & 0xFF) is raw.SQLITE_INTERRUPT or raw.SQLITE_BUSY or raw.SQLITE_LOCKED)
        {
            _cancellationToken.ThrowIfCancellationRequested();
        }

        var extended = raw.sqlite3_extended_errcode(_handle);
        throw new SqliteDatabaseException(
            raw.sqlite3_errmsg(_handle).utf8_to_string(),
            result,
            (extended & 0xFF) == (result & 0xFF) ? extended : result);
    }

    /// <summary>Retries short native lock waits while observing the bounded timeout and cancellation.</summary>
    /// <param name="operation">The native operation.</param>
    /// <returns>The final native result code.</returns>
    internal int Run(Func<int> operation)
    {
        var start = Stopwatch.GetTimestamp();
        while (true)
        {
            _cancellationToken.ThrowIfCancellationRequested();
            var result = operation();
            if ((result & 0xFF) is not raw.SQLITE_BUSY and not raw.SQLITE_LOCKED)
            {
                return result;
            }

            var elapsed = (Stopwatch.GetTimestamp() - start) * MillisecondsPerSecond / Stopwatch.Frequency;
            if (elapsed >= _busyTimeoutMilliseconds)
            {
                return result;
            }

            _ = _cancellationToken.WaitHandle.WaitOne(
                Math.Min(NativeBusySliceMilliseconds, _busyTimeoutMilliseconds - (int)elapsed));
        }
    }

    /// <summary>Forgets a statement after its native handle is finalized.</summary>
    /// <param name="statement">The released statement.</param>
    internal void Forget(SqliteStatement statement) => _ = _statements.Remove(statement);

    /// <summary>Forgets an incremental BLOB after it closes.</summary>
    /// <param name="blob">The BLOB owner.</param>
    internal void Forget(SqliteBlobStream blob) => _ = _blobs.Remove(blob);

    /// <summary>Opens and tracks a read-only incremental BLOB.</summary>
    /// <param name="table">The source table.</param>
    /// <param name="column">The source BLOB column.</param>
    /// <param name="rowId">The source row identifier.</param>
    /// <returns>The BLOB stream owner.</returns>
    internal SqliteBlobStream OpenBlob(string table, string column, long rowId)
    {
        var blob = new SqliteBlobStream(this, table, column, rowId);
        _ = _blobs.Add(blob);
        return blob;
    }

    /// <summary>Applies a passphrase without embedding it in SQL.</summary>
    /// <param name="password">The passphrase.</param>
    /// <param name="rekey">Whether to rotate an existing key.</param>
    private void SetKey(string password, bool rekey)
    {
        ArgumentExceptionHelper.ThrowIfNull(password);
        var bytes = Encoding.UTF8.GetBytes(password);
        try
        {
            Check(rekey ? raw.sqlite3_rekey(Handle, bytes) : raw.sqlite3_key(Handle, bytes));
        }
        finally
        {
            Array.Clear(bytes, 0, bytes.Length);
        }
    }
}
