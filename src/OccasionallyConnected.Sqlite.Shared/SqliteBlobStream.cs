// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using SQLitePCL;

namespace ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

/// <summary>Reads a native SQLite BLOB incrementally without allocating its entire contents.</summary>
internal sealed class SqliteBlobStream : Stream
{
    /// <summary>The database whose read snapshot owns the BLOB.</summary>
    private readonly SqliteDatabase _database;

    /// <summary>The native incremental BLOB handle.</summary>
    private readonly sqlite3_blob _handle;

    /// <summary>The next byte offset.</summary>
    private long _position;

    /// <summary>Whether this stream has released its handle.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SqliteBlobStream"/> class.</summary>
    /// <param name="database">The database in the active read snapshot.</param>
    /// <param name="table">The table name.</param>
    /// <param name="column">The BLOB column name.</param>
    /// <param name="rowId">The source row identifier.</param>
    internal SqliteBlobStream(SqliteDatabase database, string table, string column, long rowId)
    {
        _database = database;
        var result = raw.sqlite3_blob_open(database.Handle, "main", table, column, rowId, 0, out _handle);
        if (result == raw.SQLITE_OK)
        {
            return;
        }

        _handle.Dispose();
        database.Check(result);
    }

    /// <inheritdoc/>
    public override bool CanRead => !_disposed;

    /// <inheritdoc/>
    public override bool CanSeek => !_disposed;

    /// <inheritdoc/>
    public override bool CanWrite => false;

    /// <inheritdoc/>
    public override long Length
    {
        get
        {
            ThrowIfDisposed();
            return raw.sqlite3_blob_bytes(_handle);
        }
    }

    /// <inheritdoc/>
    public override long Position
    {
        get => _position;
        set => _ = Seek(value, SeekOrigin.Begin);
    }

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count)
    {
        ThrowIfDisposed();
        var destination = buffer.AsSpan(offset, count);
        var length = checked((int)Math.Min(destination.Length, Length - _position));
        if (length == 0)
        {
            return 0;
        }

        _database.Check(raw.sqlite3_blob_read(_handle, destination.Slice(0, length), checked((int)_position)));
        _position += length;
        return length;
    }

    /// <inheritdoc/>
    public override long Seek(long offset, SeekOrigin origin)
    {
        ThrowIfDisposed();
        var position = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => checked(_position + offset),
            SeekOrigin.End => checked(Length + offset),
            _ => throw new ArgumentOutOfRangeException(nameof(origin)),
        };
        if (position < 0 || position > Length)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        _position = position;
        return position;
    }

    /// <inheritdoc/>
    public override void Flush() => ThrowIfDisposed();

    /// <inheritdoc/>
    public override void SetLength(long value) => throw new NotSupportedException("The SQLite BLOB stream is read-only.");

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException("The SQLite BLOB stream is read-only.");

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _handle.Dispose();
            _database.Forget(this);
            _disposed = true;
        }

        base.Dispose(disposing);
    }

    /// <summary>Verifies both the stream and its owning database remain open.</summary>
    private void ThrowIfDisposed()
    {
        ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
        _ = _database.Handle;
    }
}
