// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Data.Common;
using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Reports a native SQLite failure without exposing a database provider.</summary>
[DebuggerDisplay("{Message,nq}; SQLite={SqliteExtendedErrorCode}")]
public sealed class SqliteDatabaseException : DbException
{
    /// <summary>Initializes a new instance of the <see cref="SqliteDatabaseException"/> class.</summary>
    public SqliteDatabaseException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SqliteDatabaseException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public SqliteDatabaseException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SqliteDatabaseException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying failure.</param>
    public SqliteDatabaseException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SqliteDatabaseException"/> class.</summary>
    /// <param name="message">The native SQLite failure message.</param>
    /// <param name="errorCode">The SQLite result code.</param>
    public SqliteDatabaseException(string message, int errorCode)
        : this(message, errorCode, errorCode)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SqliteDatabaseException"/> class.</summary>
    /// <param name="message">The native SQLite failure message.</param>
    /// <param name="errorCode">The SQLite primary result code.</param>
    /// <param name="extendedErrorCode">The SQLite extended result code.</param>
    public SqliteDatabaseException(string message, int errorCode, int extendedErrorCode)
        : base(message)
    {
        SqliteErrorCode = errorCode & 0xFF;
        SqliteExtendedErrorCode = extendedErrorCode;
    }

    /// <summary>Gets the SQLite primary result code.</summary>
    public int SqliteErrorCode { get; }

    /// <summary>Gets the SQLite extended result code.</summary>
    public int SqliteExtendedErrorCode { get; }
}
