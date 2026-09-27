// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>A SQLite connection that carries the record cipher for the store that opened it.</summary>
/// <remarks>Statement helpers resolve the cipher from the connection, so plaintext and protected stores share one code path.</remarks>
internal sealed class SqliteProtectedConnection : SqliteConnection
{
    /// <summary>Initializes a new instance of the <see cref="SqliteProtectedConnection"/> class.</summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="cipher">The record cipher.</param>
    internal SqliteProtectedConnection(string connectionString, SqliteRecordCipher cipher)
        : base(connectionString) => Cipher = cipher;

    /// <summary>Gets the record cipher.</summary>
    internal SqliteRecordCipher Cipher { get; }
}
