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

    /// <summary>Gets or sets the data version observed in the authenticated read snapshot.</summary>
    internal long? VerifiedDataVersion { get; set; }

    /// <summary>Gets or sets whether this connection journals state mutations.</summary>
    internal bool JournalInstalled { get; set; }

    /// <summary>Gets or sets whether a full proof rewrite already covered this transaction.</summary>
    internal bool FullProofRewriteCompleted { get; set; }

    /// <summary>Gets or sets the integrity check to run after the writer lock is acquired.</summary>
    internal Action<SqliteConnection, SqliteTransaction>? VerifyBeforeWrite { get; set; }

    /// <summary>Gets or sets the integrity observer update to run after commit.</summary>
    internal Action<bool>? ObserveAfterCommit { get; set; }
}
