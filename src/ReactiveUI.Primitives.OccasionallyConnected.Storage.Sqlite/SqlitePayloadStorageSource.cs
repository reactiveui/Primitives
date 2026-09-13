// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies the SQLite table payload BLOB source.</summary>
internal sealed class SqlitePayloadStorageSource
{
    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadStorageSource"/> class.</summary>
    /// <param name="rowIdIndex">The payload rowid column index.</param>
    /// <param name="tableName">The source table name.</param>
    /// <param name="payloadColumnName">The source payload column name.</param>
    internal SqlitePayloadStorageSource(int rowIdIndex, string tableName, string payloadColumnName)
    {
        RowIdIndex = rowIdIndex;
        TableName = tableName;
        PayloadColumnName = payloadColumnName;
    }

    /// <summary>Gets the payload rowid column index.</summary>
    internal int RowIdIndex { get; }

    /// <summary>Gets the source table name.</summary>
    internal string TableName { get; }

    /// <summary>Gets the source payload column name.</summary>
    internal string PayloadColumnName { get; }
}
