// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Describes one protected column value in a record protection maintenance query.</summary>
/// <param name="Column">The column name; the update statement binds it as <c>$</c> followed by the column name.</param>
/// <param name="IsBlob">Whether the column holds a raw BLOB envelope rather than Base64 text.</param>
/// <param name="Context">The record context bound into the associated data.</param>
internal readonly record struct SqliteProtectedValue(
    string Column,
    bool IsBlob,
    SqliteRecordContext Context);
