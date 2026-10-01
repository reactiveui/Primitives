// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Describes how record protection maintenance reads and rewrites one table.</summary>
/// <param name="Kind">The table, which selects its constant batch query and update statement.</param>
/// <param name="Describe">Adds the protected values of the current row, or returns false when the row cannot be bound.</param>
internal sealed record SqliteProtectedTable(
    SqliteProtectedTableKind Kind,
    Func<SqliteRows, List<SqliteProtectedValue>, bool> Describe);
