// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Holds the new stored values of one row rewritten by record protection maintenance.</summary>
/// <param name="RowId">The rowid.</param>
/// <param name="Parameters">The update parameters, including unchanged values.</param>
/// <param name="RewrittenValues">The number of values that changed.</param>
internal sealed record SqliteProtectedRowUpdate(
    long RowId,
    KeyValuePair<string, object>[] Parameters,
    int RewrittenValues);
