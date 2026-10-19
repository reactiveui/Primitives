// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>One pending operation row used by snapshot recovery bookkeeping.</summary>
/// <param name="OperationId">The operation identifier.</param>
internal readonly record struct SqliteSnapshotRecoveryPendingOperation(OperationId OperationId);
