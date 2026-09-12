// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies one operation in a persisted outbox lease batch.</summary>
/// <param name="OperationId">The operation identifier.</param>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="ClientSequence">The client sequence.</param>
internal readonly record struct SqliteOutboxLeaseMember(OperationId OperationId, StreamId StreamId, long ClientSequence);
