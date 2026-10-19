// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies the stream a recovery read targets.</summary>
/// <param name="StoreIdentity">The store identity partition.</param>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="SubscriptionId">The subscription identifier.</param>
internal readonly record struct SqliteRecoveryTarget(string StoreIdentity, StreamId StreamId, SubscriptionId SubscriptionId);
