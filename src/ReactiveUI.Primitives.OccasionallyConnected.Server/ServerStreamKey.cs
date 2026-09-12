// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Identifies a stream within an authenticated tenant boundary.</summary>
/// <param name="TenantId">The authenticated tenant identifier supplied by the host.</param>
/// <param name="StreamId">The logical stream identifier.</param>
internal readonly record struct ServerStreamKey(string TenantId, StreamId StreamId);
