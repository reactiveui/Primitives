// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes a bounded server receive page request for one stream.</summary>
/// <param name="StreamKey">The authenticated stream key.</param>
/// <param name="Cursor">The optional persisted group cursor.</param>
/// <param name="MaximumGroups">The maximum complete operation groups to return.</param>
/// <param name="MaximumEvents">The maximum events to return.</param>
/// <param name="MaximumLogicalBytes">The maximum logical bytes to return.</param>
internal sealed record ServerReceivePageRequest(
    ServerStreamKey StreamKey,
    string? Cursor,
    int MaximumGroups,
    int MaximumEvents,
    long MaximumLogicalBytes);
