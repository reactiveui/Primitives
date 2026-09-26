// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the health of an occasionally connected context.</summary>
public enum OccasionallyConnectedHealthStatus
{
    /// <summary>The local store is usable and no remote work is waiting, or the remote endpoint is online.</summary>
    Healthy = 0,

    /// <summary>Local work continues, but the remote endpoint is unavailable, retrying, circuit-open, or the context is not running.</summary>
    Degraded = 1,

    /// <summary>The context has failed permanently and needs attention.</summary>
    Unhealthy = 2,
}
