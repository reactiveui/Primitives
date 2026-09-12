// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies the component or boundary responsible for an operational fault.</summary>
public enum FaultCategory
{
    /// <summary>The configured stream or context cannot start.</summary>
    Configuration = 0,

    /// <summary>A durable storage operation failed.</summary>
    Storage = 1,

    /// <summary>A payload could not be safely encoded, decoded, or upcast.</summary>
    Serialization = 2,

    /// <summary>A transport operation failed; normal offline state alone is not a fault.</summary>
    Transport = 3,

    /// <summary>Credentials are missing, expired, or rejected.</summary>
    Authentication = 4,

    /// <summary>The authenticated identity cannot perform the requested operation.</summary>
    Authorization = 5,

    /// <summary>A peer violated the negotiated protocol.</summary>
    Protocol = 6,

    /// <summary>A bounded queue or retained store reached its capacity.</summary>
    Capacity = 7,

    /// <summary>A conflict could not be resolved under the configured policy.</summary>
    Conflict = 8,

    /// <summary>A producer or consumer callback failed.</summary>
    Observer = 9,

    /// <summary>An internal consistency or ownership invariant failed.</summary>
    InternalInvariant = 10,
}
