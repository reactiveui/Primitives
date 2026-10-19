// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Specifies the bounded result of server snapshot materialization.</summary>
public enum ServerSnapshotMaterializationStatus
{
    /// <summary>The requested allowlisted client state was materialized.</summary>
    Materialized = 0,

    /// <summary>The materializer cannot produce the requested projection.</summary>
    UnsupportedProjection = 1,

    /// <summary>The materialized response would exceed configured capacity.</summary>
    CapacityExceeded = 2,

    /// <summary>The materialized response failed structural validation.</summary>
    ValidationRejected = 3,
}
