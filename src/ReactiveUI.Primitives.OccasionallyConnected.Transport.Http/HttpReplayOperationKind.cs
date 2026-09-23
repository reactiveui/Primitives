// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Identifies the HTTP occasionally-connected operation being protected from replay.</summary>
internal enum HttpReplayOperationKind
{
    /// <summary>The connect operation.</summary>
    Connect = 0,

    /// <summary>The push operation.</summary>
    Push = 1,

    /// <summary>The receive subscription operation.</summary>
    Subscribe = 2,

    /// <summary>The receive acknowledgement operation.</summary>
    Acknowledge = 3,

    /// <summary>The snapshot recovery operation.</summary>
    SnapshotRecovery = 4,
}
