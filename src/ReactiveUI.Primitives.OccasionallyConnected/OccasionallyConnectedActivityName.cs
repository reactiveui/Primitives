// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains the finite set of diagnostic activities emitted by the Occasionally Connected runtime.</summary>
internal enum OccasionallyConnectedActivityName
{
    /// <summary>Represents context initialization.</summary>
    ContextStart = 0,

    /// <summary>Represents connecting a remote transport.</summary>
    TransportConnect = 1,

    /// <summary>Represents synchronizing a push batch.</summary>
    SyncPush = 2,

    /// <summary>Represents receiving synchronized data.</summary>
    SyncReceive = 3,

    /// <summary>Represents committing local store data.</summary>
    StoreCommit = 4,

    /// <summary>Represents resolving a conflict.</summary>
    ConflictResolve = 5,

    /// <summary>Represents compacting local store data.</summary>
    StoreCompact = 6,
}
