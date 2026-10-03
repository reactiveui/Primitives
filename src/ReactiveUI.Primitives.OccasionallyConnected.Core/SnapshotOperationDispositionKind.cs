// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies how snapshot recovery resolved one pending operation.</summary>
public enum SnapshotOperationDispositionKind
{
    /// <summary>Retained provenance proves the accepted or conflict-resolved effect is included in the checkpoint.</summary>
    IncludedAccepted = 0,

    /// <summary>Retained provenance proves the operation was permanently rejected.</summary>
    TerminalRejected = 1,

    /// <summary>The server cannot prove inclusion or terminal rejection; this is never proof of absence.</summary>
    Unknown = 2,
}
