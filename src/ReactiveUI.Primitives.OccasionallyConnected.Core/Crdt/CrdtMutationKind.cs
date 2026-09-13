// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Identifies a built-in CRDT mutation.</summary>
public enum CrdtMutationKind
{
    /// <summary>No mutation kind is specified.</summary>
    None = 0,

    /// <summary>Sets a grow-only counter actor component to an absolute nonnegative value.</summary>
    GCounterSet = 1,

    /// <summary>Sets positive and negative PN-counter actor components to independent absolute nonnegative values.</summary>
    PNCounterSet = 2,

    /// <summary>Adds an OR-set element at the operation dot assigned by the committer.</summary>
    ORSetAdd = 3,

    /// <summary>Removes exact observed OR-set dots for one element.</summary>
    ORSetRemove = 4,

    /// <summary>Assigns a pending or authoritative LWW register value.</summary>
    LwwRegisterSet = 5,
}
