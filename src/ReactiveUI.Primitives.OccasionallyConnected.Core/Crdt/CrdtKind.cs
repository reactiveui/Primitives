// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Identifies a built-in CRDT state family.</summary>
public enum CrdtKind
{
    /// <summary>No CRDT kind is specified.</summary>
    None = 0,

    /// <summary>A grow-only counter with per-actor absolute components.</summary>
    GCounter = 1,

    /// <summary>A positive-negative counter with independent positive and negative per-actor absolute components.</summary>
    PNCounter = 2,

    /// <summary>An observed-remove set keyed by authenticated operation dots.</summary>
    ORSet = 3,

    /// <summary>A last-writer-wins register ordered by server write stamp.</summary>
    LwwRegister = 4,
}
