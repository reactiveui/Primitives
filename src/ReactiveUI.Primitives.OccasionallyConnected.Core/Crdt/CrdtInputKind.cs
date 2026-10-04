// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Identifies the shape carried by a CRDT stream input.</summary>
public enum CrdtInputKind
{
    /// <summary>No input kind is specified.</summary>
    None = 0,

    /// <summary>The input carries a local mutation.</summary>
    Mutation = 1,

    /// <summary>The input carries a complete authoritative CRDT state.</summary>
    AuthoritativeState = 2,
}
