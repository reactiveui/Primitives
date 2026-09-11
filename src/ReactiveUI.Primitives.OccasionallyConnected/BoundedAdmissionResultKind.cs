// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies the result of an admission attempt.</summary>
internal enum BoundedAdmissionResultKind
{
    /// <summary>The incoming item was admitted.</summary>
    Admitted = 0,

    /// <summary>The incoming item was rejected.</summary>
    Rejected = 1,

    /// <summary>The incoming item was dropped.</summary>
    DroppedIncoming = 2,
}
