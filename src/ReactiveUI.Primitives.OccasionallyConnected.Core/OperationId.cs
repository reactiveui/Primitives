// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies a client-originated synchronization operation.</summary>
/// <param name="Value">The stable operation identifier value.</param>
[DebuggerDisplay("{Value,nq}")]
public readonly record struct OperationId(Guid Value)
{
    /// <summary>Creates a new non-empty operation identifier.</summary>
    /// <returns>A new operation identifier.</returns>
    public static OperationId New() => new(Guid.NewGuid());
}
