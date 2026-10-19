// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies a client-originated synchronization operation.</summary>
/// <param name="Value">The stable operation identifier value.</param>
[DebuggerDisplay("{Value,nq}")]
public readonly record struct OperationId(Guid Value)
{
    /// <summary>Creates a new non-empty operation identifier.</summary>
    /// <returns>A new operation identifier, using version 7 on .NET 9 or later.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static OperationId New() => New(true);

    /// <summary>Creates a new non-empty operation identifier with the selected GUID version.</summary>
    /// <param name="useVersion7">Whether to use a time-ordered version 7 GUID on .NET 9 or later.</param>
    /// <returns>A new operation identifier.</returns>
    public static OperationId New(bool useVersion7)
    {
#if NET9_0_OR_GREATER
        return new(useVersion7 ? Guid.CreateVersion7() : Guid.NewGuid());
#else
        return new(Guid.NewGuid());
#endif
    }
}
