// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Creates operation identifiers from random GUIDs.</summary>
internal sealed class GuidOperationIdSource : IOperationIdSource
{
    /// <summary>Gets the shared default source.</summary>
    public static GuidOperationIdSource Instance { get; } = new();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public OperationId New() => OperationId.New();
}
