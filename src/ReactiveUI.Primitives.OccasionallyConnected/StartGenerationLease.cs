// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores a newly created start generation and whether cancellation must launch outside the owning gate.</summary>
/// <param name="Generation">The start generation.</param>
/// <param name="LaunchCancellation">Whether cancellation must be launched outside the owning gate.</param>
internal readonly record struct StartGenerationLease(StartGeneration Generation, bool LaunchCancellation)
{
    /// <summary>Launches cancellation when the lease requested it.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LaunchCancellationIfNeeded()
    {
        if (LaunchCancellation)
        {
            Generation.Cancel();
        }
    }
}
