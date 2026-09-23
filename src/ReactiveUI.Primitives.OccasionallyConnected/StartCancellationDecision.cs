// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores an accepted start cancellation decision.</summary>
/// <param name="CancellationTask">The shared cancellation drain.</param>
/// <param name="GenerationToCancel">The generation to cancel outside the owning gate.</param>
internal readonly record struct StartCancellationDecision(Task CancellationTask, StartGeneration? GenerationToCancel)
{
    /// <summary>Gets whether cancellation must be launched outside the owning gate.</summary>
    internal bool LaunchCancellation => GenerationToCancel is not null;

    /// <summary>Launches cancellation when the decision carries a generation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void LaunchCancellationIfNeeded() => GenerationToCancel?.Cancel();
}
