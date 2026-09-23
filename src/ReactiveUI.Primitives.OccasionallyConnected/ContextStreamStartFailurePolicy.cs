// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Classifies late stream-start failures observed by context-owned tracking.</summary>
internal static class ContextStreamStartFailurePolicy
{
    /// <summary>Gets whether a late stream-start failure is the expected cancellation for its captured generation.</summary>
    /// <param name="exception">The observed stream-start exception.</param>
    /// <param name="generationToken">The token captured when the late stream start was admitted.</param>
    /// <returns><see langword="true"/> when the exception belongs to the canceled generation.</returns>
    internal static bool IsExpectedCancellation(Exception exception, CancellationToken generationToken) =>
        exception is OperationCanceledException cancellation
        && generationToken.IsCancellationRequested
        && cancellation.CancellationToken.Equals(generationToken);
}
