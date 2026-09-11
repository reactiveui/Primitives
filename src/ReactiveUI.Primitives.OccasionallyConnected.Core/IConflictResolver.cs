// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Resolves conflicts deterministically within the server's atomic apply transaction.</summary>
/// <remarks>
/// A resolver must not perform network I/O, mutate its input, or modify external state. Its effects are represented by
/// the returned decision and events, which the server validates and commits atomically before acknowledging the client.
/// </remarks>
public interface IConflictResolver
{
    /// <summary>Resolves the ordered incoming operations against the canonical server state.</summary>
    /// <param name="context">The immutable transaction input.</param>
    /// <param name="cancellationToken">The token used to cancel resolution before commit.</param>
    /// <returns>The canonical conflict decision.</returns>
    ValueTask<ConflictResolutionResult> ResolveAsync(ConflictContext context, CancellationToken cancellationToken);
}
