// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Prepares side-effect-free domain effects for one authorized operation.</summary>
internal interface IServerOperationHandler
{
    /// <summary>Prepares the terminal operation result from an immutable snapshot.</summary>
    /// <param name="context">The authorized operation context.</param>
    /// <param name="cancellationToken">The token used to cancel preparation.</param>
    /// <returns>The prepared terminal result and domain effects.</returns>
    ValueTask<ServerOperationPreparation> PrepareAsync(
        ServerOperationContext context,
        CancellationToken cancellationToken);
}
