// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Defines the internal atomic server commit journal surface used by server processors.</summary>
internal interface IServerCommitJournal
{
    /// <summary>Reads a stream revision and requested terminal operation entries atomically.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="operationKeys">The bounded operation keys requested for replay.</param>
    /// <returns>The atomic stream snapshot.</returns>
    ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys);

    /// <summary>Attempts to atomically admit a fully prepared terminal server commit.</summary>
    /// <param name="plan">The prepared commit plan.</param>
    /// <returns>The result and atomic stream snapshot observed by the attempt.</returns>
    ServerCommitResult TryCommit(ServerCommitPlan plan);
}
