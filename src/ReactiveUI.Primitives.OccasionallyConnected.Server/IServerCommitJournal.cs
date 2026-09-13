// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Defines the journal operations required by the internal server operation processor.</summary>
internal interface IServerCommitJournal
{
    /// <summary>Reads a stream snapshot and requested operation replays.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="operationKeys">The operation keys requested for replay.</param>
    /// <returns>The atomic stream snapshot.</returns>
    ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys);

    /// <summary>Attempts to admit a prepared commit.</summary>
    /// <param name="plan">The prepared commit plan.</param>
    /// <returns>The commit result.</returns>
    ServerCommitResult TryCommit(ServerCommitPlan plan);
}
