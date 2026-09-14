// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes trusted server proof for one pending snapshot recovery operation.</summary>
internal sealed record ServerSnapshotOperationDisposition
{
    /// <summary>Gets the requested pending operation identifier.</summary>
    internal required OperationId OperationId { get; init; }

    /// <summary>Gets the externally visible disposition kind.</summary>
    internal required SnapshotOperationDispositionKind Kind { get; init; }

    /// <summary>Gets the retained terminal result when one is proven.</summary>
    internal required OperationSyncResult? Result { get; init; }

    /// <summary>Gets the retained operation fingerprint when the terminal proof is known.</summary>
    internal required ServerCommitFingerprint? Fingerprint { get; init; }
}
