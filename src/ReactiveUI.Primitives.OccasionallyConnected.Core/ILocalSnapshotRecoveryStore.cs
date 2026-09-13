// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Persists a snapshot recovery mutation as one local transaction.</summary>
public interface ILocalSnapshotRecoveryStore
{
    /// <summary>Atomically applies a snapshot checkpoint and rebuilt optimistic state after a retained-history gap.</summary>
    /// <param name="mutation">The validated local snapshot recovery mutation.</param>
    /// <param name="cancellationToken">The token used to cancel persistence before commit.</param>
    /// <returns>The durable local snapshot recovery result.</returns>
    /// <remarks>
    /// Store implementations enforce local ownership, checkpoint stream and subscription binding, and transaction
    /// fences. The caller supplies the checkpoint from an authenticated remote response; the server validates its
    /// durable cursor offer when acknowledging recovery. <see cref="SnapshotRecoveryValidator"/> performs structural
    /// checks only and does not prove remote cursor authenticity.
    /// </remarks>
    ValueTask<LocalSnapshotRecoveryResult> ApplySnapshotRecoveryAsync(
        LocalSnapshotRecoveryMutation mutation,
        CancellationToken cancellationToken);
}
