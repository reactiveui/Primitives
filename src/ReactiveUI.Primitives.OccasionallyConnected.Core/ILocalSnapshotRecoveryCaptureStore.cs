// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Captures a bounded, store-owned snapshot recovery view without mutating local state.</summary>
public interface ILocalSnapshotRecoveryCaptureStore
{
    /// <summary>Captures the current snapshot, cursor, sequence, pending operations, and replay operations for recovery.</summary>
    /// <param name="request">The local capture fences and capacity limits.</param>
    /// <param name="cancellationToken">The token used to cancel capture before the result is returned.</param>
    /// <returns>The bounded immutable capture.</returns>
    ValueTask<LocalSnapshotRecoveryCapture> CaptureSnapshotRecoveryAsync(
        LocalSnapshotRecoveryCaptureRequest request,
        CancellationToken cancellationToken);
}
