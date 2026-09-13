// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes one prepared upload attempt.</summary>
/// <param name="Lease">The already leased operation batch.</param>
/// <param name="Transport">The prepared transport.</param>
/// <param name="Store">The local store owning the lease and durable barriers.</param>
/// <param name="ReconcileAsync">The ordered reconciliation callback invoked after a valid response.</param>
/// <param name="Options">The bounded upload attempt options.</param>
internal sealed record PreparedUploadAttemptRequest(
    LeasedOperationBatch Lease,
    IRemoteTransportBatchPreparer Transport,
    ILocalStoreAdapter Store,
    Func<PreparedUploadReconciliation, CancellationToken, ValueTask> ReconcileAsync,
    PreparedUploadAttemptOptions Options);
