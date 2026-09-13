// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a validated response ready for ordered local reconciliation.</summary>
/// <param name="LeaseId">The lease that still owns the result operations.</param>
/// <param name="Batch">The exact prepared synchronization batch.</param>
/// <param name="Result">The validated remote result.</param>
internal sealed record PreparedUploadReconciliation(Guid LeaseId, SyncBatch Batch, RemoteSyncResult Result);
