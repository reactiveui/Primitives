// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Validation of exactly-once delivery guarantee transitions.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies an expired lease cannot change an exactly-once operation's guarantee.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DeliveryGuaranteeTransitionsRejectExpiredLeaseAndPreserveStatus()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true, ExactlyOncePolicy);
        var operationId = created.Operation.OperationId;
        var leaseId = created.Lease.LeaseId;
        ExpireLease(database.Path, leaseId);

        await Assert.That(async () => await adapter.ExpireDeliveryGuaranteeAsync(leaseId, operationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await adapter.DowngradeDeliveryGuaranteeAsync(leaseId, operationId, DowngradeAnchor, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();

        var status = await adapter.GetOperationStatusAsync(operationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.ReasonCode).IsNull();
    }

    /// <summary>Verifies an ambiguous exactly-once operation cannot lose its unresolved outcome.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DeliveryGuaranteeTransitionsRejectAmbiguousOperationAndPreserveStatus()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true, ExactlyOncePolicy);
        var operationId = created.Operation.OperationId;
        var leaseId = created.Lease.LeaseId;
        SetOperationState(database.Path, operationId, SyncOperationState.Ambiguous);

        await Assert.That(async () => await adapter.ExpireDeliveryGuaranteeAsync(leaseId, operationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await adapter.DowngradeDeliveryGuaranteeAsync(leaseId, operationId, DowngradeAnchor, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();

        var status = await adapter.GetOperationStatusAsync(operationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(status?.ReasonCode).IsNull();
    }
}
