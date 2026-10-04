// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Exactly-once guarantee expiry and downgrade tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The attempt number sent after an explicit downgrade.</summary>
    private const int GuaranteeSecondAttempt = 2;

    /// <summary>The exactly-once policy used by guarantee tests.</summary>
    private static readonly OperationPolicy ExactlyOncePolicy =
        new(DeliveryGuarantee.ExactlyOnce, OperationDurability.Durable, Priority: 1, ConflictPolicy.Merge);

    /// <summary>The fresh at-least-once retry anchor used by downgrade tests.</summary>
    private static readonly RetryState DowngradeAnchor = RetryState.Start(new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero));

    /// <summary>Verifies guarantee expiry survives reopen and keeps blocking the stream head.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ExpireDeliveryGuaranteeSurvivesReopenAndBlocksTheStreamHead()
    {
        using var database = TempDatabase.Create();
        OperationId operationId;
        SubscriptionId subscriptionId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true, ExactlyOncePolicy);
            operationId = created.Operation.OperationId;
            subscriptionId = created.SubscriptionId;
            _ = await adapter.TryBeginRemoteAttemptAsync(created.Lease.LeaseId, operationId, FirstAttempt, CancellationToken.None);

            var expired = await adapter.ExpireDeliveryGuaranteeAsync(created.Lease.LeaseId, operationId, CancellationToken.None);
            await adapter.ReleaseLeaseAsync(created.Lease.LeaseId, CancellationToken.None);

            await Assert.That(expired.State).IsEqualTo(SyncOperationState.GuaranteeExpired);
            await Assert.That(expired.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeExpired);
            await Assert.That(expired.Attempt).IsEqualTo(FirstAttempt);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var status = await reopened.GetOperationStatusAsync(operationId, CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var leases = await CountLeasesAsync(reopened);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.GuaranteeExpired);
        await Assert.That(status?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeExpired);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(leases).IsEqualTo(0);
    }

    /// <summary>Verifies a downgrade marker and its fresh retry anchor survive later attempts, retryable results and reopen.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DowngradeDeliveryGuaranteeSurvivesAttemptsResultsAndReopen()
    {
        using var database = TempDatabase.Create();
        OperationId operationId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true, ExactlyOncePolicy);
            operationId = created.Operation.OperationId;
            var leaseId = created.Lease.LeaseId;
            _ = await adapter.TryBeginRemoteAttemptAsync(leaseId, operationId, FirstAttempt, CancellationToken.None);

            var downgraded = await adapter.DowngradeDeliveryGuaranteeAsync(leaseId, operationId, DowngradeAnchor, CancellationToken.None);
            var barrier = await adapter.TryBeginRemoteAttemptAsync(leaseId, operationId, GuaranteeSecondAttempt, CancellationToken.None);
            var attempted = await adapter.GetOperationStatusAsync(operationId, CancellationToken.None);
            await adapter.ApplySyncResultAsync(
                leaseId,
                new(leaseId, [new(operationId, OperationResultKind.Retryable, "server-busy", null)], null, null),
                CancellationToken.None);

            await Assert.That(downgraded.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
            await Assert.That(barrier.MaySend).IsTrue();
            await Assert.That(attempted?.State).IsEqualTo(SyncOperationState.Uploading);
            await Assert.That(attempted?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var status = await reopened.GetOperationStatusAsync(operationId, CancellationToken.None);
        var retryState = await reopened.GetRetryStateAsync(operationId, CancellationToken.None);

        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
        await Assert.That(retryState?.StartedUtc).IsEqualTo(DowngradeAnchor.StartedUtc);
    }

    /// <summary>Verifies guarantee transitions reject at-least-once operations and leave their state unchanged.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DeliveryGuaranteeTransitionsRejectAtLeastOnceOperations()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var created = await CreateCommittedOperationAsync(adapter, includeAuthoritativeState: true);
        var operationId = created.Operation.OperationId;

        await Assert.That(async () => await adapter.ExpireDeliveryGuaranteeAsync(created.Lease.LeaseId, operationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await adapter.DowngradeDeliveryGuaranteeAsync(created.Lease.LeaseId, operationId, DowngradeAnchor, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        var status = await adapter.GetOperationStatusAsync(operationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.ReasonCode).IsNull();
        await Assert.That(await adapter.GetRetryStateAsync(operationId, CancellationToken.None)).IsNull();
    }

    /// <summary>Counts the batches a fresh lease request returns.</summary>
    /// <param name="adapter">The adapter.</param>
    /// <returns>The number of leased batches.</returns>
    private static async Task<int> CountLeasesAsync(SqliteLocalStoreAdapter adapter)
    {
        var count = 0;
        await foreach (var batch in adapter.LeasePendingOperationsAsync(new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)), CancellationToken.None))
        {
            _ = batch;
            count++;
        }

        return count;
    }
}
