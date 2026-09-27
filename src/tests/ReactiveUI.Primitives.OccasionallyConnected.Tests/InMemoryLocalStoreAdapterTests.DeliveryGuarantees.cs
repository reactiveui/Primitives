// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies exactly-once guarantee expiry and downgrade transitions.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The attempt number sent after an explicit downgrade.</summary>
    private const int GuaranteeSecondAttempt = 2;

    /// <summary>The exactly-once policy used by guarantee tests.</summary>
    private static readonly OperationPolicy ExactlyOncePolicy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce };

    /// <summary>Verifies guarantee expiry records a blocking terminal state that stops later leases.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ExpireDeliveryGuaranteeStopsTheStreamHead()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText, ExactlyOncePolicy);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);

        var expired = await store.ExpireDeliveryGuaranteeAsync(lease.LeaseId, operation.OperationId, CancellationToken.None);
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var next = await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(expired.State).IsEqualTo(SyncOperationState.GuaranteeExpired);
        await Assert.That(expired.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeExpired);
        await Assert.That(expired.Attempt).IsEqualTo(1);
        await Assert.That(status).IsEqualTo(expired);
        await Assert.That(next).IsNull();
    }

    /// <summary>Verifies a downgrade keeps its marker across the next attempt and a retryable result.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DowngradeDeliveryGuaranteeKeepsMarkerWhileInFlight()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText, ExactlyOncePolicy);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(ExpiredBlockerAdvanceMinutes));
        lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        var anchor = RetryState.Start(clock.GetUtcNow());

        var downgraded = await store.DowngradeDeliveryGuaranteeAsync(lease.LeaseId, operation.OperationId, anchor, CancellationToken.None);
        _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, GuaranteeSecondAttempt, CancellationToken.None);
        var attempted = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await store.ApplySyncResultAsync(
            lease.LeaseId,
            new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Retryable, "server-busy", null)], null, null),
            CancellationToken.None);
        var retried = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);

        await Assert.That(downgraded.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
        await Assert.That(await store.GetRetryStateAsync(operation.OperationId, CancellationToken.None)).IsEqualTo(anchor);
        await Assert.That(attempted?.State).IsEqualTo(SyncOperationState.Uploading);
        await Assert.That(attempted?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
        await Assert.That(retried?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(retried?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeDowngraded);
    }

    /// <summary>Verifies guarantee transitions reject operations that are not exactly-once.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DeliveryGuaranteeTransitionsRejectAtLeastOnceOperations()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(async () => await store.ExpireDeliveryGuaranteeAsync(lease.LeaseId, operation.OperationId, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(async () => await store.DowngradeDeliveryGuaranteeAsync(
                lease.LeaseId,
                operation.OperationId,
                RetryState.Start(clock.GetUtcNow()),
                CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(status?.ReasonCode).IsNull();
    }
}
