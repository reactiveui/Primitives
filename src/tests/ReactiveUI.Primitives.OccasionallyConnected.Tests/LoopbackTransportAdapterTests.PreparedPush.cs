// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LoopbackTransportAdapter"/>.</summary>
/// <content>Prepared push tests for <see cref="LoopbackTransportAdapter"/>.</content>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>Verifies preparing a push validates and reserves without calling the hub until send.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparePushAsyncValidatesReservesAndSendCallsHubOnce()
    {
        var batch = CreateBatch();
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentRequests = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var preparer = (IRemoteTransportBatchPreparer)session;

        await using var prepared = await preparer.PreparePushAsync(batch, CancellationToken.None);
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => preparer.PreparePushAsync(CreateBatch(), CancellationToken.None).AsTask());
        await Assert.That(prepared.Batch).IsSameReferenceAs(batch);
        await Assert.That(prepared.EncodedSizeBytes).IsGreaterThan(0);
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);

        var result = await prepared.SendAsync(CancellationToken.None);

        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
        await Assert.That(hub.ApplyBatch).IsSameReferenceAs(batch);
    }

    /// <summary>Verifies negotiated batch bytes count payload bytes separately from logical loopback encoded bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparePushAsyncAllowsEncodedSizeAboveNegotiatedPayloadBytes()
    {
        var maximumPayloadBytes = OperationPayload.Length;
        var batch = CreateBatch();
        var hub = new RecordingHub();
        var options = CreateOptions(hub) with
        {
            PeerCapabilities = CreateCapabilities(maximumBytes: maximumPayloadBytes),
            MaximumLogicalBatchBytes = DefaultBatchBytes,
        };
        await using var adapter = new LoopbackTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);

        await Assert.That(prepared.EncodedSizeBytes).IsGreaterThan(maximumPayloadBytes);
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);
    }

    /// <summary>Verifies negotiated batch bytes reject summed operation payload bytes before the hub is called.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparePushAsyncRejectsPayloadBytesAboveNegotiatedLimit()
    {
        var maximumPayloadBytes = OperationPayload.Length - 1;
        var hub = new RecordingHub();
        var options = CreateOptions(hub) with { PeerCapabilities = CreateCapabilities(maximumBytes: maximumPayloadBytes) };
        await using var adapter = new LoopbackTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => ((IRemoteTransportBatchPreparer)session).PreparePushAsync(CreateBatch(), CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("negotiated payload byte bounds");
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);
    }

    /// <summary>Verifies disposing an idle prepared push releases the reserved request slot.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushDisposeReleasesIdleReservation()
    {
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentRequests = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var preparer = (IRemoteTransportBatchPreparer)session;
        var first = await preparer.PreparePushAsync(CreateBatch(), CancellationToken.None);

        await first.DisposeAsync();
        await first.DisposeAsync();
        await using var second = await preparer.PreparePushAsync(CreateBatch(), CancellationToken.None);

        await Assert.That(second.EncodedSizeBytes).IsGreaterThan(0);
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);
    }

    /// <summary>Verifies prepared pushes are one-shot and do not repeat server effects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushSendIsOneShot()
    {
        var batch = CreateBatch();
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);

        _ = await prepared.SendAsync(CancellationToken.None);
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => prepared.SendAsync(CancellationToken.None).AsTask());

        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
        await Assert.That(hub.UniqueServerEffects).IsEqualTo(1);
    }

    /// <summary>Verifies prepared push send honors an explicit uncanceled send token.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushSendAcceptsCancelableToken()
    {
        using var sendCancellation = new CancellationTokenSource();
        var batch = CreateBatch();
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);

        var result = await prepared.SendAsync(sendCancellation.Token);

        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
    }

    /// <summary>Verifies a canceled prepared send does not invoke the hub.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushSendCanceledBeforeHubLeavesHubUncalled()
    {
        using var sendCancellation = new CancellationTokenSource();
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(CreateBatch(), CancellationToken.None);
        await sendCancellation.CancelAsync().ConfigureAwait(false);

        await AssertCancelsAsync(prepared.SendAsync(sendCancellation.Token).AsTask());
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);
    }

    /// <summary>Verifies disposed prepared pushes clear their retained batch reference.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushBatchThrowsAfterDispose()
    {
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(CreateBatch(), CancellationToken.None);

        await prepared.DisposeAsync();

        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(
            () =>
            {
                _ = prepared.Batch;
                return Task.CompletedTask;
            });
    }

    /// <summary>Verifies overlapping prepared sends fail without a second hub call.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushRejectsOverlappingSendsWithoutDoubleEffects()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);
        var first = prepared.SendAsync(CancellationToken.None).AsTask();

        try
        {
            await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
            _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => prepared.SendAsync(CancellationToken.None).AsTask());
            await Assert.That(hub.ApplyCalls).IsEqualTo(1);
        }
        finally
        {
            _ = release.TrySetResult();
            await first.ConfigureAwait(false);
        }

        await Assert.That(hub.UniqueServerEffects).IsEqualTo(1);
    }

    /// <summary>Verifies disposing an active prepared push waits for the active send and then clears the retained batch.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushDisposeWaitsForActiveSendAndClearsBatch()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);
        var send = prepared.SendAsync(CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        var dispose = prepared.DisposeAsync().AsTask();
        await Assert.That(dispose.IsCompleted).IsFalse();
        _ = release.TrySetResult();
        _ = await send.ConfigureAwait(false);
        await dispose.ConfigureAwait(false);

        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(
            () =>
            {
                _ = prepared.Batch;
                return Task.CompletedTask;
            });
    }

    /// <summary>Verifies session disposal releases idle prepared pushes and cancels active prepared sends.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeReleasesIdlePreparedPushesAndDrainsActivePreparedSend()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentRequests = DualPreparedRequestSlots });
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var preparer = (IRemoteTransportBatchPreparer)session;
        var idle = await preparer.PreparePushAsync(CreateBatch(), CancellationToken.None);
        await using var active = await preparer.PreparePushAsync(batch, CancellationToken.None);
        var send = active.SendAsync(CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        await session.DisposeAsync();
        await idle.DisposeAsync();

        await AssertCancelsAsync(send);
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => idle.SendAsync(CancellationToken.None).AsTask());
        await using var next = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await Assert.That(next.NegotiatedCapabilities).IsEqualTo(CreateCapabilities());
    }

    /// <summary>Verifies disposing a prepared handle during session disposal waits for its active send to drain.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushDisposeDuringSessionDisposalWaitsForActiveSend()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource cancellationObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseAfterDispose = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    _ = cancellationObserved.TrySetResult();
                    await releaseAfterDispose.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
                    throw;
                }

                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var active = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);
        var send = active.SendAsync(CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        var sessionDispose = session.DisposeAsync().AsTask();
        await cancellationObserved.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        var activeDispose = active.DisposeAsync().AsTask();

        await Assert.That(activeDispose.IsCompleted).IsFalse();
        _ = releaseAfterDispose.TrySetResult();
        await AssertCancelsAsync(send);
        await sessionDispose.ConfigureAwait(false);
        await activeDispose.ConfigureAwait(false);
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(
            () =>
            {
                _ = active.Batch;
                return Task.CompletedTask;
            });
    }

    /// <summary>Verifies caller-started disposal shares the active send drain when session disposal follows.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparedPushDisposeBeforeSessionDisposalSharesActiveSendDrain()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource cancellationObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseAfterDispose = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    _ = cancellationObserved.TrySetResult();
                    await releaseAfterDispose.Task.WaitAsync(GateTimeout, CancellationToken.None).ConfigureAwait(false);
                    throw;
                }

                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var active = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);
        var send = active.SendAsync(CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        var activeDispose = active.DisposeAsync().AsTask();
        await Assert.That(activeDispose.IsCompleted).IsFalse();
        var sessionDispose = session.DisposeAsync().AsTask();
        await cancellationObserved.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        await Assert.That(activeDispose.IsCompleted).IsFalse();
        _ = releaseAfterDispose.TrySetResult();

        await AssertCancelsAsync(send);
        await sessionDispose.ConfigureAwait(false);
        await activeDispose.ConfigureAwait(false);
    }

    /// <summary>Verifies an oversized prepared push fails before a real store attempt barrier is recorded.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparePushAsyncOversizeLeavesRealStoreAttemptAtZero()
    {
        await using var store = await CreatePreparedPushStoreAsync();
        var lease = await CreatePreparedPushLeaseAsync(store);
        var operation = lease.Operations[0];
        var oversized = new SyncBatch(lease.LeaseId, [operation with { Metadata = CreateLargeMetadata() }]);
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumLogicalBatchBytes = SmallBatchBytes });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => ((IRemoteTransportBatchPreparer)session).PreparePushAsync(oversized, CancellationToken.None).AsTask());

        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.Attempt).IsEqualTo(0);
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);
    }

    /// <summary>Verifies a prepared push can be sent after a real store attempt barrier and reaches the hub once.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreparePushAsyncSuccessAfterRealStoreBarrierCallsHubOnce()
    {
        await using var store = await CreatePreparedPushStoreAsync();
        var lease = await CreatePreparedPushLeaseAsync(store);
        var operation = lease.Operations[0];
        var batch = new SyncBatch(lease.LeaseId, lease.Operations);
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await using var prepared = await ((IRemoteTransportBatchPreparer)session).PreparePushAsync(batch, CancellationToken.None);

        var barrier = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);
        var result = await prepared.SendAsync(CancellationToken.None);

        await Assert.That(barrier.MaySend).IsTrue();
        await Assert.That(barrier.Attempt).IsEqualTo(1);
        await Assert.That(result.BatchId).IsEqualTo(lease.LeaseId);
        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
        await Assert.That(hub.ApplyBatch).IsSameReferenceAs(batch);
        var status = await store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(status?.Attempt).IsEqualTo(1);
    }

    /// <summary>Creates a real local store with one committed operation for prepared push ordering tests.</summary>
    /// <returns>The initialized store.</returns>
    private static async Task<InMemoryLocalStoreAdapter> CreatePreparedPushStoreAsync()
    {
        var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(TrustedClientId, 1, false) { ClientId = TrustedClientId }, CancellationToken.None);
        _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateBatch().Operations[0];
        var snapshot = new SnapshotMutation(Stream, CreatePayload(), 1);
        _ = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        return store;
    }

    /// <summary>Leases the pending prepared push operation from a real local store.</summary>
    /// <param name="store">The real local store.</param>
    /// <returns>The leased operation batch.</returns>
    /// <exception cref="InvalidOperationException">The store did not return a pending batch.</exception>
    private static async Task<LeasedOperationBatch> CreatePreparedPushLeaseAsync(InMemoryLocalStoreAdapter store)
    {
        LeasedOperationBatch? pending = null;
        await foreach (var lease in store.LeasePendingOperationsAsync(new(Stream, 1, DefaultBatchBytes, TimeSpan.FromMinutes(1)), CancellationToken.None))
        {
            pending = lease;
        }

        return pending ?? throw new InvalidOperationException("Expected a prepared push lease.");
    }
}
