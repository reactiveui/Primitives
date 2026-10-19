// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Replay-only snapshot recovery helper methods for <see cref="SyncEngine"/> tests.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Publishes and uploads one operation so it becomes accepted replay-only work.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="stream">The receive stream.</param>
    /// <param name="store">The real store.</param>
    /// <returns>The accepted publish receipt.</returns>
    private static async Task<PublishReceipt> PublishAcceptedReplayOnlyOperationAsync(
        SyncEngine engine,
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> stream,
        ILocalStoreAdapter store)
    {
        var receipt = await stream.PublishAsync(
                new(ExpectedSingleOperation),
                CreateVolatilePublishOptions(Stream),
                CancellationToken.None)
            .AsTask()
            .WaitAsync(GuardTimeout)
            .ConfigureAwait(false);
        await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout).ConfigureAwait(false);
        await WaitForOperationStateAsync(
            store,
            receipt.OperationId,
            SyncOperationState.Synchronized)
            .ConfigureAwait(false);
        var recovered = await store
            .RecoverStreamAsync(Stream, Subscription, CancellationToken.None)
            .ConfigureAwait(false);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(recovered.ReplayOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        return receipt;
    }

    /// <summary>Restarts the stream after enabling a retained-history gap.</summary>
    /// <param name="stream">The stream under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <returns>The restart task.</returns>
    private static async Task RestartStreamIntoSnapshotRecoveryAsync(
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> stream,
        ReceiveSession session,
        TaskCompletionSource releaseGap)
    {
        try
        {
            await stream.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout)
                .ConfigureAwait(false);
            await stream.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
        }
    }

    /// <summary>Asserts that a snapshot recovery request carries one replay-only accepted operation.</summary>
    /// <param name="request">The recovery request.</param>
    /// <param name="operationId">The replay-only operation identity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertReplayOnlySnapshotRecoveryRequestAsync(
        RemoteSnapshotRecoveryRequest request,
        OperationId operationId)
    {
        await AssertSnapshotRecoveryRequestAsync(request).ConfigureAwait(false);
        await Assert.That(request.PendingOperations).IsEmpty();
        await Assert.That(request.ReplayOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(request.ReplayOperations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(request.MaximumResponseBytes).IsGreaterThan(0);
    }

    /// <summary>Asserts the materialized local counter state after stream restart.</summary>
    /// <param name="observer">The local state observer.</param>
    /// <param name="expectedSum">The expected counter sum.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertLocalCounterStateAsync(
        RecordingObserver<ReceiveCounterState> observer,
        int expectedSum)
    {
        await WaitForConditionAsync(() => observer.Values.Count != 0).ConfigureAwait(false);
        await Assert.That(observer.Values[^1].Sum).IsEqualTo(expectedSum);
    }

    /// <summary>Asserts the recovered durable counter state.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="expectedSum">The expected counter sum.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">Snapshot recovery does not preserve a durable snapshot.</exception>
    private static async Task AssertRecoveredCounterStateAsync(ILocalStoreAdapter store, int expectedSum)
    {
        var recovered = await store
            .RecoverStreamAsync(Stream, Subscription, CancellationToken.None)
            .ConfigureAwait(false);
        var snapshot = recovered.Snapshot;
        await Assert.That(snapshot).IsNotNull();
        if (snapshot is null)
        {
            throw new InvalidOperationException("Snapshot recovery did not preserve a durable snapshot.");
        }

        var state = ReceiveCounterSerializer.CreateState(snapshot.State);
        await Assert.That(state.Sum).IsEqualTo(expectedSum);
        await Assert.That(recovered.PendingOperations).IsEmpty();
        await Assert.That(recovered.ReplayOperations).IsEmpty();
    }

    /// <summary>Tracks dynamic retained-history gap emission for replay-only recovery tests.</summary>
    private sealed class ReplayGapState
    {
        /// <summary>Stores whether the next subscription emits a gap.</summary>
        private int _emitGap;

        /// <summary>Gets whether a gap should be emitted.</summary>
        public bool IsEnabled => Volatile.Read(ref _emitGap) != 0;

        /// <summary>Enables gap emission.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enable() => Volatile.Write(ref _emitGap, 1);

        /// <summary>Disables gap emission.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Disable() => Volatile.Write(ref _emitGap, 0);
    }
}
