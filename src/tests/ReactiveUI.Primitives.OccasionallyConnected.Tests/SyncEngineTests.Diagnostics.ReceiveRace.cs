// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Receive-side diagnostics race helpers for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Applies one real remote batch so later completion batches have an authoritative checkpoint.</summary>
    /// <param name="stream">The typed stream under test.</param>
    /// <param name="store">The backing local store.</param>
    /// <returns>The checkpoint task.</returns>
    private static async ValueTask SeedReceiveAuthoritativeCheckpointAsync(
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> stream,
        SqliteLocalStoreAdapter store)
    {
        var result = await stream.ApplyRemoteBatchAsync(CreateReceiveBatch(), CancellationToken.None);
        await Assert.That(result.Receipt.NextCursor).IsEqualTo(ReceiveCursor);
        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(recovered.ServerCursor).IsEqualTo(ReceiveCursor);
    }

    /// <summary>Creates a trace with the unredacted participant apply exception.</summary>
    /// <param name="participant">The capturing participant.</param>
    /// <returns>The captured exception trace.</returns>
    private static string CreateCapturedRemoteApplyTrace(CapturingRemoteApplyParticipant participant) =>
        participant.ApplyException is null
            ? $"participantApply={MissingTraceValue}"
            : $"participantApply={participant.ApplyException.GetType().Name}:{participant.ApplyException.Message}";

    /// <summary>Uploads a receive-included operation and verifies the terminal result releases queue metrics once.</summary>
    /// <param name="store">The backing local store.</param>
    /// <param name="clock">The shared manual clock.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertTerminalUploadAcknowledgementReleasesQueueOnceAsync(
        ILocalStoreAdapter store,
        ManualTimerTimeProvider clock)
    {
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var operationStates = new RecordingObserver<SyncOperationStatus>();
        var session = new PreparedSession(ExpectedSingleOperation, DiagnosticsStoreBytes);
        await using var engine = CreateEngine(
            store,
            new() { SessionOverride = session },
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, DiagnosticsStoreBytes),
            timeProvider: clock);
        var coordinator = new CapturingStreamCoordinator(engine);
        await using var stream = CreateUploadOnlyCounterStream(store, coordinator, new(), clock);
        using var registration = engine.RegisterParticipant(coordinator.Participant);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var operationSubscription = engine.OperationStates.Subscribe(operationStates);
        await engine.StartAsync(CancellationToken.None);
        await TriggerAndDrainUploadWithTraceAsync(engine, clock, session, faults, operationStates);
        var pendingMeasurements = metricCapture.GetMeasurements()
            .Where(static item => item.Name == QueuePendingMetricName)
            .Select(static item => item.Value)
            .ToArray();
        await Assert.That(pendingMeasurements.Count(static item => Math.Abs(item - ExpectedSingleOperation) <= double.Epsilon))
            .IsEqualTo(ExpectedSingleOperation);
        await Assert.That(pendingMeasurements.Count(static item => Math.Abs(item + ExpectedSingleOperation) <= double.Epsilon))
            .IsEqualTo(ExpectedSingleOperation);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(0);
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Participant wrapper that captures the real typed receive apply exception.</summary>
    /// <param name="inner">The captured typed stream participant.</param>
    private sealed class CapturingRemoteApplyParticipant(IOccasionallyConnectedStreamParticipant inner) : IOccasionallyConnectedStreamParticipant
    {
        /// <inheritdoc/>
        public StreamId StreamId => inner.StreamId;

        /// <summary>Gets the unredacted exception observed from the typed receive apply path.</summary>
        internal Exception? ApplyException { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ReceiveStreamSubscription?> PrepareReceiveAsync(CancellationToken cancellationToken) =>
            inner.PrepareReceiveAsync(cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> CommitSerializedAsync(SyncOperation operation, CancellationToken cancellationToken) =>
            inner.CommitSerializedAsync(operation, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantQueueTransitionResult> ApplySyncResultAsync(
            SyncBatch batch,
            RemoteSyncResult result,
            CancellationToken cancellationToken) =>
            inner.ApplySyncResultAsync(batch, result, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<ParticipantRemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            CancellationToken cancellationToken)
        {
            try
            {
                return await inner.ApplyRemoteBatchAsync(batch, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                ApplyException = exception;
                throw;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantQueueTransitionResult> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            CancellationToken cancellationToken) =>
            inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, cancellationToken);
    }
}
