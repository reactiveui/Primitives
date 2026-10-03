// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Diagnostics integration factories for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Creates a prepared session that returns reordered upload results.</summary>
    /// <param name="accepted">The accepted operation.</param>
    /// <param name="retryable">The retryable operation.</param>
    /// <returns>The prepared session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PreparedSession CreateReorderedDiagnosticsSession(
        SyncOperation accepted,
        SyncOperation retryable)
    {
        return new(ExpectedTwoOperations, DiagnosticsStoreBytes)
            { NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedTwoOperations, DiagnosticsStoreBytes), ResultFactory = CreateResult };

        RemoteSyncResult CreateResult(SyncBatch batch) =>
            CreateReorderedDiagnosticsResult(batch, accepted, retryable);
    }

    /// <summary>Creates an upload result whose operation result order differs from batch order.</summary>
    /// <param name="batch">The uploaded batch.</param>
    /// <param name="accepted">The accepted operation.</param>
    /// <param name="retryable">The retryable operation.</param>
    /// <returns>The reordered upload result.</returns>
    private static RemoteSyncResult CreateReorderedDiagnosticsResult(
        SyncBatch batch,
        SyncOperation accepted,
        SyncOperation retryable) =>
        new(
            batch.BatchId,
            [
                new(
                    accepted.OperationId,
                    OperationResultKind.Accepted,
                    ReasonCode: null,
                    ServerVersion: "v2"),
                new(
                    retryable.OperationId,
                    OperationResultKind.Retryable,
                    ReasonCode: null,
                    ServerVersion: null),
            ],
            serverCursor: null,
            retryAfter: null);

    /// <summary>Creates a larger operation for diagnostics byte-release checks.</summary>
    /// <returns>The larger operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncOperation CreateLargeDiagnosticsOperation() =>
        CreateOperation(
            sequence: FirstSequence + 1,
            payload:
            [
                1,
                DiagnosticsSecondPayloadByte2,
                DiagnosticsSecondPayloadByte3,
                DiagnosticsSecondPayloadByte4,
                DiagnosticsSecondPayloadByte5,
                DiagnosticsSecondPayloadByte6,
                DiagnosticsSecondPayloadByte7,
                DiagnosticsSecondPayloadByte8,
            ],
            operationId: OperationId.New());

    /// <summary>Creates diagnostics options with deterministic upload batching.</summary>
    /// <param name="maximumOperations">The maximum operation count.</param>
    /// <param name="maximumBytes">The maximum retained byte count.</param>
    /// <returns>The diagnostics options.</returns>
    private static OccasionallyConnectedOptions CreateDiagnosticsBatchOptions(
        int maximumOperations,
        long maximumBytes) =>
        CreateDiagnosticsOptions(enabled: true) with
        {
            Batching = OccasionallyConnectedOptions.Default.Batching with
            {
                MaximumOperations = maximumOperations,
                MaximumBytes = maximumBytes,
                MaximumDwellTime = TimeSpan.FromMilliseconds(ExpectedSingleOperation),
            },
        };

    /// <summary>Creates volatile publish options for in-memory real-stream diagnostics fixtures.</summary>
    /// <returns>The volatile publish options.</returns>
    private static RemotePublishOptions CreateVolatilePublishOptions() =>
        new() { StreamId = Stream, Durable = false };

    /// <summary>Creates an upload-only real stream participant for diagnostics publish fixtures.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="coordinator">The engine coordinator.</param>
    /// <param name="projection">The projection under observation.</param>
    /// <param name="timeProvider">The test clock.</param>
    /// <returns>The constructed stream participant.</returns>
    private static OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> CreateUploadOnlyCounterStream(
        ILocalStoreAdapter store,
        IOccasionallyConnectedStreamCoordinator coordinator,
        ReceiveCounterProjection projection,
        TimeProvider timeProvider)
    {
        var serializer = new ReceiveCounterSerializer();
        return new(new()
        {
            Definition = new() { StreamId = Stream, SubscriptionId = Subscription, Projection = projection, InputContractId = "counter-input", StateContractId = ReceiveCounterStateContract },
            Store = store,
            Serializer = serializer,
            TimeProvider = timeProvider,
            OperationIdSource = ReceiveOperationIdSource.Instance,
            Coordinator = coordinator,
            InputProducer = new ReceiveInputProducer(),
            LocalStateSnapshotFactory = static (payload, _) => new(ReceiveCounterSerializer.CreateState(payload)),
            RemoteInputSnapshotFactory = static (payload, _) => new(ReceiveCounterSerializer.CreateInput(payload)),
            NotificationScheduler = InlineObserverScheduler.Instance,
            NotificationOptions = new(ReceiveReplayNotificationCapacity, ReceiveReplayNotificationBytes, ObserverNotificationOverflowMode.CoalesceLatest),
            WorkCapacity = ExpectedCapacityCommitAttempts,
            LocalAdmissionRetainedBytes = PreparedUploadBytes,
            ClientId = "client",
        });
    }

    /// <summary>Creates a valid recovered receive counter snapshot for diagnostics recovery fixtures.</summary>
    /// <returns>The recovered snapshot.</returns>
    private static LocalSnapshot CreateReceiveCounterSnapshot()
    {
        ReadOnlyMemory<byte> payload = new[] { (byte)DiagnosticsRecoveredCounter };
        var envelope = new PayloadEnvelope(
            ReceiveCounterStateContract,
            1,
            ReceiveCounterContentType,
            payload,
            $"hash-{DiagnosticsRecoveredCounter}");

        return new(
            Stream,
            1,
            null,
            envelope,
            DiagnosticsRecoveredRevision,
            DateTimeOffset.UnixEpoch);
    }

    /// <summary>Creates engine options with deterministic diagnostics sampling.</summary>
    /// <param name="enabled">Whether diagnostics are enabled.</param>
    /// <returns>The options.</returns>
    private static OccasionallyConnectedOptions CreateDiagnosticsOptions(bool enabled) =>
        OccasionallyConnectedOptions.Default with
        {
            Diagnostics = OccasionallyConnectedOptions.Default.Diagnostics with
            {
                Enabled = enabled,
                ActivitySamplingRatio = 1D,
            },
        };

    /// <summary>Creates a metrics listener for the engine diagnostics source.</summary>
    /// <param name="capture">The capture receiving measurements.</param>
    /// <returns>The listener.</returns>
    private static MeterListener CreateEngineMetricListener(out EngineMetricCapture capture)
    {
        capture = new();
        var listenerCapture = capture;
        var listener = new MeterListener { InstrumentPublished = EnableEngineInstrument };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => listenerCapture.Add(instrument.Name, value, tags.ToArray()));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => listenerCapture.Add(instrument.Name, value, tags.ToArray()));
        listener.Start();
        return listener;
    }

    /// <summary>Creates a metrics listener that throws from measurement callbacks.</summary>
    /// <returns>The listener.</returns>
    private static MeterListener CreateThrowingEngineMetricListener()
    {
        var listener = new MeterListener { InstrumentPublished = EnableEngineInstrument };
        listener.SetMeasurementEventCallback<long>(static (_, _, _, _) => throw new InvalidOperationException("metric listener failed"));
        listener.SetMeasurementEventCallback<double>(static (_, _, _, _) => throw new InvalidOperationException("metric listener failed"));
        listener.Start();
        return listener;
    }

    /// <summary>Enables measurements for the engine meter.</summary>
    /// <param name="instrument">The published instrument.</param>
    /// <param name="listener">The listener to configure.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EnableEngineInstrument(Instrument instrument, MeterListener listener)
    {
        if (instrument.Meter.Name != EngineDiagnosticsName)
        {
            return;
        }

        listener.EnableMeasurementEvents(instrument);
    }

    /// <summary>Creates an activity listener for the engine diagnostics source.</summary>
    /// <param name="capture">The capture receiving activities.</param>
    /// <returns>The listener.</returns>
    private static ActivityListener CreateEngineActivityListener(out EngineActivityCapture capture)
    {
        capture = new();
        var listenerCapture = capture;
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == EngineDiagnosticsName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => listenerCapture.Add(activity.OperationName, activity.Tags.ToArray()),
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    /// <summary>Creates an activity listener that throws from activity-start callbacks.</summary>
    /// <returns>The listener.</returns>
    private static ActivityListener CreateThrowingEngineActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == EngineDiagnosticsName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = static _ => throw new InvalidOperationException("activity listener failed"),
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    /// <summary>Creates an activity listener that throws from activity-stop callbacks.</summary>
    /// <returns>The listener.</returns>
    private static ActivityListener CreateThrowingEngineActivityStopListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == EngineDiagnosticsName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = static _ => throw new InvalidOperationException("activity stop listener failed"),
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
