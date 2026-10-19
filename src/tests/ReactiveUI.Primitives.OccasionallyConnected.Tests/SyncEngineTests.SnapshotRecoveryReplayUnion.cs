// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Replay-union snapshot recovery tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The first byte used in malformed differing replay payloads.</summary>
    private const byte MalformedReplayPayloadByte1 = 9;

    /// <summary>The second byte used in malformed differing replay payloads.</summary>
    private const byte MalformedReplayPayloadByte2 = 8;

    /// <summary>The third byte used in malformed differing replay payloads.</summary>
    private const byte MalformedReplayPayloadByte3 = 7;

    /// <summary>The payload used for malformed differing replay operations.</summary>
    private static readonly byte[] MalformedReplayPayload =
        [MalformedReplayPayloadByte1, MalformedReplayPayloadByte2, MalformedReplayPayloadByte3];

    /// <summary>Verifies conflict and unknown recovery dispositions preserve runtime queue accounting.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryPreservesConflictAndUnknownRuntimeQueueAccounting()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        using var metrics = CreateEngineMetricListener(out var metricCapture);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
        var options = CreateDiagnosticsBatchOptions(
            ExpectedCapacityCommitAttempts,
            PreparedUploadBytes * ExpectedCapacityCommitAttempts);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock, options);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(
            store,
            engine,
            new(),
            clock,
            Stream,
            Subscription,
            receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None);
        var conflict = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(Stream), CancellationToken.None);
        var unknown = await stream.PublishAsync(
            new(SnapshotRecoveryUnknownCounterValue),
            CreateVolatilePublishOptions(Stream),
            CancellationToken.None);
        var unknownStatusBeforeRecovery = await store
            .GetOperationStatusAsync(unknown.OperationId, CancellationToken.None)
            .ConfigureAwait(false);
        if (unknownStatusBeforeRecovery is not { State: var unknownStateBeforeRecovery })
        {
            await Assert.That(unknownStatusBeforeRecovery).IsNotNull();
            return;
        }

        await Assert.That(unknownStateBeforeRecovery).IsEqualTo(SyncOperationState.SavedLocally);
        session.SnapshotRecoveryResult = await CreateConflictUnknownSnapshotRecoveryResultAsync(conflict, unknown);

        await RunSingleSnapshotRecoveryUntilSettledAsync(engine, session, store, faults, releaseGap, releaseRecovery);
        await AssertSnapshotRecoveryCommitDiagnosticsAsync(
            store,
            session,
            faults,
            conflict.OperationId,
            unknown.OperationId,
            unknownStateBeforeRecovery);

        var recovered = await store.RecoverStreamAsync(Stream, Subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        var expectedBytes = SyncEngine.GetOperationRetainedBytes(recovered.PendingOperations[0])
            + SyncEngine.GetOperationRetainedBytes(recovered.PendingOperations[1]);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(conflict.OperationId);
        await Assert.That(recovered.PendingOperations[1].OperationId).IsEqualTo(unknown.OperationId);
        await Assert.That(metricCapture.Sum(QueuePendingMetricName)).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(metricCapture.Sum(QueueBytesMetricName)).IsEqualTo(expectedBytes);
    }

    /// <summary>Verifies accepted replay-only work is requested, proven, and not replayed after restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryRequestsReplayOnlyAcceptedOperationAndDoesNotReplayAfterRestart()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gapState = new ReplayGapState();
        var factory = await CreateReplayOnlyAcceptedSnapshotRecoveryResultFactoryAsync();
        var session = CreateReplayOnlySnapshotRecoverySession(
            releaseGap,
            releaseRecovery,
            static state => state.IsEnabled,
            gapState,
            factory);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);

        try
        {
            await RunReplayOnlyRecoveryWithDisposedStreamAsync(
                store,
                engine,
                session,
                clock,
                releaseGap,
                releaseRecovery,
                gapState.Enable);
            gapState.Disable();
            await AssertReplayOnlyFreshStreamReconstructsAsync(store, engine, session, clock);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
        }
    }

    /// <summary>Verifies duplicate replay overlap is rejected before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsDuplicateReplayPendingOverlapBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync(CreateDuplicatePendingReplayCapture);

    /// <summary>Verifies a same-id replay overlap with different content is rejected before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsDifferingPendingReplayOverlapBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync(CreateDifferingPendingReplayCapture);

    /// <summary>Verifies returned capture union count is bounded before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsOverLimitUniqueReplayUnionBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync(CreateOverLimitReplayUnionCapture);

    /// <summary>Verifies valid pending/replay overlap is normalized at the unique union limit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryAllowsValidPendingReplayOverlapAtUnionLimitBeforeRemoteRecovery() =>
        AssertValidInitialCaptureReachesRemoteAsync(CreateUnionLimitValidOverlapCapture);

    /// <summary>Verifies matching metadata on valid pending/replay overlap reaches remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryAllowsMetadataMatchedPendingReplayOverlapBeforeRemoteRecovery() =>
        AssertValidInitialCaptureReachesRemoteAsync(
            CreateMetadataMatchedUnionLimitOverlapCapture,
            AssertMetadataMatchedOverlapRequestAsync);

    /// <summary>Verifies fresh recapture role validation fails before durable commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SnapshotRecoveryRejectsFreshOverLimitCaptureBeforeCommit()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        store.SnapshotRecoveryCaptureTransform = static (capture, call) => call == ExpectedCapacityCommitAttempts
            ? CreateOverLimitReplayUnionCapture(capture)
            : capture;
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var factory = await CreateUnknownPendingSnapshotRecoveryResultFactoryAsync().ConfigureAwait(false);
        var session = CreateReplayOnlySnapshotRecoverySession(
            releaseGap,
            releaseRecovery,
            static _ => true,
            new(),
            factory);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(
            store,
            engine,
            new(),
            clock,
            Stream,
            Subscription,
            receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(Stream), CancellationToken.None)
            .ConfigureAwait(false);

        await AssertMalformedFreshCaptureRejectedBeforeCommitAsync(
                engine,
                session,
                store,
                faults,
                releaseGap,
                releaseRecovery)
            .ConfigureAwait(false);
    }

    /// <summary>Creates a recovered result with one conflict and one unknown pending disposition.</summary>
    /// <param name="conflict">The operation preserved as conflict pending work.</param>
    /// <param name="unknown">The operation preserved as unknown pending work.</param>
    /// <returns>The recovered snapshot result.</returns>
    private static async ValueTask<RemoteSnapshotRecoveryResult> CreateConflictUnknownSnapshotRecoveryResultAsync(
        PublishReceipt conflict,
        PublishReceipt unknown)
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(
                SnapshotRecoveryCounterStateContractId,
                ExpectedSingleOperation,
                new ReceiveCounterState(1),
                CancellationToken.None)
            .ConfigureAwait(false);
        return new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload),
            OperationDispositions =
            [
                CreateConflictDisposition(conflict.OperationId),
                new() { OperationId = unknown.OperationId, Kind = SnapshotOperationDispositionKind.Unknown },
            ],
        };
    }

    /// <summary>Creates a dynamic-gap session for accepted replay-only recovery.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="emitGap">Returns whether the next subscription should emit a gap.</param>
    /// <param name="gapState">The retained-history gap state.</param>
    /// <param name="resultFactory">The snapshot recovery result factory.</param>
    /// <returns>The configured receive session.</returns>
    private static ReceiveSession CreateReplayOnlySnapshotRecoverySession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Func<ReplayGapState, bool> emitGap,
        ReplayGapState gapState,
        Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult> resultFactory) =>
        new()
        {
            SubscriptionGapFactory = request => emitGap(gapState) ? CreateSnapshotRecoveryGap(request) : null,
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            SubscriptionGapEmissionLimit = ExpectedSingleOperation,
            SnapshotRecoveryResultFactory = resultFactory,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates recovery responses that prove replay-only accepted operation inclusion.</summary>
    /// <returns>The remote recovery result factory.</returns>
    private static async ValueTask<Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult>>
        CreateReplayOnlyAcceptedSnapshotRecoveryResultFactoryAsync()
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(
                SnapshotRecoveryCounterStateContractId,
                ExpectedSingleOperation,
                new ReceiveCounterState(1),
                CancellationToken.None)
            .ConfigureAwait(false);
        return request => new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload, request.StreamId, request.SubscriptionId),
            OperationDispositions = CreateAcceptedReplayOnlyDispositions(request.ReplayOperations),
        };
    }

    /// <summary>Creates accepted dispositions for replay-only operations.</summary>
    /// <param name="operations">The replay-only operations.</param>
    /// <returns>The accepted dispositions.</returns>
    private static SnapshotOperationDisposition[] CreateAcceptedReplayOnlyDispositions(
        IReadOnlyList<SyncOperation> operations)
    {
        var dispositions = new SnapshotOperationDisposition[operations.Count];
        for (var index = 0; index < operations.Count; index++)
        {
            dispositions[index] = CreateAcceptedDisposition(operations[index].OperationId);
        }

        return dispositions;
    }

    /// <summary>Creates an accepted recovery disposition.</summary>
    /// <param name="operationId">The operation identity.</param>
    /// <returns>The accepted disposition.</returns>
    private static SnapshotOperationDisposition CreateAcceptedDisposition(OperationId operationId) =>
        new()
        {
            OperationId = operationId,
            Kind = SnapshotOperationDispositionKind.IncludedAccepted,
            Result = new(
                operationId,
                OperationResultKind.Accepted,
                ReasonCode: null,
                ServerVersion: "snapshot-version"),
        };

    /// <summary>Runs a malformed initial capture and asserts recovery fails before remote request.</summary>
    /// <param name="transform">The malformed capture transform.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMalformedInitialCaptureRejectedBeforeRemoteAsync(
        Func<LocalSnapshotRecoveryCapture, LocalSnapshotRecoveryCapture> transform)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        store.SnapshotRecoveryCaptureTransform = (capture, _) => transform(capture);
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(
            store,
            engine,
            new(),
            clock,
            Stream,
            Subscription,
            receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(Stream), CancellationToken.None)
            .ConfigureAwait(false);
        await AssertInitialCaptureFaultBeforeRemoteAsync(engine, session, faults, releaseGap, releaseRecovery)
            .ConfigureAwait(false);
    }

    /// <summary>Runs a valid initial capture and asserts remote recovery receives disjoint request roles.</summary>
    /// <param name="transform">The capture transform.</param>
    /// <returns>The assertion task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task AssertValidInitialCaptureReachesRemoteAsync(
        Func<LocalSnapshotRecoveryCapture, LocalSnapshotRecoveryCapture> transform) =>
        AssertValidInitialCaptureReachesRemoteAsync(transform, AssertUnionLimitOverlapRequestAsync);

    /// <summary>Runs a valid initial capture and asserts remote recovery receives disjoint request roles.</summary>
    /// <param name="transform">The capture transform.</param>
    /// <param name="assertRequest">The remote request assertion.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertValidInitialCaptureReachesRemoteAsync(
        Func<LocalSnapshotRecoveryCapture, LocalSnapshotRecoveryCapture> transform,
        Func<RemoteSnapshotRecoveryRequest, OperationId, Task> assertRequest)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        store.SnapshotRecoveryCaptureTransform = (capture, _) => transform(capture);
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
        await using var engine = CreateSnapshotRecoveryEngine(store, session, clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryCounterStream(
            store,
            engine,
            new(),
            clock,
            Stream,
            Subscription,
            receiveEnabled: true);
        await stream.StartAsync(CancellationToken.None).ConfigureAwait(false);
        var receipt = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(Stream), CancellationToken.None)
            .ConfigureAwait(false);
        await AssertInitialCaptureReachesRemoteAsync(engine, session, faults, releaseGap, releaseRecovery)
            .ConfigureAwait(false);
        await assertRequest(session.GetSnapshotRecoveryRequest(0), receipt.OperationId)
            .ConfigureAwait(false);
    }

    /// <summary>Asserts a valid initial capture reaches remote recovery.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="faults">The observed faults.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertInitialCaptureReachesRemoteAsync(
        SyncEngine engine,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults,
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery)
    {
        try
        {
            await engine.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            await Assert.That(HasReceivePumpFault(faults, Stream)).IsFalse();
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
        }
    }

    /// <summary>Asserts a malformed initial capture faults before any remote recovery request.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="faults">The observed faults.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertInitialCaptureFaultBeforeRemoteAsync(
        SyncEngine engine,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults,
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery)
    {
        try
        {
            await engine.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseGap.SetResult();
            await WaitForConditionAsync(() =>
                    HasReceivePumpFault(faults, Stream) || session.SnapshotRecoveryEntered.Task.IsCompleted)
                .ConfigureAwait(false);
            await Assert.That(HasReceivePumpFault(faults, Stream)).IsTrue();
            await Assert.That(session.SnapshotRecoveryEntered.Task.IsCompleted).IsFalse();
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(0);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
        }
    }

    /// <summary>Asserts a malformed fresh recapture faults before local commit or acknowledgement.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="store">The instrumented store.</param>
    /// <param name="faults">The observed faults.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMalformedFreshCaptureRejectedBeforeCommitAsync(
        SyncEngine engine,
        ReceiveSession session,
        InstrumentedSnapshotRecoveryStore store,
        RecordingObserver<OccasionallyConnectedFault> faults,
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery)
    {
        try
        {
            await engine.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseRecovery.SetResult();
            await WaitForConditionAsync(() => HasReceivePumpFault<Exception>(faults, Stream) || session.Acknowledgements.Count != 0)
                .ConfigureAwait(false);
            var observed = GetReceivePumpFaultDiagnosticMessage(faults, Stream);
            await Assert.That(observed).IsEqualTo(typeof(SnapshotRecoveryCapacityExceededException).FullName);
            await Assert.That(session.Acknowledgements).IsEmpty();
            await Assert.That(store.LastSnapshotRecoveryCommit).IsNull();
            await Assert.That(store.LastSnapshotRecoveryMutation).IsNull();
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
        }
    }

    /// <summary>Creates recovery responses that preserve all pending operations as unknown.</summary>
    /// <returns>The remote recovery result factory.</returns>
    private static async ValueTask<Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult>>
        CreateUnknownPendingSnapshotRecoveryResultFactoryAsync()
    {
        var payload = await new ReceiveCounterSerializer()
            .SerializeAsync(
                SnapshotRecoveryCounterStateContractId,
                ExpectedSingleOperation,
                new ReceiveCounterState(1),
                CancellationToken.None)
            .ConfigureAwait(false);
        return request => new()
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = CreateRecoveredCheckpoint(payload, request.StreamId, request.SubscriptionId),
            OperationDispositions = CreateUnknownDispositions(request.PendingOperations),
        };
    }

    /// <summary>Creates unknown dispositions for the supplied operations.</summary>
    /// <param name="operations">The pending operations.</param>
    /// <returns>The unknown dispositions.</returns>
    private static SnapshotOperationDisposition[] CreateUnknownDispositions(IReadOnlyList<SyncOperation> operations)
    {
        var dispositions = new SnapshotOperationDisposition[operations.Count];
        for (var index = 0; index < operations.Count; index++)
        {
            var operationId = operations[index].OperationId;
            dispositions[index] = new() { OperationId = operationId, Kind = SnapshotOperationDispositionKind.Unknown };
        }

        return dispositions;
    }

    /// <summary>Creates a capture whose replay role duplicates a pending-overlap operation.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateDuplicatePendingReplayCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        return capture with { ReplayOperations = [pending, pending] };
    }

    /// <summary>Creates a capture whose replay overlap has the same identity but different payload.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateDifferingPendingReplayCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        var changed = pending with { Payload = CreateDifferentPayload(pending.Payload) };
        return capture with { ReplayOperations = [changed] };
    }

    /// <summary>Creates a capture whose valid overlap reaches the unique operation limit.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The transformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateUnionLimitValidOverlapCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        var replay = CreateUnionLimitReplayOperations(pending);
        return capture with { ReplayOperations = replay };
    }

    /// <summary>Creates a capture whose valid metadata overlap reaches the unique operation limit.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The transformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateMetadataMatchedUnionLimitOverlapCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture) with
        {
            Metadata = new Dictionary<string, string> { ["m"] = "a" },
        };
        var replay = CreateUnionLimitReplayOperations(pending);
        return capture with { PendingOperations = [pending], ReplayOperations = replay };
    }

    /// <summary>Creates valid replay operations with the first operation overlapping pending.</summary>
    /// <param name="pending">The pending operation.</param>
    /// <returns>The replay operations.</returns>
    private static SyncOperation[] CreateUnionLimitReplayOperations(SyncOperation pending)
    {
        var limit = new SnapshotRecoveryLimits().MaximumPendingOperations;
        var replay = new SyncOperation[limit];
        replay[0] = pending;
        for (var index = 1; index < replay.Length; index++)
        {
            replay[index] = pending with
            {
                OperationId = OperationId.New(),
                ClientSequence = pending.ClientSequence + index,
                Metadata = new Dictionary<string, string>(),
            };
        }

        return replay;
    }

    /// <summary>Creates a capture whose unique operation union exceeds the limit.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateOverLimitReplayUnionCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        var replay = new SyncOperation[new SnapshotRecoveryLimits().MaximumPendingOperations];
        for (var index = 0; index < replay.Length; index++)
        {
            replay[index] = pending with
            {
                OperationId = OperationId.New(),
                ClientSequence = pending.ClientSequence + index + 1,
            };
        }

        return capture with { ReplayOperations = replay };
    }

    /// <summary>Creates a payload with the same contract and different content.</summary>
    /// <param name="payload">The original payload.</param>
    /// <returns>The changed payload.</returns>
    private static PayloadEnvelope CreateDifferentPayload(PayloadEnvelope payload) =>
        new(
            payload.ContractId,
            payload.SchemaVersion,
            payload.ContentType,
            MalformedReplayPayload,
            $"{payload.PayloadHash}-different");

    /// <summary>Gets the first pending operation from a capture.</summary>
    /// <param name="capture">The recovery capture.</param>
    /// <returns>The first pending operation.</returns>
    /// <exception cref="InvalidOperationException">The capture did not include pending operations.</exception>
    private static SyncOperation RequireFirstPendingOperation(LocalSnapshotRecoveryCapture capture) =>
        capture.PendingOperations.Count == 0
            ? throw new InvalidOperationException("The malformed capture test requires one pending operation.")
            : capture.PendingOperations[0];

    /// <summary>Asserts a valid overlap request has disjoint remote roles at the union limit.</summary>
    /// <param name="request">The remote recovery request.</param>
    /// <param name="pendingOperationId">The pending operation identity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertUnionLimitOverlapRequestAsync(
        RemoteSnapshotRecoveryRequest request,
        OperationId pendingOperationId)
    {
        var limit = new SnapshotRecoveryLimits().MaximumPendingOperations;
        await AssertSnapshotRecoveryRequestAsync(request).ConfigureAwait(false);
        await Assert.That(request.PendingOperations.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(request.PendingOperations[0].OperationId).IsEqualTo(pendingOperationId);
        await Assert.That(request.ReplayOperations.Count).IsEqualTo(limit - ExpectedSingleOperation);
        await Assert.That(ReplayOperationsContain(request, pendingOperationId)).IsFalse();
        await Assert.That(request.PendingOperations.Count + request.ReplayOperations.Count).IsEqualTo(limit);
    }

    /// <summary>Asserts a valid metadata overlap request kept the pending metadata.</summary>
    /// <param name="request">The remote recovery request.</param>
    /// <param name="pendingOperationId">The pending operation identity.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMetadataMatchedOverlapRequestAsync(
        RemoteSnapshotRecoveryRequest request,
        OperationId pendingOperationId)
    {
        await AssertUnionLimitOverlapRequestAsync(request, pendingOperationId).ConfigureAwait(false);
        await Assert.That(request.PendingOperations[0].Metadata.ContainsKey("m")).IsTrue();
        await Assert.That(request.PendingOperations[0].Metadata["m"]).IsEqualTo("a");
    }

    /// <summary>Checks whether remote replay operations contain one operation identity.</summary>
    /// <param name="request">The remote recovery request.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <returns>Whether replay contains the operation.</returns>
    private static bool ReplayOperationsContain(RemoteSnapshotRecoveryRequest request, OperationId operationId)
    {
        for (var index = 0; index < request.ReplayOperations.Count; index++)
        {
            if (request.ReplayOperations[index].OperationId == operationId)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Runs a single retained-history gap recovery until acknowledgement or recovery fault.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="store">The instrumented local store.</param>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The recovery task.</returns>
    private static async Task RunSingleSnapshotRecoveryUntilSettledAsync(
        SyncEngine engine,
        ReceiveSession session,
        InstrumentedSnapshotRecoveryStore store,
        RecordingObserver<OccasionallyConnectedFault> faults,
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery)
    {
        try
        {
            await engine.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseGap.SetResult();
            await session.SnapshotRecoveryEntered.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseRecovery.SetResult();
            await WaitForConditionAsync(() =>
                session.Acknowledgements.Count != 0 || HasSnapshotRecoveryFault(faults, Stream, store))
                .ConfigureAwait(false);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
        }
    }

    /// <summary>Asserts the durable store boundary matched conflict and unknown accounting.</summary>
    /// <param name="store">The instrumented local store.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="conflict">The conflict operation identity.</param>
    /// <param name="unknown">The unknown operation identity.</param>
    /// <param name="unknownStateBeforeRecovery">The unknown operation state before recovery.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The wrapper did not observe the store boundary.</exception>
    private static async Task AssertSnapshotRecoveryCommitDiagnosticsAsync(
        InstrumentedSnapshotRecoveryStore store,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults,
        OperationId conflict,
        OperationId unknown,
        SyncOperationState unknownStateBeforeRecovery)
    {
        var mutation = RequireSnapshotRecoveryMutation(store);
        var commit = RequireSnapshotRecoveryCommit(store);
        var recovered = await store
            .RecoverStreamAsync(Stream, Subscription, CancellationToken.None)
            .ConfigureAwait(false);
        var conflictStatus = await store
            .GetOperationStatusAsync(conflict, CancellationToken.None)
            .ConfigureAwait(false);
        var unknownStatus = await store
            .GetOperationStatusAsync(unknown, CancellationToken.None)
            .ConfigureAwait(false);
        await Assert.That(mutation.OperationDispositions.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(commit.PreservedPendingOperationCount).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(recovered.ServerCursor).IsEqualTo(SnapshotRecoveryRecoveredCursor);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(conflictStatus?.State).IsEqualTo(SyncOperationState.Conflict);
        await Assert.That(unknownStatus?.State).IsEqualTo(unknownStateBeforeRecovery);
        await Assert.That(store.LastSnapshotRecoveryException).IsNull();
        await Assert.That(HasSnapshotRecoveryFault(faults, Stream, store)).IsFalse();
        await Assert.That(session.Acknowledgements.Count).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Runs replay-only recovery with the first stream disposed before reconstruction.</summary>
    /// <param name="store">The real local store.</param>
    /// <param name="engine">The engine under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="enableGap">Enables the retained-history gap.</param>
    /// <returns>The recovery task.</returns>
    private static async Task RunReplayOnlyRecoveryWithDisposedStreamAsync(
        ILocalStoreAdapter store,
        SyncEngine engine,
        ReceiveSession session,
        TimeProvider clock,
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Action enableGap)
    {
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>? stream = null;
        try
        {
            stream = CreateSnapshotRecoveryCounterStream(
                store,
                engine,
                new(),
                clock,
                Stream,
                Subscription,
                receiveEnabled: true);
            await stream.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await engine.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await session.SubscribeEntered.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            var accepted = await PublishAcceptedReplayOnlyOperationAsync(engine, stream, store)
                .ConfigureAwait(false);
            enableGap();
            await RestartStreamIntoSnapshotRecoveryAsync(stream, session, releaseGap).ConfigureAwait(false);
            await AssertReplayOnlySnapshotRecoveryRequestAsync(
                    session.GetSnapshotRecoveryRequest(0),
                    accepted.OperationId)
                .ConfigureAwait(false);
            releaseRecovery.SetResult();
            await session.AcknowledgementEntered.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            await stream.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout)
                .ConfigureAwait(false);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
            if (stream is not null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Asserts fresh construction materializes recovered local state without resending.</summary>
    /// <param name="store">The durable store.</param>
    /// <param name="engine">The running engine.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertReplayOnlyFreshStreamReconstructsAsync(
        ILocalStoreAdapter store,
        SyncEngine engine,
        ReceiveSession session,
        TimeProvider clock)
    {
        await using var stream = CreateSnapshotRecoveryCounterStream(
            store,
            engine,
            new(),
            clock,
            Stream,
            Subscription,
            receiveEnabled: false);
        var local = new RecordingObserver<ReceiveCounterState>();
        using var localSubscription = stream.Local.Subscribe(local);
        await stream.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout).ConfigureAwait(false);
        try
        {
            await AssertRecoveredCounterStateAsync(store, ExpectedSingleOperation).ConfigureAwait(false);
            await AssertLocalCounterStateAsync(local, ExpectedSingleOperation).ConfigureAwait(false);
            var sentBatches = session.SentBatches.Count;
            await engine.TriggerSyncAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout)
                .ConfigureAwait(false);
            await Assert.That(session.SentBatches.Count).IsEqualTo(sentBatches);
        }
        finally
        {
            await stream.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Checks whether a receive-pump fault matches one stream.</summary>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="stream">The recovered stream.</param>
    /// <returns>Whether a matching receive fault was observed.</returns>
    private static bool HasReceivePumpFault(RecordingObserver<OccasionallyConnectedFault> faults, StreamId stream)
    {
        var values = faults.Values;
        for (var index = 0; index < values.Count; index++)
        {
            var fault = values[index];
            if (fault.Code == ReceivePumpFaultCode && fault.StreamId == stream)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Checks whether a receive-pump fault matches one stream and exception type.</summary>
    /// <typeparam name="TException">The expected exception type.</typeparam>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="stream">The recovered stream.</param>
    /// <returns>Whether a matching receive fault was observed.</returns>
    private static bool HasReceivePumpFault<TException>(
        RecordingObserver<OccasionallyConnectedFault> faults,
        StreamId stream)
        where TException : Exception
    {
        var values = faults.Values;
        for (var index = 0; index < values.Count; index++)
        {
            var fault = values[index];
            if (fault.Code == ReceivePumpFaultCode && fault.StreamId == stream && fault.Exception is TException)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Gets the matching receive-pump fault exception type name.</summary>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="stream">The recovered stream.</param>
    /// <returns>The bounded fault diagnostic message, if observed.</returns>
    private static string? GetReceivePumpFaultDiagnosticMessage(
        RecordingObserver<OccasionallyConnectedFault> faults,
        StreamId stream)
    {
        var values = faults.Values;
        for (var index = 0; index < values.Count; index++)
        {
            var fault = values[index];
            if (fault.Code == ReceivePumpFaultCode && fault.StreamId == stream)
            {
                return fault.Exception?.Message;
            }
        }

        return null;
    }

    /// <summary>Checks whether a receive-pump fault matches the snapshot recovery commit path.</summary>
    /// <param name="faults">The observed engine faults.</param>
    /// <param name="stream">The recovered stream.</param>
    /// <param name="store">The instrumented local store.</param>
    /// <returns>Whether a matching fault was observed.</returns>
    private static bool HasSnapshotRecoveryFault(
        RecordingObserver<OccasionallyConnectedFault> faults,
        StreamId stream,
        InstrumentedSnapshotRecoveryStore store)
    {
        var values = faults.Values;
        for (var index = 0; index < values.Count; index++)
        {
            var fault = values[index];
            if (fault.Code == ReceivePumpFaultCode
                && fault.StreamId == stream
                && store.LastSnapshotRecoveryCommit is not null)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Gets the last observed snapshot recovery mutation.</summary>
    /// <param name="store">The instrumented local store.</param>
    /// <returns>The observed recovery mutation.</returns>
    /// <exception cref="InvalidOperationException">Snapshot recovery did not reach mutation.</exception>
    private static LocalSnapshotRecoveryMutation RequireSnapshotRecoveryMutation(
        InstrumentedSnapshotRecoveryStore store) =>
        store.LastSnapshotRecoveryMutation
        ?? throw new InvalidOperationException("Snapshot recovery did not reach the local mutation boundary.");

    /// <summary>Gets the last observed snapshot recovery commit receipt.</summary>
    /// <param name="store">The instrumented local store.</param>
    /// <returns>The observed recovery commit receipt.</returns>
    /// <exception cref="InvalidOperationException">Snapshot recovery did not reach commit.</exception>
    private static LocalSnapshotRecoveryResult RequireSnapshotRecoveryCommit(
        InstrumentedSnapshotRecoveryStore store) =>
        store.LastSnapshotRecoveryCommit
        ?? throw new InvalidOperationException("Snapshot recovery did not reach the local commit receipt.");
}
