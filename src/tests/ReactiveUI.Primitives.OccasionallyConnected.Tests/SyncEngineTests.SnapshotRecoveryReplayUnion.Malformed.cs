// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Malformed replay-overlap snapshot recovery tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The logical-byte budget for malformed replay-overlap limit tests.</summary>
    private const int ReplayOverlapLogicalLimitBytes = 4096;

    /// <summary>Verifies oversized same-id replay overlap is rejected before payload comparison.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsOversizedReplayOverlapBeforePayloadCompare() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<SnapshotRecoveryCapacityExceededException>(
            CreateOversizedPendingReplayCapture);

    /// <summary>Verifies same-id replay overlap above the logical limit is rejected before payload comparison.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsReplayOverlapAboveLogicalLimitBeforePayloadCompare() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<SnapshotRecoveryCapacityExceededException>(
            CreateLogicalLimitPendingReplayCapture,
            CreateReplayOverlapLogicalLimitOptions());

    /// <summary>Verifies malformed capture identity is rejected before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsMalformedCaptureIdentityBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<ArgumentException>(
            static capture => capture with { StreamId = SnapshotRecoveryOtherStream });

    /// <summary>Verifies fresh pending identity changes fail before local commit or acknowledgement.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsFreshPendingIdentityMismatchBeforeCommit() =>
        AssertMalformedFreshCaptureRejectedBeforeCommitAsync(CreateFreshPendingIdentityMismatchCapture);

    /// <summary>Verifies duplicate pending identities are rejected before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsDuplicatePendingOperationBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<ArgumentException>(
            static capture => capture with { PendingOperations = DuplicateFirstPendingOperation(capture) });

    /// <summary>Verifies foreign pending operations are rejected before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsForeignPendingOperationBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<ArgumentException>(CreateForeignPendingOperationCapture);

    /// <summary>Verifies pending/replay metadata count mismatch is rejected before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsMetadataCountMismatchOverlapBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<ArgumentException>(CreateMetadataCountMismatchCapture);

    /// <summary>Verifies pending/replay metadata value mismatch is rejected before remote recovery.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsMetadataValueMismatchOverlapBeforeRemoteRecovery() =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<ArgumentException>(CreateMetadataValueMismatchCapture);

    /// <summary>Runs a malformed initial capture and asserts a typed receive fault before remote request.</summary>
    /// <typeparam name="TException">The expected receive fault exception type.</typeparam>
    /// <param name="transform">The malformed capture transform.</param>
    /// <returns>The assertion task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<TException>(
        Func<LocalSnapshotRecoveryCapture, LocalSnapshotRecoveryCapture> transform)
        where TException : Exception =>
        AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<TException>(
            transform,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes));

    /// <summary>Runs a malformed initial capture and asserts a typed receive fault before remote request.</summary>
    /// <typeparam name="TException">The expected receive fault exception type.</typeparam>
    /// <param name="transform">The malformed capture transform.</param>
    /// <param name="options">The engine options.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMalformedInitialCaptureRejectedBeforeRemoteAsync<TException>(
        Func<LocalSnapshotRecoveryCapture, LocalSnapshotRecoveryCapture> transform,
        OccasionallyConnectedOptions options)
        where TException : Exception
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var capturedPendingCount = 0;
        store.SnapshotRecoveryCaptureTransform = (capture, _) =>
        {
            capturedPendingCount = capture.PendingOperations.Count;
            return transform(capture);
        };
        var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = CreateSnapshotRecoverySingleGapSession(releaseGap, releaseRecovery);
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
        await stream.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(Stream), CancellationToken.None)
            .ConfigureAwait(false);
        await AssertInitialCaptureFaultBeforeRemoteAsync<TException>(
                engine,
                session,
                faults,
                releaseGap,
                releaseRecovery,
                () => capturedPendingCount)
            .ConfigureAwait(false);
    }

    /// <summary>Asserts a typed malformed initial capture fault before any remote recovery request.</summary>
    /// <typeparam name="TException">The expected receive fault exception type.</typeparam>
    /// <param name="engine">The engine under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="faults">The observed faults.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <param name="getCapturedPendingCount">Gets the pending count seen by the transform.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertInitialCaptureFaultBeforeRemoteAsync<TException>(
        SyncEngine engine,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults,
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Func<int> getCapturedPendingCount)
        where TException : Exception
    {
        try
        {
            await engine.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            releaseGap.SetResult();
            await WaitForConditionAsync(() =>
                    HasReceivePumpFault<Exception>(faults, Stream) || session.SnapshotRecoveryEntered.Task.IsCompleted)
                .ConfigureAwait(false);
            await Assert.That(getCapturedPendingCount()).IsEqualTo(ExpectedSingleOperation);
            var observed = GetReceivePumpFaultDiagnosticMessage(faults, Stream);
            await Assert.That(observed).IsEqualTo(typeof(TException).FullName);
            await Assert.That(session.SnapshotRecoveryEntered.Task.IsCompleted).IsFalse();
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(0);
        }
        finally
        {
            _ = releaseGap.TrySetResult();
            _ = releaseRecovery.TrySetResult();
        }
    }

    /// <summary>Runs malformed fresh recapture and asserts failure before local commit or acknowledgement.</summary>
    /// <param name="transform">The malformed fresh capture transform.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMalformedFreshCaptureRejectedBeforeCommitAsync(
        Func<LocalSnapshotRecoveryCapture, LocalSnapshotRecoveryCapture> transform)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        store.SnapshotRecoveryCaptureTransform = (capture, call) => call == ExpectedCapacityCommitAttempts
            ? transform(capture)
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
        await AssertFreshCaptureFaultBeforeCommitAsync(engine, session, store, faults, releaseGap, releaseRecovery)
            .ConfigureAwait(false);
    }

    /// <summary>Asserts malformed fresh recapture faults before local commit or acknowledgement.</summary>
    /// <param name="engine">The engine under test.</param>
    /// <param name="session">The remote session.</param>
    /// <param name="store">The instrumented store.</param>
    /// <param name="faults">The observed faults.</param>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertFreshCaptureFaultBeforeCommitAsync(
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
                    HasReceivePumpFault<Exception>(faults, Stream) || session.Acknowledgements.Count != 0)
                .ConfigureAwait(false);
            await Assert.That(GetReceivePumpFaultDiagnosticMessage(faults, Stream))
                .IsEqualTo(typeof(InvalidOperationException).FullName);
            await Assert.That(session.SnapshotRecoveryRequestCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(store.CaptureRequestCount).IsEqualTo(ExpectedCapacityCommitAttempts);
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

    /// <summary>Creates a capture whose pending operation belongs to another stream.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateForeignPendingOperationCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture) with { StreamId = SnapshotRecoveryOtherStream };
        return capture with { PendingOperations = [pending] };
    }

    /// <summary>Creates a fresh capture whose pending identity changed while the durable fence did not.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateFreshPendingIdentityMismatchCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture) with { OperationId = OperationId.New() };
        return capture with { PendingOperations = [pending] };
    }

    /// <summary>Duplicates the first pending operation.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The duplicated pending operation array.</returns>
    private static SyncOperation[] DuplicateFirstPendingOperation(LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        return [pending, pending];
    }

    /// <summary>Creates a capture whose pending/replay overlap differs by metadata count.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateMetadataCountMismatchCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        var replay = pending with { Metadata = new Dictionary<string, string> { ["m"] = "a" } };
        return capture with { ReplayOperations = [replay] };
    }

    /// <summary>Creates a capture whose pending/replay overlap differs by metadata value.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateMetadataValueMismatchCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture) with { Metadata = new Dictionary<string, string> { ["m"] = "a" } };
        var replay = pending with { Metadata = new Dictionary<string, string> { ["m"] = "b" } };
        return capture with { PendingOperations = [pending], ReplayOperations = [replay] };
    }

    /// <summary>Creates a capture whose same-id replay overlap has an oversized payload.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateOversizedPendingReplayCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        var payload = new byte[new SnapshotRecoveryLimits().MaximumPayloadBytes + ExpectedSingleOperation];
        var oversized = pending with { Payload = CreateOversizedPayload(pending.Payload, payload) };
        return capture with { ReplayOperations = [oversized] };
    }

    /// <summary>Creates a capture whose same-id replay overlap exceeds only the logical limit.</summary>
    /// <param name="capture">The original capture.</param>
    /// <returns>The malformed capture.</returns>
    private static LocalSnapshotRecoveryCapture CreateLogicalLimitPendingReplayCapture(
        LocalSnapshotRecoveryCapture capture)
    {
        var pending = RequireFirstPendingOperation(capture);
        var payload = new byte[ReplayOverlapLogicalLimitBytes - ExpectedSingleOperation];
        var oversized = pending with { Payload = CreateOversizedPayload(pending.Payload, payload) };
        return capture with { ReplayOperations = [oversized] };
    }

    /// <summary>Creates options where replay payload can fit but replay logical bytes cannot.</summary>
    /// <returns>The configured options.</returns>
    private static OccasionallyConnectedOptions CreateReplayOverlapLogicalLimitOptions() =>
        CreateDiagnosticsBatchOptions(ExpectedSingleOperation, ReplayOverlapLogicalLimitBytes) with
        {
            Security = OccasionallyConnectedOptions.Default.Security with
            {
                MaximumPayloadBytes = ReplayOverlapLogicalLimitBytes,
                MaximumMessageBytes = ReplayOverlapLogicalLimitBytes,
            },
        };

    /// <summary>Creates an oversized payload preserving the original protocol envelope identity.</summary>
    /// <param name="payload">The original payload.</param>
    /// <param name="oversized">The oversized payload bytes.</param>
    /// <returns>The oversized payload envelope.</returns>
    private static PayloadEnvelope CreateOversizedPayload(PayloadEnvelope payload, byte[] oversized) =>
        new(
            payload.ContractId,
            payload.SchemaVersion,
            payload.ContentType,
            oversized,
            JsonPayloadSerializer.ComputePayloadHash(oversized));
}
