// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Shared session renewal lifecycle tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies renewal joins receive cancellation already owned by a stopped and restarted stream.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RenewalSkipsReceiveCancellationAlreadyOwnedByStreamStop()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync();
        var resultFactory = await CreateReplayAcceptedPendingUnknownSnapshotResultFactoryAsync();
        var releaseOldGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOldRecovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellationEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var releaseCancellation = new ManualResetEventSlim(false);
        var oldSession = CreateGatedRenewalSession(releaseOldGap, resultFactory, cancellationEntered, releaseCancellation);
        var gatedOldSession = new GatedSnapshotRecoverySession(oldSession, releaseOldRecovery);
        var renewedSession = new ReceiveSession { NegotiatedCapabilities = oldSession.NegotiatedCapabilities };
        var transport = new RecordingTransport { Capabilities = SnapshotRecoveryRemoteFeatures };
        transport.Sessions.Enqueue(gatedOldSession);
        transport.Sessions.Enqueue(renewedSession);
        await using var engine = CreateRecoveryRenewalEngine(store, transport, clock);
        await using var recovering = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, Stream, Subscription, receiveEnabled: true);
        await using var uploading = CreateSnapshotRecoveryCounterStream(store, engine, new(), clock, SnapshotRecoveryOtherStream, SnapshotRecoveryOtherSubscription, receiveEnabled: false);
        await recovering.StartAsync(CancellationToken.None);
        await uploading.StartAsync(CancellationToken.None);
        Task? uploadSync = null;
        Task? stop = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await oldSession.SubscriptionGapReady.Task.WaitAsync(GuardTimeout);
            releaseOldGap.SetResult();
            await gatedOldSession.RecoveryEntered.Task.WaitAsync(GuardTimeout);

            stop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            await cancellationEntered.Task.WaitAsync(GuardTimeout);
            await engine.StartStreamAsync(Stream, CancellationToken.None);
            await Assert.That(stop.IsCompleted).IsFalse();

            var receipt = await uploading.PublishAsync(new(ExpectedSingleOperation), CreateVolatilePublishOptions(SnapshotRecoveryOtherStream), CancellationToken.None);
            uploadSync = engine.TriggerSyncAsync(CancellationToken.None).AsTask();
            await AwaitPausedSendEnteredAsync(oldSession, uploadSync);
            oldSession.ReleasePausedSendAttempt();
            await WaitForOperationStateAsync(store, receipt.OperationId, SyncOperationState.Synchronized);
            await uploadSync.WaitAsync(GuardTimeout);

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(stop.IsCompleted).IsFalse();
            await Assert.That(gatedOldSession.RecoveryCanceled).IsFalse();

            releaseOldRecovery.SetResult();
            releaseCancellation.Set();
            await stop.WaitAsync(GuardTimeout);
            await renewedSession.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            await Assert.That(renewedSession.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            await CompleteStoppedRenewalTestAsync(engine, oldSession, releaseOldGap, releaseOldRecovery, releaseCancellation, stop, uploadSync);
        }
    }

    /// <summary>Verifies a late renewal result cannot become active after global stop begins.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopRejectsRenewalSessionReturnedAfterCancellation()
    {
        var expired = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: null, nextCursor: FirstRenewalCursor) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var candidate = new ReceiveSession();
        var transport = new LateRenewalTransport(expired, candidate);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngineWithTransportAdapter(transport);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await transport.RenewalEntered.Task.WaitAsync(GuardTimeout);
            var stop = engine.StopAsync(CancellationToken.None).AsTask();
            await transport.RenewalCanceled.Task.WaitAsync(GuardTimeout);
            await Assert.That(stop.IsCompleted).IsFalse();

            transport.ReleaseRenewal.SetResult();
            await stop.WaitAsync(GuardTimeout);

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(candidate.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(candidate.SubscribeRequests.Count).IsEqualTo(0);
            await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();
        }
        finally
        {
            _ = transport.ReleaseRenewal.TrySetResult();
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Creates the expiring session with a test-controlled receive cancellation callback.</summary>
    /// <param name="releaseGap">The initial subscription gap gate.</param>
    /// <param name="resultFactory">The snapshot response factory.</param>
    /// <param name="cancellationEntered">The cancellation callback signal.</param>
    /// <param name="releaseCancellation">The cancellation callback release gate.</param>
    /// <returns>The configured remote session.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReceiveSession CreateGatedRenewalSession(
        TaskCompletionSource releaseGap,
        Func<RemoteSnapshotRecoveryRequest, RemoteSnapshotRecoveryResult> resultFactory,
        TaskCompletionSource cancellationEntered,
        ManualResetEventSlim releaseCancellation) =>
        CreateRecoveryRenewalSession(
            releaseGap,
            null,
            resultFactory,
            expiresOnSend: true,
            cancellationCallbackEntered: cancellationEntered,
            releaseCancellationCallback: releaseCancellation);

    /// <summary>Releases all test gates and waits for the stopped receive pump and engine.</summary>
    /// <param name="engine">The running engine.</param>
    /// <param name="oldSession">The expired transport session.</param>
    /// <param name="releaseOldGap">The old subscription gap gate.</param>
    /// <param name="releaseOldRecovery">The old recovery request gate.</param>
    /// <param name="releaseCancellation">The receive cancellation callback gate.</param>
    /// <param name="stop">The optional stream stop task.</param>
    /// <param name="uploadSync">The optional upload synchronization task.</param>
    /// <returns>The cleanup task.</returns>
    private static async Task CompleteStoppedRenewalTestAsync(
        SyncEngine engine,
        ReceiveSession oldSession,
        TaskCompletionSource releaseOldGap,
        TaskCompletionSource releaseOldRecovery,
        ManualResetEventSlim releaseCancellation,
        Task? stop,
        Task? uploadSync)
    {
        _ = releaseOldGap.TrySetResult();
        _ = releaseOldRecovery.TrySetResult();
        releaseCancellation.Set();
        oldSession.ReleasePausedSendAttempt();
        if (stop is not null)
        {
            await stop.WaitAsync(GuardTimeout);
        }

        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await AwaitTriggerCompletionAsync(uploadSync);
    }

    /// <summary>Returns a renewal candidate only after the test observes stop cancellation.</summary>
    /// <param name="initial">The initial session.</param>
    /// <param name="candidate">The late renewal candidate.</param>
    private sealed class LateRenewalTransport(ReceiveSession initial, ReceiveSession candidate) : IRemoteTransportAdapter
    {
        /// <summary>Counts transport connection attempts.</summary>
        private int _connectCalls;

        /// <summary>Gets the signal raised when renewal starts.</summary>
        public TaskCompletionSource RenewalEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal raised when stop cancels renewal.</summary>
        public TaskCompletionSource RenewalCanceled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the gate that releases the late candidate.</summary>
        public TaskCompletionSource ReleaseRenewal { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the observed connection count.</summary>
        public int ConnectCalls => Volatile.Read(ref _connectCalls);

        /// <inheritdoc/>
        public RemoteTransportCapabilities Capabilities => RecordingTransportUploadCapabilities;

        /// <inheritdoc/>
        public async ValueTask<IRemoteTransportSession> ConnectAsync(
            TransportConnectRequest request,
            CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _connectCalls) == ExpectedSingleOperation)
            {
                return initial;
            }

            await using var cancellation = cancellationToken.UnsafeRegister(
                static state => _ = ((TaskCompletionSource)state!).TrySetResult(),
                RenewalCanceled);
            _ = RenewalEntered.TrySetResult();
            await ReleaseRenewal.Task.ConfigureAwait(false);
            return candidate;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Holds a recovery request after admission so stream cancellation cannot complete it before renewal.</summary>
    /// <param name="inner">The delegated remote session.</param>
    /// <param name="releaseRecovery">The recovery request release gate.</param>
    private sealed class GatedSnapshotRecoverySession(ReceiveSession inner, TaskCompletionSource releaseRecovery) :
        IRemoteTransportSession, IRemoteTransportBatchPreparer, IRemoteSnapshotRecoverySession
    {
        /// <summary>Gets the signal raised when recovery reaches the remote session.</summary>
        public TaskCompletionSource RecoveryEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a value indicating whether the held recovery observed cancellation.</summary>
        public bool RecoveryCanceled { get; private set; }

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities => inner.NegotiatedCapabilities;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            inner.PushAsync(batch, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
            inner.SubscribeAsync(request, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            inner.AcknowledgeAsync(acknowledgement, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            inner.PreparePushAsync(batch, cancellationToken);

        /// <inheritdoc/>
        public async ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken)
        {
            _ = RecoveryEntered.TrySetResult();
            await releaseRecovery.Task.ConfigureAwait(false);
            RecoveryCanceled = cancellationToken.IsCancellationRequested;
            return await inner.GetSnapshotAsync(request, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
