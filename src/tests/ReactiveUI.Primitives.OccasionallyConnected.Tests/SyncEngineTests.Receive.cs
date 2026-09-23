// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Receive pump tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The cursor used by receive pump tests.</summary>
    private const string ReceiveCursor = "receive-cursor-1";

    /// <summary>The receive pump fault code.</summary>
    private const string ReceivePumpFaultCode = "OC.Engine.ReceivePump";

    /// <summary>The retry-after delay in seconds used by receive tests.</summary>
    private const int ReceiveRetryAfterSeconds = 3;

    /// <summary>The short retry delay in milliseconds used by receive tests.</summary>
    private const int ReceiveShortRetryMilliseconds = 100;

    /// <summary>The early retry advance in seconds used by receive tests.</summary>
    private const int ReceiveEarlyAdvanceSeconds = 2;

    /// <summary>The in-memory receive replay record cap.</summary>
    private const int ReceiveReplayRecordCount = 128;

    /// <summary>The in-memory receive replay byte cap.</summary>
    private const int ReceiveReplayStoreBytes = 4096;

    /// <summary>The receive replay notification capacity.</summary>
    private const int ReceiveReplayNotificationCapacity = 8;

    /// <summary>The receive replay notification byte cap.</summary>
    private const int ReceiveReplayNotificationBytes = 1024;

    /// <summary>Verifies unregistering a blocked receive pump cancels it without clearing a new registration for the same participant instance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnregisterBlockedReceivePumpPreservesReregisteredParticipantPump()
    {
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCanceledSubscribe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateReceiveBatch();
        var session = new ReceiveSession { ReleaseBatches = releaseBatches, ReleaseSubscribeCancellation = releaseCanceledSubscribe };
        session.SubscriptionBatches.Enqueue([]);
        session.SubscriptionBatches.Enqueue([batch]);
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        var firstRegistration = engine.RegisterParticipant(participant);

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedSingleOperation);

            firstRegistration.Dispose();
            await session.SubscribeCanceled.Task.WaitAsync(GuardTimeout);
            using var secondRegistration = engine.RegisterParticipant(participant);
            await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

            releaseCanceledSubscribe.SetResult();
            await WaitForConditionAsync(() => session.SubscribeCompletedCount == ExpectedSingleOperation);
            await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);

            releaseBatches.SetResult();
            await session.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

            await Assert.That(participant.RemoteApplyCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(participant.AppliedRemoteBatches[0]).IsSameReferenceAs(batch);
            await Assert.That(session.Acknowledgements.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.DisposeCalls).IsEqualTo(0);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedCapacityCommitAttempts);

            await engine.StopAsync(CancellationToken.None);
            await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SubscribeCompletedCount).IsEqualTo(ExpectedCapacityCommitAttempts);
        }
        finally
        {
            _ = releaseCanceledSubscribe.TrySetResult();
            _ = releaseBatches.TrySetResult();
        }
    }

    /// <summary>Verifies startup disposes a capability-rejected session without masking validation failure.</summary>
    /// <param name="disposeThrows">Whether rejected session disposal throws.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StartAsyncDisposesCapabilityRejectedSessionWithoutMaskingValidationFailure(bool disposeThrows)
    {
        var rejected = new ReceiveSession
        {
            DisposeException = disposeThrows ? new NotSupportedException("initial rejected cleanup failed") : null,
            NegotiatedCapabilities = CreateAtMostOnceCapabilities(),
        };
        var transport = new RecordingTransport { SessionOverride = rejected };
        await using var engine = CreateEngine(transport: transport);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest, DeliveryGuarantee.ExactlyOnce) };
        using var registration = engine.RegisterParticipant(participant);

        await Assert.That(async () => await engine.StartAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(rejected.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies receive acknowledgements are sent only after durable participant apply completes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpAcknowledgesOnlyAfterParticipantApplyCompletes()
    {
        var applyEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseApply = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateReceiveBatch();
        var session = new ReceiveSession { Batches = { batch } };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var participant = new RecordingParticipant
        {
            ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest),
            RemoteApplyEntered = applyEntered,
            ReleaseRemoteApply = releaseApply,
        };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await applyEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeRequests[0].StreamId).IsEqualTo(Stream);
        await Assert.That(session.SubscribeRequests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.Acknowledgements.Count).IsEqualTo(0);

        releaseApply.SetResult();
        await session.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(participant.RemoteApplyCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(participant.AppliedRemoteBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(participant.AppliedRemoteBatches[0]).IsSameReferenceAs(batch);
        await Assert.That(session.Acknowledgements.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.Acknowledgements[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.Acknowledgements[0].StreamId).IsEqualTo(Stream);
        await Assert.That(session.Acknowledgements[0].Cursor).IsEqualTo(ReceiveCursor);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies transient receive acknowledgement failures reconnect and resubscribe with the stream guarantee.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpRetriesTransientAcknowledgementFailureWithSubscriptionGuarantee()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)) };
        var second = new ReceiveSession { Batches = { batch } };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest, DeliveryGuarantee.ExactlyOnce) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await second.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(transport.ConnectRequests[0].RequiredGuarantees.Any(static guarantee => guarantee == DeliveryGuarantee.ExactlyOnce)).IsTrue();
        await Assert.That(transport.ConnectRequests[1].RequiredGuarantees.Single()).IsEqualTo(DeliveryGuarantee.ExactlyOnce);
        await Assert.That(second.SubscribeRequests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(second.Acknowledgements[0].Cursor).IsEqualTo(ReceiveCursor);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies typed Retry-After transport failures wait for the declared lower bound before reconnecting.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpHonorsTypedTransientRetryAfterBeforeReconnect()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(ReceiveRetryAfterSeconds)) };
        var second = new ReceiveSession { Batches = { batch } };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromMilliseconds(ReceiveShortRetryMilliseconds),
                MaximumDelay = TimeSpan.FromMilliseconds(ReceiveShortRetryMilliseconds),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(ReceiveRetryAfterSeconds)));

        clock.Advance(TimeSpan.FromSeconds(ReceiveEarlyAdvanceSeconds));
        await Assert.That(second.SubscribeEntered.Task.IsCompleted).IsFalse();

        clock.Advance(TimeSpan.FromSeconds(1));
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies ambiguous transport outcomes reconnect instead of becoming permanent validation failures.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpRetriesTypedAmbiguousTransportOutcome()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.AmbiguousTransportOutcome, TimeSpan.FromSeconds(1)) };
        var second = new ReceiveSession { Batches = { batch } };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies permanent participant failures publish a fault without retrying the receive pump.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpDoesNotRetryPermanentParticipantFailure()
    {
        var session = new ReceiveSession { Batches = { CreateReceiveBatch() } };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant
        {
            ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest),
            RemoteApplyException = new InvalidOperationException("corrupt receive state"),
        };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(ReceivePumpFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies typed permanent transport failures publish a fault without retrying the receive pump.</summary>
    /// <param name="failureKind">The permanent failure kind.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(RetryFailureKind.AuthorizationDenied)]
    [Arguments(RetryFailureKind.SchemaIncompatible)]
    [Arguments(RetryFailureKind.ValidationRejected)]
    public async Task ReceivePumpDoesNotRetryTypedPermanentTransportFailure(RetryFailureKind failureKind)
    {
        var session = new ReceiveSession { Batches = { CreateReceiveBatch() } };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest), RemoteApplyException = CreateTransportFailure(failureKind) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(ReceivePumpFaultCode);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies receive pump cleanup completes when retry session disposal throws.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpCompletesWhenRetrySessionDisposeFails()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)) };
        var second = new ReceiveSession
        {
            Batches = { batch },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)),
            DisposeException = new InvalidOperationException("dispose failed"),
        };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedCapacityCommitAttempts);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);
        await Assert.That(first.DisposeCalls).IsEqualTo(0);
        await engine.StopAsync(CancellationToken.None);

        await Assert.That(first.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(faults.Values[0].Code).IsEqualTo(ReceivePumpFaultCode);
        await Assert.That(faults.Values[0].Exception?.Message)
            .IsEqualTo(typeof(InvalidOperationException).ToString());
    }

    /// <summary>Verifies receive retries do not dispose the shared session required by uploads.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    [Test]
    public async Task ReceiveRetryPreservesSharedSessionForUploadAfterOneParticipantReconnects()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var uploadStream = new StreamId("sync/engine/upload");
        var uploadOperation = CreateOperation(streamId: uploadStream, operationId: OperationId.New());
        var store = CreateUploadStore([uploadOperation], timeProvider: clock);
        var batch = CreateReceiveBatch();
        var shared = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)) };
        var retryOwned = new ReceiveSession { Batches = { batch } };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(shared);
        transport.Sessions.Enqueue(retryOwned);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var receiveParticipant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        var uploadParticipant = CreateUploadParticipant(store, streamId: uploadStream);
        using var receiveRegistration = engine.RegisterParticipant(receiveParticipant);
        using var uploadRegistration = engine.RegisterParticipant(uploadParticipant);

        await engine.StartAsync(CancellationToken.None);
        await shared.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => receiveParticipant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await retryOwned.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await retryOwned.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

        engine.NotifyLocalCommitReady(uploadStream, uploadOperation);
        await WaitForReceiveSessionUploadAsync(clock, shared, faults);

        await Assert.That(shared.DisposeCalls).IsEqualTo(0);
        await Assert.That(shared.PreparedBatches[0].Operations[0]).IsSameReferenceAs(uploadOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(faults.Values.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);

        await Assert.That(shared.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(retryOwned.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies stop attempts session disposal when subscription cancellation callbacks fail.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncDisposesSessionWhenSubscribeCancellationCallbackFails()
    {
        var session = new ReceiveSession { SubscribeCancellationException = new InvalidOperationException("subscription cancellation failed") };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Exception? stopFailure = null;
        try
        {
            await engine.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            stopFailure = exception;
        }

        var failureTrace = CreateExceptionTrace(stopFailure, faults);
        await Assert.That(session.SubscribeCancellationFailures.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCancellationFailures[0]).IsSameReferenceAs(session.SubscribeCancellationException);
        await Assert.That(failureTrace).Contains(nameof(InvalidOperationException));
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies stopping one stream waits for receive cancellation callbacks before admitting a later restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamAsyncWaitsForReceiveCancellationCallbackBeforeRestart()
    {
        TaskCompletionSource callbackEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim releaseCallback = new();
        var session = new ReceiveSession { SubscribeCancellationCallbackEntered = callbackEntered, ReleaseSubscribeCancellationCallback = releaseCallback };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);
        Task? stop = null;

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        try
        {
            stop = Task.Run(async () => await engine.StopStreamAsync(Stream, CancellationToken.None).ConfigureAwait(false));
            await callbackEntered.Task.WaitAsync(GuardTimeout);

            await Assert.That(stop.IsCompleted).IsFalse();
            await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.DisposeCalls).IsEqualTo(0);

            releaseCallback.Set();
            await stop.WaitAsync(GuardTimeout);
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);

            await engine.StartStreamAsync(Stream, CancellationToken.None);
            await WaitForConditionAsync(() => session.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.DisposeCalls).IsEqualTo(0);
        }
        finally
        {
            releaseCallback.Set();
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }
        }

        await engine.StopAsync(CancellationToken.None);

        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies rejected retry sessions with successful cleanup preserve capability validation failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpDisposesCapabilityRejectedRetrySessionAfterSuccessfulCleanup()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)) };
        var rejected = new ReceiveSession { NegotiatedCapabilities = CreateAtMostOnceCapabilities() };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(rejected);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest, DeliveryGuarantee.ExactlyOnce) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await WaitForConditionAsync(() =>
            transport.ConnectCalls == ExpectedCapacityCommitAttempts
            && rejected.DisposeCalls == ExpectedSingleOperation
            && faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode));
        await engine.StopAsync(CancellationToken.None);

        var primary = faults.Values.First(static fault => fault.Code == ReceivePumpFaultCode);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(rejected.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(primary.Exception?.Message)
            .IsEqualTo(typeof(InvalidOperationException).ToString());
    }

    /// <summary>Verifies rejected retry sessions are disposed without masking capability validation failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpDisposesCapabilityRejectedRetrySessionWithoutMaskingValidationFailure()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)) };
        var rejected = new ReceiveSession { DisposeException = new NotSupportedException("rejected session cleanup failed"), NegotiatedCapabilities = CreateAtMostOnceCapabilities() };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(rejected);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest, DeliveryGuarantee.ExactlyOnce) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await WaitForConditionAsync(() =>
            transport.ConnectCalls == ExpectedCapacityCommitAttempts
            && rejected.DisposeCalls == ExpectedSingleOperation
            && faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)
            && faults.Values.Exists(
                static fault => fault.Code == "OC.Engine.RejectedSessionDisposal"));
        await engine.StopAsync(CancellationToken.None);

        var primary = faults.Values.First(static fault => fault.Code == ReceivePumpFaultCode);
        var secondary = faults.Values.First(
            static fault => fault.Code == "OC.Engine.RejectedSessionDisposal");
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(rejected.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(primary.Exception?.Message)
            .IsEqualTo(typeof(InvalidOperationException).ToString());
        await Assert.That(secondary.Exception)
            .IsTypeOf<InvalidOperationException>();
        await Assert.That(secondary.Exception?.Message)
            .IsEqualTo(typeof(NotSupportedException).ToString());
    }

    /// <summary>Verifies AtMostOnce-only receive registration does not request stronger transport guarantees.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpConnectsAtMostOnceOnlyRegistrationWithoutAtLeastOnceRequirement()
    {
        var session = new ReceiveSession { NegotiatedCapabilities = CreateAtMostOnceCapabilities() };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest, DeliveryGuarantee.AtMostOnce) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ConnectRequests[0].RequiredGuarantees.Single()).IsEqualTo(DeliveryGuarantee.AtMostOnce);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies lost acknowledgements replay the same batch without applying the projection twice.</summary>
    /// <param name="throwTelemetry">Whether metric listeners fail during receive and replay.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReceivePumpLostAcknowledgementReplayDoesNotApplyProjectionTwice(bool throwTelemetry)
    {
        using var metrics = throwTelemetry ? CreateThrowingEngineMetricListener() : null;
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        await using var store = new InMemoryLocalStoreAdapter(
            clock,
            maximumRecordCount: ReceiveReplayRecordCount,
            maximumEncodedBytes: ReceiveReplayStoreBytes,
            retentionOptions: new());
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.AmbiguousTransportOutcome, TimeSpan.FromSeconds(1)) };
        var second = new ReceiveSession { Batches = { batch } };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var options = CreateDiagnosticsOptions(enabled: true) with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        var projection = new ReceiveCounterProjection();
        await using var stream = CreateReceiveCounterStream(store, engine, projection, clock);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => projection.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await second.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(second.SubscribeRequests[0].Cursor).IsEqualTo(ReceiveCursor);
        await Assert.That(projection.RemoteApplyCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.Acknowledgements[0].Cursor).IsEqualTo(ReceiveCursor);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Waits for an upload through a receive-session transport test double.</summary>
    /// <param name="clock">The manual engine clock.</param>
    /// <param name="session">The receive-session test double.</param>
    /// <param name="faults">The observed engine faults.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">Upload progress is not observed before the guard timeout.</exception>
    private static async Task WaitForReceiveSessionUploadAsync(
        ManualTimerTimeProvider clock,
        ReceiveSession session,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        try
        {
            await WaitForConditionAsync(
                () => session.SentBatches.Count == ExpectedSingleOperation
                    || clock.HasTimerDueIn(TimeSpan.FromMilliseconds(ExpectedSingleOperation)));
            if (session.SentBatches.Count == 0)
            {
                clock.Advance(TimeSpan.FromMilliseconds(ExpectedSingleOperation));
            }

            await WaitForConditionAsync(() => session.SentBatches.Count == ExpectedSingleOperation);
        }
        catch (TimeoutException exception)
        {
            var faultTrace = string.Join(",", faults.Values.Select(static fault => fault.Code));
            throw new TimeoutException(
                $"prepare={session.PrepareCalls};sent={session.SentBatches.Count};faults={faultTrace}",
                exception);
        }
    }

    /// <summary>Creates a diagnostic exception trace from direct stop failure and published faults.</summary>
    /// <param name="exception">The direct stop exception.</param>
    /// <param name="faults">The published faults.</param>
    /// <returns>The combined exception trace.</returns>
    private static string CreateExceptionTrace(
        Exception? exception,
        RecordingObserver<OccasionallyConnectedFault> faults)
    {
        var builder = new StringBuilder();
        if (exception is not null)
        {
            _ = builder.AppendLine(exception.ToString());
        }

        for (var i = 0; i < faults.Values.Count; i++)
        {
            _ = builder.AppendLine(faults.Values[i].Exception?.ToString() ?? string.Empty);
        }

        return builder.ToString();
    }

    /// <summary>Creates capabilities that only satisfy AtMostOnce receive delivery.</summary>
    /// <returns>The negotiated capabilities.</returns>
    private static NegotiatedCapabilities CreateAtMostOnceCapabilities() =>
        new(
            new(1, 0),
            RemoteTransportCapabilities.None,
            MaximumBatchOperations: 32,
            MaximumBatchBytes: 4096,
            ServerIdempotencyRetention: null,
            ClientInboxRetentionRequired: null);

    /// <summary>Creates capabilities that satisfy exactly-once upload delivery.</summary>
    /// <param name="serverIdempotencyRetention">The optional server idempotency retention.</param>
    /// <returns>The negotiated capabilities.</returns>
    private static NegotiatedCapabilities CreateExactlyOnceCapabilities(TimeSpan? serverIdempotencyRetention = null) =>
        new(
            new(1, 0),
            RecordingTransportUploadCapabilities,
            MaximumBatchOperations: 32,
            MaximumBatchBytes: 4096,
            ServerIdempotencyRetention: serverIdempotencyRetention
                ?? TimeSpan.FromMinutes(DefaultServerIdempotencyRetentionMinutes),
            ClientInboxRetentionRequired: null);

    /// <summary>Creates capabilities that explicitly permit positive multi-operation upload batching.</summary>
    /// <param name="maximumBatchOperations">The maximum operation count.</param>
    /// <param name="maximumBatchBytes">The maximum encoded batch bytes.</param>
    /// <returns>The negotiated capabilities.</returns>
    private static NegotiatedCapabilities CreateBatchPushCapabilities(int maximumBatchOperations, long maximumBatchBytes) =>
        new(
            new(1, 0),
            RecordingTransportUploadCapabilities,
            maximumBatchOperations,
            maximumBatchBytes,
            ServerIdempotencyRetention: TimeSpan.FromMinutes(DefaultServerIdempotencyRetentionMinutes),
            ClientInboxRetentionRequired: null);

    /// <summary>Creates a remote receive batch.</summary>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateReceiveBatch()
    {
        var payload = new PayloadEnvelope("counter-input", 1, "application/json", TestPayload, "hash");
        var remoteEvent = new RemoteEvent(
            Guid.NewGuid(),
            Stream,
            ReceiveCursor,
            DateTimeOffset.UnixEpoch,
            causedByOperationId: null,
            payload,
            new Dictionary<string, string>());

        return new(Guid.NewGuid(), Stream, previousCursor: null, ReceiveCursor, [remoteEvent]);
    }

    /// <summary>Creates a stream participant backed by the real local stream implementation.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="coordinator">The engine coordinator.</param>
    /// <param name="projection">The projection under observation.</param>
    /// <param name="timeProvider">The test clock.</param>
    /// <returns>The constructed stream participant.</returns>
    private static OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> CreateReceiveCounterStream(
        ILocalStoreAdapter store,
        IOccasionallyConnectedStreamCoordinator coordinator,
        ReceiveCounterProjection projection,
        TimeProvider timeProvider)
    {
        var serializer = new ReceiveCounterSerializer();
        return new(new()
        {
            Definition = new()
            {
                StreamId = Stream,
                SubscriptionId = Subscription,
                Projection = projection,
                InputContractId = "counter-input",
                StateContractId = "counter-state",
                Subscription = new() { StreamId = Stream, SubscriptionId = Subscription, StartPosition = StartPosition.Latest, DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
            },
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

    /// <summary>Counter state used by receive replay tests.</summary>
    /// <param name="Sum">The current sum.</param>
    private readonly record struct ReceiveCounterState(int Sum);

    /// <summary>Counter input used by receive replay tests.</summary>
    /// <param name="Delta">The state delta.</param>
    private readonly record struct ReceiveCounterInput(int Delta);

    /// <summary>Counts remote projection applications.</summary>
    private sealed class ReceiveCounterProjection : ILocalProjection<ReceiveCounterState, ReceiveCounterInput>
    {
        /// <summary>Gets the number of remote projection applications.</summary>
        public int RemoteApplyCalls { get; private set; }

        /// <inheritdoc/>
        public ReceiveCounterState InitialState { get; } = new(0);

        /// <inheritdoc/>
        public ReceiveCounterState ApplyLocal(ReceiveCounterState state, ReceiveCounterInput input, SyncOperation operation) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc/>
        public ReceiveCounterState ApplyRemote(ReceiveCounterState state, ReceiveCounterInput input, RemoteEvent remoteEvent)
        {
            RemoteApplyCalls++;
            return new(state.Sum + input.Delta);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReceiveCounterState Reconcile(ReceiveCounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Serializes receive replay counter values.</summary>
    private sealed class ReceiveCounterSerializer : IPayloadSerializer
    {
        /// <inheritdoc/>
        public string ContentType => "application/json";

        /// <summary>Creates a state snapshot from a payload.</summary>
        /// <param name="payload">The payload.</param>
        /// <returns>The state snapshot.</returns>
        public static ReceiveCounterState CreateState(PayloadEnvelope payload) =>
            new(ReadValue(payload));

        /// <summary>Creates an input snapshot from a payload.</summary>
        /// <param name="payload">The payload.</param>
        /// <returns>The input snapshot.</returns>
        public static ReceiveCounterInput CreateInput(PayloadEnvelope payload) =>
            new(CreateState(payload).Sum);

        /// <inheritdoc/>
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var numeric = value switch
            {
                ReceiveCounterState state => state.Sum,
                ReceiveCounterInput input => input.Delta,
                _ => throw new InvalidOperationException("Unexpected receive replay payload."),
            };
            var payload = new[] { (byte)numeric };
            return new(new PayloadEnvelope(contractId, schemaVersion, ContentType, payload, $"hash-{numeric}"));
        }

        /// <inheritdoc/>
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (targetType == typeof(ReceiveCounterState))
            {
                return new(new ReceiveCounterState(ReadValue(envelope)));
            }

            if (targetType == typeof(ReceiveCounterInput))
            {
                return new(new ReceiveCounterInput(ReadValue(envelope)));
            }

            throw new InvalidOperationException("Unexpected receive replay target type.");
        }

        /// <summary>Reads the test value from a payload.</summary>
        /// <param name="payload">The payload.</param>
        /// <returns>The decoded value.</returns>
        private static int ReadValue(PayloadEnvelope payload) =>
            payload.Payload.Span.IsEmpty ? 0 : payload.Payload.Span[0];
    }

    /// <summary>Provides operation identities for receive replay streams.</summary>
    private sealed class ReceiveOperationIdSource : IOperationIdSource
    {
        /// <summary>Gets the singleton operation identity source.</summary>
        internal static ReceiveOperationIdSource Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OperationId New() => OperationId.New();
    }

    /// <summary>Provides the public observer for receive replay streams.</summary>
    private sealed class ReceiveInputProducer : IOccasionallyConnectedInputProducer<ReceiveCounterInput>
    {
        /// <inheritdoc/>
        public IObserver<ReceiveCounterInput> Observer { get; } = new ReceiveInputObserver();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;

        /// <summary>Rejects direct observer input in receive replay tests.</summary>
        private sealed class ReceiveInputObserver : IObserver<ReceiveCounterInput>
        {
            /// <inheritdoc/>
            public void OnCompleted()
            {
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

            /// <inheritdoc/>
            public void OnNext(ReceiveCounterInput value) => throw new NotSupportedException();
        }
    }

    /// <summary>Runs observer work immediately for receive replay tests.</summary>
    private sealed class InlineObserverScheduler : IObserverNotificationScheduler
    {
        /// <summary>Gets the singleton scheduler.</summary>
        internal static InlineObserverScheduler Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(ReactiveUI.Primitives.Concurrency.IWorkItem item) => item.Execute();
    }
}
