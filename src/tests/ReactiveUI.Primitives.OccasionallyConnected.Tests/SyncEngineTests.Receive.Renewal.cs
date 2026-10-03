// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Receive stale-session renewal tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The first cursor used by stale receive renewal tests.</summary>
    private const string FirstRenewalCursor = "cursor-1";

    /// <summary>The second cursor used by stale receive renewal tests.</summary>
    private const string SecondRenewalCursor = "cursor-2";

    /// <summary>Verifies stop cancels a stale receive renewal connect without publishing a receive fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpStopsWithoutFaultWhenExpiredSessionRenewalConnectIsCanceledByGlobalStop()
    {
        var batch = CreateReceiveBatch(previousCursor: null, nextCursor: "receive-renewal-1");
        var expired = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
        var innerTransport = new RecordingTransport();
        innerTransport.Sessions.Enqueue(expired);
        var transport = new CancelableReconnectTransport(innerTransport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngineWithTransportAdapter(transport);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);
        var started = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            await expired.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
            await transport.ReconnectEntered.Task.WaitAsync(GuardTimeout);
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            started = false;

            await transport.ReconnectCanceled.Task.WaitAsync(GuardTimeout);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(innerTransport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();
        }
        finally
        {
            if (started)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies unregistering a receive participant detaches its stale-renewal waiter without canceling shared renewal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UnregisterReceiveParticipantDetachesExpiredSessionRenewalWaiterWithoutCancelingSharedRenewal()
    {
        var batch = CreateReceiveBatch(previousCursor: null, nextCursor: "receive-renewal-1");
        var releaseExpiredDispose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expired = new ReceiveSession
        {
            Batches = { batch },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
            DisposeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ReleaseDispose = releaseExpiredDispose,
        };
        var renewed = new ReceiveSession();
        var innerTransport = new RecordingTransport();
        innerTransport.Sessions.Enqueue(expired);
        innerTransport.Sessions.Enqueue(renewed);
        var transport = new ControlledReconnectTransport(innerTransport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngineWithTransportAdapter(transport);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });

        await engine.StartAsync(CancellationToken.None);
        await expired.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await transport.ReconnectEntered.Task.WaitAsync(GuardTimeout);
        registration.Dispose();
        await expired.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ReconnectCanceled.Task.IsCompleted).IsFalse();
        transport.ReleaseReconnect.SetResult();
        await WaitForConditionAsync(() => innerTransport.ConnectCalls == ExpectedCapacityCommitAttempts);
        await expired.DisposeEntered.Task.WaitAsync(GuardTimeout);
        var stop = engine.StopAsync(CancellationToken.None).AsTask();
        await Assert.That(stop.IsCompleted).IsFalse();
        releaseExpiredDispose.SetResult();

        await stop.WaitAsync(GuardTimeout);
        await Assert.That(expired.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(renewed.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();
    }

    /// <summary>Verifies current-generation receive cursor progress permits a later stale-session renewal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveCursorAdvanceAfterRenewalAllowsNextExpiredSessionRenewal()
    {
        var first = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: null, nextCursor: FirstRenewalCursor) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var second = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: FirstRenewalCursor, nextCursor: SecondRenewalCursor) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var third = new ReceiveSession();
        var transport = CreateQueuedReceiveTransport(first, second, third);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(transport: transport);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });

        await engine.StartAsync(CancellationToken.None);
        await third.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
        await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies an unchanged empty receive cursor does not reset the stale-session renewal budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveEmptyUnchangedCursorAfterRenewalDoesNotPermitAnotherExpiredSessionRenewal()
    {
        var first = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: null, nextCursor: FirstRenewalCursor) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var second = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: FirstRenewalCursor, nextCursor: FirstRenewalCursor, includeEvent: false) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var unused = new ReceiveSession();
        var transport = CreateQueuedReceiveTransport(first, second, unused);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(transport: transport);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });

        await engine.StartAsync(CancellationToken.None);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode));

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(unused.SubscribeRequests.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies an empty durable cursor advance resets the stale-session renewal budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveEmptyCursorAdvanceAfterRenewalAllowsNextExpiredSessionRenewal()
    {
        var first = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: null, nextCursor: FirstRenewalCursor) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var second = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: FirstRenewalCursor, nextCursor: SecondRenewalCursor, includeEvent: false) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var third = new ReceiveSession();
        var transport = CreateQueuedReceiveTransport(first, second, third);
        var participant = new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null), RemoteApplyCursorAdvanced = static _ => true };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await third.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies duplicate replay with an old previous cursor does not reset stale renewal budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveDuplicateOldPreviousCursorAfterRenewalDoesNotPermitAnotherExpiredSessionRenewal()
    {
        var first = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: null, nextCursor: FirstRenewalCursor) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var second = new ReceiveSession
        {
            Batches = { CreateReceiveBatch(previousCursor: "cursor-0", nextCursor: FirstRenewalCursor, includeEvent: false) },
            AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
        };
        var unused = new ReceiveSession();
        var transport = CreateQueuedReceiveTransport(first, second, unused);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(transport: transport);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });

        await engine.StartAsync(CancellationToken.None);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode));

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(second.Acknowledgements.Count).IsEqualTo(0);
        await Assert.That(unused.SubscribeRequests.Count).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies concurrent receive stale-session failures join one shared renewal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentReceiveExpiredSessionFailuresShareOneRenewalConnect()
    {
        var otherStream = new StreamId("sync/engine/other");
        var otherSubscription = new SubscriptionId(Guid.Parse("5fb087db-385e-4fbf-bf50-490d62f15baa"));
        var first = new ReceiveSession { AcknowledgeException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
        first.SubscriptionBatches.Enqueue([CreateReceiveBatch(Stream, previousCursor: null, nextCursor: FirstRenewalCursor)]);
        first.SubscriptionBatches.Enqueue([CreateReceiveBatch(otherStream, previousCursor: null, nextCursor: SecondRenewalCursor)]);
        var renewed = new ReceiveSession();
        var innerTransport = new RecordingTransport();
        innerTransport.Sessions.Enqueue(first);
        innerTransport.Sessions.Enqueue(renewed);
        var transport = new ControlledReconnectTransport(innerTransport);
        await using var engine = CreateEngineWithTransportAdapter(transport, maxRegisteredStreams: ExpectedCapacityCommitAttempts);
        using var firstRegistration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });
        using var secondRegistration = engine.RegisterParticipant(new RecordingParticipant
        {
            StreamId = otherStream,
            ReceiveSubscription = CreateReceiveSubscription(otherStream, null, otherSubscription),
        });

        Exception? primaryFailure = null;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            await transport.ReconnectEntered.Task.WaitAsync(GuardTimeout);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await WaitForConditionAsync(() => first.AcknowledgeCalls == ExpectedCapacityCommitAttempts);
            transport.ReleaseReconnect.SetResult();
            await WaitForConditionAsync(() => renewed.SubscribeRequests.Count == ExpectedCapacityCommitAttempts);

            var attemptedAcknowledgements = first.AcknowledgementAttempts.ToArray();
            await Assert.That(innerTransport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(attemptedAcknowledgements.Length).IsEqualTo(ExpectedCapacityCommitAttempts);
            await Assert.That(Array.Exists(attemptedAcknowledgements, static acknowledgement => acknowledgement.StreamId == Stream)).IsTrue();
            await Assert.That(Array.Exists(attemptedAcknowledgements, acknowledgement => acknowledgement.StreamId == otherStream)).IsTrue();
            await Assert.That(first.Acknowledgements.Count).IsEqualTo(0);
        }
        catch (Exception exception)
        {
            primaryFailure = exception;
            throw;
        }
        finally
        {
            _ = transport.ReleaseReconnect.TrySetResult();
            try
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
            catch when (primaryFailure is not null)
            {
            }
        }
    }

    /// <summary>Verifies stop waits when unregister releases the last retired receive lease.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncWaitsForRetiredReceiveLeaseDisposalReleasedByUnregister()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var uploadStream = new StreamId("sync/engine/renewal-upload");
        var operation = CreateOperation(uploadStream, operationId: OperationId.New());
        var store = CreateUploadStore([operation], timeProvider: clock);
        store.RequeueReleasedLeases = true;
        var releaseRetiredDispose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retired = new ReceiveSession
        {
            PreparedSendException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired),
            DisposeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ReleaseDispose = releaseRetiredDispose,
        };
        var renewed = new ReceiveSession();
        var transport = CreateQueuedReceiveTransport(retired, renewed);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);
        await using var engine = CreateEngine(store, transport, options, timeProvider: clock);
        var receiveRegistration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = CreateReceiveSubscription(Stream, null) });
        using var uploadRegistration = engine.RegisterParticipant(CreateUploadParticipant(store, streamId: uploadStream));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var stopped = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await retired.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            engine.NotifyLocalCommitReady(uploadStream, operation);
            await WaitForReceiveSessionUploadAsync(clock, retired, faults);
            await WaitForReceiveSessionUploadAsync(clock, renewed, faults);
            await WaitForConditionAsync(() => store.Statuses[operation.OperationId].State == SyncOperationState.Synchronized);
            await Assert.That(store.Statuses[operation.OperationId].State).IsEqualTo(SyncOperationState.Synchronized);
            await Assert.That(retired.DisposeCalls).IsEqualTo(0);
            receiveRegistration.Dispose();
            await retired.DisposeEntered.Task.WaitAsync(GuardTimeout);
            var stop = engine.StopAsync(CancellationToken.None).AsTask();
            await Assert.That(stop.IsCompleted).IsFalse();
            releaseRetiredDispose.SetResult();
            await stop.WaitAsync(GuardTimeout);
            stopped = true;
        }
        finally
        {
            _ = releaseRetiredDispose.TrySetResult();
            receiveRegistration.Dispose();
            if (!stopped)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }

        await Assert.That(retired.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(renewed.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values).IsEmpty();
    }

    /// <summary>Creates a receive subscription for a stream.</summary>
    /// <param name="streamId">The subscribed stream.</param>
    /// <param name="cursor">The starting cursor.</param>
    /// <param name="subscriptionId">The optional subscription identity.</param>
    /// <returns>The receive subscription.</returns>
    private static ReceiveStreamSubscription CreateReceiveSubscription(
        StreamId streamId,
        string? cursor,
        SubscriptionId? subscriptionId = null) =>
        new(streamId, subscriptionId ?? Subscription, cursor, StartPosition.Latest);

    /// <summary>Creates a recording transport that returns receive sessions in order.</summary>
    /// <param name="sessions">The sessions to return.</param>
    /// <returns>The transport.</returns>
    private static RecordingTransport CreateQueuedReceiveTransport(params IRemoteTransportSession[] sessions)
    {
        var transport = new RecordingTransport();
        for (var i = 0; i < sessions.Length; i++)
        {
            transport.Sessions.Enqueue(sessions[i]);
        }

        return transport;
    }

    /// <summary>Creates a remote receive batch with explicit cursor movement.</summary>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="includeEvent">Whether the batch contains one event.</param>
    /// <returns>The remote batch.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemoteEventBatch CreateReceiveBatch(string? previousCursor, string nextCursor, bool includeEvent = true) =>
        CreateReceiveBatch(Stream, previousCursor, nextCursor, includeEvent);

    /// <summary>Creates a remote receive batch with explicit stream and cursor movement.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="includeEvent">Whether the batch contains one event.</param>
    /// <returns>The remote batch.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemoteEventBatch CreateReceiveBatch(StreamId streamId, string? previousCursor, string nextCursor, bool includeEvent = true)
    {
        if (!includeEvent)
        {
            return new(Guid.NewGuid(), streamId, previousCursor, nextCursor, []);
        }

        var payload = new PayloadEnvelope("counter-input", 1, "application/json", TestPayload, "hash");
        var remoteEvent = new RemoteEvent(Guid.NewGuid(), streamId, nextCursor, DateTimeOffset.UnixEpoch, null, payload, new Dictionary<string, string>());
        return new(Guid.NewGuid(), streamId, previousCursor, nextCursor, [remoteEvent]);
    }
}
