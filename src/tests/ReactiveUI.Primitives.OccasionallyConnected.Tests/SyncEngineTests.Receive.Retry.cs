// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Receive retry tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies receive retries survive a transient reconnect failure before a later owned session succeeds.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpRetriesTransientReconnectFailureBeforeLaterSessionSucceeds()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)) };
        var second = new ReceiveSession { Batches = { batch } };
        var transport = new RecordingTransport();
        transport.ConnectFailures.Enqueue(null);
        transport.ConnectFailures.Enqueue(CreateTransportFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(1)));
        transport.ConnectFailures.Enqueue(null);
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = TimeSpan.FromSeconds(1),
                MaximumDelay = TimeSpan.FromSeconds(1),
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts + ExpectedSingleOperation,
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
        await WaitForConditionAsync(() => transport.ConnectCalls == ExpectedCapacityCommitAttempts);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(TimeSpan.FromSeconds(1)));

        clock.Advance(TimeSpan.FromSeconds(1));
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await second.AcknowledgementEntered.Task.WaitAsync(GuardTimeout);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts + ExpectedSingleOperation);
        await Assert.That(participant.RemoteApplyCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(first.DisposeCalls).IsEqualTo(0);
        await Assert.That(second.DisposeCalls).IsEqualTo(0);
        await Assert.That(second.Acknowledgements.Count).IsEqualTo(ExpectedSingleOperation);

        await engine.StopAsync(CancellationToken.None);

        await Assert.That(first.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies an active receive subscription that ends unexpectedly reconnects through retry policy.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpRetriesWhenActiveSubscriptionCompletesUnexpectedly()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromSeconds(1);
        var first = new ReceiveSession();
        var second = new ReceiveSession();
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = retryDelay,
                MaximumDelay = retryDelay,
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);

        var started = false;
        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            first.HoldOpen.SetResult();
            await WaitForConditionAsync(() => first.SubscribeCompletedCount == ExpectedSingleOperation && clock.HasTimerDueIn(retryDelay));
            await Assert.That(first.DisposeCalls).IsEqualTo(0);

            clock.Advance(retryDelay);
            await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(first.SubscribeCompletedCount).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(first.DisposeCalls).IsEqualTo(0);
            await Assert.That(second.DisposeCalls).IsEqualTo(0);
            await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();
        }
        finally
        {
            _ = first.HoldOpen.TrySetResult();
            _ = second.HoldOpen.TrySetResult();
            if (started)
            {
                await engine.StopAsync(CancellationToken.None);
            }
        }

        await Assert.That(first.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies owned retry-session dispose failures are published when the receive pump completes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpPublishesFaultWhenOwnedRetrySessionDisposeFails()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromSeconds(1);
        var disposeFailure = new InvalidOperationException("owned receive dispose failed");
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, retryDelay) };
        var second = new ReceiveSession { DisposeException = disposeFailure };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = retryDelay,
                MaximumDelay = retryDelay,
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
        await WaitForConditionAsync(() => clock.HasTimerDueIn(retryDelay));

        clock.Advance(retryDelay);
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await engine.StopAsync(CancellationToken.None);
        await WaitForConditionAsync(() => faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode));

        var fault = faults.Values.First(static fault => fault.Code == ReceivePumpFaultCode);
        await Assert.That(fault.Exception).IsTypeOf<InvalidOperationException>();
        await Assert.That(fault.Exception?.Message).IsEqualTo(typeof(InvalidOperationException).ToString());
        await Assert.That(fault.Exception?.Message == disposeFailure.Message).IsFalse();
        await Assert.That(first.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedTwoOperations);
    }

    /// <summary>Verifies global stop cancels a retry reconnect without publishing a receive fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpStopsWithoutFaultWhenRetryReconnectIsCanceledByGlobalStop()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromSeconds(1);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, retryDelay) };
        var innerTransport = new RecordingTransport();
        innerTransport.Sessions.Enqueue(first);
        var transport = new CancelableReconnectTransport(innerTransport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = retryDelay,
                MaximumDelay = retryDelay,
                MaximumRetryAttempts = ExpectedCapacityCommitAttempts,
            },
        };
        await using var engine = CreateEngineWithTransportAdapter(transport, options, clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);
        var started = false;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            started = true;
            await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
            await WaitForConditionAsync(() => clock.HasTimerDueIn(retryDelay));

            clock.Advance(retryDelay);
            await transport.ReconnectEntered.Task.WaitAsync(GuardTimeout);
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            started = false;

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedTwoOperations);
            await Assert.That(first.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(faults.Values.Exists(static fault => fault.Code == ReceivePumpFaultCode)).IsFalse();
            await transport.ReconnectCanceled.Task.WaitAsync(GuardTimeout);
        }
        finally
        {
            if (started)
            {
                await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            }
        }
    }

    /// <summary>Verifies receive retry exhaustion publishes one terminal receive fault after the final transient failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceivePumpPublishesFaultAfterConfiguredTransientRetryLimit()
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var retryDelay = TimeSpan.FromSeconds(1);
        var batch = CreateReceiveBatch();
        var first = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, retryDelay) };
        var second = new ReceiveSession { Batches = { batch }, AcknowledgeException = CreateTransportFailure(RetryFailureKind.Transient, retryDelay) };
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(first);
        transport.Sessions.Enqueue(second);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var options = OccasionallyConnectedOptions.Default with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with
            {
                MinimumDelay = retryDelay,
                MaximumDelay = retryDelay,
                MaximumRetryAttempts = ExpectedSingleOperation,
            },
        };
        await using var engine = CreateEngine(transport: transport, options: options, timeProvider: clock);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await first.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedSingleOperation);
        await WaitForConditionAsync(() => clock.HasTimerDueIn(retryDelay));

        clock.Advance(retryDelay);
        await second.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        await WaitForConditionAsync(() => participant.RemoteApplyCalls == ExpectedCapacityCommitAttempts);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(faults.Values[0].Code).IsEqualTo(ReceivePumpFaultCode);
        await Assert.That(first.DisposeCalls).IsEqualTo(0);

        await engine.StopAsync(CancellationToken.None);

        await Assert.That(first.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(second.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }
}
