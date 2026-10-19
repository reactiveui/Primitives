// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests permanent receive validation failures at application boundaries.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies an at-most-once transport applies received data without calling an unnegotiated acknowledgement operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveWithoutAcknowledgementCapabilityAppliesBatchWithoutSendingAcknowledgement()
    {
        var batch = CreateReceiveBatch();
        var session = new ReceiveSession { NegotiatedCapabilities = CreateAtMostOnceCapabilities(), Batches = { batch } };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        ReceiveStreamSubscription subscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest, DeliveryGuarantee.AtMostOnce);
        var participant = new RecordingParticipant { ReceiveSubscription = subscription };
        using var registration = engine.RegisterParticipant(participant);

        await engine.StartAsync(CancellationToken.None);
        await session.ConfiguredBatchesConsumed.Task.WaitAsync(GuardTimeout);

        await Assert.That(participant.AppliedRemoteBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(participant.AppliedRemoteBatches[0]).IsSameReferenceAs(batch);
        await Assert.That(session.Acknowledgements).IsEmpty();
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies local schema, authorization and unexpected validation failures stop receive without acknowledging uncommitted data.</summary>
    /// <param name="kind">The application failure to inject.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(RetryFailureKind.PayloadTooLarge)]
    [Arguments(RetryFailureKind.SchemaIncompatible)]
    [Arguments(RetryFailureKind.AuthorizationDenied)]
    [Arguments(RetryFailureKind.ValidationRejected)]
    public async Task ReceiveValidationFailureDoesNotRetryOrAcknowledgeUncommittedBatch(RetryFailureKind kind)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        var session = new ReceiveSession { Batches = { CreateReceiveBatch() } };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport, timeProvider: clock);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(new RecordingParticipant
        {
            ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest),
            RemoteApplyException = CreateReceiveValidationException(kind),
        });

        await engine.StartAsync(CancellationToken.None);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(faults.Values[0].Code).IsEqualTo(ReceivePumpFaultCode);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.Acknowledgements).IsEmpty();
        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
    }

    /// <summary>Creates a concrete application validation failure without transport retry metadata.</summary>
    /// <param name="kind">The failure category.</param>
    /// <returns>The application failure.</returns>
    private static Exception CreateReceiveValidationException(RetryFailureKind kind) =>
        kind switch
        {
            RetryFailureKind.PayloadTooLarge => new PayloadSchemaException(PayloadSchemaFailureReason.PayloadTooLarge, "Input exceeds the application schema limit."),
            RetryFailureKind.SchemaIncompatible => new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, "Input contract is not registered."),
            RetryFailureKind.AuthorizationDenied => new UnauthorizedAccessException("Input access was denied."),
            _ => new FormatException("Input could not be interpreted."),
        };
}
