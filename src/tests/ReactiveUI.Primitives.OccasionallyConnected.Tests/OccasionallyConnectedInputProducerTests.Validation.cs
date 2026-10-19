// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedInputProducer{TInput}"/>.</summary>
/// <content>Validation and defensive observer path tests.</content>
public sealed partial class OccasionallyConnectedInputProducerTests
{
    /// <summary>Verifies producer construction rejects invalid required and publish option composition.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConstructorRejectsInvalidRequiredAndPublishOptions()
    {
        var capture = new RecordingInputCapture();
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        var options = CreateProducerOptions(capture, publisher.PublishAsync, faults);

        await Assert.That(
                () => new OccasionallyConnectedInputProducer<CapturedInput>(
                    options with { Admission = MissingRequired<ObserverInputOptions>() }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(
                () => new OccasionallyConnectedInputProducer<CapturedInput>(
                    options with { StreamId = default }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(
            () => new OccasionallyConnectedInputProducer<CapturedInput>(
                options with { PublishOptions = new RemotePublishOptions { StreamId = OtherStream } }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies late producer errors are ignored once the observer input producer has closed.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OnErrorAfterCompletionDoesNotPublishAnotherFault()
    {
        var capture = new RecordingInputCapture();
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnCompleted();
        producer.Observer.OnError(new InvalidOperationException("late producer fault"));

        await producer.DisposeAsync();
        await Assert.That(faults.Faults.Count).IsEqualTo(0);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
    }

    /// <summary>Verifies a producer closed by the sizing callback does not capture or publish the sized input.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task SizingCallbackCanCloseProducerBeforeReservationWithoutCapture()
    {
        var capture = new CompletingSizingInputCapture();
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);
        capture.Observer = producer.Observer;

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));

        await producer.DisposeAsync();
        await Assert.That(capture.SizedValues.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(capture.CapturedValues.Count).IsEqualTo(0);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
        await Assert.That(faults.Faults.Count).IsEqualTo(0);
    }

    /// <summary>Verifies null capture payloads are isolated as capture faults and release disposal ownership.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task NullCapturedPayloadRecordsCaptureFaultAndDisposes()
    {
        var capture = new NullReturningInputCapture();
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));

        await producer.DisposeAsync();
        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputCaptureFaultCode);
    }

    /// <summary>Verifies fatal diagnostic callback failures during publish-failure reporting fault shared disposal.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublishFaultCallbackOutOfMemoryDuringPublishFailureFaultsDispose()
    {
        var capture = new RecordingInputCapture();
        TaskCompletionSource releaseFirstFailure = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var publisher = new FailFirstPublisher { ReleaseFirstFailure = releaseFirstFailure };
        var faults = new RecordingFaultSink { AfterRecord = static _ => throw FatalCallbackFailure };
        var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.WaitForAttemptCountAsync(SingleInputCapacity);
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));
        _ = releaseFirstFailure.TrySetResult();

        var exception = await Assert.ThrowsExactlyAsync<OutOfMemoryException>(
            () => producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(SignalTimeoutSeconds)));
        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SecondInputSequence);
        await Assert.That(publisher.AttemptCount).IsEqualTo(SingleInputCapacity);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
        await Assert.That(faults.Faults.Count).IsEqualTo(SecondInputSequence);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputPublishFaultCode);
        await Assert.That(faults.Faults[1].Code).IsEqualTo(InputPublishFaultCode);
        await Assert.That(exception).IsSameReferenceAs(FatalCallbackFailure);
    }

    /// <summary>Verifies terminal pump diagnostics can publish after the original fatal callback is retained.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TerminalPumpDiagnosticPublishesAfterFirstFatalCallbackAndDisposeObservesOriginalFailure()
    {
        var capture = new RecordingInputCapture();
        TaskCompletionSource releaseFirstFailure = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var publisher = new FailFirstPublisher { ReleaseFirstFailure = releaseFirstFailure };
        var callbackCount = 0;
        var faults = new RecordingFaultSink { AfterRecord = ThrowOnlyFirstCallback };
        var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.WaitForAttemptCountAsync(SingleInputCapacity);
        _ = releaseFirstFailure.TrySetResult();
        await faults.WaitForFaultCountAsync(SecondInputSequence);

        var exception = await Assert.ThrowsExactlyAsync<OutOfMemoryException>(
            () => producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(SignalTimeoutSeconds)));
        await Assert.That(callbackCount).IsEqualTo(SecondInputSequence);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputPublishFaultCode);
        await Assert.That(faults.Faults[1].Code).IsEqualTo(InputPublishFaultCode);
        await Assert.That(exception).IsSameReferenceAs(FatalCallbackFailure);

        void ThrowOnlyFirstCallback(RecordedFault _)
        {
            callbackCount++;
            if (callbackCount != SingleInputCapacity)
            {
                return;
            }

            throw FatalCallbackFailure;
        }
    }

    /// <summary>Verifies terminal pump failure does not release admission while another observer call is still capturing.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TerminalPumpFailureWaitsForActiveCaptureBeforeDisposeFaults()
    {
        using var capture = new BlockingSecondInputCapture();
        TaskCompletionSource releaseFirstFailure = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var publisher = new FailFirstPublisher { ReleaseFirstFailure = releaseFirstFailure };
        var faults = new RecordingFaultSink { AfterRecord = static _ => throw FatalCallbackFailure };
        var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.WaitForAttemptCountAsync(SingleInputCapacity);
        var secondCapture = RunSynchronousProducer(() => producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes)));
        try
        {
            capture.WaitForSecondCapture();
            _ = releaseFirstFailure.TrySetResult();
            await faults.WaitForFaultCountAsync(SecondInputSequence);
            var dispose = producer.DisposeAsync().AsTask();

            await Assert.That(dispose.IsCompleted).IsFalse();
        }
        finally
        {
            _ = releaseFirstFailure.TrySetResult();
            capture.ReleaseSecondCapture();
            await secondCapture;
        }

        var exception = await Assert.ThrowsExactlyAsync<OutOfMemoryException>(
            () => producer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(SignalTimeoutSeconds)));
        await Assert.That(exception).IsSameReferenceAs(FatalCallbackFailure);
        await Assert.That(publisher.AttemptCount).IsEqualTo(SingleInputCapacity);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
    }
}
