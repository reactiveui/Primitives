// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedInputProducer{TInput}"/>.</summary>
public sealed partial class OccasionallyConnectedInputProducerTests
{
    /// <summary>The retained byte count used by normal captured test envelopes.</summary>
    private const long NormalRetainedBytes = 256;

    /// <summary>The first test input sequence.</summary>
    private const int FirstInputSequence = 1;

    /// <summary>The second test input sequence.</summary>
    private const int SecondInputSequence = 2;

    /// <summary>The third test input sequence.</summary>
    private const int ThirdInputSequence = 3;

    /// <summary>The mutated test input sequence.</summary>
    private const int MutatedInputSequence = 99;

    /// <summary>The default producer buffer capacity.</summary>
    private const int ProducerBufferCapacity = 8;

    /// <summary>The single-input producer buffer capacity.</summary>
    private const int SingleInputCapacity = 1;

    /// <summary>The two-input producer buffer capacity.</summary>
    private const int TwoInputCapacity = 2;

    /// <summary>The stable fault code used for input overflow.</summary>
    private const string InputOverflowFaultCode = "OC.Stream.InputOverflow";

    /// <summary>The stable fault code used for input capture failures.</summary>
    private const string InputCaptureFaultCode = "OC.Stream.InputCapture";

    /// <summary>The stable fault code used for input publish failures.</summary>
    private const string InputPublishFaultCode = "OC.Stream.InputPublish";

    /// <summary>The message emitted when producer test publication requires durable options.</summary>
    private const string DurablePublicationRequiredMessage = "Producer tests require durable publication.";

    /// <summary>An invalid retained-byte declaration.</summary>
    private const int InvalidRetainedBytes = 0;

    /// <summary>The producer retained byte capacity used by tests.</summary>
    private const int ProducerBufferCapacityBytes = 1024;

    /// <summary>A retained-byte declaration larger than the whole producer buffer.</summary>
    private const int OversizedRetainedBytes = ProducerBufferCapacityBytes + 1;

    /// <summary>The number of seconds allowed for synchronous producer signals.</summary>
    private const int SignalTimeoutSeconds = 5;

    /// <summary>The logical stream used by producer tests.</summary>
    private static readonly StreamId Stream = new("counter/input");

    /// <summary>A different logical stream used for validation failures.</summary>
    private static readonly StreamId OtherStream = new("counter/producer-other");

    /// <summary>A fatal callback failure created without invoking a runtime-reserved constructor.</summary>
    private static readonly Exception FatalCallbackFailure = CreateFatalCallbackFailure();

    /// <summary>Verifies capture order cannot reorder publication after FIFO tickets are reserved.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OnNextPublishesCapturedPayloadsInTicketOrder()
    {
        using var capture = new DelayedFirstInputCapture();
        var publisher = new RecordingPublisher();
        await using var producer = CreateProducer(capture, publisher.PublishAsync);

        var first = RunSynchronousProducer(() => producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes)));
        try
        {
            capture.WaitForFirstCapture();
            var second = RunSynchronousProducer(() => producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes)));
            await second;
            capture.WaitForSecondCapture();

            await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
        }
        finally
        {
            capture.ReleaseFirstCapture();
            await first;
        }

        await publisher.WaitForPublishedCountAsync(SecondInputSequence);

        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(FirstInputSequence);
        await Assert.That(publisher.PublishedValues[1]).IsEqualTo(SecondInputSequence);
    }

    /// <summary>Verifies captured envelopes own payloads before callers mutate after OnNext.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OnNextCapturesOwnedPayloadBeforeCallerMutatesAfterReturn()
    {
        var capture = new RecordingInputCapture();
        var publisher = new RecordingPublisher();
        await using var producer = CreateProducer(capture, publisher.PublishAsync);
        var input = new CapturedInput(FirstInputSequence, NormalRetainedBytes);

        producer.Observer.OnNext(input);
        input.Sequence = MutatedInputSequence;
        await publisher.WaitForPublishedCountAsync(SingleInputCapacity);

        await Assert.That(capture.CapturedValues[0]).IsEqualTo(FirstInputSequence);
        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(FirstInputSequence);
    }

    /// <summary>Verifies capture can reenter OnNext without taking the producer admission gate recursively.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CaptureCanReenterOnNextWithoutDeadlock()
    {
        var capture = new ReentrantInputCapture();
        var publisher = new RecordingPublisher();
        await using var producer = CreateProducer(capture, publisher.PublishAsync);
        capture.Observer = producer.Observer;

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.WaitForPublishedCountAsync(SecondInputSequence);

        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SecondInputSequence);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(SecondInputSequence);
    }

    /// <summary>Verifies overflow rejection happens before mutable input capture.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OnNextRejectsOverflowBeforeCapture()
    {
        var capture = new RecordingInputCapture();
        var publisher = new BlockingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = SingleInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes });

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.Started.Task;
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));

        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(capture.CapturedValues[0]).IsEqualTo(FirstInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputOverflowFaultCode);

        publisher.Release();
        await producer.DisposeAsync();
    }

    /// <summary>Verifies DropOldest cannot evict active capture or in-flight durable input.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DropOldestDoesNotEvictActiveOrInflightDurableInput()
    {
        var capture = new RecordingInputCapture();
        var publisher = new BlockingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.DropOldest, BufferCapacity = SingleInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes });

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.Started.Task;
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));

        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(capture.CapturedValues[0]).IsEqualTo(FirstInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputOverflowFaultCode);

        publisher.Release();
        await producer.DisposeAsync();
    }

    /// <summary>Verifies DropOldest evicts queued non-durable input behind the FIFO head.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DropOldestEvictsQueuedNonDurableInputBehindActiveCapture()
    {
        using var capture = new DelayedFirstInputCapture();
        var publisher = new RecordingPublisher { AllowVolatile = true };
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.DropOldest, BufferCapacity = TwoInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes },
            durable: false);

        var first = RunSynchronousProducer(() => producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes)));
        try
        {
            capture.WaitForFirstCapture();
            var second = RunSynchronousProducer(() => producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes)));
            await second;
            capture.WaitForSecondCapture();

            producer.Observer.OnNext(new(ThirdInputSequence, NormalRetainedBytes));
        }
        finally
        {
            capture.ReleaseFirstCapture();
            await first;
        }

        await publisher.WaitForPublishedCountAsync(SecondInputSequence);

        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(FirstInputSequence);
        await Assert.That(publisher.PublishedValues[1]).IsEqualTo(ThirdInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(0);
    }

    /// <summary>Verifies DropOldest rejects oversized input without evicting queued non-durable work.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DropOldestOversizedInputPreservesQueuedNonDurableInput()
    {
        using var capture = new DelayedFirstInputCapture();
        var publisher = new RecordingPublisher { AllowVolatile = true };
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.DropOldest, BufferCapacity = TwoInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes },
            durable: false);

        var first = RunSynchronousProducer(() => producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes)));
        try
        {
            capture.WaitForFirstCapture();
            var second = RunSynchronousProducer(() => producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes)));
            await second;
            capture.WaitForSecondCapture();

            producer.Observer.OnNext(new(ThirdInputSequence, OversizedRetainedBytes));
        }
        finally
        {
            capture.ReleaseFirstCapture();
            await first;
        }

        await producer.DisposeAsync();

        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(SecondInputSequence);
        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(FirstInputSequence);
        await Assert.That(publisher.PublishedValues[1]).IsEqualTo(SecondInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputOverflowFaultCode);
    }

    /// <summary>Verifies capture failures release admission capacity and allow later input to progress.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CaptureFailureReleasesAdmissionForLaterInput()
    {
        var capture = new RecordingInputCapture { ThrowOnSequence = FirstInputSequence };
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = SingleInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes });

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));
        await publisher.WaitForPublishedCountAsync(SingleInputCapacity);

        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(SecondInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputCaptureFaultCode);
    }

    /// <summary>Verifies retained-byte checks cover payload, object overhead, and retained UTF-16 metadata.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CapturedEnvelopeExceedingDeclaredRetainedBytesFaultsAndReleasesAdmission()
    {
        var capture = new RecordingInputCapture { UseLargeEnvelopeHeaders = true };
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = SingleInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes });

        producer.Observer.OnNext(new(FirstInputSequence, SingleInputCapacity));
        capture.UseLargeEnvelopeHeaders = false;
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));
        await publisher.WaitForPublishedCountAsync(SingleInputCapacity);

        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(SecondInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputCaptureFaultCode);
    }

    /// <summary>Verifies invalid retained-byte declarations reject before capture and release admission.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InvalidDeclaredRetainedBytesRejectsBeforeCapture()
    {
        var capture = new RecordingInputCapture();
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = SingleInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes });

        producer.Observer.OnNext(new(FirstInputSequence, InvalidRetainedBytes));
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));
        await publisher.WaitForPublishedCountAsync(SingleInputCapacity);

        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(capture.CapturedValues[0]).IsEqualTo(SecondInputSequence);
        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(SecondInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputCaptureFaultCode);
    }

    /// <summary>Verifies retained-byte sizing failures are bounded faults and do not escape OnNext.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task SizingFailureRecordsBoundedFaultAndDoesNotEscape()
    {
        var capture = new RecordingInputCapture { ThrowOnSizingSequence = FirstInputSequence };
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));

        await Assert.That(capture.CapturedValues.Count).IsEqualTo(0);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputCaptureFaultCode);
    }

    /// <summary>Verifies synchronous fault callback failures cannot escape the producer or block disposal.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FaultCallbackThrowDoesNotEscapeAndDisposeCompletes()
    {
        var capture = new RecordingInputCapture { ThrowOnSizingSequence = FirstInputSequence };
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink { AfterRecord = static _ => throw new InvalidOperationException("fault callback failed") };
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));

        await producer.DisposeAsync();
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo(InputCaptureFaultCode);
    }

    /// <summary>Verifies disposal waits for an in-progress sizing failure and its bounded fault callback.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForSizingFaultCallbackBeforeReturning()
    {
        using var sizingEntered = new ManualResetEventSlim();
        using var releaseSizing = new ManualResetEventSlim();
        using var faultEntered = new ManualResetEventSlim();
        using var releaseFault = new ManualResetEventSlim();
        var capture = new BlockingSizingFailureCapture(sizingEntered, releaseSizing);
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink { AfterRecord = BlockFaultCallback };

        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        var publish = RunSynchronousProducer(() => producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes)));
        try
        {
            WaitForSignal(sizingEntered);
            var dispose = producer.DisposeAsync().AsTask();

            await Assert.That(dispose.IsCompleted).IsFalse();

            releaseSizing.Set();
            WaitForSignal(faultEntered);

            await Assert.That(dispose.IsCompleted).IsFalse();
        }
        finally
        {
            releaseSizing.Set();
            releaseFault.Set();
            await publish;
        }

        await producer.DisposeAsync();

        void BlockFaultCallback(RecordedFault _)
        {
            faultEntered.Set();
            WaitForSignal(releaseFault);
        }
    }

    /// <summary>Verifies disposal waits for a capture fault callback after admission release.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForCaptureFaultCallbackBeforeReturning()
    {
        using var captureEntered = new ManualResetEventSlim();
        using var releaseCapture = new ManualResetEventSlim();
        using var faultEntered = new ManualResetEventSlim();
        using var releaseFault = new ManualResetEventSlim();
        var capture = new BlockingCaptureFailureCapture(captureEntered, releaseCapture);
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink { AfterRecord = BlockFaultCallback };

        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        var publish = RunSynchronousProducer(() => producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes)));
        try
        {
            WaitForSignal(captureEntered);
            var dispose = producer.DisposeAsync().AsTask();

            await Assert.That(dispose.IsCompleted).IsFalse();

            releaseCapture.Set();
            WaitForSignal(faultEntered);

            await Assert.That(dispose.IsCompleted).IsFalse();
        }
        finally
        {
            releaseCapture.Set();
            releaseFault.Set();
            await publish;
        }

        await producer.DisposeAsync();

        void BlockFaultCallback(RecordedFault _)
        {
            faultEntered.Set();
            WaitForSignal(releaseFault);
        }
    }

    /// <summary>Verifies closed producers ignore future input without sizing or overflow callbacks.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ClosedOnNextDoesNotSizeCaptureOrPublishOverflow()
    {
        var capture = new RecordingInputCapture { ThrowOnSizingSequence = FirstInputSequence };
        var publisher = new RecordingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnCompleted();
        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));

        await Assert.That(capture.SizedValues.Count).IsEqualTo(0);
        await Assert.That(capture.CapturedValues.Count).IsEqualTo(0);
        await Assert.That(publisher.PublishedValues.Count).IsEqualTo(0);
        await Assert.That(faults.Faults.Count).IsEqualTo(0);
    }

    /// <summary>Verifies OnError closes only the producer and does not cancel already accepted input.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OnErrorClosesProducerWithoutCancelingAcceptedPublish()
    {
        var capture = new RecordingInputCapture();
        var publisher = new BlockingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.Started.Task;
        producer.Observer.OnError(new InvalidOperationException("producer failed"));
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));

        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults[0].Code).IsEqualTo("OC.Stream.InputProducer");

        publisher.Release();
        await producer.DisposeAsync();
    }

    /// <summary>Verifies OnCompleted closes input admission after accepted durable work drains.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OnCompletedClosesProducerAfterAcceptedInputDrains()
    {
        var capture = new RecordingInputCapture();
        var publisher = new BlockingPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(capture, publisher.PublishAsync, faults);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.Started.Task;
        producer.Observer.OnCompleted();
        producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));
        var dispose = producer.DisposeAsync().AsTask();

        await Assert.That(dispose.IsCompleted).IsFalse();
        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SingleInputCapacity);
        await Assert.That(faults.Faults.Count).IsEqualTo(0);

        publisher.Release();
        await dispose;
    }

    /// <summary>Verifies disposal drains accepted durable input before completing producer cleanup.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForAcceptedPublishBeforeReturning()
    {
        var capture = new RecordingInputCapture();
        var publisher = new BlockingPublisher();
        await using var producer = CreateProducer(capture, publisher.PublishAsync);

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.Started.Task;
        var dispose = producer.DisposeAsync().AsTask();

        await Assert.That(dispose.IsCompleted).IsFalse();

        publisher.Release();
        await dispose;
    }

    /// <summary>Verifies disposal waits for admitted synchronous capture to produce a payload.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForActiveCaptureBeforeReturning()
    {
        using var capture = new DelayedFirstInputCapture();
        var publisher = new RecordingPublisher();
        await using var producer = CreateProducer(capture, publisher.PublishAsync);

        var publish = RunSynchronousProducer(() => producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes)));
        try
        {
            capture.WaitForFirstCapture();
            var dispose = producer.DisposeAsync().AsTask();

            await Assert.That(dispose.IsCompleted).IsFalse();
        }
        finally
        {
            capture.ReleaseFirstCapture();
            await publish;
        }

        await producer.DisposeAsync();
        await publisher.WaitForPublishedCountAsync(SingleInputCapacity);
    }

    /// <summary>Verifies publish failure callbacks run after admission is released for reentrant input.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublishFailureReleasesAdmissionBeforeFaultCallbackReentersInput()
    {
        var capture = new RecordingInputCapture();
        var publisher = new FailFirstPublisher();
        var faults = new RecordingFaultSink();
        await using var producer = CreateProducer(
            capture,
            publisher.PublishAsync,
            faults,
            new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = SingleInputCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes });
        faults.AfterRecord = _ => producer.Observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));

        producer.Observer.OnNext(new(FirstInputSequence, NormalRetainedBytes));
        await publisher.WaitForAttemptCountAsync(SecondInputSequence);

        await Assert.That(capture.CapturedValues.Count).IsEqualTo(SecondInputSequence);
        await Assert.That(publisher.PublishedValues[0]).IsEqualTo(SecondInputSequence);
        await Assert.That(faults.Faults.Count).IsEqualTo(SingleInputCapacity);
    }

    /// <summary>Creates a producer with supplied test dependencies.</summary>
    /// <param name="capture">The capture provider.</param>
    /// <param name="publish">The publish callback.</param>
    /// <param name="faults">The optional fault sink.</param>
    /// <param name="admission">The optional admission settings.</param>
    /// <param name="durable">Whether publish options are durable.</param>
    /// <returns>The configured producer.</returns>
    private static OccasionallyConnectedInputProducer<CapturedInput> CreateProducer(
        IOccasionallyConnectedInputCapture<CapturedInput> capture,
        Func<PayloadEnvelope, RemotePublishOptions?, CancellationToken, ValueTask<PublishReceipt>> publish,
        RecordingFaultSink? faults = null,
        ObserverInputOptions? admission = null,
        bool durable = true) =>
        new(CreateProducerOptions(capture, publish, faults ?? RecordingFaultSink.Ignored, admission, durable));

    /// <summary>Creates producer options with supplied test dependencies.</summary>
    /// <param name="capture">The capture provider.</param>
    /// <param name="publish">The publish callback.</param>
    /// <param name="faults">The fault sink.</param>
    /// <param name="admission">The optional admission settings.</param>
    /// <param name="durable">Whether publish options are durable.</param>
    /// <returns>The configured producer options.</returns>
    private static OccasionallyConnectedInputProducerOptions<CapturedInput> CreateProducerOptions(
        IOccasionallyConnectedInputCapture<CapturedInput> capture,
        Func<PayloadEnvelope, RemotePublishOptions?, CancellationToken, ValueTask<PublishReceipt>> publish,
        RecordingFaultSink faults,
        ObserverInputOptions? admission = null,
        bool durable = true) =>
        new()
        {
            StreamId = Stream,
            Admission = admission ?? new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = ProducerBufferCapacity, BufferCapacityBytes = ProducerBufferCapacityBytes },
            Capture = capture,
            PublishAsync = publish,
            PublishFault = faults.Record,
            PublishOptions = new() { StreamId = Stream, Durable = durable },
        };

    /// <summary>Creates a successful publish receipt.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The publish receipt.</returns>
    private static PublishReceipt CreateReceipt(long sequence) =>
        new(OperationId.New(), sequence, SyncOperationState.SavedLocally, DateTimeOffset.UnixEpoch);

    /// <summary>Parses the integer payload emitted by the capture provider.</summary>
    /// <param name="payload">The payload envelope.</param>
    /// <returns>The parsed integer.</returns>
    private static int ParsePayload(PayloadEnvelope payload)
    {
        var text = System.Text.Encoding.UTF8.GetString(payload.Payload.Span);
        return int.Parse(text, CultureInfo.InvariantCulture);
    }

    /// <summary>Runs a deliberately blocking observer call independently of the test runner's thread pool.</summary>
    /// <param name="publish">The synchronous observer call.</param>
    /// <returns>The observer completion task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task RunSynchronousProducer(Action publish) =>
        Task.Factory.StartNew(publish, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    /// <summary>Waits for a synchronous test signal.</summary>
    /// <param name="signal">The signal to wait for.</param>
    /// <exception cref="TimeoutException">The expected signal was not observed.</exception>
    private static void WaitForSignal(ManualResetEventSlim signal)
    {
        if (signal.Wait(TimeSpan.FromSeconds(SignalTimeoutSeconds)))
        {
            return;
        }

        throw new TimeoutException("The expected producer signal was not observed.");
    }

    /// <summary>Creates a runtime fatal exception without invoking a reserved constructor.</summary>
    /// <returns>The fatal exception instance.</returns>
    private static Exception CreateFatalCallbackFailure() =>
        (Exception)RuntimeHelpers.GetUninitializedObject(typeof(OutOfMemoryException));

    /// <summary>Creates a runtime null value without suppressing nullable analysis.</summary>
    /// <typeparam name="T">The reference type to return.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T"/>.</returns>
    private static T MissingRequired<T>()
        where T : class
    {
        object? missing = null;
        return Unsafe.As<object?, T>(ref missing);
    }
}
