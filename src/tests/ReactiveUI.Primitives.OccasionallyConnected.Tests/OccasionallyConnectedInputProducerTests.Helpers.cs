// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedInputProducer{TInput}"/>.</summary>
/// <content>Helper types for observer input producer tests.</content>
public sealed partial class OccasionallyConnectedInputProducerTests
{
    /// <summary>One caller-owned mutable input value.</summary>
    /// <param name="sequence">The sequence carried by the test input.</param>
    /// <param name="retainedBytes">The declared retained-byte bound for the captured envelope.</param>
    private sealed class CapturedInput(int sequence, long retainedBytes)
    {
        /// <summary>Gets or sets the mutable sequence carried by the test input.</summary>
        public int Sequence { get; set; } = sequence;

        /// <summary>Gets the declared retained-byte bound for the captured envelope.</summary>
        public long RetainedBytes { get; } = retainedBytes;
    }

    /// <summary>Records stable fault notifications from the input producer.</summary>
    private sealed class RecordingFaultSink
    {
        /// <summary>The number of polling attempts allowed while waiting for fault publication.</summary>
        private const int FaultWaitAttempts = 500;

        /// <summary>The fault polling interval in milliseconds.</summary>
        private const int FaultPollMilliseconds = 10;

        /// <summary>Gets a sink used when tests ignore fault notifications.</summary>
        public static RecordingFaultSink Ignored { get; } = new();

        /// <summary>Gets the recorded faults.</summary>
        public List<RecordedFault> Faults { get; } = [];

        /// <summary>Gets or sets the callback invoked after one producer fault is recorded.</summary>
        public Action<RecordedFault> AfterRecord { get; set; } = static _ => { };

        /// <summary>Records one producer fault.</summary>
        /// <param name="code">The stable fault code.</param>
        /// <param name="message">The diagnostic message.</param>
        /// <param name="operationId">The optional operation identifier.</param>
        /// <param name="exception">The local exception.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Record(string code, string message, OperationId? operationId, Exception exception)
        {
            var fault = new RecordedFault(code, message, operationId, exception.GetType());
            Faults.Add(fault);
            AfterRecord(fault);
        }

        /// <summary>Waits for the supplied fault count.</summary>
        /// <param name="count">The expected fault count.</param>
        /// <returns>The wait task.</returns>
        /// <exception cref="TimeoutException">The expected fault count was not observed.</exception>
        public async Task WaitForFaultCountAsync(int count)
        {
            for (var attempt = 0; attempt < FaultWaitAttempts; attempt++)
            {
                if (Faults.Count >= count)
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(FaultPollMilliseconds)).ConfigureAwait(false);
            }

            throw new TimeoutException("The expected fault count was not observed.");
        }
    }

    /// <summary>Stores one producer fault notification.</summary>
    private sealed class RecordedFault
    {
        /// <summary>Initializes a new instance of the <see cref="RecordedFault"/> class.</summary>
        /// <param name="code">The stable fault code.</param>
        /// <param name="message">The diagnostic message.</param>
        /// <param name="operationId">The optional operation identifier.</param>
        /// <param name="exceptionType">The exception type.</param>
        internal RecordedFault(string code, string message, OperationId? operationId, Type exceptionType)
        {
            Code = code;
            Message = message;
            OperationId = operationId;
            ExceptionType = exceptionType;
        }

        /// <summary>Gets the stable fault code.</summary>
        public string Code { get; }

        /// <summary>Gets the diagnostic message.</summary>
        public string Message { get; }

        /// <summary>Gets the optional operation identifier.</summary>
        public OperationId? OperationId { get; }

        /// <summary>Gets the exception type.</summary>
        public Type ExceptionType { get; }
    }

    /// <summary>Records input sizing and capture calls.</summary>
    private class RecordingInputCapture : IOccasionallyConnectedInputCapture<CapturedInput>
    {
        /// <summary>The payload schema version used by capture tests.</summary>
        private const int PayloadSchemaVersion = 1;

        /// <summary>Gets the sequence values supplied to <see cref="GetRetainedByteCount"/>.</summary>
        public List<int> SizedValues { get; } = [];

        /// <summary>Gets the sequence values supplied to <see cref="Capture"/>.</summary>
        public List<int> CapturedValues { get; } = [];

        /// <summary>Gets or sets the sequence that should fail during sizing.</summary>
        public int ThrowOnSizingSequence { get; init; }

        /// <summary>Gets or sets the sequence that should fail during capture.</summary>
        public int ThrowOnSequence { get; init; }

        /// <summary>Gets or sets a value indicating whether capture should return oversized metadata.</summary>
        public bool UseLargeEnvelopeHeaders { get; set; }

        /// <inheritdoc />
        public virtual long GetRetainedByteCount(CapturedInput value)
        {
            if (value.Sequence == ThrowOnSizingSequence)
            {
                throw new InvalidOperationException("sizing failed");
            }

            SizedValues.Add(value.Sequence);
            return value.RetainedBytes;
        }

        /// <inheritdoc />
        public virtual PayloadEnvelope Capture(CapturedInput value)
        {
            if (value.Sequence == ThrowOnSequence)
            {
                throw new InvalidOperationException("capture failed");
            }

            CapturedValues.Add(value.Sequence);
            var text = value.Sequence.ToString(CultureInfo.InvariantCulture);
            var contentType = UseLargeEnvelopeHeaders
                ? "text/plain; owner=producer; purpose=retained-envelope-boundary"
                : "text/plain";
            var hash = UseLargeEnvelopeHeaders ? $"hash-{text}-with-extra-retained-header-bytes" : $"hash-{text}";
            return new(
                "counter-input",
                PayloadSchemaVersion,
                contentType,
                System.Text.Encoding.UTF8.GetBytes(text),
                hash);
        }
    }

    /// <summary>Blocks sizing before throwing to expose disposal ownership of sizing faults.</summary>
    /// <param name="entered">The sizing-entry signal.</param>
    /// <param name="release">The signal that releases sizing.</param>
    private sealed class BlockingSizingFailureCapture(
        ManualResetEventSlim entered,
        ManualResetEventSlim release) : RecordingInputCapture
    {
        /// <inheritdoc />
        public override long GetRetainedByteCount(CapturedInput value)
        {
            entered.Set();
            WaitForSignal(release);
            throw new InvalidOperationException("sizing failed");
        }
    }

    /// <summary>Blocks capture before throwing to expose disposal ownership of capture faults.</summary>
    /// <param name="entered">The capture-entry signal.</param>
    /// <param name="release">The signal that releases capture.</param>
    private sealed class BlockingCaptureFailureCapture(
        ManualResetEventSlim entered,
        ManualResetEventSlim release) : RecordingInputCapture
    {
        /// <inheritdoc />
        public override PayloadEnvelope Capture(CapturedInput value)
        {
            entered.Set();
            WaitForSignal(release);
            throw new InvalidOperationException("capture failed");
        }
    }

    /// <summary>Delays the first capture while allowing a later capture to complete.</summary>
    private sealed class DelayedFirstInputCapture : RecordingInputCapture, IDisposable
    {
        /// <summary>Signals that the first capture entered the provider.</summary>
        private readonly ManualResetEventSlim _firstCaptureEntered = new();

        /// <summary>Signals that the first capture may complete.</summary>
        private readonly ManualResetEventSlim _releaseFirstCapture = new();

        /// <summary>Signals that the second capture completed.</summary>
        private readonly ManualResetEventSlim _secondCaptureCompleted = new();

        /// <inheritdoc />
        public override PayloadEnvelope Capture(CapturedInput value)
        {
            if (value.Sequence == FirstInputSequence)
            {
                _firstCaptureEntered.Set();
                WaitForSignal(_releaseFirstCapture);
            }

            var payload = base.Capture(value);
            if (value.Sequence == SecondInputSequence)
            {
                _secondCaptureCompleted.Set();
            }

            return payload;
        }

        /// <summary>Waits until the first capture has started.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitForFirstCapture() => WaitForSignal(_firstCaptureEntered);

        /// <summary>Waits until the second capture has completed.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitForSecondCapture() => WaitForSignal(_secondCaptureCompleted);

        /// <summary>Allows the first capture to complete.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseFirstCapture() => _releaseFirstCapture.Set();

        /// <inheritdoc />
        public void Dispose()
        {
            _firstCaptureEntered.Dispose();
            _releaseFirstCapture.Dispose();
            _secondCaptureCompleted.Dispose();
        }

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
    }

    /// <summary>Closes the producer from retained-byte sizing before reservation occurs.</summary>
    private sealed class CompletingSizingInputCapture : RecordingInputCapture
    {
        /// <summary>Gets or sets the observer to complete during sizing.</summary>
        public IObserver<CapturedInput>? Observer { get; set; }

        /// <inheritdoc />
        public override long GetRetainedByteCount(CapturedInput value)
        {
            var retainedBytes = base.GetRetainedByteCount(value);
            Observer?.OnCompleted();
            return retainedBytes;
        }
    }

    /// <summary>Returns a runtime null payload to verify producer capture validation.</summary>
    private sealed class NullReturningInputCapture : RecordingInputCapture
    {
        /// <inheritdoc />
        public override PayloadEnvelope Capture(CapturedInput value)
        {
            CapturedValues.Add(value.Sequence);
            return MissingRequired<PayloadEnvelope>();
        }
    }

    /// <summary>Blocks the second capture before copying caller state.</summary>
    private sealed class BlockingSecondInputCapture : RecordingInputCapture, IDisposable
    {
        /// <summary>Signals that the second capture entered the provider.</summary>
        private readonly ManualResetEventSlim _secondCaptureEntered = new();

        /// <summary>Signals that the second capture may copy caller state.</summary>
        private readonly ManualResetEventSlim _releaseSecondCapture = new();

        /// <inheritdoc />
        public override PayloadEnvelope Capture(CapturedInput value)
        {
            if (value.Sequence == SecondInputSequence)
            {
                _secondCaptureEntered.Set();
                WaitForSignal(_releaseSecondCapture);
            }

            return base.Capture(value);
        }

        /// <summary>Waits until the second capture has started.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitForSecondCapture() => WaitForSignal(_secondCaptureEntered);

        /// <summary>Allows the second capture to complete.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseSecondCapture() => _releaseSecondCapture.Set();

        /// <inheritdoc />
        public void Dispose()
        {
            _secondCaptureEntered.Dispose();
            _releaseSecondCapture.Dispose();
        }
    }

    /// <summary>Reenters the producer observer while capture is running.</summary>
    private sealed class ReentrantInputCapture : RecordingInputCapture
    {
        /// <summary>Gets or sets the observer to reenter during the first capture.</summary>
        public IObserver<CapturedInput>? Observer { get; set; }

        /// <inheritdoc />
        public override PayloadEnvelope Capture(CapturedInput value)
        {
            if (value.Sequence == FirstInputSequence && Observer is { } observer)
            {
                observer.OnNext(new(SecondInputSequence, NormalRetainedBytes));
            }

            return base.Capture(value);
        }
    }

    /// <summary>Records successful publish callbacks.</summary>
    private sealed class RecordingPublisher
    {
        /// <summary>The number of polling attempts allowed while waiting for publication.</summary>
        private const int PublishWaitAttempts = 500;

        /// <summary>The publication polling interval in milliseconds.</summary>
        private const int PublishPollMilliseconds = 10;

        /// <summary>Gets the published payload values.</summary>
        public List<int> PublishedValues { get; } = [];

        /// <summary>Gets or sets whether volatile publish options are accepted.</summary>
        public bool AllowVolatile { get; init; }

        /// <summary>Publishes one captured payload.</summary>
        /// <param name="payload">The captured input payload.</param>
        /// <param name="options">The publish options.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The publish receipt.</returns>
        /// <exception cref="InvalidOperationException">The test publish options are not durable.</exception>
        public ValueTask<PublishReceipt> PublishAsync(
            PayloadEnvelope payload,
            RemotePublishOptions? options,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!AllowVolatile && options is { Durable: false })
            {
                throw new InvalidOperationException(DurablePublicationRequiredMessage);
            }

            var value = ParsePayload(payload);
            PublishedValues.Add(value);

            return new(CreateReceipt(PublishedValues.Count));
        }

        /// <summary>Waits for the supplied publish count.</summary>
        /// <param name="count">The expected publish count.</param>
        /// <returns>The wait task.</returns>
        /// <exception cref="TimeoutException">The expected publish count was not observed.</exception>
        public async Task WaitForPublishedCountAsync(int count)
        {
            for (var attempt = 0; attempt < PublishWaitAttempts; attempt++)
            {
                if (PublishedValues.Count >= count)
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(PublishPollMilliseconds)).ConfigureAwait(false);
            }

            throw new TimeoutException("The expected publish count was not observed.");
        }
    }

    /// <summary>Fails the first publish attempt and records later successful attempts.</summary>
    private sealed class FailFirstPublisher
    {
        /// <summary>The number of polling attempts allowed while waiting for publication.</summary>
        private const int PublishWaitAttempts = 500;

        /// <summary>The publication polling interval in milliseconds.</summary>
        private const int PublishPollMilliseconds = 10;

        /// <summary>Gets the published payload values.</summary>
        public List<int> PublishedValues { get; } = [];

        /// <summary>Gets the number of publish attempts.</summary>
        public int AttemptCount { get; private set; }

        /// <summary>Gets or sets an optional release gate for the first failing publication.</summary>
        public TaskCompletionSource? ReleaseFirstFailure { get; init; }

        /// <summary>Publishes one captured payload, failing only the first attempt.</summary>
        /// <param name="payload">The captured input payload.</param>
        /// <param name="options">The publish options.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The publish receipt.</returns>
        /// <exception cref="InvalidOperationException">The first publish attempt fails.</exception>
        public async ValueTask<PublishReceipt> PublishAsync(
            PayloadEnvelope payload,
            RemotePublishOptions? options,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (options is { Durable: false })
            {
                throw new InvalidOperationException(DurablePublicationRequiredMessage);
            }

            AttemptCount++;
            var value = ParsePayload(payload);
            if (AttemptCount == SingleInputCapacity)
            {
                if (ReleaseFirstFailure is { } release)
                {
                    await release.Task
                        .WaitAsync(TimeSpan.FromSeconds(SignalTimeoutSeconds), cancellationToken)
                        .ConfigureAwait(false);
                }

                throw new InvalidOperationException("publish failed");
            }

            PublishedValues.Add(value);
            return CreateReceipt(AttemptCount);
        }

        /// <summary>Waits for the supplied publish attempt count.</summary>
        /// <param name="count">The expected attempt count.</param>
        /// <returns>The wait task.</returns>
        /// <exception cref="TimeoutException">The expected publish attempt count was not observed.</exception>
        public async Task WaitForAttemptCountAsync(int count)
        {
            for (var attempt = 0; attempt < PublishWaitAttempts; attempt++)
            {
                if (AttemptCount >= count)
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(PublishPollMilliseconds)).ConfigureAwait(false);
            }

            throw new TimeoutException("The expected publish attempt count was not observed.");
        }
    }

    /// <summary>Blocks publication until the test releases the callback.</summary>
    private sealed class BlockingPublisher
    {
        /// <summary>Stores the signal that allows the publisher to finish.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when publishing starts.</summary>
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Publishes one captured payload after the test releases it.</summary>
        /// <param name="payload">The captured input payload.</param>
        /// <param name="options">The publish options.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The publish receipt.</returns>
        /// <exception cref="InvalidOperationException">The test publish options are not durable.</exception>
        public async ValueTask<PublishReceipt> PublishAsync(
            PayloadEnvelope payload,
            RemotePublishOptions? options,
            CancellationToken cancellationToken)
        {
            if (options is { Durable: false })
            {
                throw new InvalidOperationException(DurablePublicationRequiredMessage);
            }

            _ = Started.TrySetResult();
            await _release.Task
                .WaitAsync(TimeSpan.FromSeconds(SignalTimeoutSeconds), cancellationToken)
                .ConfigureAwait(false);
            return CreateReceipt(ParsePayload(payload));
        }

        /// <summary>Releases the blocked publish callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release() => _ = _release.TrySetResult();
    }
}
