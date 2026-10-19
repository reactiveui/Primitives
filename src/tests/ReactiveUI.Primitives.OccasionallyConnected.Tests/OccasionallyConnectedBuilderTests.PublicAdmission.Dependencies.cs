// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Fixtures for public producer admission tests of contexts composed by <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The operation limit that leaves byte capacity as the only binding outbox limit.</summary>
    private const int PublicAdmissionOperationLimit = 10;

    /// <summary>The outbox byte budget used by public admission tests.</summary>
    private const long PublicAdmissionOutboxBytes = 4096;

    /// <summary>A padded payload size that fits an empty outbox once but not twice.</summary>
    private const int HalfOutboxPayloadDelta = 2500;

    /// <summary>A padded payload size that cannot fit even an empty outbox.</summary>
    private const int OversizedPayloadDelta = 5000;

    /// <summary>The retained bytes each observer input declares.</summary>
    private const long DeclaredObserverInputBytes = 512;

    /// <summary>The observer byte budget that admits one declared input but not two.</summary>
    private const long ObserverInputBufferBytes = 768;

    /// <summary>The observer item budget that leaves byte capacity as the only binding observer limit.</summary>
    private const int ObserverInputBufferCount = 10;

    /// <summary>The first observer input delta, which also gates its committed state serialization.</summary>
    private const int GatedInputDelta = 3;

    /// <summary>The observer input delta expected to be rejected.</summary>
    private const int RejectedInputDelta = 4;

    /// <summary>The observer input delta published by an independent producer.</summary>
    private const int IndependentInputDelta = 6;

    /// <summary>The number of cancelled blocked publishers used to detect leaked admission.</summary>
    private const int CancelledPublisherRounds = 3;

    /// <summary>The second local client sequence and the two-item count used by assertions.</summary>
    private const long SecondClientSequence = 2L;

    /// <summary>The fault code for observer input overflow.</summary>
    private const string InputOverflowFaultCode = "OC.Stream.InputOverflow";

    /// <summary>The fault code for observer producer termination by OnError.</summary>
    private const string InputProducerFaultCode = "OC.Stream.InputProducer";

    /// <summary>The content type written by public admission payloads.</summary>
    private const string PaddedContentType = "text/padded";

    /// <summary>Creates an uninitialized SQLite store in a fresh test directory.</summary>
    /// <param name="prefix">The test directory prefix.</param>
    /// <returns>The SQLite store.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SqliteLocalStoreAdapter CreatePublicAdmissionStore(string prefix) =>
        new(Path.Combine(SqliteTestDirectory.Create(prefix).FullName, RecoveredUploadDatabaseFileName));

    /// <summary>Creates a context builder with explicit outbox limits and a padded payload serializer.</summary>
    /// <param name="store">The borrowed store.</param>
    /// <param name="transport">The borrowed transport.</param>
    /// <param name="serializer">The payload serializer.</param>
    /// <param name="outbox">The outbox limits.</param>
    /// <returns>The configured builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedBuilder CreatePublicAdmissionBuilder(
        ILocalStoreAdapter store,
        IRemoteTransportAdapter transport,
        IPayloadSerializer serializer,
        OutboxOptions outbox) =>
        CreateBuilder()
            .UseClient(new(ClientId))
            .UseBorrowedStore(store)
            .UseBorrowedTransport(transport)
            .UseSerializer(serializer)
            .UseStoreIdentity(StoreIdentity)
            .UseOptions(OccasionallyConnectedOptions.Default with { Outbox = outbox });

    /// <summary>Creates outbox limits bound only by bytes.</summary>
    /// <returns>The outbox limits.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OutboxOptions CreateByteBoundOutbox() =>
        new() { MaxOperations = PublicAdmissionOperationLimit, MaxBytes = PublicAdmissionOutboxBytes };

    /// <summary>Creates outbox limits that hold exactly one unresolved operation.</summary>
    /// <param name="maximumBlockedPublishers">The maximum concurrent local publishers.</param>
    /// <returns>The outbox limits.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OutboxOptions CreateSingleOperationOutbox(int maximumBlockedPublishers) =>
        new() { MaxOperations = 1, MaxBytes = PublicAdmissionOutboxBytes, MaximumBlockedPublishers = maximumBlockedPublishers };

    /// <summary>Creates publish options for the default public admission stream.</summary>
    /// <param name="strategy">The admission strategy.</param>
    /// <param name="durable">Whether the publication is durable.</param>
    /// <returns>The publish options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemotePublishOptions CreatePublicPublishOptions(BufferStrategy strategy, bool durable) =>
        new() { StreamId = Stream, AdmissionStrategy = strategy, Durable = durable };

    /// <summary>Creates a stream definition whose observer input bridge is bounded by bytes.</summary>
    /// <param name="strategy">The observer input strategy.</param>
    /// <param name="bufferCount">The observer input item capacity.</param>
    /// <returns>The stream definition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static StreamDefinition<CounterState, CounterInput> CreateObserverInputDefinition(BufferStrategy strategy, int bufferCount) =>
        CreateDefinition() with
        {
            Publish = CreatePublicPublishOptions(BufferStrategy.Reject, durable: false),
            Input = new() { BufferStrategy = strategy, BufferCapacity = bufferCount, BufferCapacityBytes = ObserverInputBufferBytes },
            InputCapture = new DeclaredCounterInputCapture(),
        };

    /// <summary>Asserts the pending operation payload values for the default stream in commit order.</summary>
    /// <param name="store">The store.</param>
    /// <param name="subscriptionId">The durable subscription identity.</param>
    /// <param name="expectedFirst">The first expected pending value.</param>
    /// <param name="expectedSecond">The optional second expected pending value.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertPendingValuesAsync(
        SqliteLocalStoreAdapter store,
        SubscriptionId subscriptionId,
        int expectedFirst,
        int? expectedSecond = null)
    {
        var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).ConfigureAwait(false);
        var pending = recovered.PendingOperations;
        await Assert.That(pending.Count).IsEqualTo(expectedSecond is null ? 1 : (int)SecondClientSequence);
        await Assert.That(PaddedCounterPayloadSerializer.Parse(pending[0].Payload)).IsEqualTo(expectedFirst);
        if (expectedSecond is { } second)
        {
            await Assert.That(PaddedCounterPayloadSerializer.Parse(pending[1].Payload)).IsEqualTo(second);
        }
    }

    /// <summary>Asserts the observed stream fault codes in publication order.</summary>
    /// <param name="faults">The observed faults.</param>
    /// <param name="expectedFirst">The first expected fault code.</param>
    /// <param name="expectedSecond">The optional second expected fault code.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertFaultCodesAsync(
        RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault> faults,
        string expectedFirst,
        string? expectedSecond = null)
    {
        var values = faults.Values;
        await Assert.That(values.Count).IsEqualTo(expectedSecond is null ? 1 : (int)SecondClientSequence);
        await Assert.That(values[0].Code).IsEqualTo(expectedFirst);
        if (expectedSecond is not null)
        {
            await Assert.That(values[1].Code).IsEqualTo(expectedSecond);
        }
    }

    /// <summary>Serializes counter values as zero-padded text so tests control payload size through the value.</summary>
    /// <param name="gatedStateSum">The committed state sum whose serialization waits for <see cref="ReleaseGate"/>.</param>
    private sealed class PaddedCounterPayloadSerializer(int gatedStateSum = int.MinValue) : IPayloadSerializer
    {
        /// <summary>The signal released by the test to unblock gated state serialization.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the signal set when gated state serialization starts.</summary>
        public TaskCompletionSource GateEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc />
        public string ContentType => PaddedContentType;

        /// <summary>Parses a padded counter payload.</summary>
        /// <param name="envelope">The payload envelope.</param>
        /// <returns>The encoded value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Parse(PayloadEnvelope envelope) =>
            int.Parse(Encoding.UTF8.GetString(envelope.Payload.Span), CultureInfo.InvariantCulture);

        /// <summary>Creates a payload whose length equals the input delta when the delta is wider than its digits.</summary>
        /// <param name="contractId">The payload contract.</param>
        /// <param name="value">The encoded value.</param>
        /// <returns>The payload envelope.</returns>
        public static PayloadEnvelope CreateInputPayload(string contractId, int value)
        {
            var text = value.ToString(CultureInfo.InvariantCulture).PadLeft(value, '0');
            return new(contractId, 1, PaddedContentType, Encoding.UTF8.GetBytes(text), $"hash-{value.ToString(CultureInfo.InvariantCulture)}");
        }

        /// <summary>Releases gated state serialization.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseGate() => _release.TrySetResult();

        /// <inheritdoc />
        public async ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            switch (value)
            {
                case CounterInput input:
                {
                    return CreateInputPayload(contractId, input.Delta);
                }

                case CounterState state:
                {
                    if (state.Sum == gatedStateSum)
                    {
                        _ = GateEntered.TrySetResult();
                        await _release.Task.WaitAsync(GuardTimeout, cancellationToken).ConfigureAwait(false);
                    }

                    var text = state.Sum.ToString(CultureInfo.InvariantCulture);
                    return new(contractId, schemaVersion, ContentType, Encoding.UTF8.GetBytes(text), $"hash-{text}");
                }

                default:
                {
                    throw new InvalidOperationException("Unexpected payload type.");
                }
            }
        }

        /// <inheritdoc />
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            var value = Parse(envelope);
            if (targetType == typeof(CounterInput))
            {
                return new(new CounterInput(value));
            }

            if (targetType == typeof(CounterState))
            {
                return new(new CounterState(value));
            }

            throw new InvalidOperationException("Unexpected target type.");
        }
    }

    /// <summary>Captures observer input with a fixed declared retained-byte bound.</summary>
    private sealed class DeclaredCounterInputCapture : IOccasionallyConnectedInputCapture<CounterInput>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRetainedByteCount(CounterInput value) => DeclaredObserverInputBytes;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PayloadEnvelope Capture(CounterInput value) => PaddedCounterPayloadSerializer.CreateInputPayload(InputContract, value.Delta);
    }
}
