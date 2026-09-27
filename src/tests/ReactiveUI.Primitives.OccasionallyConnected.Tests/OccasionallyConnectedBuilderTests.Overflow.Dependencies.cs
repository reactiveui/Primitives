// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Fixtures for outbox overflow strategy tests of contexts composed by <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The outbox operation limit used by overflow tests.</summary>
    private const int OverflowOutboxOperations = 2;

    /// <summary>The second publication delta used by overflow tests.</summary>
    private const int OverflowSecondDelta = 2;

    /// <summary>The third publication delta used by overflow tests.</summary>
    private const int OverflowThirdDelta = 3;

    /// <summary>The fourth publication delta used by overflow tests.</summary>
    private const int OverflowFourthDelta = 4;

    /// <summary>The local sum after the first publication is evicted from 1, 2, and 3.</summary>
    private const int OverflowSumAfterFirstEviction = 5;

    /// <summary>The fault code for a handled outbox overflow.</summary>
    private const string OutboxOverflowFaultCode = "OC.Stream.OutboxOverflow";

    /// <summary>The dead-letter reason code for a DropOldest eviction.</summary>
    private const string DroppedOldestReasonCode = "OC.Overflow.DroppedOldest";

    /// <summary>The dead-letter reason code for a custom policy eviction.</summary>
    private const string CustomEvictedReasonCode = "OC.Overflow.CustomEvicted";

    /// <summary>The overflow metric instrument name.</summary>
    private const string QueueOverflowInstrumentName = "oc.queue.overflow";

    /// <summary>The meter name owned by the synchronization engine.</summary>
    private const string OccasionallyConnectedMeterName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The lease duration used when a test simulates an in-flight upload.</summary>
    private static readonly TimeSpan SimulatedUploadLeaseDuration = TimeSpan.FromMinutes(1);

    /// <summary>Creates a context builder whose outbox holds two unresolved operations.</summary>
    /// <param name="store">The borrowed store.</param>
    /// <param name="transport">The borrowed transport.</param>
    /// <returns>The configured builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedBuilder CreateOverflowBuilder(ILocalStoreAdapter store, IRemoteTransportAdapter transport) =>
        CreatePublicAdmissionBuilder(
            store,
            transport,
            new PaddedCounterPayloadSerializer(),
            new() { MaxOperations = OverflowOutboxOperations, MaxBytes = PublicAdmissionOutboxBytes });

    /// <summary>Leases the head operation of the default stream to simulate an in-flight upload.</summary>
    /// <param name="store">The store.</param>
    /// <returns>The lease.</returns>
    /// <exception cref="InvalidOperationException">No operation could be leased.</exception>
    private static async Task<LeasedOperationBatch> LeaseHeadAsync(SqliteLocalStoreAdapter store)
    {
        var request = new OutboxLeaseRequest(Stream, 1, PublicAdmissionOutboxBytes, SimulatedUploadLeaseDuration);
        await using var enumerator = store.LeasePendingOperationsAsync(request, CancellationToken.None).GetAsyncEnumerator();
        return await enumerator.MoveNextAsync().ConfigureAwait(false)
            ? enumerator.Current
            : throw new InvalidOperationException("The stream head could not be leased.");
    }

    /// <summary>Asserts an operation was dead-lettered with a reason code.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operationId">The operation.</param>
    /// <param name="reasonCode">The expected reason code.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertDeadLetteredAsync(SqliteLocalStoreAdapter store, OperationId operationId, string reasonCode)
    {
        var status = await store.GetOperationStatusAsync(operationId, CancellationToken.None).ConfigureAwait(false);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.DeadLettered);
        await Assert.That(status?.ReasonCode).IsEqualTo(reasonCode);
    }

    /// <summary>Creates a listener that counts overflow measurements from any occasionally connected meter.</summary>
    /// <param name="counter">The shared measurement counter.</param>
    /// <returns>The started listener.</returns>
    private static MeterListener CreateOverflowListener(OverflowMeasurementCounter counter)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == OccasionallyConnectedMeterName && instrument.Name == QueueOverflowInstrumentName)
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => counter.Add(value));
        listener.Start();
        return listener;
    }

    /// <summary>Counts overflow measurements across threads.</summary>
    private sealed class OverflowMeasurementCounter
    {
        /// <summary>Stores the measured total.</summary>
        private long _total;

        /// <summary>Gets the measured total.</summary>
        public long Total => Interlocked.Read(ref _total);

        /// <summary>Adds a measurement.</summary>
        /// <param name="value">The measured value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(long value) => Interlocked.Add(ref _total, value);
    }

    /// <summary>Returns scripted overflow decisions and records every context it receives.</summary>
    /// <param name="decide">The scripted decision function.</param>
    private sealed class ScriptedOverflowPolicy(Func<BufferOverflowContext, BufferOverflowDecision> decide) : IBufferOverflowPolicy
    {
        /// <summary>Protects the recorded contexts.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores the recorded contexts.</summary>
        private readonly List<BufferOverflowContext> _contexts = [];

        /// <summary>Gets a stable snapshot of the recorded contexts.</summary>
        public List<BufferOverflowContext> Contexts
        {
            get
            {
                lock (_gate)
                {
                    return [.. _contexts];
                }
            }
        }

        /// <inheritdoc />
        public BufferOverflowDecision Decide(BufferOverflowContext context)
        {
            lock (_gate)
            {
                _contexts.Add(context);
            }

            return decide(context);
        }
    }
}
