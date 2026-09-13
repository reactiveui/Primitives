// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Records privacy-preserving runtime metrics for occasionally connected streams.</summary>
internal sealed class OccasionallyConnectedMetrics : IDisposable
{
    /// <summary>The stable meter name for occasionally connected streams.</summary>
    private const string MeterName = "ReactiveUI.Primitives.OccasionallyConnected";

    /// <summary>The operations unit.</summary>
    private const string OperationsUnit = "operations";

    /// <summary>Tracks whether this recorder has been disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedMetrics"/> class.</summary>
    /// <param name="enabled">Whether this recorder should create instruments and emit measurements.</param>
    internal OccasionallyConnectedMetrics(bool enabled)
    {
        if (!enabled)
        {
            return;
        }

        Meter = new(MeterName);
        OperationsPublished = Meter.CreateCounter<long>("oc.operations.published", unit: OperationsUnit);
        OperationsSynchronized = Meter.CreateCounter<long>("oc.operations.synchronized", unit: OperationsUnit);
        OperationsRejected = Meter.CreateCounter<long>("oc.operations.rejected", unit: OperationsUnit);
        Conflicts = Meter.CreateCounter<long>("oc.conflicts", unit: "conflicts");
        Retries = Meter.CreateCounter<long>("oc.retries", unit: "retries");
        Duplicates = Meter.CreateCounter<long>("oc.duplicates", unit: "events");
        QueuePending = Meter.CreateUpDownCounter<long>("oc.queue.pending", unit: OperationsUnit);
        QueueBytes = Meter.CreateUpDownCounter<long>("oc.queue.bytes", unit: "bytes");
        SyncBatchSize = Meter.CreateHistogram<long>("oc.sync.batch.size", unit: OperationsUnit);
        SyncDuration = Meter.CreateHistogram<double>("oc.sync.duration", unit: "milliseconds");
        StoreCommitDuration = Meter.CreateHistogram<double>("oc.store.commit.duration", unit: "milliseconds");
        ConnectionStateChanges = Meter.CreateCounter<long>("oc.connection.state_changes", unit: "transitions");
        DeadLetters = Meter.CreateCounter<long>("oc.dead_letters", unit: OperationsUnit);
    }

    /// <summary>Gets the meter owned by this recorder, if enabled.</summary>
    internal Meter? Meter { get; }

    /// <summary>Gets the published operation counter.</summary>
    private Counter<long>? OperationsPublished { get; }

    /// <summary>Gets the synchronized operation counter.</summary>
    private Counter<long>? OperationsSynchronized { get; }

    /// <summary>Gets the rejected operation counter.</summary>
    private Counter<long>? OperationsRejected { get; }

    /// <summary>Gets the conflict counter.</summary>
    private Counter<long>? Conflicts { get; }

    /// <summary>Gets the retry counter.</summary>
    private Counter<long>? Retries { get; }

    /// <summary>Gets the duplicate counter.</summary>
    private Counter<long>? Duplicates { get; }

    /// <summary>Gets the pending queue delta counter.</summary>
    private UpDownCounter<long>? QueuePending { get; }

    /// <summary>Gets the queue byte delta counter.</summary>
    private UpDownCounter<long>? QueueBytes { get; }

    /// <summary>Gets the synchronization batch size histogram.</summary>
    private Histogram<long>? SyncBatchSize { get; }

    /// <summary>Gets the synchronization duration histogram.</summary>
    private Histogram<double>? SyncDuration { get; }

    /// <summary>Gets the store commit duration histogram.</summary>
    private Histogram<double>? StoreCommitDuration { get; }

    /// <summary>Gets the connection state transition counter.</summary>
    private Counter<long>? ConnectionStateChanges { get; }

    /// <summary>Gets the dead letter counter.</summary>
    private Counter<long>? DeadLetters { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        Meter?.Dispose();
    }

    /// <summary>Records published operations.</summary>
    /// <param name="count">The number of operations.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordOperationPublished(long count = 1) => RecordMonotonic(OperationsPublished, count);

    /// <summary>Records synchronized operations.</summary>
    /// <param name="count">The number of operations.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordOperationSynchronized(long count = 1) => RecordMonotonic(OperationsSynchronized, count);

    /// <summary>Records rejected operations.</summary>
    /// <param name="count">The number of operations.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordOperationRejected(long count = 1) => RecordMonotonic(OperationsRejected, count);

    /// <summary>Records resolved conflicts.</summary>
    /// <param name="count">The number of conflicts.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordConflict(long count = 1) => RecordMonotonic(Conflicts, count);

    /// <summary>Records retry attempts.</summary>
    /// <param name="count">The number of retries.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordRetry(long count = 1) => RecordMonotonic(Retries, count);

    /// <summary>Records duplicate events.</summary>
    /// <param name="count">The number of duplicate events.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordDuplicate(long count = 1) => RecordMonotonic(Duplicates, count);

    /// <summary>Records a pending queue delta.</summary>
    /// <param name="delta">The operation count delta.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordQueuePending(long delta) => RecordDelta(QueuePending, delta);

    /// <summary>Records a queue byte delta.</summary>
    /// <param name="delta">The byte count delta.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordQueueBytes(long delta) => RecordDelta(QueueBytes, delta);

    /// <summary>Records the size of a synchronization batch.</summary>
    /// <param name="count">The number of operations in the batch.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordSyncBatchSize(long count) => RecordHistogram(SyncBatchSize, count);

    /// <summary>Records synchronization duration.</summary>
    /// <param name="duration">The elapsed duration.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordSyncDuration(TimeSpan duration) => RecordDuration(SyncDuration, duration);

    /// <summary>Records store commit duration.</summary>
    /// <param name="duration">The elapsed duration.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordStoreCommitDuration(TimeSpan duration) => RecordDuration(StoreCommitDuration, duration);

    /// <summary>Records a connection lifecycle state transition.</summary>
    /// <param name="count">The number of transitions.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordConnectionStateChange(long count = 1) => RecordMonotonic(ConnectionStateChanges, count);

    /// <summary>Records dead-lettered operations.</summary>
    /// <param name="count">The number of operations.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RecordDeadLetter(long count = 1) => RecordMonotonic(DeadLetters, count);

    /// <summary>Records a positive value on a monotonic instrument.</summary>
    /// <param name="instrument">The instrument to record.</param>
    /// <param name="value">The positive value to record.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordMonotonic(Counter<long>? instrument, long value)
    {
        if (value <= 0 || Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        instrument?.Add(value);
    }

    /// <summary>Records a non-zero queue delta.</summary>
    /// <param name="instrument">The instrument to record.</param>
    /// <param name="value">The signed delta to record.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordDelta(UpDownCounter<long>? instrument, long value)
    {
        if (value == 0 || Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        instrument?.Add(value);
    }

    /// <summary>Records a positive histogram value.</summary>
    /// <param name="instrument">The instrument to record.</param>
    /// <param name="value">The positive value to record.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordHistogram(Histogram<long>? instrument, long value)
    {
        if (value <= 0 || Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        instrument?.Record(value);
    }

    /// <summary>Records a non-negative duration in milliseconds.</summary>
    /// <param name="instrument">The instrument to record.</param>
    /// <param name="duration">The duration to record.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RecordDuration(Histogram<double>? instrument, TimeSpan duration)
    {
        if (duration < TimeSpan.Zero || Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        instrument?.Record(duration.TotalMilliseconds);
    }
}
