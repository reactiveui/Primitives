// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.Metrics;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedMetrics"/>.</summary>
public sealed class OccasionallyConnectedMetricsTests
{
    /// <summary>The precision used when comparing measurements.</summary>
    private const double MeasurementTolerance = 0.000001;

    /// <summary>The recorded test value.</summary>
    private const long RecordedValue = 7;

    /// <summary>The number of duration instruments.</summary>
    private const int DurationCount = 2;

    /// <summary>The required instrument count.</summary>
    private const int InstrumentCount = 13;

    /// <summary>The concurrent write count.</summary>
    private const int ConcurrentWrites = 256;

    /// <summary>The counter kind name.</summary>
    private const string CounterKind = "Counter";

    /// <summary>The operations unit name.</summary>
    private const string OperationsUnit = "operations";

    /// <summary>The pending queue instrument name.</summary>
    private const string PendingName = "oc.queue.pending";

    /// <summary>The pending queue increase.</summary>
    private const long PendingIncrease = 3;

    /// <summary>The pending queue decrease.</summary>
    private const long PendingDecrease = -1;

    /// <summary>The queue byte increase.</summary>
    private const long ByteIncrease = 20;

    /// <summary>The queue byte decrease.</summary>
    private const long ByteDecrease = -5;

    /// <summary>The queue measurement count.</summary>
    private const int QueueMeasurementCount = 4;

    /// <summary>Verifies all specified instruments publish untagged values.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RecordsSpecifiedMeasurementsWithoutTags()
    {
        using var recorder = new OccasionallyConnectedMetrics(enabled: true);
        using var listener = CreateListener(recorder, out var capture);

        recorder.RecordOperationPublished();
        recorder.RecordOperationSynchronized();
        recorder.RecordOperationRejected();
        recorder.RecordConflict();
        recorder.RecordRetry();
        recorder.RecordDuplicate();
        recorder.RecordQueuePending(1);
        recorder.RecordQueueBytes(1);
        recorder.RecordSyncBatchSize(1);
        recorder.RecordSyncDuration(TimeSpan.FromMilliseconds(1));
        recorder.RecordStoreCommitDuration(TimeSpan.FromMilliseconds(1));
        recorder.RecordConnectionStateChange();
        recorder.RecordDeadLetter();

        var instruments = capture.GetInstruments();
        var measurements = capture.GetMeasurements();
        await Assert.That(instruments).Count().IsEqualTo(InstrumentCount);
        await Assert.That(measurements).Count().IsEqualTo(InstrumentCount);
        await Assert.That(measurements.TrueForAll(static measurement => measurement.Tags.Length == 0)).IsTrue();
        await Assert.That(instruments.Exists(static item => item.Name == "oc.operations.published" && item.Kind == CounterKind && item.Unit == OperationsUnit)).IsTrue();
        await Assert.That(instruments.Exists(static item => item.Name == PendingName && item.Kind == "UpDownCounter" && item.Unit == OperationsUnit)).IsTrue();
        await Assert.That(instruments.Exists(static item => item.Name == "oc.sync.duration" && item.Kind == "Histogram" && item.Unit == "milliseconds")).IsTrue();
        await Assert.That(instruments.Exists(static item => item.Name == "oc.dead_letters" && item.Kind == CounterKind && item.Unit == OperationsUnit)).IsTrue();
    }

    /// <summary>Verifies queue deltas and recorder lifetime behavior.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RecordsQueueDeltasAndStopsAfterDisposal()
    {
        var recorder = new OccasionallyConnectedMetrics(enabled: true);
        using var listener = CreateListener(recorder, out var capture);

        recorder.RecordQueuePending(PendingIncrease);
        recorder.RecordQueuePending(PendingDecrease);
        recorder.RecordQueueBytes(ByteIncrease);
        recorder.RecordQueueBytes(ByteDecrease);
        recorder.Dispose();
        recorder.Dispose();
        recorder.RecordOperationPublished();

        var measurements = capture.GetMeasurements();
        await Assert.That(measurements.Exists(static item => item.Name == PendingName && Math.Abs(item.Value - PendingIncrease) < MeasurementTolerance)).IsTrue();
        await Assert.That(measurements.Exists(static item => item.Name == PendingName && Math.Abs(item.Value - PendingDecrease) < MeasurementTolerance)).IsTrue();
        await Assert.That(measurements.Exists(static item => item.Name == "oc.queue.bytes" && Math.Abs(item.Value - ByteIncrease) < MeasurementTolerance)).IsTrue();
        await Assert.That(measurements.Exists(static item => item.Name == "oc.queue.bytes" && Math.Abs(item.Value - ByteDecrease) < MeasurementTolerance)).IsTrue();
        await Assert.That(measurements).Count().IsEqualTo(QueueMeasurementCount);
    }

    /// <summary>Verifies disabled recorders and concurrent writes do not leak measurements.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DoesNotRecordWhenDisabledAndSupportsConcurrentWrites()
    {
        using var disabled = new OccasionallyConnectedMetrics(enabled: false);
        using var disabledListener = CreateListener(disabled, out var disabledCapture);
        disabled.RecordOperationPublished();
        await Assert.That(disabledCapture.GetMeasurements()).Count().IsEqualTo(0);

        using var recorder = new OccasionallyConnectedMetrics(enabled: true);
        using var listener = CreateListener(recorder, out var capture);
        _ = Parallel.For(0, ConcurrentWrites, _ => recorder.RecordOperationPublished());
        await Assert.That(capture.GetMeasurements()).Count().IsEqualTo(ConcurrentWrites);
    }

    /// <summary>Verifies every metric name, kind, unit, and emitted value.</summary>
    /// <param name="name">The stable instrument name.</param>
    /// <param name="kind">The expected instrument kind.</param>
    /// <param name="unit">The expected unit.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("oc.operations.published", "Counter", "operations")]
    [Arguments("oc.operations.synchronized", "Counter", "operations")]
    [Arguments("oc.operations.rejected", "Counter", "operations")]
    [Arguments("oc.conflicts", "Counter", "conflicts")]
    [Arguments("oc.retries", "Counter", "retries")]
    [Arguments("oc.duplicates", "Counter", "events")]
    [Arguments("oc.queue.pending", "UpDownCounter", "operations")]
    [Arguments("oc.queue.bytes", "UpDownCounter", "bytes")]
    [Arguments("oc.sync.batch.size", "Histogram", "operations")]
    [Arguments("oc.sync.duration", "Histogram", "milliseconds")]
    [Arguments("oc.store.commit.duration", "Histogram", "milliseconds")]
    [Arguments("oc.connection.state_changes", "Counter", "transitions")]
    [Arguments("oc.dead_letters", "Counter", "operations")]
    public async Task PublishesExpectedDefinitionAndValue(string name, string kind, string unit)
    {
        using var recorder = new OccasionallyConnectedMetrics(enabled: true);
        using var listener = CreateListener(recorder, out var capture);
        RecordAll(recorder, RecordedValue);
        var definition = capture.GetInstruments().Single(item => item.Name == name);
        var measurement = capture.GetMeasurements().Single(item => item.Name == name);
        await Assert.That(definition.Kind).IsEqualTo(kind);
        await Assert.That(definition.Unit).IsEqualTo(unit);
        await Assert.That(measurement.Value).IsEqualTo(RecordedValue);
        await Assert.That(measurement.Tags).Count().IsEqualTo(0);
    }

    /// <summary>Verifies disabled and disposed instruments stay silent for every instrument kind.</summary>
    /// <param name="enabled">Whether to create the meter.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task StopsEveryInstrumentAfterDisposal(bool enabled)
    {
        var recorder = new OccasionallyConnectedMetrics(enabled);
        using var listener = CreateListener(recorder, out var capture);
        if (!enabled)
        {
            RecordAll(recorder, 1);
        }

        recorder.Dispose();
        recorder.Dispose();
        RecordAll(recorder, 1);
        await Assert.That(capture.GetMeasurements()).Count().IsEqualTo(0);
    }

    /// <summary>Verifies invalid counter values are ignored while zero durations remain meaningful.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IgnoresInvalidValuesAndRecordsZeroDuration()
    {
        using var recorder = new OccasionallyConnectedMetrics(enabled: true);
        using var listener = CreateListener(recorder, out var capture);
        recorder.RecordOperationPublished(-1);
        recorder.RecordOperationPublished(0);
        recorder.RecordQueuePending(0);
        recorder.RecordQueueBytes(0);
        recorder.RecordSyncBatchSize(-1);
        recorder.RecordSyncBatchSize(0);
        recorder.RecordSyncDuration(TimeSpan.FromMilliseconds(-1));
        recorder.RecordStoreCommitDuration(TimeSpan.FromMilliseconds(-1));
        await Assert.That(capture.GetMeasurements()).Count().IsEqualTo(0);
        recorder.RecordSyncDuration(TimeSpan.Zero);
        recorder.RecordStoreCommitDuration(TimeSpan.Zero);
        var measurements = capture.GetMeasurements();
        await Assert.That(measurements).Count().IsEqualTo(DurationCount);
        await Assert.That(measurements.TrueForAll(static item => item.Value == 0)).IsTrue();
    }

    /// <summary>Preserves submillisecond timings for fast local store commits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PreservesFractionalMilliseconds()
    {
        const double elapsedMilliseconds = 0.5;
        using var recorder = new OccasionallyConnectedMetrics(enabled: true);
        using var listener = CreateListener(recorder, out var capture);
        recorder.RecordStoreCommitDuration(TimeSpan.FromMilliseconds(elapsedMilliseconds));
        var measurement = capture.GetMeasurements().Single();
        await Assert.That(measurement.Value).IsEqualTo(elapsedMilliseconds);
    }

    /// <summary>Records a value through every instrument.</summary>
    /// <param name="recorder">The recorder.</param>
    /// <param name="value">The measurement value.</param>
    private static void RecordAll(OccasionallyConnectedMetrics recorder, long value)
    {
        recorder.RecordOperationPublished(value);
        recorder.RecordOperationSynchronized(value);
        recorder.RecordOperationRejected(value);
        recorder.RecordConflict(value);
        recorder.RecordRetry(value);
        recorder.RecordDuplicate(value);
        recorder.RecordQueuePending(value);
        recorder.RecordQueueBytes(value);
        recorder.RecordSyncBatchSize(value);
        recorder.RecordSyncDuration(TimeSpan.FromMilliseconds(value));
        recorder.RecordStoreCommitDuration(TimeSpan.FromMilliseconds(value));
        recorder.RecordConnectionStateChange(value);
        recorder.RecordDeadLetter(value);
    }

    /// <summary>Creates an active listener for a recorder.</summary>
    /// <param name="recorder">The recorder to observe.</param>
    /// <param name="capture">The listener capture.</param>
    /// <returns>The configured listener.</returns>
    private static MeterListener CreateListener(OccasionallyConnectedMetrics recorder, out MetricCapture capture)
    {
        capture = new();
        var listenerCapture = capture;
        var listener = new MeterListener { InstrumentPublished = (instrument, meterListener) => EnableInstrument(recorder, listenerCapture, instrument, meterListener), };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => listenerCapture.AddMeasurement(instrument.Name, value, tags.ToArray()));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => listenerCapture.AddMeasurement(instrument.Name, value, tags.ToArray()));
        listener.Start();
        return listener;
    }

    /// <summary>Enables an instrument owned by the recorder.</summary>
    /// <param name="recorder">The recorder to observe.</param>
    /// <param name="capture">The listener capture.</param>
    /// <param name="instrument">The published instrument.</param>
    /// <param name="listener">The active listener.</param>
    private static void EnableInstrument(OccasionallyConnectedMetrics recorder, MetricCapture capture, Instrument instrument, MeterListener listener)
    {
        if (!ReferenceEquals(instrument.Meter, recorder.Meter))
        {
            return;
        }

        capture.AddInstrument(new(instrument.Name, GetKind(instrument), instrument.Unit));
        listener.EnableMeasurementEvents(instrument);
    }

    /// <summary>Gets the type specified by an instrument.</summary>
    /// <param name="instrument">The instrument to classify.</param>
    /// <returns>The metric type.</returns>
    /// <exception cref="InvalidOperationException">The instrument type is unexpected.</exception>
    private static string GetKind(Instrument instrument) => instrument switch
    {
        Counter<long> => "Counter",
        UpDownCounter<long> => "UpDownCounter",
        Histogram<long> or Histogram<double> => "Histogram",
        _ => throw new InvalidOperationException("Unexpected metric instrument type."),
    };

    /// <summary>Captures listener callbacks safely during concurrent recording.</summary>
    private sealed class MetricCapture
    {
        /// <summary>Protects captured data.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores observed measurements.</summary>
        private readonly List<Measurement> _measurements = [];

        /// <summary>Stores observed instruments.</summary>
        private readonly List<InstrumentDefinition> _instruments = [];

        /// <summary>Adds a measurement.</summary>
        /// <param name="name">The instrument name.</param>
        /// <param name="value">The measurement value.</param>
        /// <param name="tags">The attached tags.</param>
        internal void AddMeasurement(string name, double value, KeyValuePair<string, object?>[] tags)
        {
            lock (_gate)
            {
                _measurements.Add(new(name, value, tags));
            }
        }

        /// <summary>Adds an instrument definition.</summary>
        /// <param name="instrument">The instrument definition.</param>
        internal void AddInstrument(InstrumentDefinition instrument)
        {
            lock (_gate)
            {
                _instruments.Add(instrument);
            }
        }

        /// <summary>Gets captured measurements.</summary>
        /// <returns>A stable measurement copy.</returns>
        internal List<Measurement> GetMeasurements()
        {
            lock (_gate)
            {
                return [.. _measurements];
            }
        }

        /// <summary>Gets captured instruments.</summary>
        /// <returns>A stable instrument copy.</returns>
        internal List<InstrumentDefinition> GetInstruments()
        {
            lock (_gate)
            {
                return [.. _instruments];
            }
        }
    }

    /// <summary>Stores one observed measurement.</summary>
    /// <param name="Name">The instrument name.</param>
    /// <param name="Value">The observed value.</param>
    /// <param name="Tags">The attached tags.</param>
    private sealed record Measurement(string Name, double Value, KeyValuePair<string, object?>[] Tags);

    /// <summary>Stores one observed instrument definition.</summary>
    /// <param name="Name">The instrument name.</param>
    /// <param name="Kind">The metric type.</param>
    /// <param name="Unit">The metric unit.</param>
    private sealed record InstrumentDefinition(string Name, string Kind, string? Unit);
}
