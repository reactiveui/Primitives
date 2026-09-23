// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Diagnostics capture helpers for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Stores one metric measurement.</summary>
    /// <param name="Name">The instrument name.</param>
    /// <param name="Value">The measured value.</param>
    /// <param name="Tags">The measurement tags.</param>
    private readonly record struct EngineMetricMeasurement(string Name, double Value, KeyValuePair<string, object?>[] Tags);

    /// <summary>Stores one activity record.</summary>
    /// <param name="Name">The activity operation name.</param>
    /// <param name="Tags">The activity tags.</param>
    private readonly record struct EngineActivityRecord(string Name, KeyValuePair<string, string?>[] Tags);

    /// <summary>No-op disposable for captured registrations.</summary>
    private sealed class NoopRegistration : IDisposable
    {
        /// <summary>Gets the shared no-op instance.</summary>
        internal static NoopRegistration Instance { get; } = new();

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }

    /// <summary>Captures engine metric measurements.</summary>
    private sealed class EngineMetricCapture
    {
        /// <summary>Protects captured measurements.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores captured measurements.</summary>
        private readonly List<EngineMetricMeasurement> _measurements = [];

        /// <summary>Gets a value indicating whether all measurements are untagged.</summary>
        internal bool AllMeasurementsUntagged => GetMeasurements().TrueForAll(static measurement => measurement.Tags.Length == 0);

        /// <summary>Adds one measurement.</summary>
        /// <param name="name">The instrument name.</param>
        /// <param name="value">The recorded value.</param>
        /// <param name="tags">The tags attached to the measurement.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Add(string name, double value, KeyValuePair<string, object?>[] tags)
        {
            lock (_gate)
            {
                _measurements.Add(new(name, value, tags));
            }
        }

        /// <summary>Gets captured measurements.</summary>
        /// <returns>The captured measurements.</returns>
        internal List<EngineMetricMeasurement> GetMeasurements()
        {
            lock (_gate)
            {
                return [.. _measurements];
            }
        }

        /// <summary>Checks whether a measurement with an exact value was captured.</summary>
        /// <param name="name">The instrument name.</param>
        /// <param name="value">The expected value.</param>
        /// <returns>Whether the measurement exists.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HasMeasurement(string name, double value) =>
            GetMeasurements().Exists(item => item.Name == name && Math.Abs(item.Value - value) <= double.Epsilon);

        /// <summary>Checks whether a non-negative measurement was captured.</summary>
        /// <param name="name">The instrument name.</param>
        /// <returns>Whether the measurement exists.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HasNonNegativeMeasurement(string name) =>
            GetMeasurements().Exists(item => item.Name == name && item.Value >= 0D);

        /// <summary>Checks whether a positive measurement was captured.</summary>
        /// <param name="name">The instrument name.</param>
        /// <returns>Whether the measurement exists.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HasPositiveMeasurement(string name) =>
            GetMeasurements().Exists(item => item.Name == name && item.Value > 0D);

        /// <summary>Sums all measurements for one instrument.</summary>
        /// <param name="name">The instrument name.</param>
        /// <returns>The summed value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal double Sum(string name) =>
            GetMeasurements().Where(item => item.Name == name).Sum(static item => item.Value);
    }

    /// <summary>Captures engine activities.</summary>
    private sealed class EngineActivityCapture
    {
        /// <summary>Stores captured activities.</summary>
        private readonly ConcurrentBag<EngineActivityRecord> _activities = [];

        /// <summary>Gets a value indicating whether all captured activities are untagged.</summary>
        internal bool AllActivitiesUntagged => GetActivities().TrueForAll(static activity => activity.Tags.Length == 0);

        /// <summary>Adds one activity.</summary>
        /// <param name="name">The activity name.</param>
        /// <param name="tags">The tags attached to the activity.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Add(string name, KeyValuePair<string, string?>[] tags) =>
            _activities.Add(new(name, tags));

        /// <summary>Gets captured activities.</summary>
        /// <returns>The captured activities.</returns>
        internal List<EngineActivityRecord> GetActivities() => [.. _activities];

        /// <summary>Checks whether an activity was captured.</summary>
        /// <param name="name">The activity name.</param>
        /// <returns>Whether the activity exists.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool HasActivity(string name) =>
            GetActivities().Exists(activity => activity.Name == name);
    }

    /// <summary>Time provider with independent monotonic and UTC time for diagnostics tests.</summary>
    /// <param name="timestamp">The starting UTC timestamp.</param>
    private sealed class DivergentTimeProvider(DateTimeOffset timestamp) : TimeProvider
    {
        /// <summary>The timestamp frequency used by this provider.</summary>
        private const long Frequency = TimeSpan.TicksPerSecond;

        /// <summary>The current UTC timestamp.</summary>
        private DateTimeOffset _utc = timestamp;

        /// <summary>The current monotonic timestamp.</summary>
        private long _timestamp;

        /// <inheritdoc/>
        public override long TimestampFrequency => Frequency;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _utc;

        /// <inheritdoc/>
        public override long GetTimestamp() => _timestamp;

        /// <summary>Advances monotonic time while moving UTC backwards.</summary>
        /// <param name="elapsed">The monotonic elapsed time.</param>
        /// <param name="utcRollback">The UTC rollback amount.</param>
        public void AdvanceMonotonicAndRollbackUtc(TimeSpan elapsed, TimeSpan utcRollback)
        {
            _timestamp += elapsed.Ticks;
            _utc -= utcRollback;
        }
    }
}
