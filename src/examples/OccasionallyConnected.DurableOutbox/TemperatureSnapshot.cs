// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.DurableOutbox;

/// <summary>The optimistic local snapshot rebuilt from durable operations.</summary>
/// <param name="ReadingCount">The number of readings included in the local projection.</param>
/// <param name="LastDeviceId">The device id from the last reading.</param>
/// <param name="LastReading">The last temperature value.</param>
/// <param name="LastObservedAtUtc">The last observation time.</param>
/// <param name="TotalReading">The sum of all locally included readings.</param>
internal sealed record TemperatureSnapshot(
    int ReadingCount,
    string LastDeviceId,
    double LastReading,
    DateTimeOffset LastObservedAtUtc,
    double TotalReading)
{
    /// <summary>Gets the empty state for a newly initialized database.</summary>
    internal static TemperatureSnapshot Empty { get; } = new(
        ReadingCount: 0,
        LastDeviceId: string.Empty,
        LastReading: 0,
        LastObservedAtUtc: DateTimeOffset.UnixEpoch,
        TotalReading: 0);

    /// <summary>Applies a reading to the optimistic snapshot.</summary>
    /// <param name="reading">The reading to apply.</param>
    /// <returns>The next snapshot.</returns>
    internal TemperatureSnapshot Apply(TemperatureReading reading) =>
        new(ReadingCount + 1, reading.DeviceId, reading.Value, reading.ObservedAtUtc, TotalReading + reading.Value);
}
