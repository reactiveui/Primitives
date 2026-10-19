// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.DurableOutbox;

/// <summary>A typed input payload appended while the client may be offline.</summary>
/// <param name="DeviceId">The logical device identity that produced the reading.</param>
/// <param name="Value">The observed temperature value.</param>
/// <param name="Unit">The reading unit.</param>
/// <param name="ObservedAtUtc">The client observation time.</param>
internal sealed record TemperatureReading(
    string DeviceId,
    double Value,
    string Unit,
    DateTimeOffset ObservedAtUtc);
