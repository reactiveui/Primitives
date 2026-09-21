// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>A time provider whose timestamp and frequency are set directly by the test.</summary>
/// <param name="frequency">The timestamp ticks per second.</param>
internal sealed class SettableTimeProvider(long frequency) : TimeProvider
{
    /// <inheritdoc/>
    public override long TimestampFrequency { get; } = frequency;

    /// <summary>Gets or sets the value returned by <see cref="GetTimestamp"/>.</summary>
    internal long Timestamp { get; set; }

    /// <inheritdoc/>
    public override long GetTimestamp() => Timestamp;
}
