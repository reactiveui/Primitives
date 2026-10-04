// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures bounded, privacy-preserving diagnostic emission.</summary>
[System.Diagnostics.DebuggerDisplay("Enabled={Enabled}; Sampling={ActivitySamplingRatio}; Faults={MaximumQueuedFaults}; FaultBytes={MaximumQueuedFaultBytes}")]
public sealed record DiagnosticsOptions
{
    /// <summary>The default activity sampling ratio.</summary>
    private const double DefaultActivitySamplingRatio = 0.1;

    /// <summary>The default queued fault bound.</summary>
    private const int DefaultMaximumQueuedFaults = 256;

    /// <summary>The default queued fault byte bound.</summary>
    private const int DefaultMaximumQueuedFaultBytes = 64 * 1024;

    /// <summary>The default high-water capacity ratio.</summary>
    private const double DefaultHighWaterMark = 0.8;

    /// <summary>Gets a value indicating whether diagnostics are enabled.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Gets a value indicating whether hashed identifiers are included in diagnostic tags.</summary>
    public bool IncludeHashedIdentifiers { get; init; }

    /// <summary>Gets the proportion of activities selected for sampling.</summary>
    public double ActivitySamplingRatio { get; init; } = DefaultActivitySamplingRatio;

    /// <summary>Gets the maximum number of faults retained for observers.</summary>
    public int MaximumQueuedFaults { get; init; } = DefaultMaximumQueuedFaults;

    /// <summary>Gets the maximum encoded bytes retained for queued faults.</summary>
    public int MaximumQueuedFaultBytes { get; init; } = DefaultMaximumQueuedFaultBytes;

    /// <summary>Gets the capacity ratio at which diagnostics report pressure.</summary>
    public double HighWaterMark { get; init; } = DefaultHighWaterMark;

    /// <summary>Validates the configured diagnostic limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A diagnostic limit is invalid.</exception>
    public void Validate()
    {
        ValidateActivitySamplingRatio();
        ValidateMaximumQueuedFaults();
        ValidateMaximumQueuedFaultBytes();
        ValidateHighWaterMark();
    }

    /// <summary>Validates the queued fault byte bound.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The configured fault byte bound is invalid.</exception>
    private void ValidateMaximumQueuedFaultBytes()
    {
        if (MaximumQueuedFaultBytes > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(MaximumQueuedFaultBytes), MaximumQueuedFaultBytes, "MaximumQueuedFaultBytes must be positive.");
    }

    /// <summary>Validates the configured activity sampling ratio.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The configured ratio is invalid.</exception>
    private void ValidateActivitySamplingRatio()
    {
        if (!double.IsNaN(ActivitySamplingRatio) && !double.IsInfinity(ActivitySamplingRatio) && ActivitySamplingRatio >= 0 && ActivitySamplingRatio <= 1)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(ActivitySamplingRatio), ActivitySamplingRatio, "ActivitySamplingRatio must be finite and between zero and one inclusive.");
    }

    /// <summary>Validates the queued fault bound.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The configured fault bound is invalid.</exception>
    private void ValidateMaximumQueuedFaults()
    {
        if (MaximumQueuedFaults > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(MaximumQueuedFaults), MaximumQueuedFaults, "MaximumQueuedFaults must be positive.");
    }

    /// <summary>Validates the diagnostic high-water mark.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The configured high-water mark is invalid.</exception>
    private void ValidateHighWaterMark()
    {
        if (!double.IsNaN(HighWaterMark) && !double.IsInfinity(HighWaterMark) && HighWaterMark > 0 && HighWaterMark < 1)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(HighWaterMark), HighWaterMark, "HighWaterMark must be finite and between zero and one exclusive.");
    }
}
