// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures a per-endpoint circuit breaker.</summary>
[DebuggerDisplay("Failures={FailureThreshold,nq}; Open={OpenDuration,nq}")]
public sealed record CircuitBreakerOptions
{
    /// <summary>Defines the number of consecutive transient failures that opens the breaker.</summary>
    private const int DefaultFailureThreshold = 5;

    /// <summary>Defines the default duration for which an opened breaker rejects work.</summary>
    private static readonly TimeSpan DefaultOpenDuration = TimeSpan.FromSeconds(30);

    /// <summary>Gets the number of consecutive transient failures that opens the breaker.</summary>
    public int FailureThreshold { get; init; } = DefaultFailureThreshold;

    /// <summary>Gets the duration for which an opened breaker rejects work before one probe may run.</summary>
    public TimeSpan OpenDuration { get; init; } = DefaultOpenDuration;

    /// <summary>Validates this option record.</summary>
    /// <exception cref="InvalidOperationException">An option is not a usable positive finite value.</exception>
    public void Validate()
    {
        if (FailureThreshold <= 0)
        {
            throw new InvalidOperationException("FailureThreshold must be positive.");
        }

        if (OpenDuration > TimeSpan.Zero && OpenDuration != TimeSpan.MaxValue)
        {
            return;
        }

        throw new InvalidOperationException("OpenDuration must be positive and finite.");
    }
}
