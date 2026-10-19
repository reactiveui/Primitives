// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Computes the next durable retry decision for an operation failure.</summary>
public interface IRetryPolicy
{
    /// <summary>Gets the next retry decision for a failure and the persisted retry state.</summary>
    /// <param name="failure">The classified operation failure.</param>
    /// <param name="state">The current persisted retry state.</param>
    /// <returns>The retry decision and state to persist.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="failure"/> or <paramref name="state"/> is null.</exception>
    /// <exception cref="ArgumentException">The persisted state or server delay is invalid.</exception>
    /// <remarks>Evaluate newly observed failures. Resume persisted decisions by their stored due time instead of drawing jitter again.</remarks>
    RetryDecision GetDecision(RetryFailure failure, RetryState state);
}
