// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Derives health reports from synchronization states.</summary>
public static class OccasionallyConnectedHealth
{
    /// <summary>The reason code the engine reports while its circuit breaker rejects connections.</summary>
    private const string CircuitOpenReasonCode = "OC.Transport.CircuitOpen";

    /// <summary>Gets the reason code reported while the context is created, starting, stopping, or stopped.</summary>
    public static string NotRunningReasonCode { get; } = "OC.Health.NotRunning";

    /// <summary>Gets the reason code reported while remote work waits for an unavailable endpoint.</summary>
    public static string PendingRemoteWorkReasonCode { get; } = "OC.Health.PendingRemoteWork";

    /// <summary>Gets the reason code reported when a faulted state has no more specific reason.</summary>
    public static string FaultedReasonCode { get; } = "OC.Health.Faulted";

    /// <summary>Evaluates the health of a synchronization state.</summary>
    /// <param name="state">The latest synchronization state.</param>
    /// <param name="nowUtc">The current time used to compute the state age.</param>
    /// <returns>The health report.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="state"/> is <see langword="null"/>.</exception>
    public static OccasionallyConnectedHealthReport Evaluate(SyncState state, DateTimeOffset nowUtc)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        var (status, reasonCode) = state.Status switch
        {
            SyncLifecycleStatus.Online or SyncLifecycleStatus.Synchronizing => (OccasionallyConnectedHealthStatus.Healthy, (string?)null),
            SyncLifecycleStatus.Faulted => (OccasionallyConnectedHealthStatus.Unhealthy, state.ReasonCode ?? FaultedReasonCode),
            SyncLifecycleStatus.Offline or SyncLifecycleStatus.Connecting or SyncLifecycleStatus.Degraded => EvaluateDisconnected(state),
            _ => (OccasionallyConnectedHealthStatus.Degraded, NotRunningReasonCode),
        };

        var age = nowUtc > state.ChangedAtUtc ? nowUtc - state.ChangedAtUtc : TimeSpan.Zero;
        return new(status, state.Status, reasonCode, state.PendingOperations, state.PendingBytes, state.RetryAfter, age);
    }

    /// <summary>Evaluates a state whose remote endpoint is unavailable.</summary>
    /// <param name="state">The synchronization state.</param>
    /// <returns>The health status and reason code.</returns>
    private static (OccasionallyConnectedHealthStatus Status, string? ReasonCode) EvaluateDisconnected(SyncState state)
    {
        if (string.Equals(state.ReasonCode, CircuitOpenReasonCode, StringComparison.Ordinal))
        {
            return (OccasionallyConnectedHealthStatus.Degraded, CircuitOpenReasonCode);
        }

        return state.PendingOperations > 0 || state.PendingBytes > 0
            ? (OccasionallyConnectedHealthStatus.Degraded, state.ReasonCode ?? PendingRemoteWorkReasonCode)
            : (OccasionallyConnectedHealthStatus.Healthy, null);
    }
}
