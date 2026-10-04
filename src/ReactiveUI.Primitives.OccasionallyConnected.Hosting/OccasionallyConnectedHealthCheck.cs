// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting;

/// <summary>Reports occasionally connected context health using counts, ages, and reason codes only.</summary>
/// <param name="monitor">The health monitor.</param>
public sealed class OccasionallyConnectedHealthCheck(OccasionallyConnectedHealthMonitor monitor) : IHealthCheck
{
    /// <summary>Gets the data key for the lifecycle status.</summary>
    public static string LifecycleStatusKey { get; } = "lifecycleStatus";

    /// <summary>Gets the data key for the pending operation count.</summary>
    public static string PendingOperationsKey { get; } = "pendingOperations";

    /// <summary>Gets the data key for the pending byte count.</summary>
    public static string PendingBytesKey { get; } = "pendingBytes";

    /// <summary>Gets the data key for the state age in milliseconds.</summary>
    public static string StateAgeKey { get; } = "stateAgeMilliseconds";

    /// <summary>Gets the data key for the retry delay in milliseconds.</summary>
    public static string RetryAfterKey { get; } = "retryAfterMilliseconds";

    /// <summary>Gets the data key for the reason code.</summary>
    public static string ReasonCodeKey { get; } = "reasonCode";

    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        ArgumentExceptionHelper.ThrowIfNull(monitor);
        cancellationToken.ThrowIfCancellationRequested();
        var report = monitor.Current;
        var data = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            [LifecycleStatusKey] = report.LifecycleStatus.ToString(),
            [PendingOperationsKey] = report.PendingOperations,
            [PendingBytesKey] = report.PendingBytes,
            [StateAgeKey] = (long)report.StateAge.TotalMilliseconds,
        };
        if (report.RetryAfter is { } retryAfter)
        {
            data[RetryAfterKey] = (long)retryAfter.TotalMilliseconds;
        }

        if (report.ReasonCode is { } reasonCode)
        {
            data[ReasonCodeKey] = reasonCode;
        }

        var status = report.Status switch
        {
            OccasionallyConnectedHealthStatus.Healthy => HealthStatus.Healthy,
            OccasionallyConnectedHealthStatus.Degraded => HealthStatus.Degraded,
            _ => context?.Registration?.FailureStatus ?? HealthStatus.Unhealthy,
        };
        return Task.FromResult(new HealthCheckResult(status, report.ReasonCode, exception: null, data));
    }
}
