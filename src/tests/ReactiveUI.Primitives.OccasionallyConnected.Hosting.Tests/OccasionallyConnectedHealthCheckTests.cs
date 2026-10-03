// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedHealthCheck"/>.</summary>
public sealed class OccasionallyConnectedHealthCheckTests
{
    /// <summary>The retry delay used by the health check case.</summary>
    private const int RetryAfterMilliseconds = 2750;

    /// <summary>The pending operation count used by the retry delay case.</summary>
    private const int PendingOperationCount = 3;

    /// <summary>Verifies the health check exposes the retry delay in milliseconds.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HealthCheckReportsRetryDelayInMilliseconds()
    {
        var context = new HostingTestDoubles.RecordingContext();
        using var monitor = new OccasionallyConnectedHealthMonitor(context, TimeProvider.System);
        var retryAfter = TimeSpan.FromMilliseconds(RetryAfterMilliseconds);
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Offline, PendingOperationCount, "OC.Transport.Unavailable", retryAfter));

        var registration = new HealthCheckRegistration("occasionally-connected", static _ => throw new NotSupportedException(), HealthStatus.Unhealthy, []);
        var result = await new OccasionallyConnectedHealthCheck(monitor).CheckHealthAsync(new HealthCheckContext { Registration = registration });

        await Assert.That(result.Status).IsEqualTo(HealthStatus.Degraded);
        await Assert.That(result.Data[OccasionallyConnectedHealthCheck.RetryAfterKey]).IsEqualTo((long)retryAfter.TotalMilliseconds);
    }

    /// <summary>Verifies unhealthy states fall back when the health check context or registration is missing.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HealthCheckUsesUnhealthyFallbackForMissingContextOrRegistration()
    {
        var context = new HostingTestDoubles.RecordingContext();
        using var monitor = new OccasionallyConnectedHealthMonitor(context, TimeProvider.System);
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Faulted, 0, reasonCode: null));
        var check = new OccasionallyConnectedHealthCheck(monitor);

        var missingContext = await check.CheckHealthAsync(null!);
        var missingRegistration = await check.CheckHealthAsync(new HealthCheckContext { Registration = null! });

        await Assert.That(missingContext.Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(missingRegistration.Status).IsEqualTo(HealthStatus.Unhealthy);
    }
}
