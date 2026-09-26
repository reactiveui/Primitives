// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedHostingExtensions"/>.</summary>
public sealed class OccasionallyConnectedHostingExtensionsTests
{
    /// <summary>The pending operation count used by degraded cases.</summary>
    private const int PendingCount = 2;

    /// <summary>The custom health check name.</summary>
    private const string CustomName = "sync";

    /// <summary>Verifies hosting registration is idempotent and the hosted service drives the context lifecycle.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HostedServiceStartsAndGracefullyStopsContext()
    {
        var context = new HostingTestDoubles.RecordingContext();
        var services = CreateServices(context);
        _ = services.AddOccasionallyConnectedHosting().AddOccasionallyConnectedHosting();
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var hostedServices = provider.GetServices<IHostedService>().ToArray();
        using var shutdown = new CancellationTokenSource();

        await Assert.That(hostedServices.Length).IsEqualTo(1);
        await hostedServices[0].StartAsync(CancellationToken.None);
        await hostedServices[0].StopAsync(shutdown.Token);

        await Assert.That(context.StartCalls).IsEqualTo(1);
        await Assert.That(context.StopCalls).IsEqualTo(1);
        await Assert.That(context.LastStopToken).IsEqualTo(shutdown.Token);
    }

    /// <summary>Verifies the registered health check reports context health through the health check service.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HealthCheckServiceReportsContextHealth()
    {
        var context = new HostingTestDoubles.RecordingContext();
        var services = CreateServices(context);
        _ = services.AddHealthChecks().AddOccasionallyConnected();
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var healthChecks = provider.GetRequiredService<HealthCheckService>();

        var notStarted = await healthChecks.CheckHealthAsync();
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Online, 0, reasonCode: null));
        var online = await healthChecks.CheckHealthAsync();
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Offline, PendingCount, reasonCode: null));
        var offline = await healthChecks.CheckHealthAsync();
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Faulted, 0, reasonCode: null));
        var faulted = await healthChecks.CheckHealthAsync();

        var entryName = OccasionallyConnectedHostingExtensions.DefaultHealthCheckName;
        await Assert.That(notStarted.Entries[entryName].Status).IsEqualTo(HealthStatus.Degraded);
        await Assert.That(notStarted.Entries[entryName].Description).IsEqualTo(OccasionallyConnectedHealth.NotRunningReasonCode);
        await Assert.That(online.Entries[entryName].Status).IsEqualTo(HealthStatus.Healthy);
        await Assert.That(offline.Entries[entryName].Status).IsEqualTo(HealthStatus.Degraded);
        await Assert.That(offline.Entries[entryName].Data[OccasionallyConnectedHealthCheck.PendingOperationsKey]).IsEqualTo(PendingCount);
        await Assert.That(faulted.Entries[entryName].Status).IsEqualTo(HealthStatus.Unhealthy);
        await Assert.That(faulted.Entries[entryName].Data[OccasionallyConnectedHealthCheck.ReasonCodeKey]).IsEqualTo(OccasionallyConnectedHealth.FaultedReasonCode);
    }

    /// <summary>Verifies a custom name and failure status are honored.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HealthCheckUsesCustomNameAndFailureStatus()
    {
        var context = new HostingTestDoubles.RecordingContext();
        var services = CreateServices(context);
        _ = services.AddHealthChecks().AddOccasionallyConnected(CustomName);
        _ = services.Configure<HealthCheckServiceOptions>(static options =>
        {
            foreach (var registration in options.Registrations)
            {
                registration.FailureStatus = HealthStatus.Degraded;
            }
        });
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Faulted, 0, "OC.Engine.Connect"));

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync();

        await Assert.That(report.Entries.ContainsKey(CustomName)).IsTrue();
        await Assert.That(report.Entries[CustomName].Status).IsEqualTo(HealthStatus.Degraded);
        await Assert.That(report.Entries[CustomName].Description).IsEqualTo("OC.Engine.Connect");
    }

    /// <summary>Verifies null receivers and names are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RegistrationRejectsNullArguments()
    {
        IServiceCollection? missingServices = null;
        IHealthChecksBuilder? missingBuilder = null;
        var builder = new ServiceCollection().AddHealthChecks();
        const string? missingName = null;

        await Assert.That(() => missingServices!.AddOccasionallyConnectedHosting()).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => missingBuilder!.AddOccasionallyConnected(CustomName)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => builder.AddOccasionallyConnected(missingName!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a service collection with the supplied context.</summary>
    /// <param name="context">The context.</param>
    /// <returns>The services.</returns>
    private static ServiceCollection CreateServices(HostingTestDoubles.RecordingContext context)
    {
        var services = new ServiceCollection();
        _ = services.AddSingleton<IOccasionallyConnectedContext>(context);
        _ = services.AddLogging();
        return services;
    }
}
