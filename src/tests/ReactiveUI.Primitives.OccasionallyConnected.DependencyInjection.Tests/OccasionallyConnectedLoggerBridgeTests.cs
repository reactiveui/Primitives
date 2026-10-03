// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Tests for the dependency-injection logging bridge.</summary>
public sealed class OccasionallyConnectedLoggerBridgeTests
{
    /// <summary>Verifies startup logging is redacted and does not consume the public startup failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StartupFailureLoggingIsBoundedRedactedAndLeavesStartupTaskFaulted()
    {
        const string Secret = "token-raw-secret";
        var loggerProvider = new DependencyInjectionTestDoubles.RecordingLoggerProvider();
        await using var provider = CreateProvider(loggerProvider, new InvalidOperationException(Secret));
        var context = provider.GetRequiredService<OccasionallyConnectedContext>();

        await Assert.That(async () => await context.StartupTask)
            .ThrowsExactly<InvalidOperationException>();

        var entry = await loggerProvider.WaitForEntryAsync(DependencyInjectionTestDoubles.GuardTimeout);
        await Assert.That(entry.Exception).IsNull();
        await Assert.That(entry.Message).DoesNotContain(Secret);
        await Assert.That(entry.Message).DoesNotContain("InvalidOperationException");
        await Assert.That(entry.Message.Length <= 256).IsTrue();
        await Assert.That(entry.Message).Contains("OC.Startup");
        await Assert.That(context.StartupTask.IsFaulted).IsTrue();
    }

    /// <summary>Verifies logger callback failures do not replace the public startup failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ThrowingStartupLoggerLeavesStartupTaskFaultedWithOriginalFailure()
    {
        var startupFailure = new InvalidOperationException("startup failed");
        var loggerProvider = new DependencyInjectionTestDoubles.ThrowingLoggerProvider();
        await using var provider = CreateProvider(loggerProvider, startupFailure);
        var context = provider.GetRequiredService<OccasionallyConnectedContext>();

        await Assert.That(async () => await context.StartupTask)
            .ThrowsExactly<InvalidOperationException>();
        await loggerProvider.WaitForLogAsync(DependencyInjectionTestDoubles.GuardTimeout);
        await Assert.That(context.StartupTask.Exception?.InnerException).IsSameReferenceAs(startupFailure);
    }

    /// <summary>Verifies an already completed startup task does not emit the startup failure log.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CompletedStartupTaskDoesNotLogFailure()
    {
        var loggerProvider = new DependencyInjectionTestDoubles.RecordingLoggerProvider();
        await using var provider = CreateProvider(loggerProvider, null, autoStart: false);
        var context = provider.GetRequiredService<OccasionallyConnectedContext>();

        await Assert.That(context.StartupTask.IsCompletedSuccessfully).IsTrue();
        await Assert.That(loggerProvider.HasEntry).IsFalse();
    }

    /// <summary>Verifies logger factory failures happen before the context starts borrowed dependencies.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task LoggerFactoryFailurePreventsContextBuildBeforeStoreInitialization()
    {
        var loggerFailure = new InvalidOperationException("logger factory failed");
        ServiceCollection services = new();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.CounterProjection>();
        _ = services.AddSingleton<ILoggerFactory>(new DependencyInjectionTestDoubles.ThrowingLoggerFactory(loggerFailure));
        ConfigureOccasionallyConnected(services);
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();

        var exception = Assert.ThrowsExactly<InvalidOperationException>(
            () => provider.GetRequiredService<OccasionallyConnectedContext>());

        await Assert.That(exception).IsSameReferenceAs(loggerFailure);
        await Assert.That(store.Initialization).IsNull();
    }

    /// <summary>Creates a provider with auto-start and the supplied logger provider.</summary>
    /// <param name="loggerProvider">The logger provider.</param>
    /// <param name="startupFailure">The optional startup failure.</param>
    /// <param name="autoStart">Whether the context should auto-start.</param>
    /// <returns>The service provider.</returns>
    private static ServiceProvider CreateProvider(
        ILoggerProvider loggerProvider,
        Exception? startupFailure,
        bool autoStart = true)
    {
        ServiceCollection services = new();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton(new DependencyInjectionTestDoubles.RecordingTransportAdapter { ConnectException = startupFailure });
        _ = services.AddSingleton(loggerProvider);
        _ = services.AddLogging();
        ConfigureOccasionallyConnected(services, autoStart);
        return services.BuildServiceProvider(validateScopes: true);
    }

    /// <summary>Configures occasionally connected services for logging tests.</summary>
    /// <param name="services">The service collection.</param>
    /// <param name="autoStart">Whether the context should auto-start.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ConfigureOccasionallyConnected(IServiceCollection services, bool autoStart = true) =>
        _ = services.AddOccasionallyConnected(builder =>
        {
            _ = builder
                .UseClient(new(DependencyInjectionTestDoubles.ClientId))
                .UseStoreIdentity(DependencyInjectionTestDoubles.StoreIdentity)
                .UseStoreInitialization(DependencyInjectionTestDoubles.CreateStoreInitialization())
                .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
                .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
                .UseJsonSerializer()
                .ConfigureOptions(options => options with { AutoStart = autoStart })
                .AddJsonContract(
                    DependencyInjectionTestDoubles.InputContract,
                    1,
                    DependencyInjectionJsonContext.Default.CounterInput)
                .AddJsonContract(
                    DependencyInjectionTestDoubles.StateContract,
                    1,
                    DependencyInjectionJsonContext.Default.CounterState);
        });
}
