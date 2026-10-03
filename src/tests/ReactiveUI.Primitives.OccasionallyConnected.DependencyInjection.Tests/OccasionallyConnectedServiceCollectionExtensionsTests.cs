// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedServiceCollectionExtensions"/>.</summary>
public sealed class OccasionallyConnectedServiceCollectionExtensionsTests
{
    /// <summary>The counter stream name used by publication tests.</summary>
    private const string CounterName = "counter";

    /// <summary>The delta published by serializer tests.</summary>
    private const int SerializerDelta = 7;

    /// <summary>The delta published by over-limit payload tests.</summary>
    private const int OverLimitDelta = 8;

    /// <summary>Verifies the concrete context and interface alias resolve to one singleton.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRegistersSingletonContextAndInterfaceAlias()
    {
        await using var provider = CreateServices().BuildServiceProvider(validateScopes: true);

        var concrete = provider.GetRequiredService<OccasionallyConnectedContext>();
        var contract = provider.GetRequiredService<IOccasionallyConnectedContext>();
        var secondConcrete = provider.GetRequiredService<OccasionallyConnectedContext>();

        await Assert.That(contract).IsSameReferenceAs(concrete);
        await Assert.That(secondConcrete).IsSameReferenceAs(concrete);
    }

    /// <summary>Verifies a selected store type must be registered as a singleton.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsTransientStoreRegistration()
    {
        var services = new ServiceCollection();
        _ = services.AddTransient<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();

        await Assert.That(() => services.AddOccasionallyConnected(ConfigureRequired))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a selected transport type must be registered as a singleton.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsScopedTransportRegistration()
    {
        var services = new ServiceCollection();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddScoped<DependencyInjectionTestDoubles.RecordingTransportAdapter>();

        await Assert.That(() => services.AddOccasionallyConnected(ConfigureRequired))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the effective last unkeyed store descriptor is validated.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsEffectiveTransientStoreRegistration()
    {
        var services = new ServiceCollection();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddTransient<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();

        await Assert.That(() => services.AddOccasionallyConnected(ConfigureRequired))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies an earlier transient does not hide an effective singleton descriptor.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedAcceptsEffectiveSingletonStoreRegistration()
    {
        var services = new ServiceCollection();
        _ = services.AddTransient<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();

        _ = services.AddOccasionallyConnected(ConfigureRequired);
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var context = provider.GetRequiredService<OccasionallyConnectedContext>();

        await Assert.That(context).IsNotNull();
    }

    /// <summary>Verifies keyed service descriptors do not satisfy unkeyed dependencies.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsKeyedStoreRegistrationOnly()
    {
        var services = new ServiceCollection();
        _ = services.AddKeyedSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>("store");
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();

        await Assert.That(() => services.AddOccasionallyConnected(ConfigureRequired))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a selected serializer type must be registered as a singleton.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsTransientSerializerRegistration()
    {
        var services = CreateDependencyServices();
        _ = services.AddTransient(static _ => DependencyInjectionTestDoubles.CreateJsonPayloadSerializer());

        await Assert.That(() => services.AddOccasionallyConnected(ConfigureRequiredWithSerializer))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies later service mutations cannot invalidate the captured singleton serializer.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextRejectsLaterTransientSerializerMutation()
    {
        var services = CreateServicesWithSerializer();
        _ = services.AddTransient(static _ => DependencyInjectionTestDoubles.CreateJsonPayloadSerializer());
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the store identity fallback is used when explicit initialization is omitted.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextUsesStoreIdentityWhenInitializationIsOmitted()
    {
        var services = CreateDependencyServices();
        _ = services.AddOccasionallyConnected(static builder => _ = builder
            .UseClient(new(DependencyInjectionTestDoubles.ClientId))
            .UseStoreIdentity(DependencyInjectionTestDoubles.StoreIdentity)
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
            .UseJsonSerializer()
            .AddJsonContract(
                DependencyInjectionTestDoubles.InputContract,
                1,
                DependencyInjectionJsonContext.Default.CounterInput)
            .AddJsonContract(
                DependencyInjectionTestDoubles.StateContract,
                1,
                DependencyInjectionJsonContext.Default.CounterState));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var context = provider.GetRequiredService<OccasionallyConnectedContext>();
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();

        await context.StartAsync(CancellationToken.None);

        await Assert.That(store.Initialization?.StoreIdentity).IsEqualTo(DependencyInjectionTestDoubles.StoreIdentity);
        await context.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies explicit serializer registrations are resolved when JSON metadata is not selected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextUsesExplicitSerializerService()
    {
        var services = CreateServicesWithSerializer(static builder => builder.AddStream(
            CounterName,
            DependencyInjectionTestDoubles.CreateDefinition));
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var persisted = await PublishAndRecoverCounterAsync(provider, SerializerDelta);

        await Assert.That(persisted.Sum).IsEqualTo(SerializerDelta);
    }

    /// <summary>Verifies JSON serializer registration accepts payloads at the explicit byte limit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextAcceptsPayloadAtJsonSerializerLimit()
    {
        var payloadBytes = SerializedInputLength(SerializerDelta);
        var services = CreateServices(builder => builder
            .UseJsonSerializer(payloadBytes)
            .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition));
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var persisted = await PublishAndRecoverCounterAsync(provider, SerializerDelta);

        await Assert.That(persisted.Sum).IsEqualTo(SerializerDelta);
    }

    /// <summary>Verifies JSON serializer registration rejects payloads over the explicit byte limit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextRejectsPayloadOverJsonSerializerLimitWithoutStoreMutation()
    {
        var payloadBytes = SerializedInputLength(OverLimitDelta);
        var services = CreateServices(builder => builder
            .UseJsonSerializer(payloadBytes - 1)
            .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        var stream = streams.GetRequiredStream(CounterKey(CounterName));

        await Assert.That(async () => await stream.PublishAsync(
                new(OverLimitDelta),
                null,
                CancellationToken.None).AsTask().WaitAsync(DependencyInjectionTestDoubles.GuardTimeout))
            .ThrowsExactly<PayloadSchemaException>();
        await Assert.That(store.LastCommittedOperation).IsNull();
        await Assert.That(store.LastSnapshotMutation).IsNull();
    }

    /// <summary>Verifies invalid maximum stream counts fail through options validation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InvalidMaximumNamedStreamsOptionFailsWhenContextIsResolved()
    {
        var services = CreateServices();
        _ = services.Configure<OccasionallyConnectedServiceOptions>(static options => options.MaximumNamedStreams = 0);
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<OptionsValidationException>();
    }

    /// <summary>Verifies invalid maximum stream name lengths fail through options validation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InvalidMaximumStreamNameLengthOptionFailsWhenContextIsResolved()
    {
        var services = CreateServices();
        _ = services.Configure<OccasionallyConnectedServiceOptions>(static options => options.MaximumStreamNameLength = 0);
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<OptionsValidationException>();
    }

    /// <summary>Verifies later service mutations cannot invalidate the captured singleton transport.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextRejectsLaterTransientTransportMutation()
    {
        var services = CreateServices();
        _ = services.AddTransient<DependencyInjectionTestDoubles.RecordingTransportAdapter>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies startup failures are observed even when no logger factory is registered.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AutoStartFailureWithoutLoggerLeavesStartupTaskFaulted()
    {
        var failure = new InvalidOperationException("startup failed");
        var services = CreateServices(
            services => _ = services.AddSingleton(new DependencyInjectionTestDoubles.RecordingTransportAdapter { ConnectException = failure }),
            static builder => builder.ConfigureOptions(static options => options with { AutoStart = true }));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var context = provider.GetRequiredService<OccasionallyConnectedContext>();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await context.StartupTask.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout));

        await Assert.That(exception).IsSameReferenceAs(failure);
        await Assert.That(context.StartupTask.Exception?.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>Verifies provider-owned store and transport instances are borrowed by the context.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextBorrowsStoreAndTransportFromProvider()
    {
        var provider = CreateServices().BuildServiceProvider(validateScopes: true);
        var providerDisposed = false;
        try
        {
            var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
            var transport = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingTransportAdapter>();
            var context = provider.GetRequiredService<OccasionallyConnectedContext>();
            var alias = provider.GetRequiredService<IOccasionallyConnectedContext>();

            await Assert.That(alias).IsSameReferenceAs(context);
            await context.DisposeAsync();
            await alias.DisposeAsync();
            await Assert.That(store.DisposeCalls).IsEqualTo(0);
            await Assert.That(transport.DisposeCalls).IsEqualTo(0);
            await provider.DisposeAsync();
            providerDisposed = true;
            await Assert.That(store.DisposeCalls).IsEqualTo(1);
            await Assert.That(transport.DisposeCalls).IsEqualTo(1);
        }
        finally
        {
            if (!providerDisposed)
            {
                await provider.DisposeAsync();
            }
        }
    }

    /// <summary>Verifies invalid immutable runtime options fail when the singleton context is created.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InvalidRuntimeOptionsFailWhenContextIsResolved()
    {
        var services = CreateServices(static builder => builder.ConfigureOptions(
            static options => options with { MaxConcurrentStreams = 0 }));
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<OptionsValidationException>();
    }

    /// <summary>Verifies later service mutations cannot invalidate the captured singleton selection.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextRejectsLaterTransientStoreMutation()
    {
        var services = CreateServices();
        _ = services.AddTransient<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies removing the selected store descriptor after registration is rejected at resolution.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextRejectsRemovedSelectedStoreDescriptor()
    {
        var services = CreateServices();
        RemoveLastUnkeyedDescriptor<DependencyInjectionTestDoubles.RecordingStoreAdapter>(services);
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies keyed descriptors cannot replace the selected unkeyed descriptor after registration.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ResolvedContextRejectsKeyedStoreReplacementAfterRegistration()
    {
        var services = CreateServices();
        RemoveLastUnkeyedDescriptor<DependencyInjectionTestDoubles.RecordingStoreAdapter>(services);
        _ = services.AddKeyedSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>("store");
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies runtime options are captured when the singleton context is created.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RuntimeOptionsAreCapturedAtContextCreationAndDoNotTrackLaterDelegateState()
    {
        var maximumStreams = 4;
        var services = CreateServices(builder => builder.ConfigureOptions(
            options => options with { MaxConcurrentStreams = maximumStreams }));
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var context = provider.GetRequiredService<OccasionallyConnectedContext>();
        maximumStreams = 0;

        await context.StartAsync(CancellationToken.None);
        await Assert.That(context.StartupTask.IsCompletedSuccessfully).IsTrue();
        await context.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies startup failures are still visible on the public context task.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AutoStartFailureLeavesStartupTaskFaulted()
    {
        var failure = new InvalidOperationException("token-raw-secret");
        var services = CreateServices(
            services => _ = services.AddSingleton(new DependencyInjectionTestDoubles.RecordingTransportAdapter { ConnectException = failure }),
            static builder => builder.ConfigureOptions(static options => options with { AutoStart = true }));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var context = provider.GetRequiredService<OccasionallyConnectedContext>();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
            await context.StartupTask.WaitAsync(DependencyInjectionTestDoubles.GuardTimeout));

        await Assert.That(exception).IsSameReferenceAs(failure);
    }

    /// <summary>Creates the default service collection used by DI tests.</summary>
    /// <returns>The configured service collection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServiceCollection CreateServices() => CreateServices(static _ => { }, static _ => { });

    /// <summary>Creates the default service collection used by DI tests.</summary>
    /// <param name="configure">The additional builder configuration.</param>
    /// <returns>The configured service collection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServiceCollection CreateServices(Action<OccasionallyConnectedDependencyInjectionBuilder> configure) =>
        CreateServices(static _ => { }, configure);

    /// <summary>Creates the default service collection used by DI tests.</summary>
    /// <param name="configureServices">The service configuration hook before builder capture.</param>
    /// <param name="configure">The additional builder configuration.</param>
    /// <returns>The configured service collection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServiceCollection CreateServices(
        Action<IServiceCollection> configureServices,
        Action<OccasionallyConnectedDependencyInjectionBuilder> configure)
    {
        var services = CreateDependencyServices();
        configureServices(services);
        _ = services.AddOccasionallyConnected(builder =>
        {
            ConfigureRequired(builder);
            configure(builder);
        });
        return services;
    }

    /// <summary>Creates the default service collection with a custom serializer service.</summary>
    /// <returns>The configured service collection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServiceCollection CreateServicesWithSerializer() => CreateServicesWithSerializer(static _ => { });

    /// <summary>Creates the default service collection with a custom serializer service.</summary>
    /// <param name="configure">The additional builder configuration.</param>
    /// <returns>The configured service collection.</returns>
    private static ServiceCollection CreateServicesWithSerializer(
        Action<OccasionallyConnectedDependencyInjectionBuilder> configure)
    {
        var services = CreateDependencyServices();
        _ = services.AddSingleton(static _ => DependencyInjectionTestDoubles.CreateJsonPayloadSerializer());
        _ = services.AddOccasionallyConnected(builder =>
        {
            ConfigureRequiredWithSerializer(builder);
            configure(builder);
        });
        return services;
    }

    /// <summary>Publishes a counter input and recovers the persisted counter snapshot.</summary>
    /// <param name="provider">The service provider.</param>
    /// <param name="delta">The counter delta.</param>
    /// <returns>The recovered counter state.</returns>
    private static async Task<DependencyInjectionTestDoubles.CounterState> PublishAndRecoverCounterAsync(
        IServiceProvider provider,
        int delta)
    {
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        var stream = streams.GetRequiredStream(CounterKey(CounterName));

        _ = await stream.PublishAsync(new(delta), null, CancellationToken.None)
            .AsTask()
            .WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);
        var snapshot = await store.RecoverLastSnapshotAsync();
        return DependencyInjectionTestDoubles.DeserializeCounterState(snapshot);
    }

    /// <summary>Computes the generated JSON payload length for a counter input.</summary>
    /// <param name="delta">The counter delta.</param>
    /// <returns>The serialized payload byte length.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SerializedInputLength(int delta) =>
        JsonSerializer.SerializeToUtf8Bytes(
            new(delta),
            DependencyInjectionJsonContext.Default.CounterInput).Length;

    /// <summary>Creates a typed counter stream key.</summary>
    /// <param name="name">The stream name.</param>
    /// <returns>The stream key.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStreamKey<
        DependencyInjectionTestDoubles.CounterState,
        DependencyInjectionTestDoubles.CounterInput> CounterKey(string name) => new(name);

    /// <summary>Creates dependency services without the occasionally connected registrations.</summary>
    /// <returns>The service collection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServiceCollection CreateDependencyServices()
    {
        ServiceCollection services = new();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.CounterProjection>();
        return services;
    }

    /// <summary>Removes the effective unkeyed descriptor for the supplied service type.</summary>
    /// <typeparam name="T">The service type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <exception cref="InvalidOperationException">The selected descriptor was not found.</exception>
    private static void RemoveLastUnkeyedDescriptor<T>(ServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType != typeof(T) || services[i].IsKeyedService)
            {
                continue;
            }

            services.RemoveAt(i);
            return;
        }

        throw new InvalidOperationException("The selected descriptor was not found.");
    }

    /// <summary>Applies the required builder configuration used by tests.</summary>
    /// <param name="builder">The DI builder.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ConfigureRequired(OccasionallyConnectedDependencyInjectionBuilder builder) =>
        _ = builder
            .UseClient(new(DependencyInjectionTestDoubles.ClientId))
            .UseStoreIdentity(DependencyInjectionTestDoubles.StoreIdentity)
            .UseStoreInitialization(DependencyInjectionTestDoubles.CreateStoreInitialization())
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
            .UseJsonSerializer()
            .AddJsonContract(
                DependencyInjectionTestDoubles.InputContract,
                1,
                DependencyInjectionJsonContext.Default.CounterInput)
            .AddJsonContract(
                DependencyInjectionTestDoubles.StateContract,
                1,
                DependencyInjectionJsonContext.Default.CounterState);

    /// <summary>Applies the required builder configuration with a selected serializer service.</summary>
    /// <param name="builder">The DI builder.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ConfigureRequiredWithSerializer(OccasionallyConnectedDependencyInjectionBuilder builder) =>
        _ = builder
            .UseClient(new(DependencyInjectionTestDoubles.ClientId))
            .UseStoreIdentity(DependencyInjectionTestDoubles.StoreIdentity)
            .UseStoreInitialization(DependencyInjectionTestDoubles.CreateStoreInitialization())
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
            .UseSerializer(typeof(JsonPayloadSerializer));
}
