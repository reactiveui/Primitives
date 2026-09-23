// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Registers occasionally connected services.</summary>
public static class OccasionallyConnectedServiceCollectionExtensions
{
    /// <summary>Occasionally connected service registration helpers.</summary>
    /// <param name="services">The service collection.</param>
    extension(IServiceCollection services)
    {
        /// <summary>Registers a singleton occasionally connected context and named stream registry.</summary>
        /// <param name="configure">The configuration callback.</param>
        /// <returns>The service collection.</returns>
        public IServiceCollection AddOccasionallyConnected(
            Action<OccasionallyConnectedDependencyInjectionBuilder> configure)
        {
            ArgumentExceptionHelper.ThrowIfNull(services);
            ArgumentExceptionHelper.ThrowIfNull(configure);
            var builder = new OccasionallyConnectedDependencyInjectionBuilder(services);
            configure(builder);
            var configuration = builder.BuildConfiguration();
            _ = services
                .AddSingleton(configuration)
                .AddSingleton<IValidateOptions<OccasionallyConnectedServiceOptions>>(
                    static _ => new OccasionallyConnectedServiceOptionsValidator())
                .Configure<OccasionallyConnectedServiceOptions>(options =>
                {
                    options.Options = configuration.Options;
                    options.MaximumNamedStreams = configuration.MaximumNamedStreams;
                    options.MaximumStreamNameLength = configuration.MaximumStreamNameLength;
                })
                .AddSingleton(CreateContext)
                .AddSingleton<IOccasionallyConnectedContext>(static provider =>
                    provider.GetRequiredService<OccasionallyConnectedContext>())
                .AddSingleton<IOccasionallyConnectedStreamRegistry>(static provider =>
                    new OccasionallyConnectedStreamRegistry(
                        provider.GetRequiredService<OccasionallyConnectedServiceConfiguration>(),
                        provider.GetRequiredService<OccasionallyConnectedContext>(),
                        provider));
            return services;
        }
    }

    /// <summary>Creates the singleton occasionally connected context.</summary>
    /// <param name="services">The service provider.</param>
    /// <returns>The configured context.</returns>
    private static OccasionallyConnectedContext CreateContext(IServiceProvider services)
    {
        var configuration = services.GetRequiredService<OccasionallyConnectedServiceConfiguration>();
        var options = services.GetRequiredService<IOptions<OccasionallyConnectedServiceOptions>>().Value;
        var logger = CreateLogger(services);
        ValidateSelectedDescriptor(configuration, configuration.StoreType, configuration.StoreDescriptor);
        ValidateSelectedDescriptor(configuration, configuration.TransportType, configuration.TransportDescriptor);
        var contextBuilder = new OccasionallyConnectedBuilder()
            .UseClient(configuration.Client)
            .UseOptions(options.Options)
            .WithRegistryCapacity(options.MaximumNamedStreams)
            .UseBorrowedStore((ILocalStoreAdapter)services.GetRequiredService(configuration.StoreType))
            .UseBorrowedTransport((IRemoteTransportAdapter)services.GetRequiredService(configuration.TransportType));
        ConfigureStoreInitialization(contextBuilder, configuration);
        ConfigureSerializer(contextBuilder, configuration, services);
        var context = contextBuilder.Build();
        OccasionallyConnectedLoggerBridge.ObserveStartup(context, logger);
        return context;
    }

    /// <summary>Creates the optional logger before the context is built.</summary>
    /// <param name="services">The service provider.</param>
    /// <returns>The optional logger.</returns>
    private static ILogger? CreateLogger(IServiceProvider services)
    {
        var loggerFactory = services.GetService<ILoggerFactory>();
        return loggerFactory?.CreateLogger("ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection");
    }

    /// <summary>Validates that the selected dependency still resolves from the captured singleton descriptor.</summary>
    /// <param name="configuration">The immutable configuration snapshot.</param>
    /// <param name="serviceType">The selected service type.</param>
    /// <param name="expectedDescriptor">The captured descriptor.</param>
    /// <exception cref="InvalidOperationException">The selected descriptor is no longer effective.</exception>
    private static void ValidateSelectedDescriptor(
        OccasionallyConnectedServiceConfiguration configuration,
        Type serviceType,
        ServiceDescriptor expectedDescriptor)
    {
        var actualDescriptor = FindEffectiveUnkeyedDescriptor(configuration.Services, serviceType);
        if (ReferenceEquals(actualDescriptor, expectedDescriptor) && actualDescriptor.Lifetime == ServiceLifetime.Singleton)
        {
            return;
        }

        throw new InvalidOperationException("Selected occasionally connected dependencies must remain singleton services.");
    }

    /// <summary>Finds the effective unkeyed descriptor for the requested service type.</summary>
    /// <param name="services">The service collection to inspect.</param>
    /// <param name="serviceType">The service type.</param>
    /// <returns>The effective descriptor, or <see langword="null"/> when none exists.</returns>
    private static ServiceDescriptor? FindEffectiveUnkeyedDescriptor(IServiceCollection services, Type serviceType)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType == serviceType && !descriptor.IsKeyedService)
            {
                return descriptor;
            }
        }

        return null;
    }

    /// <summary>Applies store identity or explicit initialization to the context builder.</summary>
    /// <param name="builder">The context builder.</param>
    /// <param name="configuration">The immutable configuration snapshot.</param>
    private static void ConfigureStoreInitialization(
        OccasionallyConnectedBuilder builder,
        OccasionallyConnectedServiceConfiguration configuration)
    {
        if (configuration.StoreInitialization is { } initialization)
        {
            _ = builder.UseStoreInitialization(initialization);
            return;
        }

        if (configuration.StoreIdentity is { } storeIdentity)
        {
            _ = builder.UseStoreIdentity(storeIdentity);
        }
    }

    /// <summary>Applies payload serialization to the context builder.</summary>
    /// <param name="builder">The context builder.</param>
    /// <param name="configuration">The immutable configuration snapshot.</param>
    /// <param name="services">The service provider.</param>
    /// <exception cref="InvalidOperationException">The serializer configuration is incomplete or invalid.</exception>
    private static void ConfigureSerializer(
        OccasionallyConnectedBuilder builder,
        OccasionallyConnectedServiceConfiguration configuration,
        IServiceProvider services)
    {
        if (configuration.ExplicitSerializerSelection is { } explicitSelection)
        {
            ValidateSelectedDescriptor(
                configuration,
                explicitSelection.ServiceType,
                explicitSelection.Descriptor);
            _ = builder.UseSerializer((IPayloadSerializer)services.GetRequiredService(explicitSelection.ServiceType));
            return;
        }

        var registry = new SchemaRegistry();
        var contracts = configuration.JsonContracts;
        for (var i = 0; i < contracts.Length; i++)
        {
            contracts[i].Register(registry);
        }

        if (configuration.MaximumPayloadBytes is { } maximumPayloadBytes)
        {
            _ = builder.UseJsonSerializer(registry, maximumPayloadBytes);
            return;
        }

        _ = builder.UseJsonSerializer(registry);
    }
}
