// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting;

/// <summary>Registers host lifecycle and health check integration for an occasionally connected context.</summary>
public static class OccasionallyConnectedHostingExtensions
{
    /// <summary>Gets the default health check registration name.</summary>
    public static string DefaultHealthCheckName { get; } = "occasionally-connected";

    /// <summary>Health check registration helpers.</summary>
    /// <param name="builder">The health checks builder.</param>
    extension(IHealthChecksBuilder builder)
    {
        /// <summary>Adds the occasionally connected health check with the default name.</summary>
        /// <returns>The health checks builder.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IHealthChecksBuilder AddOccasionallyConnected() => builder.AddOccasionallyConnected(DefaultHealthCheckName);

        /// <summary>Adds the occasionally connected health check.</summary>
        /// <param name="name">The health check registration name.</param>
        /// <returns>The health checks builder.</returns>
        public IHealthChecksBuilder AddOccasionallyConnected(string name)
        {
            ArgumentExceptionHelper.ThrowIfNull(builder);
            ArgumentExceptionHelper.ThrowIfNull(name);
            _ = builder.Services.AddOccasionallyConnectedHosting();
            return builder.AddCheck<OccasionallyConnectedHealthCheck>(name);
        }
    }

    /// <summary>Host registration helpers.</summary>
    /// <param name="services">The service collection.</param>
    extension(IServiceCollection services)
    {
        /// <summary>Starts the registered context with the host, stops it gracefully on shutdown, and registers its health monitor.</summary>
        /// <returns>The service collection.</returns>
        /// <remarks>Register the context first with <c>AddOccasionallyConnected</c>.</remarks>
        public IServiceCollection AddOccasionallyConnectedHosting()
        {
            ArgumentExceptionHelper.ThrowIfNull(services);
            services.TryAddSingleton(static provider => new OccasionallyConnectedHealthMonitor(
                provider.GetRequiredService<IOccasionallyConnectedContext>(),
                provider.GetService<TimeProvider>() ?? TimeProvider.System));
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OccasionallyConnectedHostedService>(
                static provider => new(provider.GetRequiredService<IOccasionallyConnectedContext>())));
            return services;
        }
    }
}
