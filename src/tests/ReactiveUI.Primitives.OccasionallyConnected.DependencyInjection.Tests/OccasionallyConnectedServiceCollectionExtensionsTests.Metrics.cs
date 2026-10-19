// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Tests host-owned meter composition.</summary>
public sealed partial class OccasionallyConnectedServiceCollectionExtensionsTests
{
    /// <summary>Verifies context disposal does not dispose a host-owned meter factory.</summary>
    /// <param name="enabled">Whether diagnostics are enabled.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task AddOccasionallyConnectedBorrowsHostMeterFactory(bool enabled)
    {
        using var factory = new RecordingMeterFactory();
        var services = CreateServices(
            collection => collection.AddSingleton<IMeterFactory>(factory),
            builder => builder.ConfigureOptions(options => options with
            {
                Diagnostics = options.Diagnostics with { Enabled = enabled },
            }));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var context = provider.GetRequiredService<OccasionallyConnectedContext>();

        await Assert.That(factory.CreateCalls).IsEqualTo(enabled ? 1 : 0);
        await context.DisposeAsync();
        await Assert.That(factory.DisposeCalls).IsEqualTo(0);
    }

    /// <summary>Verifies separate host containers keep their meter lifetimes isolated.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddOccasionallyConnectedKeepsHostMeterFactoriesIsolated()
    {
        var firstFactory = new RecordingMeterFactory();
        var secondFactory = new RecordingMeterFactory();
        var firstServices = CreateServices(collection => collection.AddSingleton<IMeterFactory>(_ => firstFactory), static _ => { });
        var secondServices = CreateServices(collection => collection.AddSingleton<IMeterFactory>(_ => secondFactory), static _ => { });
        await using var firstProvider = firstServices.BuildServiceProvider(validateScopes: true);
        await using var secondProvider = secondServices.BuildServiceProvider(validateScopes: true);
        _ = firstProvider.GetRequiredService<OccasionallyConnectedContext>();
        _ = secondProvider.GetRequiredService<OccasionallyConnectedContext>();

        await Assert.That(firstFactory.CreatedMeter).IsNotNull();
        await Assert.That(secondFactory.CreatedMeter).IsNotNull();
        await Assert.That(firstFactory.CreatedMeter).IsNotSameReferenceAs(secondFactory.CreatedMeter);
        await firstProvider.DisposeAsync();
        await Assert.That(firstFactory.DisposeCalls).IsEqualTo(1);
        await Assert.That(secondFactory.DisposeCalls).IsEqualTo(0);
    }

    /// <summary>Tracks the factory contract and owns its cached meter.</summary>
    private sealed class RecordingMeterFactory : IMeterFactory
    {
        /// <summary>Gets the number of meter requests.</summary>
        internal int CreateCalls { get; private set; }

        /// <summary>Gets the number of factory disposal calls.</summary>
        internal int DisposeCalls { get; private set; }

        /// <summary>Gets the host-owned meter.</summary>
        internal Meter? CreatedMeter { get; private set; }

        /// <inheritdoc />
        public Meter Create(MeterOptions options)
        {
            CreateCalls++;
            return CreatedMeter ??= new(options);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            DisposeCalls++;
            CreatedMeter?.Dispose();
        }
    }
}
