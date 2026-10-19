// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.Metrics;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Host metrics factory tests for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Verifies the builder forwards the host factory only when diagnostics are enabled.</summary>
    /// <param name="enabled">Whether diagnostics are enabled.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task UseMeterFactoryForwardsFactoryWithoutTakingOwnership(bool enabled)
    {
        using var factory = new BuilderMeterFactory();
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var builder = CreateReadyBuilder(store, transport)
            .ConfigureOptions(options => options with { Diagnostics = options.Diagnostics with { Enabled = enabled } });
        await Assert.That(builder.UseMeterFactory(factory)).IsSameReferenceAs(builder);
        var context = builder.Build();
        await Assert.That(factory.CreateCount).IsEqualTo(enabled ? 1 : 0);
        await context.DisposeAsync();
        await Assert.That(factory.IsDisposed).IsFalse();
        await Assert.That(() => builder.UseMeterFactory(factory)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the factory argument is required.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UseMeterFactoryRejectsNull() =>
        await Assert.That(static () => CreateBuilder().UseMeterFactory(NullReference<IMeterFactory>())).ThrowsExactly<ArgumentNullException>();

    /// <summary>Records host factory calls from a composed engine.</summary>
    private sealed class BuilderMeterFactory : IMeterFactory
    {
        /// <summary>The meter owned by this factory.</summary>
        private readonly Meter _meter = new("ReactiveUI.Primitives.OccasionallyConnected");

        /// <summary>Gets the number of meter requests.</summary>
        internal int CreateCount { get; private set; }

        /// <summary>Gets whether the host factory was disposed.</summary>
        internal bool IsDisposed { get; private set; }

        /// <inheritdoc />
        public Meter Create(MeterOptions options)
        {
            _ = options;
            CreateCount++;
            return _meter;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            IsDisposed = true;
            _meter.Dispose();
        }
    }
}
