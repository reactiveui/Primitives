// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Tests for service option validation.</summary>
public sealed class OccasionallyConnectedServiceOptionsValidatorTests
{
    /// <summary>Verifies missing runtime options are rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ValidateRejectsMissingRuntimeOptions()
    {
        var result = Validate(new() { Options = NullReference<OccasionallyConnectedOptions>() });

        await Assert.That(result.Failed).IsTrue();
        await Assert.That(result.FailureMessage).Contains("Options");
    }

    /// <summary>Verifies non-positive stream counts are rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ValidateRejectsNonPositiveMaximumNamedStreams()
    {
        var result = Validate(new() { Options = OccasionallyConnectedOptions.Default, MaximumNamedStreams = 0, MaximumStreamNameLength = 1 });

        await Assert.That(result.Failed).IsTrue();
        await Assert.That(result.FailureMessage).Contains(nameof(OccasionallyConnectedServiceOptions.MaximumNamedStreams));
    }

    /// <summary>Verifies non-positive stream name lengths are rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ValidateRejectsNonPositiveMaximumStreamNameLength()
    {
        var result = Validate(new() { Options = OccasionallyConnectedOptions.Default, MaximumNamedStreams = 1, MaximumStreamNameLength = 0 });

        await Assert.That(result.Failed).IsTrue();
        await Assert.That(result.FailureMessage).Contains(
            nameof(OccasionallyConnectedServiceOptions.MaximumStreamNameLength));
    }

    /// <summary>Verifies invalid runtime option snapshots are returned as validation failures.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ValidateRejectsInvalidRuntimeOptions()
    {
        var result = Validate(new()
        { Options = OccasionallyConnectedOptions.Default with { MaxConcurrentStreams = 0 }, MaximumNamedStreams = 1, MaximumStreamNameLength = 1 });

        await Assert.That(result.Failed).IsTrue();
        await Assert.That(result.FailureMessage).Contains(nameof(OccasionallyConnectedOptions.MaxConcurrentStreams));
    }

    /// <summary>Validates the supplied options.</summary>
    /// <param name="options">The options to validate.</param>
    /// <returns>The validation result.</returns>
    private static ValidateOptionsResult Validate(OccasionallyConnectedServiceOptions options)
    {
        using var provider = CreateValidationServices().BuildServiceProvider(validateScopes: true);
        var validator = provider.GetRequiredService<IValidateOptions<OccasionallyConnectedServiceOptions>>();
        return validator.Validate(null, options);
    }

    /// <summary>Creates a null reference for a non-nullable malformed-input fixture.</summary>
    /// <typeparam name="T">The non-nullable reference type.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T" />.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T NullReference<T>()
        where T : class
    {
        object? value = null;
        return Unsafe.As<object?, T>(ref value);
    }

    /// <summary>Creates services that register the public options validation service.</summary>
    /// <returns>The configured service collection.</returns>
    private static ServiceCollection CreateValidationServices()
    {
        ServiceCollection services = new();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();
        _ = services.AddOccasionallyConnected(static builder => _ = builder
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
                DependencyInjectionJsonContext.Default.CounterState));
        return services;
    }
}
