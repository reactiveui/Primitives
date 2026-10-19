// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET8_0 && !NET9_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Checks private hub composition preserves the host's SignalR configuration.</summary>
public sealed class SignalRServiceCollectionExtensionsTests
{
    /// <summary>The custom host receive limit.</summary>
    private const long HostMaximumBytes = 12_345;

    /// <summary>The custom host stream capacity.</summary>
    private const int HostStreamCapacity = 7;

    /// <summary>The custom host invocation capacity.</summary>
    private const int HostInvocationCapacity = 3;

    /// <summary>The carrier's outer framing safety multiplier.</summary>
    private const long FramingMultiplier = 2;

    /// <summary>The private hub's invocation capacity.</summary>
    private const int InvocationCapacity = 8;

    /// <summary>The real connection test deadline.</summary>
    private const int DeadlineSeconds = 30;

    /// <summary>Checks both registration orders and repeated calls preserve unrelated hub settings.</summary>
    /// <param name="hostFirst">Whether the host configures SignalR first.</param>
    /// <param name="explicitProtocols">Whether the host supplies a protocol list.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(true, true)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(false, false)]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task RegistrationPreservesHostOptions(bool hostFirst, bool explicitProtocols)
    {
        var services = new ServiceCollection();
        var baseline = new ServiceCollection();
        ConfigureHost(baseline, explicitProtocols);
        await using var baselineProvider = baseline.BuildServiceProvider();
        var baselineOptions = baselineProvider.GetRequiredService<IOptions<HubOptions>>().Value;
        if (hostFirst)
        {
            ConfigureHost(services, explicitProtocols);
        }

        _ = services.AddOccasionallyConnectedSignalR();
        _ = services.AddOccasionallyConnectedSignalR();
        if (!hostFirst)
        {
            ConfigureHost(services, explicitProtocols);
        }

        await using var provider = services.BuildServiceProvider();
        var global = provider.GetRequiredService<IOptions<HubOptions>>().Value;
        var unrelated = provider.GetRequiredService<IOptions<HubOptions<UnrelatedHub>>>().Value;
        var privateHub = provider.GetRequiredService<IOptions<HubOptions<OccasionallyConnectedHub>>>().Value;
        var json = provider.GetRequiredService<IOptions<JsonHubProtocolOptions>>().Value;
        await Assert.That(global.MaximumReceiveMessageSize).IsEqualTo(HostMaximumBytes);
        await Assert.That(unrelated.MaximumReceiveMessageSize).IsEqualTo(HostMaximumBytes);
        await Assert.That(unrelated.StreamBufferCapacity).IsEqualTo(HostStreamCapacity);
        await Assert.That(unrelated.MaximumParallelInvocationsPerClient).IsEqualTo(HostInvocationCapacity);
        await Assert.That(unrelated.EnableDetailedErrors).IsTrue();
        await Assert.That(string.Join(',', global.SupportedProtocols!)).IsEqualTo(string.Join(',', baselineOptions.SupportedProtocols!));
        await Assert.That(string.Join(',', unrelated.SupportedProtocols!)).IsEqualTo(string.Join(',', global.SupportedProtocols!));
        await Assert.That(json.PayloadSerializerOptions.TypeInfoResolver).IsSameReferenceAs(UnrelatedJsonContext.Default);
        await Assert.That(json.PayloadSerializerOptions.PropertyNameCaseInsensitive).IsFalse();
        await Assert.That(privateHub.MaximumReceiveMessageSize).IsEqualTo(SignalRCarrier.MaximumBytes * FramingMultiplier);
        await Assert.That(privateHub.StreamBufferCapacity).IsEqualTo(1);
        await Assert.That(privateHub.MaximumParallelInvocationsPerClient).IsEqualTo(InvocationCapacity);
        await Assert.That(privateHub.EnableDetailedErrors).IsFalse();
        await Assert.That(string.Join(',', privateHub.SupportedProtocols!)).IsEqualTo(SignalRHubProtocol.ProtocolName);
        await Assert.That(provider.GetServices<IHubProtocol>().Count(static protocol => protocol is SignalRHubProtocol)).IsEqualTo(1);
    }

    /// <summary>Checks unrelated DTO calls and carrier calls coexist with a custom host serializer.</summary>
    /// <param name="hostFirst">Whether the host configures its serializer first.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task HostDtoAndCarrierUseIndependentSerializers(bool hostFirst)
    {
        await using var server = await SignalRRemoteTransportAdapterTests.TestServer.StartAsync(
            configureServices: static services => ConfigureHost(services, false),
            configureHostFirst: hostFirst);
        await using var standard = new HubConnectionBuilder().WithUrl(server.UnrelatedEndpoint)
            .AddJsonProtocol(static options => options.PayloadSerializerOptions.TypeInfoResolver = UnrelatedJsonContext.Default).Build();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(DeadlineSeconds));
        await standard.StartAsync(deadline.Token);
        var message = new UnrelatedMessage("host DTO");
        await Assert.That(await standard.InvokeAsync<UnrelatedMessage>("Echo", message, deadline.Token)).IsEqualTo(message);
        await using var adapter = server.Adapter();
        await using var session = await adapter.ConnectAsync(
            new(new(new(1, 0), new(1, 0)), new("client", "forged-tenant"), [DeliveryGuarantee.AtLeastOnce]),
            deadline.Token);
        await Assert.That(session).IsNotNull();
    }

    /// <summary>Composes options owned by the host.</summary>
    /// <param name="services">The host services.</param>
    /// <param name="explicitProtocols">Whether to supply an explicit supported list.</param>
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ConfigureHost(IServiceCollection services, bool explicitProtocols) =>
        services.AddSignalR(options =>
        {
            options.MaximumReceiveMessageSize = HostMaximumBytes;
            options.StreamBufferCapacity = HostStreamCapacity;
            options.MaximumParallelInvocationsPerClient = HostInvocationCapacity;
            options.EnableDetailedErrors = true;
            if (explicitProtocols)
            {
                options.SupportedProtocols = ["host-protocol"];
            }
        }).AddHubOptions<UnrelatedHub>(static options => options.KeepAliveInterval = TimeSpan.FromSeconds(HostStreamCapacity))
        .AddJsonProtocol(static options =>
        {
            options.PayloadSerializerOptions.TypeInfoResolver = UnrelatedJsonContext.Default;
            options.PayloadSerializerOptions.PropertyNameCaseInsensitive = false;
        });
}
