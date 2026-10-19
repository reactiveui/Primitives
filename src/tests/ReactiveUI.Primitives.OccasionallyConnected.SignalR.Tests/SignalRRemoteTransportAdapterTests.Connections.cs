// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET8_0 && !NET9_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Tests connection admission, cancellation and error cleanup.</summary>
public sealed partial class SignalRRemoteTransportAdapterTests
{
    /// <summary>Checks actual SignalR negotiation failures expose the engine's retry classification.</summary>
    /// <param name="status">The rejected negotiation status.</param>
    /// <param name="kind">The expected classification.</param>
    /// <returns>The test task.</returns>
    [Test]
    [Arguments(401, HttpTransportFailureKind.Authentication)]
    [Arguments(403, HttpTransportFailureKind.AuthorizationDenied)]
    [Arguments(503, HttpTransportFailureKind.Transient)]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task NegotiationFailureIsClassified(int status, HttpTransportFailureKind kind)
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = new SignalRRemoteTransportAdapter(new()
        {
            Endpoint = server.Endpoint,
            AllowInsecureLoopbackHttp = true,
            ConfigureConnection = options => options.Headers.Add("X-Negotiation-Failure", status.ToString(CultureInfo.InvariantCulture)),
        });
        var failure = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        await Assert.That(failure!.Kind).IsEqualTo(kind);
        await Assert.That((int)failure.StatusCode!).IsEqualTo(status);
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks factory failure after a physical connection opens releases admission.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task ProtocolFactoryFailureDoesNotLeakConnectionAdmission()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = new SignalRRemoteTransportAdapter(new()
        {
            Endpoint = server.Endpoint,
            AllowInsecureLoopbackHttp = true,
            MaximumSessions = 1,
            ConfigureConnection = static options => options.Headers.Add("Authorization", "Bearer test"),
            ConfigureProtocol = static _ => throw new InvalidOperationException("factory failure"),
        });
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks the default generated protocol composition works without a custom factory.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task DefaultProtocolConfigurationPushesThroughRealServer()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = new SignalRRemoteTransportAdapter(new()
        {
            Endpoint = server.Endpoint,
            AllowInsecureLoopbackHttp = true,
            ConfigureConnection = static options => options.Headers.Add("Authorization", "Bearer test"),
        });
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        await Assert.That(adapter.Capabilities).IsEqualTo(session.NegotiatedCapabilities.Features);
        var result = await session.PushAsync(Batch(), CancellationToken.None);
        await Assert.That(result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Checks the absent credential configuration cannot bypass host authentication.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task AbsentConnectionConfigurationIsUnauthenticated()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = new SignalRRemoteTransportAdapter(new()
        { Endpoint = server.Endpoint, AllowInsecureLoopbackHttp = true });
        var failure = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        await Assert.That(failure!.Kind).IsEqualTo(HttpTransportFailureKind.Authentication);
    }

    /// <summary>Checks failed host configuration unwinds pending admission before a physical session exists.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task ConnectionConfigurationFailureReleasesAdmission()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = new SignalRRemoteTransportAdapter(new()
        {
            Endpoint = server.Endpoint,
            AllowInsecureLoopbackHttp = true,
            ConfigureConnection = static _ => throw new InvalidOperationException("configuration failure"),
        });
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks disposal cancels and drains a pending connect while rejecting excess concurrent admission.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task DisposalCancelsPendingConnectAndConcurrentCapacityIsFinite()
    {
        await using var server = await TestServer.StartAsync();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var adapter = new SignalRRemoteTransportAdapter(new()
        {
            Endpoint = server.Endpoint,
            AllowInsecureLoopbackHttp = true,
            MaximumSessions = 1,
            ConfigureConnection = options => options.HttpMessageHandlerFactory = inner => new BlockingNegotiationHandler(inner, started),
        });
        var connect = adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(DeadlineSeconds));
        var excess = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        await Assert.That(excess!.IsTransient).IsTrue();
        await adapter.DisposeAsync();
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => connect);
    }

    /// <summary>Checks canceling one overlapping connection does not cancel or miscount the other.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task ConcurrentConnectCancellationIsIsolated()
    {
        await using var server = await TestServer.StartAsync();
        TaskCompletionSource[] started =
        [
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
        ];
        var next = -1;
        using var firstCancellation = new CancellationTokenSource();
        using var secondCancellation = new CancellationTokenSource();
        await using var adapter = new SignalRRemoteTransportAdapter(new()
        {
            Endpoint = server.Endpoint,
            AllowInsecureLoopbackHttp = true,
            MaximumSessions = SecondSequence,
            ConfigureConnection = options => options.HttpMessageHandlerFactory =
                inner => new BlockingNegotiationHandler(inner, started[Interlocked.Increment(ref next)]),
        });
        var first = adapter.ConnectAsync(ConnectRequest(), firstCancellation.Token).AsTask();
        var second = adapter.ConnectAsync(ConnectRequest(), secondCancellation.Token).AsTask();
        await Task.WhenAll(started[0].Task, started[1].Task).WaitAsync(TimeSpan.FromSeconds(DeadlineSeconds));
        await firstCancellation.CancelAsync();
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => first);
        await Assert.That(second.IsCompleted).IsFalse();
        await secondCancellation.CancelAsync();
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() => second);
    }

    /// <summary>Pauses real connection negotiation until its owning adapter cancels it.</summary>
    /// <param name="inner">The SignalR HTTP handler.</param>
    /// <param name="started">The arrival signal.</param>
    private sealed class BlockingNegotiationHandler(HttpMessageHandler inner, TaskCompletionSource started) : DelegatingHandler(inner)
    {
        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            _ = started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return await base.SendAsync(request, cancellationToken);
        }
    }
}
