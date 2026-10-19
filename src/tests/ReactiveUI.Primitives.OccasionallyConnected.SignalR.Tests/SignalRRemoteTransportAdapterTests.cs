// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET8_0 && !NET9_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
#endif
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Exercises the real SignalR client, Kestrel hub, protocol endpoint and core journal.</summary>
public sealed partial class SignalRRemoteTransportAdapterTests
{
    /// <summary>The second operation sequence and expected committed call count.</summary>
    private const int SecondSequence = 2;

    /// <summary>The receive cancellation delay.</summary>
    private const int CancellationMilliseconds = 250;

    /// <summary>The maximum integration wait.</summary>
    private const int DeadlineSeconds = 15;

    /// <summary>Checks durable logical resume, terminal operation deduplication and receive ACKs across physical reconnects.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task PushReceiveAcknowledgeAndReconnectPreserveLogicalSubscription()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter();
        var batch = Batch();
        var subscription = SubscriptionId.New();
        var first = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var result = await first.PushAsync(batch, CancellationToken.None);
        await Assert.That(result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        var page = await ReadAsync(first, subscription);
        await Assert.That(page.Events[0].Origin!.ClientId).IsEqualTo("client");
        await first.AcknowledgeAsync(new(subscription, Stream, page.NextCursor), CancellationToken.None);
        await first.AcknowledgeAsync(new(subscription, Stream, page.NextCursor), CancellationToken.None);
        await first.DisposeAsync();

        await using var second = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var duplicate = await second.PushAsync(new(Guid.NewGuid(), batch.Operations), CancellationToken.None);
        await Assert.That(duplicate.Operations[0]).IsEqualTo(result.Operations[0]);
        await Assert.That(server.Domain.Calls).IsEqualTo(1);
        _ = await second.PushAsync(Batch(SecondSequence), CancellationToken.None);
        var next = await ReadAsync(second, subscription, page.NextCursor);
        await Assert.That(next.PreviousCursor).IsEqualTo(page.NextCursor);
        await Assert.That(next.Events).Count().IsEqualTo(1);
        await Assert.That(server.Domain.Calls).IsEqualTo(SecondSequence);
        await Assert.That((second.NegotiatedCapabilities.Features & RemoteTransportCapabilities.StreamingReceive) != 0).IsFalse();
    }

    /// <summary>Checks a lost network ACK never repeats a committed domain effect after engine reconnect.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task LostPushAcknowledgementCanBeRetriedAfterExplicitReconnect()
    {
        await using var server = await TestServer.StartAsync(dropPushResponse: true);
        await using var adapter = server.Adapter();
        var batch = Batch();
        await using var first = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var failure = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(() => first.PushAsync(batch, CancellationToken.None).AsTask());
        await Assert.That(failure!.IsTransient).IsTrue();
        await Assert.That(server.Domain.Calls).IsEqualTo(1);
        await using var second = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var result = await second.PushAsync(new(Guid.NewGuid(), batch.Operations), CancellationToken.None);
        await Assert.That(result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(server.Domain.Calls).IsEqualTo(1);
    }

    /// <summary>Checks that authentication comes from the host, not a forged request identity.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task UnauthenticatedConnectionCannotHandshake()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter(authenticate: false);
        var failure = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        await Assert.That(failure!.Kind).IsEqualTo(HttpTransportFailureKind.Authentication);
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks that wire client identity cannot replace the authenticated identity.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task ForgedClientIdentityIsRejected()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter();
        _ = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => adapter.ConnectAsync(ConnectRequest("forged-client"), CancellationToken.None).AsTask());
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks authorization before server journal effects.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task CrossStreamPushIsDeniedBeforeDomainEffects()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter();
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var operation = Batch().Operations[0] with { StreamId = new("denied-stream") };
        var failure = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => session.PushAsync(new(Guid.NewGuid(), [operation]), CancellationToken.None).AsTask());
        await Assert.That(failure!.Kind).IsEqualTo(HttpTransportFailureKind.AuthorizationDenied);
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks negotiated batch limits at the client and server boundary.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task NegotiatedOperationLimitRejectsOversizedBatch()
    {
        await using var server = await TestServer.StartAsync(maximumOperations: 1);
        await using var adapter = server.Adapter();
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        _ = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => session.PushAsync(new(Guid.NewGuid(), [Batch().Operations[0], Batch(SecondSequence).Operations[0]]), CancellationToken.None).AsTask());
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks payload bounds before any network effect.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task PayloadLimitRejectsOversizedEnvelope()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter(maximumPayloadBytes: 1);
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        _ = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => session.PushAsync(Batch(), CancellationToken.None).AsTask());
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks cancellation unwinds a real pending poll and does not prevent subsequent work.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task CanceledReceiveCanBeFollowedByPush()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter();
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(CancellationMilliseconds));
        await using var enumerator = session.SubscribeAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            cancellation.Token).GetAsyncEnumerator(cancellation.Token);
        await Assert.That(await enumerator.MoveNextAsync()).IsFalse();
        await Assert.That(cancellation.IsCancellationRequested).IsTrue();
        var result = await session.PushAsync(Batch(), CancellationToken.None);
        await Assert.That(result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Checks adapter disposal stops active sessions and is safe to repeat.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task AdapterDisposalDrainsSessionsAndRejectsNewConnections()
    {
        await using var server = await TestServer.StartAsync();
        var adapter = server.Adapter();
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        await adapter.DisposeAsync();
        await adapter.DisposeAsync();
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(
            () => session.PushAsync(Batch(), CancellationToken.None).AsTask());
    }

    /// <summary>Checks disconnect surfaces instead of running a hidden reconnect loop.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task ServerShutdownFailsPendingReceiveWithoutReconnect()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter();
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(DeadlineSeconds));
        await using var enumerator = session.SubscribeAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            deadline.Token).GetAsyncEnumerator(deadline.Token);
        var pending = enumerator.MoveNextAsync().AsTask();
        await server.StopAsync();
        _ = await Assert.ThrowsAsync<Exception>(() => pending);
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks typed snapshot recovery and accepted operation inclusion through the real connection.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task SnapshotRecoveryUsesAuthorizedCapturedStateAndOperationLedger()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter();
        await using var session = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var subscription = SubscriptionId.New();
        var batch = Batch();
        _ = await session.PushAsync(batch, CancellationToken.None);
        var first = await ReadAsync(session, subscription);
        _ = await session.PushAsync(Batch(SecondSequence), CancellationToken.None);
        var request = new RemoteSnapshotRecoveryRequest
        {
            StreamId = Stream,
            SubscriptionId = subscription,
            ExpiredCursor = first.NextCursor,
            ClientStateContractId = "test",
            ClientStateSchemaVersion = 1,
            SnapshotFormatVersion = 1,
            PendingOperations = batch.Operations,
            MaximumResponseBytes = MaximumBodyBytes,
        };
        var recovered = await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(request, CancellationToken.None);
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint!.StreamId).IsEqualTo(Stream);
        await Assert.That(recovered.Checkpoint.FrontierCursor).IsNotEqualTo(first.NextCursor);
        await Assert.That(recovered.OperationDispositions[0].Kind).IsEqualTo(SnapshotOperationDispositionKind.IncludedAccepted);
        await session.AcknowledgeAsync(new(subscription, Stream, recovered.Checkpoint.FrontierCursor), CancellationToken.None);
    }

    /// <summary>Checks unsupported protocol versions are classified as permanent before effects.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task UnsupportedProtocolHandshakeFailsWithoutDomainEffects()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter();
        var request = new TransportConnectRequest(new(new(SecondSequence, 0), new(SecondSequence, 0)), new(ClientName), []);
        _ = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => adapter.ConnectAsync(request, CancellationToken.None).AsTask());
        await Assert.That(server.Domain.Calls).IsEqualTo(0);
    }

    /// <summary>Checks finite physical session admission and resource release after disposal.</summary>
    /// <returns>The test task.</returns>
    [Test]
#if NET8_0 && !NET9_0_OR_GREATER
    [RequiresUnreferencedCode("Hosts ASP.NET Core 8 SignalR, which does not support trimming.")]
#endif
    public async Task SessionCapacityIsReleasedAfterSessionDisposal()
    {
        await using var server = await TestServer.StartAsync();
        await using var adapter = server.Adapter(maximumSessions: 1);
        var first = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var failure = await Assert.ThrowsExactlyAsync<HttpRemoteTransportException>(
            () => adapter.ConnectAsync(ConnectRequest(), CancellationToken.None).AsTask());
        await Assert.That(failure!.IsTransient).IsTrue();
        await first.DisposeAsync();
        await using var second = await adapter.ConnectAsync(ConnectRequest(), CancellationToken.None);
        var result = await second.PushAsync(Batch(), CancellationToken.None);
        await Assert.That(result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }
}
