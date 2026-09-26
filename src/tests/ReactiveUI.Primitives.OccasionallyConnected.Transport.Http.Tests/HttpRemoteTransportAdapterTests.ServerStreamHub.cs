// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>End-to-end tests that drive <see cref="HttpRemoteTransportAdapter"/> through a real endpoint and SQLite hub.</summary>
public sealed partial class HttpRemoteTransportAdapterTests
{
    /// <summary>The initial server version created for the hub stream.</summary>
    private const string HubInitialVersion = "v0";

    /// <summary>The first canonical server version.</summary>
    private const string HubFirstVersion = "v1";

    /// <summary>The batch identifier used when the client retries an operation in a new batch.</summary>
    private const string HubRetryBatchIdText = "00000000-0000-0000-0000-000000000901";

    /// <summary>The subscription identifier used by the end-to-end test.</summary>
    private const string HubSubscriptionIdText = "00000000-0000-0000-0000-000000000902";

    /// <summary>Verifies push, subscribe, acknowledge and duplicate push over HTTP against a durable SQLite hub.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionRoundTripsThroughEndpointBackedBySqliteServerStreamHub()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"rxui-http-hub-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(directory);
        try
        {
            await RoundTripThroughSqliteHubAsync(Path.Combine(directory, "journal.db"));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Runs the end-to-end flow against one SQLite database, reopening the hub between phases.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The asynchronous test operation.</returns>
    private static async Task RoundTripThroughSqliteHubAsync(string databasePath)
    {
        var timeProvider = new ReplayTimeProvider(ReplayObservedUtc);
        var subscriptionId = new SubscriptionId(Guid.Parse(HubSubscriptionIdText));
        var first = CreateOperation(1);
        var second = CreateOperation(SecondSequence);
        var firstDomain = new HubDomainHandler();
        OperationSyncResult original;
        string acknowledgedCursor;
        await using (var hub = ServerStreamHub.CreateSqlite(databasePath, CreateHubOptions(firstDomain, timeProvider)))
        {
            await using var endpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(hub, timeProvider));
            using var handler = new ReplayEndpointHandler(endpoint);
            using var httpClient = CreateHttpClient(handler);
            await using var adapter = CreateResolvedReplayAdapter(httpClient, timeProvider);
            await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

            var pushed = await session.PushAsync(new(Guid.NewGuid(), [first]), CancellationToken.None);
            var duplicate = await session.PushAsync(new(Guid.Parse(HubRetryBatchIdText), [first]), CancellationToken.None);
            original = pushed.Operations[0];
            await Assert.That(original.Kind).IsEqualTo(OperationResultKind.Accepted);
            await Assert.That(original.ServerVersion).IsEqualTo(HubFirstVersion);
            await Assert.That(duplicate.Operations[0]).IsEqualTo(original);

            var page = await ReadHubPageAsync(session, subscriptionId, null);
            await Assert.That(page.Events).Count().IsEqualTo(1);
            await Assert.That(page.Events[0].CausedByOperationId).IsEqualTo(first.OperationId);
            await Assert.That(page.Events[0].Origin?.ClientId).IsEqualTo(ReplayEndpointClientId);
            acknowledgedCursor = page.NextCursor;
            await session.AcknowledgeAsync(new(subscriptionId, CreateStreamId(), acknowledgedCursor), CancellationToken.None);
        }

        var reopenedDomain = new HubDomainHandler();
        await using var reopened = ServerStreamHub.CreateSqlite(databasePath, CreateHubOptions(reopenedDomain, timeProvider));
        await using var reopenedEndpoint = new HttpServerEndpoint(CreateReplayEndpointOptions(reopened, timeProvider));
        using var reopenedHandler = new ReplayEndpointHandler(reopenedEndpoint);
        using var reopenedClient = CreateHttpClient(reopenedHandler);
        await using var reopenedAdapter = CreateResolvedReplayAdapter(reopenedClient, timeProvider);
        await using var reopenedSession = await reopenedAdapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var replayed = await reopenedSession.PushAsync(new(Guid.NewGuid(), [first]), CancellationToken.None);
        var next = await reopenedSession.PushAsync(new(Guid.NewGuid(), [second]), CancellationToken.None);
        var resumed = await ReadHubPageAsync(reopenedSession, subscriptionId, acknowledgedCursor);

        await Assert.That(replayed.Operations[0]).IsEqualTo(original);
        await Assert.That(next.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(firstDomain.CallCount).IsEqualTo(1);
        await Assert.That(reopenedDomain.CallCount).IsEqualTo(1);
        await Assert.That(resumed.PreviousCursor).IsEqualTo(acknowledgedCursor);
        await Assert.That(resumed.Events).Count().IsEqualTo(1);
        await Assert.That(resumed.Events[0].CausedByOperationId).IsEqualTo(second.OperationId);
    }

    /// <summary>Reads one subscription page over HTTP.</summary>
    /// <param name="session">The connected transport session.</param>
    /// <param name="subscriptionId">The durable subscription identifier.</param>
    /// <param name="cursor">The resume cursor.</param>
    /// <returns>The first page.</returns>
    /// <exception cref="InvalidOperationException">The subscription completed without a page.</exception>
    private static async Task<RemoteEventBatch> ReadHubPageAsync(IRemoteTransportSession session, SubscriptionId subscriptionId, string? cursor)
    {
        var request = new RemoteSubscribeRequest(CreateStreamId(), subscriptionId, cursor, StartPosition.FromSequence(0));
        await using var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        var hasPage = await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(AwaitTimeoutSeconds));
        return hasPage ? enumerator.Current : throw new InvalidOperationException("The subscription completed before delivering a page.");
    }

    /// <summary>Creates hub options that register the shared test stream.</summary>
    /// <param name="domain">The domain handler.</param>
    /// <param name="timeProvider">The deterministic clock.</param>
    /// <returns>The hub options.</returns>
    private static ServerStreamHubOptions CreateHubOptions(HubDomainHandler domain, TimeProvider timeProvider)
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = HubVersionFactory.Instance });
        return new()
        {
            AuthorizationPolicy = HubAuthorizationPolicy.Instance,
            ConflictHandler = new()
            {
                Streams =
                [
                    new()
                    {
                        StreamId = CreateStreamId(),
                        InitialStateFactory = HubInitialStateFactory.Instance,
                        LastWriterWinsResolver = resolver,
                        MergeResolver = resolver,
                        CustomResolver = resolver,
                        DomainHandler = domain,
                    },
                ],
            },
            TimeProvider = timeProvider,
        };
    }

    /// <summary>Authorizes the trusted principal supplied by the host transport.</summary>
    private sealed class HubAuthorizationPolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>Gets the shared policy.</summary>
        internal static HubAuthorizationPolicy Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));
    }

    /// <summary>Creates the initial state for the hub stream.</summary>
    private sealed class HubInitialStateFactory : IServerInitialStateFactory
    {
        /// <summary>Gets the shared factory.</summary>
        internal static HubInitialStateFactory Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerState> CreateInitialStateAsync(StreamId streamId, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerState(streamId, HubInitialVersion, new("contract", 1, "application/json", "{}"u8.ToArray(), "sha256-initial")));
    }

    /// <summary>Creates the next numbered server version.</summary>
    private sealed class HubVersionFactory : IServerConflictVersionFactory
    {
        /// <summary>Gets the shared factory.</summary>
        internal static HubVersionFactory Instance { get; } = new();

        /// <inheritdoc/>
        public string CreateNextVersion(ConflictContext context, SyncOperation operation)
        {
            var current = int.Parse(context.Current.Version.AsSpan(1), NumberStyles.None, CultureInfo.InvariantCulture);
            return string.Create(CultureInfo.InvariantCulture, $"v{current + 1}");
        }
    }

    /// <summary>Counts domain effects and emits one event per accepted operation.</summary>
    private sealed class HubDomainHandler : IServerDomainHandler
    {
        /// <summary>The number of domain effects.</summary>
        private int _callCount;

        /// <summary>Gets the number of domain effects.</summary>
        internal int CallCount => Volatile.Read(ref _callCount);

        /// <inheritdoc/>
        public ValueTask<ServerDomainApplyResult> ApplyAsync(ServerDomainApplyContext context, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _callCount);
            return ValueTask.FromResult(new ServerDomainApplyResult
            {
                NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload),
                Events = [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }],
            });
        }
    }
}
