// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net.Http;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>The controllable protocol peer and server doubles used by the shared transport suite.</summary>
public sealed partial class IRemoteTransportAdapterTests
{
    /// <summary>Selects how the peer corrupts the next push result.</summary>
    private enum PushResultFault
    {
        /// <summary>The result is returned unchanged.</summary>
        None = 0,

        /// <summary>The last per-operation result is removed.</summary>
        Truncate = 1,

        /// <summary>The last per-operation result names an operation that was not pushed.</summary>
        ForeignOperation = 2,
    }

    /// <summary>Delegates to a real hub and injects duplicate, reordered, lost and truncated protocol messages.</summary>
    /// <param name="inner">The real hub.</param>
    private sealed class FaultInjectingServerStreamHub(ServerStreamHub inner) : IServerStreamHub, IServerSnapshotRecoveryHub
    {
        /// <summary>Synchronizes fault arming across transport threads.</summary>
#if NET9_0_OR_GREATER
        private readonly Lock _gate = new();
#else
        private readonly object _gate = new();
#endif

        /// <summary>The number of apply calls that reached the peer.</summary>
        private int _applyCalls;

        /// <summary>The number of acknowledgement calls that reached the peer.</summary>
        private int _acknowledgeCalls;

        /// <summary>The number of subscribe calls that reached the peer.</summary>
        private int _subscribeCalls;

        /// <summary>The armed push-result fault.</summary>
        private PushResultFault _pushFault;

        /// <summary>Whether the next acknowledgement is lost before the hub records it.</summary>
        private bool _dropAcknowledgement;

        /// <summary>Whether the last delivered batch is delivered again on the next move.</summary>
        private bool _duplicateBatch;

        /// <summary>Whether the next two pages are delivered in swapped order.</summary>
        private bool _reorderBatches;

        /// <summary>The last batch the peer delivered.</summary>
        private RemoteEventBatch? _lastBatch;

        /// <summary>Gets the number of apply calls that reached the peer.</summary>
        internal int ApplyCalls => Volatile.Read(ref _applyCalls);

        /// <summary>Gets the number of acknowledgement calls that reached the peer.</summary>
        internal int AcknowledgeCalls => Volatile.Read(ref _acknowledgeCalls);

        /// <summary>Gets the number of subscribe calls that reached the peer.</summary>
        internal int SubscribeCalls => Volatile.Read(ref _subscribeCalls);

        /// <inheritdoc/>
        public async ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _applyCalls);
            var applied = await inner.ApplyOperationsAsync(batch, client, cancellationToken);
            PushResultFault fault;
            lock (_gate)
            {
                fault = _pushFault;
                _pushFault = PushResultFault.None;
            }

            return fault == PushResultFault.None ? applied : new(Corrupt(applied.Result, fault), applied.ProducedEvents);
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _acknowledgeCalls);
            bool drop;
            lock (_gate)
            {
                drop = _dropAcknowledgement;
                _dropAcknowledgement = false;
            }

            return drop
                ? ValueTask.FromException(new InvalidOperationException("The acknowledgement was lost before the hub recorded it."))
                : inner.AcknowledgeAsync(acknowledgement, client, cancellationToken);
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ServerAuthenticatedClient client,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _subscribeCalls);
            await using var upstream = inner.SubscribeStreamAsync(request, client, cancellationToken).GetAsyncEnumerator(cancellationToken);
            while (true)
            {
                if (TryTakeDuplicate(out var duplicate))
                {
                    yield return duplicate;
                    continue;
                }

                if (!await upstream.MoveNextAsync())
                {
                    yield break;
                }

                var first = upstream.Current;
                if (!TryTakeReorder())
                {
                    Remember(first);
                    yield return first;
                    continue;
                }

                if (!await upstream.MoveNextAsync())
                {
                    Remember(first);
                    yield return first;
                    yield break;
                }

                var second = upstream.Current;
                Remember(second);
                yield return second;
                Remember(first);
                yield return first;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken) =>
            ((IServerSnapshotRecoveryHub)inner).GetSnapshotAsync(request, client, cancellationToken);

        /// <summary>Arms a fault for the next push result.</summary>
        /// <param name="fault">The fault.</param>
        internal void CorruptNextPushResult(PushResultFault fault)
        {
            lock (_gate)
            {
                _pushFault = fault;
            }
        }

        /// <summary>Arms the loss of the next acknowledgement.</summary>
        internal void DropNextAcknowledgement()
        {
            lock (_gate)
            {
                _dropAcknowledgement = true;
            }
        }

        /// <summary>Arms a duplicate delivery of the last batch.</summary>
        internal void DuplicateNextBatch()
        {
            lock (_gate)
            {
                _duplicateBatch = true;
            }
        }

        /// <summary>Arms a swapped delivery of the next two pages.</summary>
        internal void ReorderNextBatches()
        {
            lock (_gate)
            {
                _reorderBatches = true;
            }
        }

        /// <summary>Corrupts a push result.</summary>
        /// <param name="result">The original result.</param>
        /// <param name="fault">The fault.</param>
        /// <returns>The corrupted result.</returns>
        private static RemoteSyncResult Corrupt(RemoteSyncResult result, PushResultFault fault)
        {
            var operations = result.Operations.Take(result.Operations.Count - 1).ToList();
            if (fault == PushResultFault.ForeignOperation)
            {
                var last = result.Operations[result.Operations.Count - 1];
                operations.Add(last with { OperationId = new(Guid.NewGuid()) });
            }

            return new(result.BatchId, operations, result.ServerCursor, result.RetryAfter);
        }

        /// <summary>Takes the armed duplicate delivery.</summary>
        /// <param name="duplicate">The batch to deliver again.</param>
        /// <returns><see langword="true"/> when a duplicate is due.</returns>
        private bool TryTakeDuplicate(out RemoteEventBatch duplicate)
        {
            lock (_gate)
            {
                if (_duplicateBatch && _lastBatch is { } last)
                {
                    _duplicateBatch = false;
                    duplicate = last;
                    return true;
                }
            }

            duplicate = null!;
            return false;
        }

        /// <summary>Takes the armed reorder.</summary>
        /// <returns><see langword="true"/> when the next two pages must be swapped.</returns>
        private bool TryTakeReorder()
        {
            lock (_gate)
            {
                var reorder = _reorderBatches;
                _reorderBatches = false;
                return reorder;
            }
        }

        /// <summary>Remembers the last delivered batch.</summary>
        /// <param name="batch">The batch.</param>
        private void Remember(RemoteEventBatch batch)
        {
            lock (_gate)
            {
                _lastBatch = batch;
            }
        }
    }

    /// <summary>
    /// Forwards adapter HTTP requests into the in-process endpoint as the host transport. Like a socket transport, a
    /// request the client canceled ends with <see cref="OperationCanceledException"/> instead of a late response.
    /// </summary>
    /// <param name="endpoint">The endpoint.</param>
    private sealed class EndpointHandler(HttpServerEndpoint endpoint) : HttpMessageHandler
    {
        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await endpoint.HandleAsync(request, new(Tenant, Client), cancellationToken);
            if (!cancellationToken.IsCancellationRequested)
            {
                return response;
            }

            response.Dispose();
            throw new OperationCanceledException(cancellationToken);
        }
    }

    /// <summary>Counts domain effects and emits a fixed number of events per accepted operation.</summary>
    /// <param name="eventsPerOperation">The number of events per operation.</param>
    private sealed class HubDomainHandler(int eventsPerOperation) : IServerDomainHandler
    {
        /// <summary>The number of domain effects.</summary>
        private int _callCount;

        /// <summary>Gets the number of domain effects.</summary>
        internal int CallCount => Volatile.Read(ref _callCount);

        /// <inheritdoc/>
        public ValueTask<ServerDomainApplyResult> ApplyAsync(ServerDomainApplyContext context, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _callCount);
            var operationId = context.Operation.OperationId.Value;
            var events = new ServerProducedEvent[eventsPerOperation];
            for (var index = 0; index < events.Length; index++)
            {
                events[index] = new() { EventId = DeriveEventId(operationId, index), Payload = context.Operation.Payload };
            }

            var state = new ServerState(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload);
            return ValueTask.FromResult(new ServerDomainApplyResult { NewState = state, Events = events });
        }

        /// <summary>Derives a stable event identifier from an operation identifier.</summary>
        /// <param name="operationId">The operation identifier.</param>
        /// <param name="index">The event index.</param>
        /// <returns>The event identifier.</returns>
        private static Guid DeriveEventId(Guid operationId, int index)
        {
            if (index == 0)
            {
                return operationId;
            }

            var bytes = operationId.ToByteArray();
            bytes[0] = (byte)(bytes[0] ^ index);
            return new(bytes);
        }
    }

    /// <summary>Authorizes the trusted principal supplied by the host transport.</summary>
    private sealed class HubAuthorizationPolicy : IServerStreamAuthorizationPolicy, IServerSnapshotRecoveryAuthorizationPolicy
    {
        /// <summary>Gets the shared policy.</summary>
        internal static HubAuthorizationPolicy Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) => Scope(client);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) => Scope(client);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) => Scope(client);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) => Scope(client);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSnapshotRecoveryAsync(
            ServerAuthenticatedClient client,
            RemoteSnapshotRecoveryRequest request,
            CancellationToken cancellationToken) => Scope(client);

        /// <summary>Creates the scope for the trusted principal.</summary>
        /// <param name="client">The trusted principal.</param>
        /// <returns>The scope.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ValueTask<ServerStreamAuthorizationScope> Scope(ServerAuthenticatedClient client) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(client.TenantId, client.ClientId));
    }

    /// <summary>Materializes a fixed client state for snapshot recovery.</summary>
    private sealed class FixedSnapshotMaterializer : IServerSnapshotMaterializer
    {
        /// <summary>Gets the shared materializer.</summary>
        internal static FixedSnapshotMaterializer Instance { get; } = new();

        /// <summary>Gets the materialized client state.</summary>
        internal static PayloadEnvelope ClientState { get; } = new(Contract, 1, ContentType, "{\"snapshot\":true}"u8.ToArray(), "sha256-snapshot");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
            ServerSnapshotMaterializationContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerSnapshotMaterializationResult { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = ClientState });
    }

    /// <summary>Creates the initial state for the shared stream.</summary>
    private sealed class HubInitialStateFactory : IServerInitialStateFactory
    {
        /// <summary>Gets the shared factory.</summary>
        internal static HubInitialStateFactory Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerState> CreateInitialStateAsync(StreamId streamId, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerState(streamId, InitialVersion, new(Contract, 1, ContentType, "{}"u8.ToArray(), "sha256-initial")));
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

    /// <summary>Allows replay admission for the trusted principal.</summary>
    private sealed class AllowReplayAuthorizer : IHttpReplayAuthorizer
    {
        /// <summary>Gets the shared authorizer.</summary>
        internal static AllowReplayAuthorizer Instance { get; } = new();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> AuthorizeReplayAsync(HttpReplayAuthorizationContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(true);
    }
}
