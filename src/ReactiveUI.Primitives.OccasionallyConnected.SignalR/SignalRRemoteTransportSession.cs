// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.SignalR.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Owns one physical connection and the common protocol's pull subscription lifetime.</summary>
internal sealed class SignalRRemoteTransportSession : IRemoteTransportSession, IRemoteSnapshotRecoverySession
{
    /// <summary>The physical connection.</summary>
    private readonly HubConnection _connection;

    /// <summary>The owned common protocol adapter.</summary>
    private readonly HttpRemoteTransportAdapter _adapter;

    /// <summary>The owned RPC client facade.</summary>
    private readonly HttpClient _client;

    /// <summary>The common protocol session.</summary>
    private readonly IRemoteTransportSession _session;

    /// <summary>Protects shared disposal completion.</summary>
    private readonly Lock _gate = new();

    /// <summary>Releases adapter ownership when disposal finishes.</summary>
    private readonly Action<SignalRRemoteTransportSession> _onDisposed;

    /// <summary>The shared disposal task.</summary>
    private Task? _disposeTask;

    /// <summary>Initializes a new instance of the <see cref="SignalRRemoteTransportSession"/> class.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="adapter">The protocol adapter.</param>
    /// <param name="client">The RPC client.</param>
    /// <param name="session">The protocol session.</param>
    /// <param name="onDisposed">The ownership-release callback.</param>
    private SignalRRemoteTransportSession(
        HubConnection connection,
        HttpRemoteTransportAdapter adapter,
        HttpClient client,
        IRemoteTransportSession session,
        Action<SignalRRemoteTransportSession> onDisposed)
    {
        _connection = connection;
        _adapter = adapter;
        _client = client;
        _session = session;
        _onDisposed = onDisposed;
    }

    /// <inheritdoc/>
    public NegotiatedCapabilities NegotiatedCapabilities => _session.NegotiatedCapabilities;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
        _session.PushAsync(batch, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(RemoteSubscribeRequest request, CancellationToken cancellationToken) =>
        _session.SubscribeAsync(request, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
        _session.AcknowledgeAsync(acknowledgement, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
        RemoteSnapshotRecoveryRequest request,
        CancellationToken cancellationToken) =>
        ((IRemoteSnapshotRecoverySession)_session).GetSnapshotAsync(request, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        Task task;
        lock (_gate)
        {
            _disposeTask ??= DisposeCoreAsync();
            task = _disposeTask;
        }

        await task.ConfigureAwait(false);
    }

    /// <summary>Performs the protocol handshake after SignalR starts.</summary>
    /// <param name="connection">The started connection.</param>
    /// <param name="options">The configuration.</param>
    /// <param name="request">The handshake.</param>
    /// <param name="onDisposed">The ownership-release callback.</param>
    /// <param name="cancellationToken">The connect cancellation token.</param>
    /// <returns>The owned session.</returns>
    internal static async Task<SignalRRemoteTransportSession> CreateAsync(
        HubConnection connection,
        SignalRRemoteTransportOptions options,
        TransportConnectRequest request,
        Action<SignalRRemoteTransportSession> onDisposed,
        CancellationToken cancellationToken)
    {
        var client = new HttpClient(new SignalRRpcHandler(connection));
        try
        {
            var protocolOptions = options.ConfigureProtocol?.Invoke(client) ?? new HttpRemoteTransportOptions
            { HttpClient = client, BaseAddress = new("https://signalr.invalid/") };
            var adapter = new HttpRemoteTransportAdapter(protocolOptions with { HttpClient = client });
            try
            {
                var session = await adapter.ConnectAsync(request, cancellationToken).ConfigureAwait(false);
                return new(connection, adapter, client, session, onDisposed);
            }
            catch
            {
                await adapter.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>Drains protocol requests before closing the physical connection.</summary>
    /// <returns>The disposal task.</returns>
    private async Task DisposeCoreAsync()
    {
        try
        {
            try
            {
                await _session.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                await _adapter.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _client.Dispose();
            try
            {
                await _connection.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                _onDisposed(this);
            }
        }
    }
}
