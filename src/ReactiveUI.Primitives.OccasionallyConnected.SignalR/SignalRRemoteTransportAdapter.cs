// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Runs the bounded synchronization protocol over real SignalR connections. The engine owns reconnect.</summary>
[System.Diagnostics.DebuggerDisplay("{Capabilities}")]
public sealed class SignalRRemoteTransportAdapter : IRemoteTransportAdapter
{
    /// <summary>The implemented protocol features. Receive is pull-based and has no unsolicited event queue.</summary>
    private const RemoteTransportCapabilities Features =
        RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge | RemoteTransportCapabilities.SnapshotRecovery;

    /// <summary>The immutable adapter configuration.</summary>
    private readonly SignalRRemoteTransportOptions _options;

    /// <summary>Protects session admission and disposal.</summary>
    private readonly Lock _gate = new();

    /// <summary>The owned live sessions.</summary>
    private readonly HashSet<SignalRRemoteTransportSession> _sessions = [];

    /// <summary>Cancels pending connects.</summary>
    private readonly CancellationTokenSource _shutdown = new();

    /// <summary>Signals that pending connects have unwound.</summary>
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The shared disposal task.</summary>
    private Task? _disposeTask;

    /// <summary>The pending connect count.</summary>
    private int _connecting;

    /// <summary>Initializes a new instance of the <see cref="SignalRRemoteTransportAdapter"/> class.</summary>
    /// <param name="options">The adapter options.</param>
    public SignalRRemoteTransportAdapter(SignalRRemoteTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
    }

    /// <inheritdoc/>
    public RemoteTransportCapabilities Capabilities => Features;

    /// <inheritdoc/>
    public async ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposeTask is not null, this);
            if (_sessions.Count + _connecting >= _options.MaximumSessions)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.Transient);
            }

            _connecting++;
        }

        HubConnection? connection = null;
        SignalRRemoteTransportSession? session = null;
        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdown.Token);
            connection = BuildConnection();
            await connection.StartAsync(linked.Token).ConfigureAwait(false);
            session = await SignalRRemoteTransportSession.CreateAsync(connection, _options, request, RemoveSession, linked.Token).ConfigureAwait(false);
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_disposeTask is not null, this);
                _ = _sessions.Add(session);
            }

            return session;
        }
        catch (Exception exception)
        {
            if (session is not null)
            {
                await session.DisposeAsync().ConfigureAwait(false);
            }
            else if (connection is not null)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }

            if (exception is HttpRequestException requestFailure)
            {
                throw new HttpRemoteTransportException(
                    ClassifyConnectionFailure(requestFailure.StatusCode),
                    requestFailure.StatusCode,
                    retryAfter: null,
                    requestFailure);
            }

            throw;
        }
        finally
        {
            lock (_gate)
            {
                _connecting--;
                if (_connecting == 0 && _disposeTask is not null)
                {
                    _ = _drained.TrySetResult();
                }
            }
        }
    }

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

    /// <summary>Classifies authentication and network failures from SignalR negotiation.</summary>
    /// <param name="status">The optional negotiation status.</param>
    /// <returns>The stable protocol classification.</returns>
    private static HttpTransportFailureKind ClassifyConnectionFailure(HttpStatusCode? status) => status switch
    {
        HttpStatusCode.Unauthorized => HttpTransportFailureKind.Authentication,
        HttpStatusCode.Forbidden => HttpTransportFailureKind.AuthorizationDenied,
        _ => HttpTransportFailureKind.Transient,
    };

    /// <summary>Creates a physical connection without retry or reconnect handlers.</summary>
    /// <returns>The unstarted connection.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HubConnection BuildConnection() => new HubConnectionBuilder()
        .WithUrl(_options.Endpoint, options => _options.ConfigureConnection?.Invoke(options))
        .AddJsonProtocol(static options => options.PayloadSerializerOptions.TypeInfoResolver = SignalRCarrierJsonContext.Default)
        .Build();

    /// <summary>Releases a disposed session from the adapter's ownership set.</summary>
    /// <param name="session">The disposed session.</param>
    private void RemoveSession(SignalRRemoteTransportSession session)
    {
        lock (_gate)
        {
            _ = _sessions.Remove(session);
        }
    }

    /// <summary>Cancels and drains all resources owned by this adapter.</summary>
    /// <returns>The disposal task.</returns>
    private async Task DisposeCoreAsync()
    {
        await _shutdown.CancelAsync().ConfigureAwait(false);
        SignalRRemoteTransportSession[] sessions;
        lock (_gate)
        {
            sessions = [.. _sessions];
            if (_connecting == 0)
            {
                _ = _drained.TrySetResult();
            }
        }

        try
        {
            var tasks = Array.ConvertAll(sessions, static session => session.DisposeAsync().AsTask());
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        finally
        {
            await _drained.Task.ConfigureAwait(false);
            _shutdown.Dispose();
        }
    }
}
