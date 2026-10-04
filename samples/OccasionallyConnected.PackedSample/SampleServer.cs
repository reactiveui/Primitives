// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace OccasionallyConnected.PackedSample;

/// <summary>
/// An in-process sync server: a SQLite <see cref="ServerStreamHub"/> behind an <see cref="HttpServerEndpoint"/>.
/// It starts "down" so clients begin offline, and it can be started later to simulate the network returning.
/// </summary>
internal sealed class SampleServer : IAsyncDisposable
{
    /// <summary>The trusted tenant used by every sample client.</summary>
    internal const string Tenant = "sample-tenant";

    /// <summary>The base address the HTTP transport targets. Requests never leave the process.</summary>
    internal static readonly Uri BaseAddress = new("https://sync.example.test/");

    /// <summary>The shared G-counter stream.</summary>
    internal static readonly StreamId Stream = new("sample/visits");

    private readonly string _databasePath;
    private readonly object _gate = new();
    private readonly SampleCounterLog _requests = new();
    private ServerStreamHub? _hub;
    private HttpServerEndpoint? _endpoint;

    /// <summary>Initializes a new instance of the <see cref="SampleServer"/> class.</summary>
    /// <param name="databasePath">The server SQLite database path.</param>
    internal SampleServer(string databasePath) => _databasePath = databasePath;

    /// <summary>Gets a value indicating whether the server accepts requests.</summary>
    internal bool IsRunning
    {
        get
        {
            lock (_gate)
            {
                return _endpoint is not null;
            }
        }
    }

    /// <summary>Gets the capabilities the server declares to clients.</summary>
    internal static NegotiatedCapabilities Capabilities { get; } = new(
        new Version(1, 0),
        RemoteTransportCapabilities.BatchPush
            | RemoteTransportCapabilities.CursorResume
            | RemoteTransportCapabilities.ReceiveAcknowledgements
            | RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge,
        16,
        256 * 1024,
        TimeSpan.FromHours(1),
        TimeSpan.FromHours(1));

    /// <summary>Starts the hub and the endpoint over the server database.</summary>
    internal void Start()
    {
        lock (_gate)
        {
            if (_endpoint is not null)
            {
                return;
            }

            _hub = ServerStreamHub.CreateSqlite(_databasePath, new ServerStreamHubOptions
            {
                AuthorizationPolicy = TrustedClientAuthorizationPolicy.Instance,
                ConflictHandler = new ServerConflictHandlerOptions
                {
                    Streams = [CrdtServerStreamRegistration.Create(new CrdtServerStreamRegistrationOptions { StreamId = Stream, Kind = CrdtKind.GCounter })],
                },
                EmptyPollDelay = TimeSpan.FromMilliseconds(100),
                TimeProvider = TimeProvider.System,
            });
            _endpoint = new HttpServerEndpoint(new HttpServerEndpointOptions
            {
                Hub = _hub,
                DeclaredCapabilities = Capabilities,
                ReplayAuthorizer = TrustedClientAuthorizationPolicy.Instance,
                LongPollTimeout = TimeSpan.FromMilliseconds(500),
                TimeProvider = TimeProvider.System,
            });
        }
    }

    /// <summary>Creates the HTTP client a sample client uses. It routes requests to this server in process.</summary>
    /// <param name="clientId">The trusted client identifier the host assigns to requests.</param>
    /// <param name="refuseWhenDown">
    /// <see langword="true"/> to fail like a refused TCP connection while the server is down; <see langword="false"/>
    /// to answer 503 Service Unavailable, as a gateway in front of a stopped server does.
    /// </param>
    /// <returns>The HTTP client.</returns>
    internal HttpClient CreateHttpClient(string clientId, bool refuseWhenDown) =>
        new(new InProcessHandler(this, new ServerAuthenticatedClient(Tenant, clientId), refuseWhenDown));

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        HttpServerEndpoint? endpoint;
        ServerStreamHub? hub;
        lock (_gate)
        {
            endpoint = _endpoint;
            hub = _hub;
            _endpoint = null;
            _hub = null;
        }

        if (endpoint is not null)
        {
            await endpoint.DisposeAsync().ConfigureAwait(false);
        }

        if (hub is not null)
        {
            await hub.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Describes the requests the server handled, grouped by client, route and outcome.</summary>
    /// <returns>The request summary.</returns>
    internal string DescribeRequests() =>
        _requests.Describe();

    private void Record(string key) => _requests.Record(key);

    /// <summary>Gets the running endpoint, or <see langword="null"/> while the server is down.</summary>
    /// <returns>The endpoint.</returns>
    private HttpServerEndpoint? GetEndpoint()
    {
        lock (_gate)
        {
            return _endpoint;
        }
    }

    /// <summary>Hands each request to the endpoint with the principal a real host would authenticate.</summary>
    private sealed class InProcessHandler(SampleServer server, ServerAuthenticatedClient client, bool refuseWhenDown) : HttpMessageHandler
    {
        /// <inheritdoc/>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var endpoint = server.GetEndpoint();
            if (endpoint is not null)
            {
                var route = request.RequestUri?.AbsolutePath ?? "?";
                try
                {
                    var response = await endpoint.HandleAsync(request, client, cancellationToken).ConfigureAwait(false);
                    server.Record($"{client.ClientId} {route} {(int)response.StatusCode}");
                    return response;
                }
                catch (Exception exception)
                {
                    server.Record($"{client.ClientId} {route} {exception.GetType().Name}");
                    throw;
                }
            }

            if (refuseWhenDown)
            {
                throw new HttpRequestException("The sample server is not running (connection refused).");
            }

            return new HttpResponseMessage(System.Net.HttpStatusCode.ServiceUnavailable) { RequestMessage = request };
        }
    }
}
