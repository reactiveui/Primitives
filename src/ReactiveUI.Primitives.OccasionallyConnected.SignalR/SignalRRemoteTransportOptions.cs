// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.Http.Connections.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>Configures a SignalR connection without automatic or stateful reconnect.</summary>
[System.Diagnostics.DebuggerDisplay("{Endpoint}")]
public sealed record SignalRRemoteTransportOptions
{
    /// <summary>The default physical session capacity.</summary>
    private const int DefaultMaximumSessions = 8;

    /// <summary>Gets the HTTPS hub address.</summary>
    public required Uri Endpoint { get; init; }

    /// <summary>Gets whether explicit loopback HTTP is allowed for local testing.</summary>
    public bool AllowInsecureLoopbackHttp { get; init; }

    /// <summary>Gets the maximum concurrently owned physical sessions, including pending connections.</summary>
    public int MaximumSessions { get; init; } = DefaultMaximumSessions;

    /// <summary>Gets the host callback for credentials, headers, or HTTP transport selection.</summary>
    public Action<HttpConnectionOptions>? ConfigureConnection { get; init; }

    /// <summary>Gets the optional factory for protocol limits. Its HTTP client is adapter-owned and sends only SignalR RPCs.</summary>
    public Func<HttpClient, HttpRemoteTransportOptions>? ConfigureProtocol { get; init; }

    /// <summary>Validates the endpoint.</summary>
    /// <exception cref="ArgumentException">The endpoint is not a secure absolute hub URI.</exception>
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(Endpoint);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaximumSessions);
        if (!Endpoint.IsAbsoluteUri
            || !string.IsNullOrEmpty(Endpoint.UserInfo)
            || !string.IsNullOrEmpty(Endpoint.Fragment)
            || (Endpoint.Scheme != Uri.UriSchemeHttps
                && !(AllowInsecureLoopbackHttp && Endpoint.IsLoopback && Endpoint.Scheme == Uri.UriSchemeHttp)))
        {
            throw new ArgumentException("SignalR requires HTTPS or explicitly enabled loopback HTTP.", nameof(Endpoint));
        }
    }
}
