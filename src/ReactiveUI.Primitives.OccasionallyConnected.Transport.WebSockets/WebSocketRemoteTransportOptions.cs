// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

/// <summary>Configures a WebSocket remote transport adapter.</summary>
[DebuggerDisplay("{Endpoint}")]
public sealed record WebSocketRemoteTransportOptions
{
    /// <summary>The smallest accepted message and receive buffer size.</summary>
    private const int MinimumConfiguredBytes = 1024;

    /// <summary>The largest accepted protocol message size.</summary>
    private const int MaximumConfiguredMessageBytes = 64 * 1024 * 1024;

    /// <summary>Gets or initializes the WebSocket endpoint.</summary>
    public required Uri Endpoint { get; init; }

    /// <summary>Gets or initializes the maximum complete protocol message size.</summary>
    public int MaximumMessageBytes { get; init; } = 1_048_576;

    /// <summary>Gets or initializes the receive buffer size.</summary>
    public int ReceiveBufferBytes { get; init; } = 16_384;

    /// <summary>Gets or initializes the maximum queued wire bytes per subscription.</summary>
    public int MaximumBufferedSubscriptionBytes { get; init; } = 4_194_304;

    /// <summary>Validates the options.</summary>
    /// <exception cref="ArgumentException">The endpoint does not use <c>ws</c> or <c>wss</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured size limit is invalid.</exception>
    public void Validate()
    {
        if (Endpoint is null || (Endpoint.Scheme != Uri.UriSchemeWs && Endpoint.Scheme != Uri.UriSchemeWss))
        {
            throw new ArgumentException("The endpoint must use ws or wss.", nameof(Endpoint));
        }

        if (MaximumMessageBytes < MinimumConfiguredBytes || MaximumMessageBytes > MaximumConfiguredMessageBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(MaximumMessageBytes));
        }

        if (ReceiveBufferBytes < MinimumConfiguredBytes || ReceiveBufferBytes > MaximumMessageBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(ReceiveBufferBytes));
        }

        if (MaximumBufferedSubscriptionBytes < MinimumConfiguredBytes || MaximumBufferedSubscriptionBytes > MaximumConfiguredMessageBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(MaximumBufferedSubscriptionBytes));
        }
    }
}
