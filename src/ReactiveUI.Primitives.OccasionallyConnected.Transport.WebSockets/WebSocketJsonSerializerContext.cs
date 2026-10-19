// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

/// <summary>Provides reflection-free JSON metadata for the WebSocket wire protocol.</summary>
[JsonSerializable(typeof(WebSocketProtocol.Frame))]
[JsonSerializable(typeof(TransportConnectRequest))]
[JsonSerializable(typeof(NegotiatedCapabilities))]
[JsonSerializable(typeof(SyncBatch))]
[JsonSerializable(typeof(RemoteSyncResult))]
[JsonSerializable(typeof(RemoteSubscribeRequest))]
[JsonSerializable(typeof(ReceiveAcknowledgement))]
[JsonSerializable(typeof(WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession.ProtocolError))]
[JsonSerializable(typeof(WebSocketRemoteTransportAdapter.WebSocketRemoteTransportSession.EventEnvelope))]
internal sealed partial class WebSocketJsonSerializerContext : JsonSerializerContext
{
}
