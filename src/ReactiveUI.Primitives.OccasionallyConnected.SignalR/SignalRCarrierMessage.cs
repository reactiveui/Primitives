// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR;

/// <summary>The source-generated RPC carrier.</summary>
/// <param name="Method">The protocol request verb, absent in a response.</param>
/// <param name="PathAndQuery">The protocol request route, absent in a response.</param>
/// <param name="StatusCode">The protocol response status.</param>
/// <param name="Headers">The bounded protocol headers, including replay proofs.</param>
/// <param name="Body">The bounded source-generated protocol body.</param>
internal sealed record SignalRCarrierMessage(
    string? Method,
    string? PathAndQuery,
    int StatusCode,
    Dictionary<string, string[]> Headers,
    byte[] Body);
