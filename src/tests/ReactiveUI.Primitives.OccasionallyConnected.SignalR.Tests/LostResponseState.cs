// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Injects one lost committed acknowledgement at the real SignalR boundary.</summary>
internal sealed class LostResponseState
{
    /// <summary>Gets or sets whether the first push response must be lost.</summary>
    internal bool DropPushResponse { get; set; }

    /// <summary>Gets or sets a malicious peer response for protocol-negative tests.</summary>
    internal SignalRPeerFailure PeerFailure { get; set; }
}
