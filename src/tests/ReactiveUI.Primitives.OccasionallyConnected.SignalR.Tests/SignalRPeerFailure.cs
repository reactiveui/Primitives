// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.SignalR.Tests;

/// <summary>Faults injected by a real SignalR protocol peer.</summary>
public enum SignalRPeerFailure
{
    /// <summary>No fault.</summary>
    None = 0,

    /// <summary>Missing response.</summary>
    EmptyResponse = 1,

    /// <summary>A hub failure.</summary>
    HubFailure = 2,

    /// <summary>Malformed carrier JSON.</summary>
    MalformedBody = 3,

    /// <summary>Invalid response status.</summary>
    InvalidStatus = 4,

    /// <summary>Request fields appear in a response.</summary>
    RequestAsResponse = 5,
}
