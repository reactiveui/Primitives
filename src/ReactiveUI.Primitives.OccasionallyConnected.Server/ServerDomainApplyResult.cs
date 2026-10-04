// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Contains canonical state and event proposals produced by a domain handler.</summary>
[System.Diagnostics.DebuggerDisplay("{NewState.Version,nq} {Events.Count}")]
public sealed record ServerDomainApplyResult
{
    /// <summary>Gets the new canonical server state.</summary>
    public required ServerState NewState { get; init; }

    /// <summary>Gets the event proposals to stamp after durable admission.</summary>
    public IReadOnlyList<ServerProducedEvent> Events
    {
        get;
        init => field = ServerCollectionCopy.List(value, nameof(Events));
    } = [];
}
