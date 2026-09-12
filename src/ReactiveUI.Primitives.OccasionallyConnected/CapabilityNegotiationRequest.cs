// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the requirements and offers used to negotiate one stream.</summary>
internal sealed record CapabilityNegotiationRequest
{
    /// <summary>Gets the validated context configuration.</summary>
    internal required OccasionallyConnectedOptions Options { get; init; }

    /// <summary>Gets the operation delivery requirements.</summary>
    internal required OperationPolicy Policy { get; init; }

    /// <summary>Gets the local store's advertised capabilities.</summary>
    internal required LocalStoreCapabilities StoreCapabilities { get; init; }

    /// <summary>Gets the transport adapter's advertised capabilities.</summary>
    internal required RemoteTransportCapabilities TransportCapabilities { get; init; }

    /// <summary>Gets the authenticated peer's capability offer.</summary>
    internal required NegotiatedCapabilities PeerOffer { get; init; }

    /// <summary>Gets whether this stream requires durable cursor recovery.</summary>
    internal bool RequiresCursorResume { get; init; }

    /// <summary>Gets whether multiple workers may drain the outbox concurrently.</summary>
    internal bool RequiresConcurrentDrain { get; init; }

    /// <summary>Gets whether the store must coordinate multiple writer processes.</summary>
    internal bool RequiresMultipleProcesses { get; init; }
}
