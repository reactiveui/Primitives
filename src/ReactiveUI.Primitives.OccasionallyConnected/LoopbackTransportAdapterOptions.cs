// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures the in-process loopback transport adapter.</summary>
[DebuggerDisplay("Loopback; Client={AuthenticatedClient,nq}; Features={PeerCapabilities.Features,nq}")]
public sealed record LoopbackTransportAdapterOptions
{
    /// <summary>The default maximum logical encoded batch bytes accepted by the loopback adapter.</summary>
    private const long DefaultMaximumLogicalBatchBytes = 64L * 1024L * 1024L;

    /// <summary>Gets the server hub supplied by the trusted host.</summary>
    public required IServerStreamHub Hub { get; init; }

    /// <summary>Gets the client identity authenticated by the trusted host.</summary>
    public required ServerAuthenticatedClient AuthenticatedClient { get; init; }

    /// <summary>Gets the peer capabilities authenticated by the trusted host.</summary>
    public required NegotiatedCapabilities PeerCapabilities { get; init; }

    /// <summary>Gets the maximum number of concurrent push requests admitted per session.</summary>
    public int MaximumConcurrentRequests { get; init; } = 8;

    /// <summary>Gets the maximum number of concurrent acknowledgement requests admitted per session.</summary>
    public int MaximumConcurrentAcknowledgements { get; init; } = 1;

    /// <summary>Gets the maximum number of concurrent subscriptions admitted per session.</summary>
    public int MaximumConcurrentSubscriptions { get; init; } = 4;

    /// <summary>Gets the maximum number of events accepted in one received batch.</summary>
    public int MaximumReceiveEvents { get; init; } = 1024;

    /// <summary>Gets the maximum logical encoded bytes accepted for one loopback batch.</summary>
    /// <remarks>
    /// Loopback counts UTF-8 string and payload bytes plus fixed-width identifiers, timestamps, policies,
    /// collection counts and nullable markers. This logical representation excludes managed object overhead.
    /// The negotiated batch limit is applied to the same complete representation.
    /// </remarks>
    public long MaximumLogicalBatchBytes { get; init; } = DefaultMaximumLogicalBatchBytes;

    /// <summary>Gets the maximum number of completed operation declarations accepted in one received batch.</summary>
    public int MaximumCompletedOperations { get; init; } = 1024;

    /// <summary>Gets the maximum number of metadata entries accepted on one operation or event.</summary>
    public int MaximumMetadataEntries { get; init; } = 32;

    /// <summary>Gets the maximum strict UTF-8 byte count accepted for one protocol string.</summary>
    public int MaximumStringBytes { get; init; } = 4096;
}
