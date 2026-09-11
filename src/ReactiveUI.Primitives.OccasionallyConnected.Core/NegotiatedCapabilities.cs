// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes capabilities negotiated between the client and remote peer.</summary>
/// <param name="ProtocolVersion">The selected protocol version.</param>
/// <param name="Features">The selected remote features.</param>
/// <param name="MaximumBatchOperations">The maximum operation count in one batch.</param>
/// <param name="MaximumBatchBytes">The maximum operation payload bytes in one batch.</param>
/// <param name="ServerIdempotencyRetention">The server idempotency retention window.</param>
/// <param name="ClientInboxRetentionRequired">The client inbox retention required by the server.</param>
[System.Diagnostics.DebuggerDisplay("{ProtocolVersion,nq}")]
public sealed record NegotiatedCapabilities(
    Version ProtocolVersion,
    RemoteTransportCapabilities Features,
    int MaximumBatchOperations,
    long MaximumBatchBytes,
    TimeSpan? ServerIdempotencyRetention,
    TimeSpan? ClientInboxRetentionRequired);
