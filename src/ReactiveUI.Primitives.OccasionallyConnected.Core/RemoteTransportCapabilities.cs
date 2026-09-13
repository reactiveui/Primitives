// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies remote transport capabilities that may be advertised after conformance testing.</summary>
[Flags]
public enum RemoteTransportCapabilities
{
    /// <summary>No optional transport capabilities are available.</summary>
    None = 0,

    /// <summary>The transport can push multiple operations in one request.</summary>
    BatchPush = 1 << 0,

    /// <summary>The transport can resume a subscription from an opaque cursor.</summary>
    CursorResume = 1 << 1,

    /// <summary>The transport supports durable receive acknowledgements.</summary>
    ReceiveAcknowledgements = 1 << 2,

    /// <summary>The remote peer supports operation idempotency.</summary>
    ServerIdempotency = 1 << 3,

    /// <summary>The remote peer atomically applies effects and records acknowledgements.</summary>
    AtomicApplyAndAcknowledge = 1 << 4,

    /// <summary>The transport supports streaming receive.</summary>
    StreamingReceive = 1 << 5,

    /// <summary>The remote peer can recover a retained-history gap with a bounded snapshot checkpoint.</summary>
    SnapshotRecovery = 1 << 6,
}
