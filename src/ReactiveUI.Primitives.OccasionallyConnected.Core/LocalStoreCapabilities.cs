// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies local store capabilities that may be advertised after conformance testing.</summary>
[Flags]
public enum LocalStoreCapabilities
{
    /// <summary>No optional store capabilities are available.</summary>
    None = 0,

    /// <summary>The store can atomically commit a local operation and optimistic snapshot mutation.</summary>
    AtomicLocalCommit = 1 << 0,

    /// <summary>The store can atomically deduplicate remote events, update snapshot state, and advance cursors.</summary>
    AtomicRemoteApply = 1 << 1,

    /// <summary>The store keeps durable inbox deduplication records.</summary>
    DurableInbox = 1 << 2,

    /// <summary>The store supports leased outbox ownership.</summary>
    LeasedOutbox = 1 << 3,

    /// <summary>The store coordinates safe access across processes.</summary>
    MultiProcessCoordination = 1 << 4,

    /// <summary>The store supports authenticated encryption at rest.</summary>
    AuthenticatedEncryptionAtRest = 1 << 5,

    /// <summary>The store persists acknowledged local operation and snapshot commits across process restarts.</summary>
    DurableLocalCommit = 1 << 6,

    /// <summary>The store binds initialized local partitions to a client identity.</summary>
    ClientIdentityBinding = 1 << 7,
}
