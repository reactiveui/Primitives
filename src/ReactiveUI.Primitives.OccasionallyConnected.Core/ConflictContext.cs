// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides the ordered operation batch and canonical state to a conflict resolver.</summary>
[System.Diagnostics.DebuggerDisplay("{Current.StreamId,nq} {Incoming.Count}")]
public sealed record ConflictContext
{
    /// <summary>Initializes a new instance of the <see cref="ConflictContext"/> class.</summary>
    /// <param name="current">The current canonical server state.</param>
    /// <param name="incoming">The client operations in ascending client-sequence order.</param>
    /// <param name="client">The identity bound to the authenticated caller.</param>
    public ConflictContext(ServerState current, IReadOnlyList<SyncOperation> incoming, ClientIdentity client)
    {
        Current = current;
        Incoming = CollectionCopy.List(incoming);
        Client = client;
    }

    /// <summary>Gets the canonical state observed by the server transaction.</summary>
    public ServerState Current { get; }

    /// <summary>Gets the immutable ordered operation batch.</summary>
    public IReadOnlyList<SyncOperation> Incoming { get; }

    /// <summary>Gets the authenticated client identity.</summary>
    public ClientIdentity Client { get; }
}
