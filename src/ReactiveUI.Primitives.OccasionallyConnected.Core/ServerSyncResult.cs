// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a server synchronization result and any produced events.</summary>
[System.Diagnostics.DebuggerDisplay("{Result,nq}")]
public sealed record ServerSyncResult
{
    /// <summary>Initializes a new instance of the <see cref="ServerSyncResult"/> class.</summary>
    /// <param name="result">The remote synchronization result.</param>
    /// <param name="producedEvents">The events produced while applying operations.</param>
    public ServerSyncResult(RemoteSyncResult result, IReadOnlyList<RemoteEvent> producedEvents)
    {
        Result = result;
        ProducedEvents = CollectionCopy.List(producedEvents);
    }

    /// <summary>Gets the remote synchronization result.</summary>
    public RemoteSyncResult Result { get; }

    /// <summary>Gets the events produced while applying operations.</summary>
    public IReadOnlyList<RemoteEvent> ProducedEvents { get; }
}
