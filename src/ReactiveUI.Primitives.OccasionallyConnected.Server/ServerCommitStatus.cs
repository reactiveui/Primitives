// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes the result of a server commit journal compare-and-swap attempt.</summary>
internal enum ServerCommitStatus
{
    /// <summary>The prepared commit was durably admitted.</summary>
    Committed = 0,

    /// <summary>The stream revision changed before the prepared commit reached the journal.</summary>
    StaleRevision = 1,

    /// <summary>An operation key already exists with a different canonical fingerprint.</summary>
    IntentMismatch = 2,

    /// <summary>The prepared commit would exceed retained in-memory journal capacity.</summary>
    CapacityExceeded = 3,

    /// <summary>The stream revision cannot advance without overflowing.</summary>
    RevisionOverflow = 4,

    /// <summary>The event sequence cannot advance without overflowing.</summary>
    EventSequenceOverflow = 5,
}
