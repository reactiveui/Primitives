// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies the decision returned by an overflow admission policy.</summary>
internal enum BoundedAdmissionDecisionKind
{
    /// <summary>Rejects the incoming item.</summary>
    Reject = 0,

    /// <summary>Drops queued items from the oldest eligible side.</summary>
    DropOldest = 1,

    /// <summary>Drops the incoming item.</summary>
    DropNewest = 2,

    /// <summary>Waits for admission without evicting existing items.</summary>
    Block = 3,
}
