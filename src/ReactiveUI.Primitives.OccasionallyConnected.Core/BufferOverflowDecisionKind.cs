// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies the outcome selected by a buffer overflow policy.</summary>
public enum BufferOverflowDecisionKind
{
    /// <summary>Waits for outbox capacity before retrying admission.</summary>
    Block = 0,

    /// <summary>Rejects the incoming publication.</summary>
    Reject = 1,

    /// <summary>Evicts one listed non-durable candidate and retries admission.</summary>
    Evict = 2,
}
