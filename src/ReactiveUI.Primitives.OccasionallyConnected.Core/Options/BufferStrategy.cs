// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies how a bounded queue admits work when it reaches capacity.</summary>
public enum BufferStrategy
{
    /// <summary>Drops the oldest queued item to admit the new item.</summary>
    DropOldest = 0,

    /// <summary>Drops the new item and keeps the existing queue contents.</summary>
    DropNewest = 1,

    /// <summary>Waits asynchronously until capacity is available.</summary>
    Block = 2,

    /// <summary>Rejects the new item immediately.</summary>
    Reject = 3,

    /// <summary>Uses an explicitly registered caller-supplied policy.</summary>
    Custom = 4,
}
