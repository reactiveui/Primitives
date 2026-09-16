// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>The progress of the terminal notification in a <see cref="CurrentValueDelivery{T}"/>.</summary>
internal enum CurrentValueTerminalState
{
    /// <summary>No terminal notification has been requested.</summary>
    None = 0,

    /// <summary>A terminal notification is being recorded and cannot be delivered yet.</summary>
    Recording = 1,

    /// <summary>A terminal notification has been recorded and not yet delivered.</summary>
    Requested = 2,

    /// <summary>The terminal notification has been delivered.</summary>
    Delivered = 3,
}
