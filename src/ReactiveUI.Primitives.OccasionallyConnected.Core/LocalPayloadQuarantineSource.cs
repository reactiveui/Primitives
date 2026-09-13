// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies the local record kind that supplied a quarantined payload.</summary>
public enum LocalPayloadQuarantineSource
{
    /// <summary>The payload came from a recovered snapshot.</summary>
    Snapshot = 0,

    /// <summary>The payload came from a recovered outbox operation.</summary>
    OutboxOperation = 1,

    /// <summary>The payload came from a received remote event.</summary>
    RemoteEvent = 2,
}
