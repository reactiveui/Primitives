// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Reports the per-publication observer dispatch outcome.</summary>
internal enum ObserverNotificationPublishResult
{
    /// <summary>The dispatcher or subscription has already reached a terminal state.</summary>
    Stopped = 0,

    /// <summary>The notification was queued for at least one observer.</summary>
    Queued = 1,

    /// <summary>The notification replaced queued state for at least one observer.</summary>
    Coalesced = 2,

    /// <summary>At least one observer was disconnected by the publication.</summary>
    Disconnected = 3,

    /// <summary>The scheduler rejected the drain work, but the subscription can reschedule later.</summary>
    SchedulerRejected = 4,
}
