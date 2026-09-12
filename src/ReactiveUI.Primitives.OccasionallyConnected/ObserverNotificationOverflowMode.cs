// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies how a subscription handles notification queue overflow.</summary>
internal enum ObserverNotificationOverflowMode
{
    /// <summary>Replaces queued state notifications with the newest state notification.</summary>
    CoalesceLatest = 0,

    /// <summary>Disconnects the observer with a typed overflow error.</summary>
    Disconnect = 1,
}
