// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies the synchronization engine lifecycle state.</summary>
public enum SyncLifecycleStatus
{
    /// <summary>The engine has been constructed but not initialized.</summary>
    Created = 0,

    /// <summary>The engine is initializing local state and capabilities.</summary>
    Initializing = 1,

    /// <summary>The engine is locally available but has no usable network path.</summary>
    Offline = 2,

    /// <summary>The engine is opening a remote connection.</summary>
    Connecting = 3,

    /// <summary>The engine is exchanging pending work or remote events.</summary>
    Synchronizing = 4,

    /// <summary>The engine is connected and synchronized.</summary>
    Online = 5,

    /// <summary>The engine is running with reduced capability.</summary>
    Degraded = 6,

    /// <summary>The engine is stopping.</summary>
    Stopping = 7,

    /// <summary>The engine has stopped.</summary>
    Stopped = 8,

    /// <summary>The engine has reached a terminal local fault.</summary>
    Faulted = 9,
}
