// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Reports lifecycle hints from one mobile window or an application-wide host.</summary>
public interface IMobileLifecycle
{
    /// <summary>Occurs when the host asks synchronization to stop.</summary>
    event EventHandler? Suspending;

    /// <summary>Occurs when the host permits synchronization to resume.</summary>
    event EventHandler? Resuming;

    /// <summary>Gets a value indicating whether the host is suspended.</summary>
    bool IsSuspended { get; }
}
