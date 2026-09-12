// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Schedules isolated observer notification work.</summary>
internal interface IObserverNotificationScheduler
{
    /// <summary>Schedules the supplied work item for asynchronous execution.</summary>
    /// <param name="item">The observer work item.</param>
    void Schedule(IWorkItem item);
}
