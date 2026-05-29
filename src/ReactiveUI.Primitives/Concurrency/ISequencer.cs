// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Represents an object that schedules units of work.
/// </summary>
public interface ISequencer
{
    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    DateTimeOffset Now { get; }

    /// <summary>
    /// Gets the sequencer's monotonic timestamp.
    /// </summary>
    long Timestamp { get; }

    /// <summary>
    /// Schedules a work item for execution as soon as possible.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    void Schedule(IWorkItem item);

    /// <summary>
    /// Schedules a work item for execution at an absolute monotonic timestamp.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    void Schedule(IWorkItem item, long dueTimestamp);
}
