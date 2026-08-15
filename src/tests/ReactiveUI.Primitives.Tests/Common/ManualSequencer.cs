// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// A sequencer that queues scheduled work instead of running it, so a test can decide exactly when a timer
/// fires. <see cref="RunStaleTick"/> re-runs the most recently fired work item, modelling a timer that fires a
/// second time after the operator has already consumed the value it was scheduled for.
/// </summary>
internal sealed class ManualSequencer : ISequencer
{
    /// <summary>The work items scheduled and not yet run.</summary>
    private readonly List<IWorkItem> _pending = [];

    /// <summary>The most recently run work item.</summary>
    private IWorkItem? _lastRun;

    /// <summary>Gets the sequencer's notion of current time.</summary>
    public DateTimeOffset Now { get; private set; } = DateTimeOffset.UnixEpoch;

    /// <summary>Gets the sequencer's monotonic timestamp.</summary>
    public long Timestamp => Now.Ticks;

    /// <summary>Queues a work item.</summary>
    /// <param name="item">The work item to queue.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _pending.Add(item);

    /// <summary>Queues a work item, ignoring its due time; the test decides when it runs.</summary>
    /// <param name="item">The work item to queue.</param>
    /// <param name="dueTimestamp">The due time, which this sequencer does not honor.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "The relative and absolute Schedule overloads of this test-double sequencer intentionally behave the same "
            + "way; both are required by the ISequencer contract and, as distinct interface overloads, cannot forward "
            + "to one another.")]
    public void Schedule(IWorkItem item, long dueTimestamp) => _pending.Add(item);

    /// <summary>Moves the sequencer's clock forward without running any work.</summary>
    /// <param name="time">The amount of time to move forward by.</param>
    internal void Advance(TimeSpan time) => Now += time;

    /// <summary>Runs every work item queued so far.</summary>
    internal void RunPending()
    {
        var items = _pending.ToArray();
        _pending.Clear();
        foreach (var item in items)
        {
            _lastRun = item;
            item.Execute();
        }
    }

    /// <summary>Runs the most recently run work item again, modelling a stale timer tick.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void RunStaleTick() => _lastRun?.Execute();
}
