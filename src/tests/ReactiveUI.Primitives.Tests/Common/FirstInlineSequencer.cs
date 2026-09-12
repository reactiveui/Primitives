// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Runs the first item inline and queues subsequent work. Clock advancement and queue draining are explicit.</summary>
/// <param name="advanceBeforeFirst">The amount the clock moves forward before the first item runs.</param>
[System.Diagnostics.DebuggerDisplay("FirstInlineSequencer: Now = {Now}, Started = {_started}, Pending = {_pending.Count}")]
public sealed class FirstInlineSequencer(TimeSpan advanceBeforeFirst) : ISequencer
{
    /// <summary>The work items queued after the first, still waiting to run.</summary>
    private readonly List<IWorkItem> _pending = [];

    /// <summary>Whether the inline first item has already run.</summary>
    private bool _started;

    /// <summary>Gets the sequencer's notion of current time.</summary>
    public DateTimeOffset Now { get; private set; } = DateTimeOffset.UnixEpoch;

    /// <summary>Gets the sequencer's monotonic timestamp.</summary>
    public long Timestamp => Now.Ticks;

    /// <summary>Runs the first work item inline and queues every later one.</summary>
    /// <param name="item">The work item to run or queue.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => Schedule(item, Timestamp);

    /// <summary>Runs the first work item inline and queues every later one, ignoring the due time.</summary>
    /// <param name="item">The work item to run or queue.</param>
    /// <param name="dueTimestamp">The due time, which this sequencer does not honor.</param>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        if (_started)
        {
            _pending.Add(item);
            return;
        }

        _started = true;
        Advance(advanceBeforeFirst);
        item.Execute();
    }

    /// <summary>Moves the sequencer's clock forward without running any work.</summary>
    /// <param name="time">The amount of time to move forward by.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(TimeSpan time) => Now += time;

    /// <summary>Runs every work item queued so far, leaving anything they queue for the next call.</summary>
    public void RunPending()
    {
        var items = _pending.ToArray();
        _pending.Clear();
        foreach (var item in items)
        {
            item.Execute();
        }
    }
}
