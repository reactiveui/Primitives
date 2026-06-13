// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>A minimal virtual-time sequencer used to exercise scheduling edge branches.</summary>
internal sealed class MinimalVirtualClock : VirtualTimeSequencerBase<long, long>
{
    /// <summary>The scheduled work items keyed by their absolute due time.</summary>
    private readonly SortedDictionary<long, Queue<Scheduled>> _scheduled = [];

    /// <summary>Initializes a new instance of the <see cref="MinimalVirtualClock"/> class.</summary>
    public MinimalVirtualClock()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MinimalVirtualClock"/> class.</summary>
    /// <param name="comparer">The comparer used to order scheduled times.</param>
    public MinimalVirtualClock(IComparer<long> comparer)
        : base(0L, comparer)
    {
    }

    /// <summary>Schedules an action at the specified absolute due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the action.</typeparam>
    /// <param name="state">The state passed to the action when invoked.</param>
    /// <param name="dueTime">The absolute due time at which to run the action.</param>
    /// <param name="action">The action to run when the due time is reached.</param>
    /// <returns>A disposable that cancels the scheduled action.</returns>
    public override IDisposable ScheduleAbsolute<TState>(
        TState state,
        long dueTime,
        Func<ISequencer, TState, IDisposable> action)
    {
        Scheduled scheduled = new(dueTime, () => action(this, state));
        if (!_scheduled.TryGetValue(dueTime, out var queue))
        {
            queue = new();
            _scheduled.Add(dueTime, queue);
        }

        queue.Enqueue(scheduled);
        return new ActionDisposable(() => scheduled.IsCancelled = true);
    }

    /// <summary>Adds a relative offset to an absolute time.</summary>
    /// <param name="absolute">The absolute time.</param>
    /// <param name="relative">The relative offset to add.</param>
    /// <returns>The resulting absolute time.</returns>
    protected override long Add(long absolute, long relative) => absolute + relative;

    /// <summary>Returns the next non-cancelled scheduled item, if any.</summary>
    /// <returns>The next scheduled item, or <see langword="null"/> when none remain.</returns>
    protected override IScheduledItem<long>? GetNext()
    {
        while (_scheduled.Count > 0)
        {
            using var enumerator = _scheduled.GetEnumerator();
            enumerator.MoveNext();
            var first = enumerator.Current;
            var item = first.Value.Dequeue();
            if (first.Value.Count == 0)
            {
                _scheduled.Remove(first.Key);
            }

            if (!item.IsCancelled)
            {
                return item;
            }
        }

        return null;
    }

    /// <summary>Converts an absolute time to a <see cref="DateTimeOffset"/>.</summary>
    /// <param name="absolute">The absolute time to convert.</param>
    /// <returns>The equivalent <see cref="DateTimeOffset"/>.</returns>
    protected override DateTimeOffset ToDateTimeOffset(long absolute) => DateTimeOffset.UnixEpoch.AddTicks(absolute);

    /// <summary>Converts a time span to the relative tick representation.</summary>
    /// <param name="timeSpan">The time span to convert.</param>
    /// <returns>The number of ticks represented by the time span.</returns>
    protected override long ToRelative(TimeSpan timeSpan) => timeSpan.Ticks;

    /// <summary>A single scheduled work item tracked by the virtual clock.</summary>
    private sealed class Scheduled : IScheduledItem<long>
    {
        /// <summary>The action to run when the item is invoked.</summary>
        private readonly Func<IDisposable> _action;

        /// <summary>Initializes a new instance of the <see cref="Scheduled"/> class.</summary>
        /// <param name="dueTime">The absolute due time for the item.</param>
        /// <param name="action">The action to run when invoked.</param>
        public Scheduled(long dueTime, Func<IDisposable> action)
        {
            DueTime = dueTime;
            _action = action;
        }

        /// <summary>Gets the absolute due time for the item.</summary>
        public long DueTime { get; }

        /// <summary>Gets or sets a value indicating whether the item has been cancelled.</summary>
        public bool IsCancelled { get; set; }

        /// <summary>Invokes the scheduled action unless the item has been cancelled.</summary>
        public void Invoke()
        {
            if (IsCancelled)
            {
                return;
            }

            _action().Dispose();
        }
    }
}
