// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Mutable state and mechanics backing the virtual-time sequencers. A single sequencer owns one of these inline
/// and forwards its public surface here, so the virtual-time logic lives in one place without inheritance or
/// composition between the sequencer types. Per-clock arithmetic is supplied as delegates rather than overrides.
/// </summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
/// <typeparam name="TRelative">Relative time representation type.</typeparam>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Performance",
    "SST1803:Make record struct readonly",
    Justification = "This is mutable scheduler state; its members mutate the clock and running flag in place, so it cannot be readonly.")]
internal record struct VirtualTimeState<TAbsolute, TRelative>
    where TAbsolute : IComparable<TAbsolute>
{
    /// <summary>Thread-safe queue of scheduled virtual-time work.</summary>
    private readonly SynchronizedSequencerQueue<TAbsolute> _queue;

    /// <summary>Comparer used to order absolute time values.</summary>
    private readonly IComparer<TAbsolute> _comparer;

    /// <summary>Adds a relative time value to an absolute time value.</summary>
    private readonly Func<TAbsolute, TRelative, TAbsolute> _add;

    /// <summary>Converts an absolute time value to a <see cref="DateTimeOffset"/>.</summary>
    private readonly Func<TAbsolute, DateTimeOffset> _toDateTimeOffset;

    /// <summary>Converts a <see cref="TimeSpan"/> to a relative time value.</summary>
    private readonly Func<TimeSpan, TRelative> _toRelative;

    /// <summary>The current absolute clock value.</summary>
    private TAbsolute _clock;

    /// <summary>Whether the scheduler is running work.</summary>
    private bool _isEnabled;

    /// <summary>Initializes a new instance of the <see cref="VirtualTimeState{TAbsolute, TRelative}"/> struct.</summary>
    /// <param name="initialClock">Initial value for the clock.</param>
    /// <param name="comparer">Comparer to determine causality of events based on absolute time.</param>
    /// <param name="add">Adds a relative time value to an absolute time value.</param>
    /// <param name="toDateTimeOffset">Converts an absolute time value to a <see cref="DateTimeOffset"/>.</param>
    /// <param name="toRelative">Converts a <see cref="TimeSpan"/> to a relative time value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="comparer"/>, <paramref name="add"/>, <paramref name="toDateTimeOffset"/>, or <paramref name="toRelative"/> is <c>null</c>.</exception>
    public VirtualTimeState(
        TAbsolute initialClock,
        IComparer<TAbsolute> comparer,
        Func<TAbsolute, TRelative, TAbsolute> add,
        Func<TAbsolute, DateTimeOffset> toDateTimeOffset,
        Func<TimeSpan, TRelative> toRelative)
    {
        _queue = new();
        _clock = initialClock;
        _comparer = comparer ?? throw new ArgumentNullException(nameof(comparer));
        _add = add ?? throw new ArgumentNullException(nameof(add));
        _toDateTimeOffset = toDateTimeOffset ?? throw new ArgumentNullException(nameof(toDateTimeOffset));
        _toRelative = toRelative ?? throw new ArgumentNullException(nameof(toRelative));
    }

    /// <summary>Gets the scheduler's absolute time clock value.</summary>
    public readonly TAbsolute Clock => _clock;

    /// <summary>Gets a value indicating whether the scheduler is enabled to run work.</summary>
    public readonly bool IsEnabled => _isEnabled;

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public readonly DateTimeOffset Now => _toDateTimeOffset(_clock);

    /// <summary>Gets the virtual clock as a monotonic timestamp.</summary>
    public readonly long Timestamp => Now.UtcTicks;

    /// <summary>Advances the scheduler's clock by the specified relative time, running all work scheduled for that timespan.</summary>
    /// <param name="time">Relative time to advance the scheduler's clock by.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="time"/> is negative.</exception>
    /// <exception cref="InvalidOperationException">The scheduler is already running.</exception>
    public void AdvanceBy(TRelative time)
    {
        var dt = _add(_clock, time);

        var dueToClock = _comparer.Compare(dt, _clock);
        if (dueToClock < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(time));
        }

        if (dueToClock == 0)
        {
            return;
        }

        if (_isEnabled)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "{0} cannot be called when the scheduler is already running. Try using Sleep instead.", nameof(AdvanceBy)));
        }

        AdvanceTo(dt);
    }

    /// <summary>Advances the scheduler's clock to the specified time, running all work till that point.</summary>
    /// <param name="time">Absolute time to advance the scheduler's clock to.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="time"/> is in the past.</exception>
    /// <exception cref="InvalidOperationException">The scheduler is already running.</exception>
    public void AdvanceTo(TAbsolute time)
    {
        var dueToClock = _comparer.Compare(time, _clock);
        if (dueToClock < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(time));
        }

        if (dueToClock == 0)
        {
            return;
        }

        if (_isEnabled)
        {
            throw new InvalidOperationException(string.Format(CultureInfo.CurrentCulture, "{0} cannot be called when the scheduler is already running. Try using Sleep instead.", nameof(AdvanceTo)));
        }

        _isEnabled = true;
        do
        {
            var next = GetNext();
            if (next is not null && _comparer.Compare(next.DueTime, time) <= 0)
            {
                if (_comparer.Compare(next.DueTime, _clock) > 0)
                {
                    _clock = next.DueTime;
                }

                next.Invoke();
            }
            else
            {
                _isEnabled = false;
            }
        }
        while (_isEnabled);

        _clock = time;
    }

    /// <summary>Advances the scheduler's clock by the specified relative time without running work.</summary>
    /// <param name="time">Relative time to advance the scheduler's clock by.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="time"/> is negative.</exception>
    public void Sleep(TRelative time)
    {
        var dt = _add(_clock, time);

        var dueToClock = _comparer.Compare(dt, _clock);
        if (dueToClock < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(time));
        }

        _clock = dt;
    }

    /// <summary>Starts the virtual time scheduler, running all scheduled work.</summary>
    public void Start()
    {
        if (_isEnabled)
        {
            return;
        }

        _isEnabled = true;
        do
        {
            var next = GetNext();
            if (next is not null)
            {
                if (_comparer.Compare(next.DueTime, _clock) > 0)
                {
                    _clock = next.DueTime;
                }

                next.Invoke();
            }
            else
            {
                _isEnabled = false;
            }
        }
        while (_isEnabled);
    }

    /// <summary>Stops the virtual time scheduler.</summary>
    public void Stop() => _isEnabled = false;

    /// <summary>Schedules an action to be executed at the current clock.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="owner">The sequencer passed to the scheduled action.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <c>null</c>.</exception>
    public readonly IDisposable Schedule<TState>(ISequencer owner, TState state, Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        return ScheduleAbsolute(owner, state, _clock, action);
    }

    /// <summary>Schedules an action to be executed after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="owner">The sequencer passed to the scheduled action.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <c>null</c>.</exception>
    public readonly IDisposable Schedule<TState>(ISequencer owner, TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        return ScheduleRelative(owner, state, _toRelative(dueTime), action);
    }

    /// <summary>Schedules an action to be executed at an absolute date-time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="owner">The sequencer passed to the scheduled action.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute date-time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <c>null</c>.</exception>
    public readonly IDisposable Schedule<TState>(ISequencer owner, TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        return ScheduleRelative(owner, state, _toRelative(dueTime - Now), action);
    }

    /// <summary>Schedules a work item to be executed at the current virtual clock.</summary>
    /// <param name="owner">The sequencer passed to the scheduled action.</param>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public readonly void Schedule(ISequencer owner, IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        ScheduleAbsolute(owner, item, _clock, static (_, workItem) =>
        {
            if (!Sequencer.IsCancelled(workItem))
            {
                workItem.Execute();
            }

            return EmptyDisposable.Instance;
        });
    }

    /// <summary>Schedules a work item to be executed at a sequencer timestamp.</summary>
    /// <param name="owner">The sequencer passed to the scheduled action.</param>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute sequencer timestamp.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public readonly void Schedule(ISequencer owner, IWorkItem item, long dueTimestamp)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        var delta = Sequencer.ToTimeSpanDelta(dueTimestamp - Timestamp);
        ScheduleRelative(owner, item, _toRelative(delta), static (_, workItem) =>
        {
            if (!Sequencer.IsCancelled(workItem))
            {
                workItem.Execute();
            }

            return EmptyDisposable.Instance;
        });
    }

    /// <summary>Schedules an action to be executed at an absolute due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="owner">The sequencer passed to the scheduled action.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <c>null</c>.</exception>
    public readonly IDisposable ScheduleAbsolute<TState>(ISequencer owner, TState state, TAbsolute dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        // Copy the reference-typed queue into a local so the self-removal closure synchronizes through that
        // reference rather than capturing the enclosing struct's "this" (not permitted for struct members).
        var queue = _queue;

        ScheduledItem<TAbsolute> si = new(dueTime, _comparer, self =>
        {
            queue.Remove(self);
            return action(owner, state);
        });

        _queue.Enqueue(si);

        return si;
    }

    /// <summary>Schedules an action to be executed after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="owner">The sequencer passed to the scheduled action.</param>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <c>null</c>.</exception>
    public readonly IDisposable ScheduleRelative<TState>(ISequencer owner, TState state, TRelative dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(action);

        var runAt = _add(_clock, dueTime);

        return ScheduleAbsolute(owner, state, runAt, action);
    }

    /// <summary>Gets the next non-cancelled scheduled item to be executed, leaving it on the queue.</summary>
    /// <returns>The next scheduled item, or <see langword="null"/> when none remain.</returns>
    public readonly IScheduledItem<TAbsolute>? GetNext() => _queue.GetNextLive();
}
