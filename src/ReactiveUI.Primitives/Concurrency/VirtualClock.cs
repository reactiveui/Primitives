// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Deterministic virtual scheduler backed by <see cref="DateTimeOffset"/> and <see cref="TimeSpan"/>.</summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class VirtualClock : ISequencer, IServiceProvider, IStopwatchProvider
{
    /// <summary>Adds a normalized relative time to an absolute time.</summary>
    private static readonly Func<DateTimeOffset, TimeSpan, DateTimeOffset> Adder = static (absolute, relative) =>
        absolute + Sequencer.Normalize(relative);

    /// <summary>Identity conversion from the absolute clock to a <see cref="DateTimeOffset"/>.</summary>
    private static readonly Func<DateTimeOffset, DateTimeOffset> Identity = static absolute => absolute;

    /// <summary>Normalizes a <see cref="TimeSpan"/> to the relative time representation.</summary>
    private static readonly Func<TimeSpan, TimeSpan> Normalizer = Sequencer.Normalize;

    /// <summary>The virtual-time state and mechanics; see <see cref="VirtualTimeState{TAbsolute, TRelative}"/>.</summary>
    private VirtualTimeState<DateTimeOffset, TimeSpan> _state;

    /// <summary>Initializes a new instance of the <see cref="VirtualClock"/> class at the default clock value.</summary>
    public VirtualClock()
        : this(default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="VirtualClock"/> class.</summary>
    /// <param name="initialClock">Initial virtual time.</param>
    public VirtualClock(DateTimeOffset initialClock) =>
        _state = new(initialClock, Comparer<DateTimeOffset>.Default, Adder, Identity, Normalizer);

    /// <summary>Gets the scheduler's absolute time clock value.</summary>
    public DateTimeOffset Clock => _state.Clock;

    /// <summary>Gets a value indicating whether the scheduler is enabled to run work.</summary>
    public bool IsEnabled => _state.IsEnabled;

    /// <inheritdoc/>
    public DateTimeOffset Now => _state.Now;

    /// <inheritdoc/>
    public long Timestamp => _state.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Advances the scheduler's clock by the specified relative time, running all work scheduled for that timespan.</summary>
    /// <param name="time">Relative time to advance the scheduler's clock by.</param>
    public void AdvanceBy(TimeSpan time) => _state.AdvanceBy(time);

    /// <summary>Advances the scheduler's clock to the specified time, running all work till that point.</summary>
    /// <param name="time">Absolute time to advance the scheduler's clock to.</param>
    public void AdvanceTo(DateTimeOffset time) => _state.AdvanceTo(time);

    /// <summary>Advances the scheduler's clock by the specified relative time without running work.</summary>
    /// <param name="time">Relative time to advance the scheduler's clock by.</param>
    public void Sleep(TimeSpan time) => _state.Sleep(time);

    /// <summary>Starts the virtual time scheduler.</summary>
    public void Start() => _state.Start();

    /// <summary>Stops the virtual time scheduler.</summary>
    public void Stop() => _state.Stop();

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) => _state.Schedule(this, item);

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(this, item, dueTimestamp);

    /// <summary>Schedules an action to be executed at the current clock.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action) =>
        _state.Schedule(this, state, action);

    /// <summary>Schedules an action to be executed after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action) =>
        _state.Schedule(this, state, dueTime, action);

    /// <summary>Schedules an action to be executed at an absolute date-time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute date-time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    public IDisposable Schedule<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<ISequencer, TState, IDisposable> action) => _state.Schedule(this, state, dueTime, action);

    /// <summary>Schedules an action to be executed at an absolute due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    public IDisposable ScheduleAbsolute<TState>(
        TState state,
        DateTimeOffset dueTime,
        Func<ISequencer, TState, IDisposable> action) => _state.ScheduleAbsolute(this, state, dueTime, action);

    /// <summary>Schedules an action to be executed after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    public IDisposable ScheduleRelative<TState>(
        TState state,
        TimeSpan dueTime,
        Func<ISequencer, TState, IDisposable> action) => _state.ScheduleRelative(this, state, dueTime, action);

    /// <summary>Starts a new stopwatch object.</summary>
    /// <returns>New stopwatch object; started at the time of the request.</returns>
    public IStopwatch StartStopwatch() => new VirtualTimeStopwatch(() => Now, Now);

    /// <inheritdoc/>
    object? IServiceProvider.GetService(Type serviceType) =>
        serviceType == typeof(IStopwatchProvider) ? this : null;
}
