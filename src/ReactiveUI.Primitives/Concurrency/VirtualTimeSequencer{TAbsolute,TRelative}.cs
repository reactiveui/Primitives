// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Virtual time scheduler that runs scheduled work against a controllable clock. Per-clock arithmetic is supplied
/// as delegates at construction, so a single sealed type serves every <typeparamref name="TAbsolute"/>/
/// <typeparamref name="TRelative"/> pairing without an inheritance hierarchy.
/// </summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
/// <typeparam name="TRelative">Relative time representation type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class VirtualTimeSequencer<TAbsolute, TRelative> : ISequencer, IServiceProvider, IStopwatchProvider
    where TAbsolute : IComparable<TAbsolute>
{
    /// <summary>The virtual-time state and mechanics; see <see cref="VirtualTimeState{TAbsolute, TRelative}"/>.</summary>
    private VirtualTimeState<TAbsolute, TRelative> _state;

    /// <summary>Initializes a new instance of the <see cref="VirtualTimeSequencer{TAbsolute, TRelative}"/> class.</summary>
    /// <param name="initialClock">Initial value for the clock.</param>
    /// <param name="comparer">Comparer to determine causality of events based on absolute time.</param>
    /// <param name="add">Adds a relative time value to an absolute time value.</param>
    /// <param name="toDateTimeOffset">Converts an absolute time value to a <see cref="DateTimeOffset"/>.</param>
    /// <param name="toRelative">Converts a <see cref="TimeSpan"/> to a relative time value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="comparer"/>, <paramref name="add"/>, <paramref name="toDateTimeOffset"/>, or <paramref name="toRelative"/> is <c>null</c>.</exception>
    public VirtualTimeSequencer(
        TAbsolute initialClock,
        IComparer<TAbsolute> comparer,
        Func<TAbsolute, TRelative, TAbsolute> add,
        Func<TAbsolute, DateTimeOffset> toDateTimeOffset,
        Func<TimeSpan, TRelative> toRelative) =>
        _state = new(initialClock, comparer, add, toDateTimeOffset, toRelative);

    /// <summary>Gets the scheduler's absolute time clock value.</summary>
    public TAbsolute Clock => _state.Clock;

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
    public void AdvanceBy(TRelative time) => _state.AdvanceBy(time);

    /// <summary>Advances the scheduler's clock to the specified time, running all work till that point.</summary>
    /// <param name="time">Absolute time to advance the scheduler's clock to.</param>
    public void AdvanceTo(TAbsolute time) => _state.AdvanceTo(time);

    /// <summary>Advances the scheduler's clock by the specified relative time without running work.</summary>
    /// <param name="time">Relative time to advance the scheduler's clock by.</param>
    public void Sleep(TRelative time) => _state.Sleep(time);

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
        TAbsolute dueTime,
        Func<ISequencer, TState, IDisposable> action) => _state.ScheduleAbsolute(this, state, dueTime, action);

    /// <summary>Schedules an action to be executed after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    public IDisposable ScheduleRelative<TState>(
        TState state,
        TRelative dueTime,
        Func<ISequencer, TState, IDisposable> action) => _state.ScheduleRelative(this, state, dueTime, action);

    /// <summary>Starts a new stopwatch object.</summary>
    /// <returns>New stopwatch object; started at the time of the request.</returns>
    public IStopwatch StartStopwatch() => new VirtualTimeStopwatch(() => Now, Now);

    /// <inheritdoc/>
    object? IServiceProvider.GetService(Type serviceType) =>
        serviceType == typeof(IStopwatchProvider) ? this : null;
}
