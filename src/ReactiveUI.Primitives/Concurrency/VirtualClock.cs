// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Deterministic virtual scheduler backed by <see cref="DateTimeOffset"/> and <see cref="TimeSpan"/> that also serves as a <see cref="TimeProvider"/>.</summary>
/// <remarks>
/// As a <see cref="TimeProvider"/> the clock reports <see cref="Now"/> as UTC time and counts timestamps in ticks of <see cref="Now"/>.
/// Its timers fire when <see cref="AdvanceBy"/>, <see cref="AdvanceTo"/> or <see cref="Start"/> reaches them.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class VirtualClock : TimeProvider, ISequencer, IServiceProvider, IStopwatchProvider
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

    /// <summary>Gets the number of timestamp ticks per second, which is <see cref="TimeSpan.TicksPerSecond"/>.</summary>
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    /// <summary>Gets the debugger display text.</summary>
    [ExcludeFromCodeCoverage]
    [DebuggerBrowsable(DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Advances the scheduler's clock by the specified relative time, running all work scheduled for that timespan.</summary>
    /// <param name="time">Relative time to advance the scheduler's clock by.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AdvanceBy(TimeSpan time) => _state.AdvanceBy(time);

    /// <summary>Advances the scheduler's clock to the specified time, running all work till that point.</summary>
    /// <param name="time">Absolute time to advance the scheduler's clock to.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AdvanceTo(DateTimeOffset time) => _state.AdvanceTo(time);

    /// <summary>Advances the scheduler's clock by the specified relative time without running work.</summary>
    /// <param name="time">Relative time to advance the scheduler's clock by.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Sleep(TimeSpan time) => _state.Sleep(time);

    /// <summary>Starts the virtual time scheduler.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _state.Start();

    /// <summary>Stops the virtual time scheduler.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Stop() => _state.Stop();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item) => _state.Schedule(this, item);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(this, item, dueTimestamp);

    /// <summary>Schedules an action to be executed at the current clock.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action) =>
        _state.Schedule(this, state, action);

    /// <summary>Schedules an action to be executed after a relative due time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action) =>
        _state.Schedule(this, state, dueTime, action);

    /// <summary>Schedules an action to be executed at an absolute date-time.</summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute date-time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action (best effort).</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "The dueTime type binds this body to a different state overload than the relative one it matches textually.")]
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable ScheduleRelative<TState>(
        TState state,
        TimeSpan dueTime,
        Func<ISequencer, TState, IDisposable> action) => _state.ScheduleRelative(this, state, dueTime, action);

    /// <summary>Starts a new stopwatch object.</summary>
    /// <returns>New stopwatch object; started at the time of the request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IStopwatch StartStopwatch() => new VirtualTimeStopwatch(() => Now, Now);

    /// <summary>Gets the virtual time as a UTC time.</summary>
    /// <returns><see cref="Now"/>.</returns>
    public override DateTimeOffset GetUtcNow() => Now;

    /// <summary>Gets the virtual time as a timestamp counted at <see cref="TimestampFrequency"/>.</summary>
    /// <returns>The ticks of <see cref="Now"/>.</returns>
    public override long GetTimestamp() => Timestamp;

    /// <summary>Creates a timer whose firings run when the virtual clock advances to them.</summary>
    /// <param name="callback">The callback invoked at each firing.</param>
    /// <param name="state">The state passed to <paramref name="callback"/>.</param>
    /// <param name="dueTime">The virtual delay before the first firing, or <see cref="Timeout.InfiniteTimeSpan"/> to leave the timer stopped.</param>
    /// <param name="period">The virtual interval between later firings, or <see cref="Timeout.InfiniteTimeSpan"/> or <see cref="TimeSpan.Zero"/> for a single firing.</param>
    /// <returns>The timer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="callback"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="dueTime"/> or <paramref name="period"/> is negative and not <see cref="Timeout.InfiniteTimeSpan"/>.</exception>
    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
        VirtualClockTimer.Start(this, callback, state, dueTime, period);

    /// <inheritdoc/>
    object? IServiceProvider.GetService(Type serviceType) =>
        serviceType == typeof(IStopwatchProvider) ? this : null;
}
