// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>A timer whose firings are queued on a <see cref="VirtualClock"/> and run when the clock advances.</summary>
internal sealed class VirtualClockTimer : ITimer
{
    /// <summary>Serializes changes to the pending firing.</summary>
    private readonly Lock _gate = new();

    /// <summary>The clock whose virtual time schedules the firings.</summary>
    private readonly VirtualClock _clock;

    /// <summary>The callback invoked at each firing.</summary>
    private readonly TimerCallback _callback;

    /// <summary>The state passed to the callback.</summary>
    private readonly object? _state;

    /// <summary>The queued firing that has not run yet, or <see langword="null"/> when none is queued.</summary>
    private IDisposable? _pending;

    /// <summary>The interval between firings after the first, or <see cref="TimeSpan.Zero"/> for a one-shot timer.</summary>
    private TimeSpan _period;

    /// <summary>Whether the timer has been disposed.</summary>
    private bool _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="VirtualClockTimer"/> class.</summary>
    /// <param name="clock">The clock whose virtual time schedules the firings.</param>
    /// <param name="callback">The callback invoked at each firing.</param>
    /// <param name="state">The state passed to the callback.</param>
    private VirtualClockTimer(VirtualClock clock, TimerCallback callback, object? state)
    {
        _clock = clock;
        _callback = callback;
        _state = state;
    }

    /// <inheritdoc/>
    public bool Change(TimeSpan dueTime, TimeSpan period)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(dueTime, Timeout.InfiniteTimeSpan);
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, Timeout.InfiniteTimeSpan);

        IDisposable? previous;
        lock (_gate)
        {
            if (_isDisposed)
            {
                return false;
            }

            previous = _pending;
            _period = period > TimeSpan.Zero ? period : TimeSpan.Zero;
            _pending = dueTime == Timeout.InfiniteTimeSpan ? null : Queue(dueTime);
        }

        previous?.Dispose();
        return true;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        IDisposable? previous;
        lock (_gate)
        {
            _isDisposed = true;
            previous = _pending;
            _pending = null;
        }

        previous?.Dispose();
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    /// <summary>Creates a timer and queues its first firing.</summary>
    /// <param name="clock">The clock whose virtual time schedules the firings.</param>
    /// <param name="callback">The callback invoked at each firing.</param>
    /// <param name="state">The state passed to the callback.</param>
    /// <param name="dueTime">The delay before the first firing, or <see cref="Timeout.InfiniteTimeSpan"/> to leave the timer stopped.</param>
    /// <param name="period">The interval between later firings, or <see cref="Timeout.InfiniteTimeSpan"/> or <see cref="TimeSpan.Zero"/> for a single firing.</param>
    /// <returns>The timer.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="callback"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="dueTime"/> or <paramref name="period"/> is negative and not <see cref="Timeout.InfiniteTimeSpan"/>.</exception>
    internal static VirtualClockTimer Start(
        VirtualClock clock,
        TimerCallback callback,
        object? state,
        TimeSpan dueTime,
        TimeSpan period)
    {
        ArgumentExceptionHelper.ThrowIfNull(callback);

        VirtualClockTimer timer = new(clock, callback, state);
        _ = timer.Change(dueTime, period);
        return timer;
    }

    /// <summary>Queues the next periodic firing, then invokes the callback unless the timer is disposed.</summary>
    /// <returns>An empty disposable; the queued firing needs no further cancellation.</returns>
    internal EmptyDisposable Fire()
    {
        lock (_gate)
        {
            if (_isDisposed)
            {
                return EmptyDisposable.Instance;
            }

            _pending = _period > TimeSpan.Zero ? Queue(_period) : null;
        }

        _callback(_state);
        return EmptyDisposable.Instance;
    }

    /// <summary>Queues a firing on the virtual clock.</summary>
    /// <param name="dueTime">The virtual delay before the firing.</param>
    /// <returns>The handle that cancels the queued firing.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable Queue(TimeSpan dueTime) =>
        _clock.Schedule(this, dueTime, static (_, timer) => timer.Fire());
}
