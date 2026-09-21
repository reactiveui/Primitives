// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Sink that hands out a window signal of a fixed duration, opening a new window every time shift.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Windows overlap when the time shift is shorter than the duration and leave a gap when it is longer. Windows end when
/// the source ends. Notifications are delivered outside the sink's lock.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SliceTimeWitness: TimeSpan = {_timeSpan}, TimeShift = {_timeShift}, Open = {_windows.Count}, Done = {_done}")]
public sealed class SliceTimeWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>Serializes access to the open windows, the timer schedule and the terminal latch.</summary>
    private readonly Lock _gate = new();

    /// <summary>The duration of each window.</summary>
    private readonly TimeSpan _timeSpan;

    /// <summary>The time between the starts of consecutive windows.</summary>
    private readonly TimeSpan _timeShift;

    /// <summary>The sequencer that schedules the timer.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>The windows that have not ended, oldest first.</summary>
    private readonly List<SliceWindow<T>> _windows = [];

    /// <summary>Delivers windows and their values in the order they were posted.</summary>
    private readonly SliceRouter<IObservable<T>, T> _router;

    /// <summary>The source subscription.</summary>
    private readonly MultipleDisposable _upstream = [];

    /// <summary>The pending timer.</summary>
    private readonly SingleReplaceableDisposable _timer = new();

    /// <summary>The time from subscription to the last scheduled boundary.</summary>
    private TimeSpan _totalTime;

    /// <summary>The time from subscription to the next window end.</summary>
    private TimeSpan _nextSpan;

    /// <summary>The time from subscription to the next window start.</summary>
    private TimeSpan _nextShift;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="SliceTimeWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer of the windows.</param>
    /// <param name="timeSpan">The duration of each window.</param>
    /// <param name="timeShift">The time between the starts of consecutive windows.</param>
    /// <param name="sequencer">The sequencer that schedules the timer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="timeShift"/> is zero or negative.</exception>
    public SliceTimeWitness(IObserver<IObservable<T>> observer, TimeSpan timeSpan, TimeSpan timeShift, ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentExceptionHelper.ThrowIfNull(sequencer);
        SliceTimeGuard.ThrowIfNotPositive(timeSpan);
        SliceTimeGuard.ThrowIfNotPositive(timeShift);
        _timeSpan = timeSpan;
        _timeShift = timeShift;
        _sequencer = sequencer;
        Subscription = new();
        Subscription.Attach(_upstream);
        _router = new(observer, Subscription);
    }

    /// <summary>Gets the subscription handed to the outer subscriber; the source stays subscribed while any window is.</summary>
    public SharedSubscription Subscription { get; }

    /// <summary>Opens the first window and starts the timer before the source is subscribed.</summary>
    public void Start()
    {
        lock (_gate)
        {
            _nextSpan = _timeSpan;
            _nextShift = _timeShift;
            OpenWindow();
        }

        Deliver();
        ScheduleTick();
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            for (var i = 0; i < _windows.Count; i++)
            {
                _router.Publish(_windows[i], value);
            }
        }

        Deliver();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Terminate(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Terminate(null);

    /// <summary>Assigns the source subscription, disposing the incoming one when this sink has been disposed.</summary>
    /// <param name="subscription">The source subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => _upstream.Add(subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        Subscription.Release();
        _timer.Dispose();
        _upstream.Dispose();
    }

    /// <summary>Opens a window and posts it to the outer observer; the caller holds the lock.</summary>
    private void OpenWindow()
    {
        SliceWindow<T> window = new(Subscription);
        _windows.Add(window);
        _router.Open(window);
    }

    /// <summary>Schedules the next window end, window start or both.</summary>
    private void ScheduleTick()
    {
        bool isSpan;
        bool isShift;
        TimeSpan delay;
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            isSpan = _nextSpan <= _nextShift;
            isShift = _nextShift <= _nextSpan;
            var next = isSpan ? _nextSpan : _nextShift;
            delay = next - _totalTime;
            _totalTime = next;
            if (isSpan)
            {
                _nextSpan += _timeShift;
            }

            if (isShift)
            {
                _nextShift += _timeShift;
            }
        }

        _timer.Create(_sequencer.Schedule((Sink: this, IsSpan: isSpan, IsShift: isShift), delay, static state => state.Sink.Tick(state.IsSpan, state.IsShift)));
    }

    /// <summary>Ends the oldest window, starts a new one, or both, then schedules the next tick.</summary>
    /// <param name="isSpan">Whether the oldest window ends.</param>
    /// <param name="isShift">Whether a new window starts.</param>
    private void Tick(bool isSpan, bool isShift)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            if (isSpan)
            {
                var oldest = _windows[0];
                _windows.RemoveAt(0);
                _router.Complete(oldest);
            }

            if (isShift)
            {
                OpenWindow();
            }
        }

        Deliver();
        ScheduleTick();
    }

    /// <summary>Delivers the posted notifications, tearing the sink down when a downstream observer throws.</summary>
    private void Deliver()
    {
        try
        {
            _router.Flush();
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>Ends every open window and the outer sequence, then tears the sink down.</summary>
    /// <param name="error">The error, or <see langword="null"/> to complete.</param>
    private void Terminate(Exception? error)
    {
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            Volatile.Write(ref _done, 1);
            _router.Finish(_windows, error);
        }

        try
        {
            _router.Flush();
        }
        finally
        {
            Dispose();
        }
    }
}
