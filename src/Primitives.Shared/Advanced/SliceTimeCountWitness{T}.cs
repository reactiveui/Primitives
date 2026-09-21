// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Sink that hands out a window signal that ends when it holds a number of values or a duration has elapsed, whichever comes first.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Each new window restarts the duration. Windows end when the source ends. Notifications are delivered outside the
/// sink's lock.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SliceTimeCountWitness: TimeSpan = {_timeSpan}, Count = {_count}, Index = {_index}, Done = {_done}")]
public sealed class SliceTimeCountWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>Serializes access to the current window, the counters and the terminal latch.</summary>
    private readonly Lock _gate = new();

    /// <summary>The maximum duration of each window.</summary>
    private readonly TimeSpan _timeSpan;

    /// <summary>The maximum number of values in each window.</summary>
    private readonly int _count;

    /// <summary>The sequencer that schedules the timer.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Delivers windows and their values in the order they were posted.</summary>
    private readonly SliceRouter<IObservable<T>, T> _router;

    /// <summary>The source subscription.</summary>
    private readonly MultipleDisposable _upstream = [];

    /// <summary>The pending timer.</summary>
    private readonly SingleReplaceableDisposable _timer = new();

    /// <summary>The window currently receiving values.</summary>
    private SliceWindow<T> _current;

    /// <summary>The number of values in the current window.</summary>
    private int _index;

    /// <summary>The identity of the current window, so a timer that fires late for an earlier window is ignored.</summary>
    private int _windowId;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="SliceTimeCountWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer of the windows.</param>
    /// <param name="timeSpan">The maximum duration of each window.</param>
    /// <param name="count">The maximum number of values in each window.</param>
    /// <param name="sequencer">The sequencer that schedules the timer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="count"/> is zero or negative.</exception>
    public SliceTimeCountWitness(IObserver<IObservable<T>> observer, TimeSpan timeSpan, int count, ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentExceptionHelper.ThrowIfNull(sequencer);
        SliceTimeGuard.ThrowIfNotPositive(timeSpan);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);
        _timeSpan = timeSpan;
        _count = count;
        _sequencer = sequencer;
        Subscription = new();
        Subscription.Attach(_upstream);
        _router = new(observer, Subscription);
        _current = new(Subscription);
    }

    /// <summary>Gets the subscription handed to the outer subscriber; the source stays subscribed while any window is.</summary>
    public SharedSubscription Subscription { get; }

    /// <summary>Opens the first window and starts its timer before the source is subscribed.</summary>
    public void Start()
    {
        lock (_gate)
        {
            _router.Open(_current);
        }

        Deliver();
        ScheduleTick(0);
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        var restart = false;
        var windowId = 0;
        lock (_gate)
        {
            if (_done != 0)
            {
                return;
            }

            _router.Publish(_current, value);
            _index++;
            if (_index == _count)
            {
                windowId = Advance();
                restart = true;
            }
        }

        Deliver();
        if (restart)
        {
            ScheduleTick(windowId);
        }
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

    /// <summary>Ends the current window and opens the next; the caller holds the lock.</summary>
    /// <returns>The identity of the new window.</returns>
    private int Advance()
    {
        _index = 0;
        _windowId++;
        _router.Complete(_current);
        _current = new(Subscription);
        _router.Open(_current);
        return _windowId;
    }

    /// <summary>Schedules the end of a window after the duration.</summary>
    /// <param name="windowId">The identity of the window the timer belongs to.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ScheduleTick(int windowId) =>
        _timer.Create(_sequencer.Schedule((Sink: this, WindowId: windowId), _timeSpan, static state => state.Sink.Tick(state.WindowId)));

    /// <summary>Ends the window the timer belongs to and starts the next, unless the window has already ended.</summary>
    /// <param name="windowId">The identity of the window the timer belongs to.</param>
    private void Tick(int windowId)
    {
        int nextId;
        lock (_gate)
        {
            if (_done != 0 || windowId != _windowId)
            {
                return;
            }

            nextId = Advance();
        }

        Deliver();
        ScheduleTick(nextId);
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

    /// <summary>Ends the current window and the outer sequence, then tears the sink down.</summary>
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
            _router.Finish([_current], error);
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
