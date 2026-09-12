// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>A signal that replays buffered values to new subscribers.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ReplaySignal<T> : ISignal<T>
{
    /// <summary>The maximum number of values replayed to a new subscriber.</summary>
    private readonly int _bufferSize;

    /// <summary>The maximum age of a replayed value.</summary>
    private readonly TimeSpan _window;

    /// <summary>The clock reading that buffered intervals are measured from.</summary>
    private readonly DateTimeOffset _startTime;

    /// <summary>The sequencer supplying the clock used for window trimming.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Whether a finite replay window is in effect.</summary>
    private readonly bool _usesWindow;

    /// <summary>Serializes buffer mutation, broadcast and subscription.</summary>
    private readonly Lock _observerLock = new();

    /// <summary>The observers values are broadcast to.</summary>
    private Broadcaster<T> _broadcaster;

    /// <summary>Whether the signal has terminated.</summary>
    private bool _isStopped;

    /// <summary>The terminal error replayed to later subscribers.</summary>
    private Exception? _lastError;

    /// <summary>The buffered values and their intervals, used when a window or an unbounded buffer is in effect.</summary>
    private Queue<TimeInterval<T>>? _queue;

    /// <summary>The fixed-size ring buffer, used when the buffer is bounded and no window is in effect.</summary>
    private T[]? _ring;

    /// <summary>The number of values held in the ring.</summary>
    private int _ringCount;

    /// <summary>The ring index the next value is written to.</summary>
    private int _ringNext;

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="bufferSize">The maximum number of values to replay.</param>
    /// <param name="window">The maximum age of a replayed value.</param>
    /// <param name="scheduler">The sequencer supplying the clock used for window trimming.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bufferSize"/> or <paramref name="window"/> is negative.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public ReplaySignal(int bufferSize, TimeSpan window, ISequencer scheduler)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(bufferSize);

        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(window, TimeSpan.Zero);

        _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
        _bufferSize = bufferSize;
        _window = window;
        _usesWindow = window != TimeSpan.MaxValue;
        _startTime = _usesWindow ? scheduler.Now : DateTimeOffset.MinValue;
        _broadcaster = default;
        if (_usesWindow || bufferSize == int.MaxValue)
        {
            _queue = new();
        }
        else
        {
            _ring = bufferSize == 0 ? [] : new T[bufferSize];
        }
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="bufferSize">The maximum number of values to replay.</param>
    /// <param name="window">The maximum age of a replayed value.</param>
    public ReplaySignal(int bufferSize, TimeSpan window)
        : this(bufferSize, window, Sequencer.CurrentThread)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    public ReplaySignal()
        : this(int.MaxValue, TimeSpan.MaxValue, Sequencer.CurrentThread)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="scheduler">The sequencer supplying the clock used for window trimming.</param>
    public ReplaySignal(ISequencer scheduler)
        : this(int.MaxValue, TimeSpan.MaxValue, scheduler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="bufferSize">The maximum number of values to replay.</param>
    /// <param name="scheduler">The sequencer supplying the clock used for window trimming.</param>
    public ReplaySignal(int bufferSize, ISequencer scheduler)
        : this(bufferSize, TimeSpan.MaxValue, scheduler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="bufferSize">The maximum number of values to replay.</param>
    public ReplaySignal(int bufferSize)
        : this(bufferSize, TimeSpan.MaxValue, Sequencer.CurrentThread)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="window">The maximum age of a replayed value.</param>
    /// <param name="scheduler">The sequencer supplying the clock used for window trimming.</param>
    public ReplaySignal(TimeSpan window, ISequencer scheduler)
        : this(int.MaxValue, window, scheduler) => _window = window;

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="window">The maximum age of a replayed value.</param>
    public ReplaySignal(TimeSpan window)
        : this(int.MaxValue, window, Sequencer.CurrentThread)
    {
    }

    /// <summary>Gets a value indicating whether the signal has observers and has not terminated.</summary>
    public bool HasObservers => _broadcaster.HasObservers && !_isStopped;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    public bool IsDisposed { get; private set; }

    /// <summary>Gets the gate shared by replay and live delivery.</summary>
    internal Lock Gate => _observerLock;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Drops the buffered values and observers and marks the signal disposed.</summary>
    public void Dispose()
    {
        if (IsDisposed)
        {
            return;
        }

        lock (_observerLock)
        {
            _broadcaster.Clear();
            _lastError = null;
            _queue = null;
            _ring = null;
            _ringCount = 0;
            _ringNext = 0;
        }

        IsDisposed = true;
    }

    /// <summary>Terminates the signal and completes every observer; later calls are ignored.</summary>
    public void OnCompleted()
    {
        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            if (_queue is not null)
            {
                Trim();
            }

            _broadcaster.Completed();
            _broadcaster.Clear();
        }
    }

    /// <summary>Terminates the signal with the error, forwarding it to every observer and replaying it to later subscribers.</summary>
    /// <param name="error">The terminating exception.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _lastError = error;
            if (_queue is not null)
            {
                Trim();
            }

            _broadcaster.Error(error);
            _broadcaster.Clear();
        }
    }

    /// <summary>Buffers the value for replay and broadcasts it to the current observers.</summary>
    /// <param name="value">The value to emit.</param>
    /// <remarks>Concurrent subscription receives each value once, through replay or live delivery, in emission order.</remarks>
    public void OnNext(T value)
    {
        var interval = _usesWindow ? _scheduler.Now - _startTime : TimeSpan.Zero;
        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            if (_ring is not null)
            {
                AppendToRing(value);
            }
            else
            {
                _queue!.Enqueue(new(value, interval));
                Trim();
            }

            _broadcaster.Next(value);
        }
    }

    /// <summary>Replays the buffered values to the observer, then attaches it unless the signal has terminated.</summary>
    /// <param name="observer">The observer to attach.</param>
    /// <returns>A disposable that detaches the observer, or an empty disposable when the signal has terminated.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Exception? ex;
        var subscription = default(ObserverHandler);

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (!_isStopped)
            {
                _broadcaster.Add(observer);
                subscription = new(this, observer);
            }

            ex = _lastError;
            if (_ring is not null)
            {
                ReplayRing(observer);
            }
            else
            {
                Trim();
                foreach (var item in _queue!)
                {
                    observer.OnNext(item.Value);
                }
            }
        }

        if (subscription is not null)
        {
            return subscription;
        }

        if (ex is not null)
        {
            observer.OnError(ex);
        }
        else
        {
            observer.OnCompleted();
        }

        return EmptyDisposable.Instance;
    }

    /// <summary>Throws when the signal has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">The signal has been disposed.</exception>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }

    /// <summary>Drops queued values beyond the buffer size and older than the replay window.</summary>
    private void Trim()
    {
        while (_queue!.Count > _bufferSize)
        {
            _ = _queue.Dequeue();
        }

        if (!_usesWindow)
        {
            return;
        }

        var elapsedTime = Sequencer.Normalize(_scheduler.Now - _startTime);

        while (_queue.Count > 0 && elapsedTime.Subtract(_queue.Peek().Interval).CompareTo(_window) > 0)
        {
            _ = _queue.Dequeue();
        }
    }

    /// <summary>Writes the value into the ring, overwriting the oldest entry once it is full.</summary>
    /// <param name="value">The value to buffer.</param>
    private void AppendToRing(T value)
    {
        var ring = _ring!;
        if (ring.Length == 0)
        {
            return;
        }

        ring[_ringNext] = value;
        _ringNext++;
        if (_ringNext == ring.Length)
        {
            _ringNext = 0;
        }

        if (_ringCount >= ring.Length)
        {
            return;
        }

        _ringCount++;
    }

    /// <summary>Replays the ring contents to the observer in arrival order.</summary>
    /// <param name="observer">The observer receiving the replay.</param>
    private void ReplayRing(IObserver<T> observer)
    {
        var ring = _ring!;
        if (_ringCount == 0 || ring.Length == 0)
        {
            return;
        }

        var index = _ringNext - _ringCount;
        if (index < 0)
        {
            index += ring.Length;
        }

        for (var i = 0; i < _ringCount; i++)
        {
            observer.OnNext(ring[index]);
            index++;
            if (index == ring.Length)
            {
                index = 0;
            }
        }
    }

    /// <summary>Detaches one observer from the signal when disposed.</summary>
    /// <param name="subject">The signal the observer is attached to.</param>
    /// <param name="observer">The observer to detach.</param>
    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "The field references the owning signal, not a resource this subscription owns.")]
    private sealed class ObserverHandler(ReplaySignal<T> subject, IObserver<T> observer) : IDisposable
    {
        /// <summary>Serializes concurrent disposal.</summary>
        private readonly Lock _lock = new();

        /// <summary>The signal the observer is attached to; null once disposed.</summary>
        private ReplaySignal<T>? _subject = subject;

        /// <summary>The observer to detach; null once disposed.</summary>
        private IObserver<T>? _observer = observer;

        /// <summary>Removes the observer from the signal and clears both references.</summary>
        public void Dispose()
        {
            lock (_lock)
            {
                if (_subject is not null)
                {
                    lock (_subject._observerLock)
                    {
                        _subject._broadcaster.Remove(_observer!);
                        _observer = null;
                        _subject = null;
                    }
                }
            }
        }
    }
}
