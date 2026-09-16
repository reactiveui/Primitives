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
/// <remarks>
/// Buffered values and live notifications are posted to each subscriber under the gate, so a subscriber sees its replay first
/// and every later notification once in emission order, and are delivered after the gate is released; no observer runs while
/// it is held.
/// </remarks>
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

    /// <summary>Orders buffer mutation, posting and subscription; never held while an observer runs.</summary>
    private readonly Lock _observerLock = new();

    /// <summary>The subscribers notifications are posted to.</summary>
    private SerializedBroadcaster<T> _broadcaster;

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

    /// <summary>Gets the gate that orders replay and live posting.</summary>
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
        SerializedBroadcast<T> broadcast;
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

            broadcast = _broadcaster.PostCompleted();
            _broadcaster.Clear();
        }

        broadcast.Flush();
    }

    /// <summary>Terminates the signal with the error, forwarding it to every observer and replaying it to later subscribers.</summary>
    /// <param name="error">The terminating exception.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        SerializedBroadcast<T> broadcast;
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

            broadcast = _broadcaster.PostError(error);
            _broadcaster.Clear();
        }

        broadcast.Flush();
    }

    /// <summary>Buffers the value for replay and broadcasts it to the current observers.</summary>
    /// <param name="value">The value to emit.</param>
    /// <remarks>Concurrent subscription receives each value once, through replay or live delivery, in emission order.</remarks>
    public void OnNext(T value)
    {
        var interval = _usesWindow ? _scheduler.Now - _startTime : TimeSpan.Zero;
        SerializedBroadcast<T> broadcast;
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

            broadcast = _broadcaster.PostNext(value);
        }

        broadcast.Flush();
    }

    /// <summary>Replays the buffered values to the observer, then attaches it unless the signal has terminated.</summary>
    /// <param name="observer">The observer to attach.</param>
    /// <returns>A disposable that detaches the observer, or an empty disposable when the signal has terminated.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SerializedWitness<T> witness = new(observer);
        ObserverHandler? subscription = null;
        T[] replay;
        lock (_observerLock)
        {
            ThrowIfDisposed();

            // A new witness is always claimable; claiming it first queues live values and the terminal behind the replay.
            _ = witness.TryClaim();
            replay = _ring is not null ? SnapshotRing() : SnapshotQueue();
            if (!_isStopped)
            {
                _broadcaster.Add(witness);
                subscription = new(this, witness);
            }
            else if (_lastError is not null)
            {
                _ = witness.PostError(_lastError);
            }
            else
            {
                _ = witness.PostCompleted();
            }
        }

        witness.DeliverClaimed(replay);
        if (subscription is not null)
        {
            return subscription;
        }

        // The terminal was queued behind the replay without a signal, so it is delivered here.
        witness.Flush();
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

    /// <summary>Copies the ring contents in arrival order for replay outside the gate.</summary>
    /// <returns>The buffered values, oldest first.</returns>
    private T[] SnapshotRing()
    {
        var ring = _ring!;
        if (_ringCount == 0)
        {
            return [];
        }

        var snapshot = new T[_ringCount];
        var index = _ringNext - _ringCount;
        if (index < 0)
        {
            index += ring.Length;
        }

        var tail = Math.Min(_ringCount, ring.Length - index);
        Array.Copy(ring, index, snapshot, 0, tail);
        Array.Copy(ring, 0, snapshot, tail, _ringCount - tail);
        return snapshot;
    }

    /// <summary>Trims the timed buffer and copies what remains for replay outside the gate.</summary>
    /// <returns>The buffered values, oldest first.</returns>
    private T[] SnapshotQueue()
    {
        Trim();
        var queue = _queue!;
        if (queue.Count == 0)
        {
            return [];
        }

        var snapshot = new T[queue.Count];
        var i = 0;
        foreach (var item in queue)
        {
            snapshot[i] = item.Value;
            i++;
        }

        return snapshot;
    }

    /// <summary>Detaches one observer from the signal when disposed.</summary>
    /// <param name="subject">The signal the observer is attached to.</param>
    /// <param name="witness">The subscriber to detach.</param>
    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "The field references the owning signal, not a resource this subscription owns.")]
    private sealed class ObserverHandler(ReplaySignal<T> subject, SerializedWitness<T> witness) : IDisposable
    {
        /// <summary>Serializes concurrent disposal.</summary>
        private readonly Lock _lock = new();

        /// <summary>The signal the observer is attached to; null once disposed.</summary>
        private ReplaySignal<T>? _subject = subject;

        /// <summary>The subscriber to detach; null once disposed.</summary>
        private SerializedWitness<T>? _witness = witness;

        /// <summary>Removes the subscriber from the signal and clears both references.</summary>
        public void Dispose()
        {
            lock (_lock)
            {
                if (_subject is not null)
                {
                    lock (_subject._observerLock)
                    {
                        _subject._broadcaster.Remove(_witness!);
                        _witness = null;
                        _subject = null;
                    }
                }
            }
        }
    }
}
