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
/// <typeparam name="T">The Type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ReplaySignal<T> : ISignal<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly int _bufferSize;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly TimeSpan _window;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly DateTimeOffset _startTime;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly bool _usesWindow;

    /// <summary>Executes the new operation.</summary>
    /// <returns>The result.</returns>
    private readonly Lock _observerLock = new();

    /// <summary>Stores state for the signal implementation.</summary>
    private Broadcaster<T> _broadcaster;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _isStopped;

    /// <summary>Stores state for the signal implementation.</summary>
    private Exception? _lastError;

    /// <summary>Stores state for the signal implementation.</summary>
    private Queue<TimeInterval<T>>? _queue;

    /// <summary>Stores state for the signal implementation.</summary>
    private T[]? _ring;

    /// <summary>Stores state for the signal implementation.</summary>
    private int _ringCount;

    /// <summary>Stores state for the signal implementation.</summary>
    private int _ringNext;

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="bufferSize">Size of the buffer.</param>
    /// <param name="window">The window.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// bufferSize
    /// or
    /// window.
    /// </exception>
    /// <exception cref="ArgumentNullException">scheduler.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Style",
        "IDE0001:Simplify Names",
        Justification = "The argument validation uses ArgumentExceptionHelper")]
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
    /// <param name="bufferSize">Size of the buffer.</param>
    /// <param name="window">The window.</param>
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
    /// <param name="scheduler">The scheduler.</param>
    public ReplaySignal(ISequencer scheduler)
        : this(int.MaxValue, TimeSpan.MaxValue, scheduler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="bufferSize">Size of the buffer.</param>
    /// <param name="scheduler">The scheduler.</param>
    public ReplaySignal(int bufferSize, ISequencer scheduler)
        : this(bufferSize, TimeSpan.MaxValue, scheduler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="bufferSize">Size of the buffer.</param>
    public ReplaySignal(int bufferSize)
        : this(bufferSize, TimeSpan.MaxValue, Sequencer.CurrentThread)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="window">The window.</param>
    /// <param name="scheduler">The scheduler.</param>
    public ReplaySignal(TimeSpan window, ISequencer scheduler)
        : this(int.MaxValue, window, scheduler) => _window = window;

    /// <summary>Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.</summary>
    /// <param name="window">The window.</param>
    public ReplaySignal(TimeSpan window)
        : this(int.MaxValue, window, Sequencer.CurrentThread)
    {
    }

    /// <summary>Gets a value indicating whether this instance has observers.</summary>
    /// <value>
    ///   <c>true</c> if this instance has observers; otherwise, <c>false</c>.
    /// </value>
    public bool HasObservers => _broadcaster.HasObservers && !_isStopped;

    /// <summary>Gets a value indicating whether this instance is disposed.</summary>
    /// <value>
    ///   <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed { get; private set; }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
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

    /// <summary>Called when [completed].</summary>
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

    /// <summary>Called when [error].</summary>
    /// <param name="error">The exception.</param>
    /// <exception cref="ArgumentNullException">error.</exception>
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

    /// <summary>Called when [next].</summary>
    /// <param name="value">The value.</param>
    /// <remarks>
    /// The buffer append and the broadcast happen together under <see cref="_observerLock"/>, which is the
    /// same gate <see cref="Subscribe"/> holds while it adds an observer and replays the buffer. A value is
    /// therefore atomically either buffered-and-broadcast before a new observer is added (so that observer
    /// receives it only via replay) or buffered-and-broadcast after the observer's replay completes (so it
    /// receives it only live) - never both, and never out of order.
    /// </remarks>
    public void OnNext(T value)
    {
        // Read the scheduler clock outside the lock; the window inputs are immutable.
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

    /// <summary>Subscribes the specified observer.</summary>
    /// <param name="observer">The observer.</param>
    /// <returns>A Disposable.</returns>
    /// <exception cref="ArgumentNullException">observer.</exception>
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

    /// <summary>Executes the ThrowIfDisposed operation.</summary>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }

    /// <summary>Executes the Trim operation.</summary>
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

    /// <summary>Executes the AppendToRing operation.</summary>
    /// <param name="value">The value.</param>
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

    /// <summary>Executes the ReplayRing operation.</summary>
    /// <param name="observer">The observer value.</param>
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

    /// <summary>Represents the ObserverHandler class.</summary>
    /// <param name="subject">The subject value.</param>
    /// <param name="observer">The observer value.</param>
    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "_subject is the signal that owns this subscription, not a resource it owns; disposing it would tear down the signal when one observer unsubscribes.")]
    private sealed class ObserverHandler(ReplaySignal<T> subject, IObserver<T> observer) : IDisposable
    {
        /// <summary>Executes the new operation.</summary>
        /// <returns>The result.</returns>
        private readonly Lock _lock = new();

        /// <summary>Stores state for the signal implementation.</summary>
        private ReplaySignal<T>? _subject = subject;

        /// <summary>Stores state for the signal implementation.</summary>
        private IObserver<T>? _observer = observer;

        /// <summary>Executes the Dispose operation.</summary>
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
