// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// ReplaySignal.
/// </summary>
/// <typeparam name="T">The Type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public class ReplaySignal<T> : ISignal<T>
{
    private readonly int _bufferSize;
    private readonly TimeSpan _window;
    private readonly DateTimeOffset _startTime;
    private readonly ISequencer _scheduler;
    private readonly bool _usesWindow;
    private readonly object _observerLock = new();
    private Broadcaster<T> _broadcaster;
    private bool _isStopped;
    private Exception? _lastError;
    private Queue<TimeInterval<T>>? _queue;
    private T[]? _ring;
    private int _ringCount;
    private int _ringNext;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    /// <param name="bufferSize">Size of the buffer.</param>
    /// <param name="window">The window.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// bufferSize
    /// or
    /// window.
    /// </exception>
    /// <exception cref="ArgumentNullException">scheduler.</exception>
    public ReplaySignal(int bufferSize, TimeSpan window, ISequencer scheduler)
    {
        if (bufferSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferSize));
        }

        if (window < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(window));
        }

        _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
        _bufferSize = bufferSize;
        _window = window;
        _usesWindow = window != TimeSpan.MaxValue;
        _startTime = _usesWindow ? scheduler.Now : DateTimeOffset.MinValue;
        if (_usesWindow || bufferSize == int.MaxValue)
        {
            _queue = new Queue<TimeInterval<T>>();
        }
        else
        {
            _ring = bufferSize == 0 ? Array.Empty<T>() : new T[bufferSize];
        }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    /// <param name="bufferSize">Size of the buffer.</param>
    /// <param name="window">The window.</param>
    public ReplaySignal(int bufferSize, TimeSpan window)
      : this(bufferSize, window, Sequencer.CurrentThread)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    public ReplaySignal()
      : this(int.MaxValue, TimeSpan.MaxValue, Sequencer.CurrentThread)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    /// <param name="scheduler">The scheduler.</param>
    public ReplaySignal(ISequencer scheduler)
      : this(int.MaxValue, TimeSpan.MaxValue, scheduler)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    /// <param name="bufferSize">Size of the buffer.</param>
    /// <param name="scheduler">The scheduler.</param>
    public ReplaySignal(int bufferSize, ISequencer scheduler)
      : this(bufferSize, TimeSpan.MaxValue, scheduler)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    /// <param name="bufferSize">Size of the buffer.</param>
    public ReplaySignal(int bufferSize)
      : this(bufferSize, TimeSpan.MaxValue, Sequencer.CurrentThread)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    /// <param name="window">The window.</param>
    /// <param name="scheduler">The scheduler.</param>
    public ReplaySignal(TimeSpan window, ISequencer scheduler)
      : this(int.MaxValue, window, scheduler) => _window = window;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplaySignal{T}"/> class.
    /// </summary>
    /// <param name="window">The window.</param>
    public ReplaySignal(TimeSpan window)
      : this(int.MaxValue, window, Sequencer.CurrentThread)
    {
    }

    /// <summary>
    /// Gets a value indicating whether this instance has observers.
    /// </summary>
    /// <value>
    ///   <c>true</c> if this instance has observers; otherwise, <c>false</c>.
    /// </value>
    public bool HasObservers => _broadcaster.HasObservers && !_isStopped;

    /// <summary>
    /// Gets a value indicating whether this instance is disposed.
    /// </summary>
    /// <value>
    ///   <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed { get; private set; }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Called when [completed].
    /// </summary>
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
            if (_queue != null)
            {
                Trim();
            }
        }

        _broadcaster.Completed();
        _broadcaster.Clear();
    }

    /// <summary>
    /// Called when [error].
    /// </summary>
    /// <param name="exception">The exception.</param>
    /// <exception cref="ArgumentNullException">exception.</exception>
    public void OnError(Exception exception)
    {
        if (exception == null)
        {
            throw new ArgumentNullException(nameof(exception));
        }

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _lastError = exception;
            if (_queue != null)
            {
                Trim();
            }
        }

        _broadcaster.Error(exception);
        _broadcaster.Clear();
    }

    /// <summary>
    /// Called when [next].
    /// </summary>
    /// <param name="value">The value.</param>
    public void OnNext(T value)
    {
        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            if (_ring != null)
            {
                AppendToRing(value);
            }
            else
            {
                var interval = _usesWindow ? _scheduler.Now - _startTime : TimeSpan.Zero;
                _queue!.Enqueue(new TimeInterval<T>(value, interval));
                Trim();
            }

        }

        _broadcaster.Next(value);
    }

    /// <summary>
    /// Subscribes the specified observer.
    /// </summary>
    /// <param name="observer">The observer.</param>
    /// <returns>A Disposable.</returns>
    /// <exception cref="ArgumentNullException">observer.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        var ex = default(Exception);
        var subscription = default(ObserverHandler);

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (!_isStopped)
            {
                _broadcaster.Add(observer);
                subscription = new ObserverHandler(this, observer);
            }

            ex = _lastError;
            if (_ring != null)
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

        if (subscription != null)
        {
            return subscription;
        }
        else if (ex != null)
        {
            observer.OnError(ex);
        }
        else
        {
            observer.OnCompleted();
        }

        return Disposable.Empty;
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!IsDisposed)
        {
            if (disposing)
            {
                lock (_observerLock)
                {
                    _broadcaster.Clear();
                    _lastError = null;
                    _queue = null;
                    _ring = null;
                    _ringCount = 0;
                    _ringNext = 0;
                }
            }

            IsDisposed = true;
        }
    }

    private void ThrowIfDisposed()
    {
        if (IsDisposed)
        {
            throw new ObjectDisposedException(string.Empty);
        }
    }

    private void Trim()
    {
        while (_queue!.Count > _bufferSize)
        {
            _queue.Dequeue();
        }

        if (!_usesWindow)
        {
            return;
        }

        var elapsedTime = Sequencer.Normalize(_scheduler.Now - _startTime);

        while (_queue.Count > 0 && elapsedTime.Subtract(_queue.Peek().Interval).CompareTo(_window) > 0)
        {
            _queue.Dequeue();
        }
    }

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

        if (_ringCount < ring.Length)
        {
            _ringCount++;
        }
    }

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

    private class ObserverHandler : IDisposable
    {
        private readonly object _lock = new();
        private ReplaySignal<T>? _subject;
        private IObserver<T>? _observer;

        public ObserverHandler(ReplaySignal<T> subject, IObserver<T> observer)
        {
            _subject = subject;
            _observer = observer;
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_subject != null)
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
