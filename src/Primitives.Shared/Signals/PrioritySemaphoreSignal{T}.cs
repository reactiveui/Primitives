// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>A signal that limits forwarded values with priority-based semaphore semantics.</summary>
/// <typeparam name="T">The comparable value type used for priority ordering.</typeparam>
public sealed class PrioritySemaphoreSignal<T> : ISignal<T>
    where T : IComparable<T>
{
    /// <summary>The inner signal that receives values once capacity is available.</summary>
    private readonly ISignal<T> _inner;

    /// <summary>Guards queue and terminal-state mutations.</summary>
    private readonly Lock _gate = new();

    /// <summary>The queued values waiting for available capacity.</summary>
    private PriorityQueue<T>? _nextItems = new();

    /// <summary>The number of forwarded values currently consuming capacity.</summary>
    private int _count;

    /// <summary>The configured capacity.</summary>
    private int _maximumCount;

    /// <summary>Prevents multiple concurrent drain loops from forwarding downstream at once.</summary>
    private bool _isDraining;

    /// <summary>Initializes a new instance of the <see cref="PrioritySemaphoreSignal{T}"/> class.</summary>
    /// <param name="maxCount">The maximum number of values to forward before release is required.</param>
    public PrioritySemaphoreSignal(int maxCount)
        : this(maxCount, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PrioritySemaphoreSignal{T}"/> class.</summary>
    /// <param name="maxCount">The maximum number of values to forward before release is required.</param>
    /// <param name="sched">The sequencer used when emitting values to observers.</param>
    public PrioritySemaphoreSignal(int maxCount, ISequencer? sched)
    {
        _inner = sched is not null ? new ScheduledSignal<T>(sched) : new Signal<T>();
        _maximumCount = maxCount;
    }

    /// <inheritdoc />
    public bool HasObservers => _inner.HasObservers;

    /// <inheritdoc />
    public bool IsDisposed => _inner.IsDisposed;

    /// <summary>Gets or sets the maximum number of values allowed through before release is required.</summary>
    public int MaximumCount
    {
        get => Volatile.Read(ref _maximumCount);
        set
        {
            Volatile.Write(ref _maximumCount, value);
            YieldUntilEmptyOrBlocked();
        }
    }

    /// <inheritdoc />
    public void OnNext(T value)
    {
        if (!Enqueue(value))
        {
            return;
        }

        YieldUntilEmptyOrBlocked();
    }

    /// <summary>Releases one semaphore slot and drains queued values when capacity is available.</summary>
    public void Release()
    {
        int previousCount;
        do
        {
            previousCount = Volatile.Read(ref _count);
            if (previousCount <= 0)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(ref _count, previousCount - 1, previousCount) != previousCount);

        YieldUntilEmptyOrBlocked();
    }

    /// <inheritdoc />
    public void OnCompleted()
    {
        var queue = CompleteQueue();
        if (queue is null)
        {
            return;
        }

        while (queue.Count > 0)
        {
            _inner.OnNext(queue.Dequeue());
        }

        _inner.OnCompleted();
    }

    /// <inheritdoc />
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        lock (_gate)
        {
            _nextItems = null;
        }

        _inner.OnError(error);
    }

    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<T> observer) => _inner.Subscribe(observer);

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            _nextItems = null;
        }

        _inner.Dispose();
    }

    /// <summary>Completes the priority queue and returns the remaining queued values.</summary>
    /// <returns>The completed queue, or <see langword="null"/> when the signal was already terminal.</returns>
    private PriorityQueue<T>? CompleteQueue()
    {
        lock (_gate)
        {
            var queue = _nextItems;
            _nextItems = null;
            return queue;
        }
    }

    /// <summary>Queues a value when the signal is still accepting input.</summary>
    /// <param name="value">The value to enqueue.</param>
    /// <returns><see langword="true"/> when the value was queued; otherwise, <see langword="false"/>.</returns>
    private bool Enqueue(T value)
    {
        lock (_gate)
        {
            var queue = _nextItems;
            if (queue is null)
            {
                return false;
            }

            queue.Enqueue(value);
            return true;
        }
    }

    /// <summary>Dequeue and forwards values while capacity is available.</summary>
    private void YieldUntilEmptyOrBlocked()
    {
        T next;
        var ownsDrain = false;
        lock (_gate)
        {
            if (_isDraining || !TryDequeue(out next))
            {
                return;
            }

            _isDraining = true;
            ownsDrain = true;
        }

        try
        {
            while (true)
            {
                _inner.OnNext(next);

                lock (_gate)
                {
                    if (!TryDequeue(out next))
                    {
                        _isDraining = false;
                        ownsDrain = false;
                        return;
                    }
                }
            }
        }
        finally
        {
            if (ownsDrain)
            {
                lock (_gate)
                {
                    _isDraining = false;
                }
            }
        }
    }

    /// <summary>Attempts to dequeue the next value when capacity is available.</summary>
    /// <param name="next">The dequeued value.</param>
    /// <returns><see langword="true"/> when a value was dequeued; otherwise, <see langword="false"/>.</returns>
    private bool TryDequeue(out T next)
    {
        var queue = _nextItems;
        if (queue is null || queue.Count == 0 || Volatile.Read(ref _count) >= Volatile.Read(ref _maximumCount))
        {
            next = default!;
            return false;
        }

        next = queue.Dequeue();
        _ = Interlocked.Increment(ref _count);
        return true;
    }
}
