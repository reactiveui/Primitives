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

    /// <summary>Queued terminal state waiting to be emitted by the drain owner.</summary>
    private TerminalNotification _terminalNotification;

    /// <summary>Queued values flushed on completion, ignoring semaphore capacity.</summary>
    private PriorityQueue<T>? _terminalQueue;

    /// <summary>Terminal error to forward when terminating with error.</summary>
    private Exception? _terminalError;

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

    /// <summary>Terminal states consumed by the drain owner.</summary>
    private enum TerminalNotification
    {
        /// <summary>No terminal notification has been queued.</summary>
        None,

        /// <summary>Completion notification.</summary>
        Completed,

        /// <summary>Error notification.</summary>
        Error,
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
        lock (_gate)
        {
            if (_nextItems is null)
            {
                return;
            }

            _terminalQueue = _nextItems;
            _nextItems = null;
            _terminalNotification = TerminalNotification.Completed;
            _terminalError = null;
        }

        YieldUntilEmptyOrBlocked();
    }

    /// <inheritdoc />
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        lock (_gate)
        {
            if (_nextItems is null)
            {
                return;
            }

            _nextItems = null;
            _terminalQueue = null;
            _terminalNotification = TerminalNotification.Error;
            _terminalError = error;
        }

        YieldUntilEmptyOrBlocked();
    }

    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<T> observer) => _inner.Subscribe(observer);

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            _nextItems = null;
            _terminalQueue = null;
            _terminalNotification = TerminalNotification.None;
            _terminalError = null;
        }

        _inner.Dispose();
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
    /// <remarks>
    /// Only a single thread ever delivers downstream at a time. A caller that finds a drain
    /// already in progress hands its work to the active owner and returns; the owner re-checks
    /// for newly queued work under the gate before relinquishing ownership, so no wakeup is lost
    /// and no two threads can deliver to the inner signal concurrently.
    /// </remarks>
    private void YieldUntilEmptyOrBlocked()
    {
        if (!TryBeginDrain())
        {
            return;
        }

        var owned = true;
        try
        {
            while (TryTakeNextDrainItem(out var item))
            {
                Deliver(item);
            }

            // TryTakeNextDrainItem cleared ownership when it returned false.
            owned = false;
        }
        finally
        {
            if (owned)
            {
                EndDrain();
            }
        }
    }

    /// <summary>Attempts to become the single drain owner.</summary>
    /// <returns><see langword="true"/> when this caller owns the drain.</returns>
    private bool TryBeginDrain()
    {
        lock (_gate)
        {
            if (_isDraining)
            {
                return false;
            }

            _isDraining = true;
            return true;
        }
    }

    /// <summary>Captures the next drain item, or relinquishes ownership when nothing is left to deliver.</summary>
    /// <param name="item">The next item to drain.</param>
    /// <returns><see langword="true"/> when an item was captured and the caller retains ownership.</returns>
    private bool TryTakeNextDrainItem(out DrainItem item)
    {
        lock (_gate)
        {
            if (TryTakeNextDrainItemCore(out item))
            {
                return true;
            }

            // No work remains; release ownership atomically with the empty check so a
            // producer that queues work after this point will be able to begin a fresh drain.
            _isDraining = false;
            return false;
        }
    }

    /// <summary>Releases drain ownership after an exceptional exit from the drain loop.</summary>
    private void EndDrain()
    {
        lock (_gate)
        {
            _isDraining = false;
        }
    }

    /// <summary>Attempts to take the next drain item.</summary>
    /// <param name="item">The next item to drain.</param>
    /// <returns><see langword="true"/> when an item was captured.</returns>
    private bool TryTakeNextDrainItemCore(out DrainItem item)
    {
        if (TryDequeue(out var next))
        {
            item = DrainItem.Next(next);
            return true;
        }

        return TryTakeTerminalNotification(out item);
    }

    /// <summary>Attempts to dequeue and capture the next value when capacity is available.</summary>
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

    /// <summary>Attempts to read and consume a pending terminal notification.</summary>
    /// <param name="item">The terminal drain item.</param>
    /// <returns><see langword="true"/> when a terminal notification was found.</returns>
    private bool TryTakeTerminalNotification(out DrainItem item)
    {
        var terminalNotification = _terminalNotification;
        item = terminalNotification switch
        {
            TerminalNotification.Completed => DrainItem.Completed(_terminalQueue),
            TerminalNotification.Error => DrainItem.Error(_terminalError!),
            _ => DrainItem.Empty
        };

        if (terminalNotification == TerminalNotification.None)
        {
            return false;
        }

        _terminalNotification = TerminalNotification.None;
        _terminalQueue = null;
        _terminalError = null;
        return true;
    }

    /// <summary>Delivers one captured drain item.</summary>
    /// <param name="item">The item to deliver.</param>
    private void Deliver(DrainItem item)
    {
        switch (item.Kind)
        {
            case TerminalNotification.Completed:
            {
                while (item.Queue is { Count: > 0 })
                {
                    _inner.OnNext(item.Queue.Dequeue());
                }

                _inner.OnCompleted();
                break;
            }

            case TerminalNotification.Error:
            {
                _inner.OnError(item.Exception!);
                break;
            }

            default:
            {
                _inner.OnNext(item.Value);
                break;
            }
        }
    }

    /// <summary>
    /// A captured value or terminal notification to deliver outside the gate. A readonly struct: one is produced
    /// per delivered value on the drain path, so it is passed by value instead of allocating per item.
    /// </summary>
    private readonly record struct DrainItem
    {
        /// <summary>An empty drain item used for failed capture paths.</summary>
        public static readonly DrainItem Empty = new(TerminalNotification.None, default!, null, null);

        /// <summary>Initializes a new instance of the <see cref="DrainItem"/> struct.</summary>
        /// <param name="kind">The captured item kind.</param>
        /// <param name="value">The captured value.</param>
        /// <param name="queue">The completion queue to flush.</param>
        /// <param name="error">The captured error.</param>
        private DrainItem(TerminalNotification kind, T value, PriorityQueue<T>? queue, Exception? error)
        {
            Kind = kind;
            Value = value;
            Queue = queue;
            Exception = error;
        }

        /// <summary>Gets the captured item kind.</summary>
        public TerminalNotification Kind { get; }

        /// <summary>Gets the captured value.</summary>
        public T Value { get; }

        /// <summary>Gets the completion queue to flush.</summary>
        public PriorityQueue<T>? Queue { get; }

        /// <summary>Gets the captured error.</summary>
        public Exception? Exception { get; }

        /// <summary>Creates a value drain item.</summary>
        /// <param name="value">The value to deliver.</param>
        /// <returns>The captured drain item.</returns>
        public static DrainItem Next(T value) => new(TerminalNotification.None, value, null, null);

        /// <summary>Creates a completion drain item.</summary>
        /// <param name="queue">The queued values to flush before completion.</param>
        /// <returns>The captured drain item.</returns>
        public static DrainItem Completed(PriorityQueue<T>? queue) => new(TerminalNotification.Completed, default!, queue, null);

        /// <summary>Creates an error drain item.</summary>
        /// <param name="exception">The error to deliver.</param>
        /// <returns>The captured drain item.</returns>
        public static DrainItem Error(Exception exception) => new(TerminalNotification.Error, default!, null, exception);
    }
}
