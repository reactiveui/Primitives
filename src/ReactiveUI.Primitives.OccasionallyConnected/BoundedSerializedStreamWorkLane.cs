// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Runs admitted stream mutations one at a time with a bounded FIFO backlog.</summary>
internal sealed partial class BoundedSerializedStreamWorkLane : IDisposable
{
    /// <summary>Protects admission, queued work, and lifecycle state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores admitted work that is waiting behind the active item.</summary>
    private readonly LinkedList<QueuedWorkItem> _queue = [];

    /// <summary>Stores the maximum number of active and queued items.</summary>
    private readonly int _capacity;

    /// <summary>Schedules admitted work outside completing stacks.</summary>
    private readonly Action<Action> _schedule;

    /// <summary>Completes when the lane becomes idle.</summary>
    private TaskCompletionSource<bool>? _idleWaiter;

    /// <summary>Tracks whether a work item is currently running.</summary>
    private bool _running;

    /// <summary>Tracks whether new work is rejected and queued work is canceled.</summary>
    private bool _disposed;

    /// <summary>Tracks the active plus queued work count.</summary>
    private int _admitted;

    /// <summary>Initializes a new instance of the <see cref="BoundedSerializedStreamWorkLane"/> class.</summary>
    /// <param name="capacity">The maximum active plus queued work count.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is not positive.</exception>
    internal BoundedSerializedStreamWorkLane(int capacity)
        : this(capacity, QueueWork)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BoundedSerializedStreamWorkLane"/> class.</summary>
    /// <param name="capacity">The maximum active plus queued work count.</param>
    /// <param name="schedule">The scheduler used to run dequeued work.</param>
    /// <exception cref="ArgumentNullException"><paramref name="schedule"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is not positive.</exception>
    internal BoundedSerializedStreamWorkLane(int capacity, Action<Action> schedule)
    {
        ArgumentExceptionHelper.ThrowIfNull(schedule);
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "Capacity must be positive.");
        }

        _capacity = capacity;
        _schedule = schedule;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        QueuedWorkItem[] canceled;

        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            canceled = CopyQueued();
            _queue.Clear();
            _admitted -= canceled.Length;
        }

        for (var i = 0; i < canceled.Length; i++)
        {
            canceled[i].DisposeRegistration();
            canceled[i].CancelBecauseDisposed();
        }
    }

    /// <summary>Admits work to the lane.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="work">The asynchronous work to run.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The task completed by the admitted work.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="work"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The lane is full.</exception>
    /// <exception cref="ObjectDisposedException">The lane has been disposed.</exception>
    internal Task<T> EnqueueAsync<T>(Func<CancellationToken, ValueTask<T>> work, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(work);
        cancellationToken.ThrowIfCancellationRequested();

        var item = new QueuedWorkItem<T>(this, work, cancellationToken);
        item.RegisterCancellation();

        try
        {
            var runNow = Admit(item, cancellationToken);
            ScheduleIfReady(item, runNow);
        }
        catch
        {
            item.DisposeRegistration();
            throw;
        }

        return item.Task;
    }

    /// <summary>Waits for active and queued work to finish.</summary>
    /// <param name="cancellationToken">The cancellation token for the wait.</param>
    /// <returns>The idle wait task.</returns>
    /// <exception cref="ObjectDisposedException">The lane has been disposed.</exception>
    internal Task WhenIdleAsync(CancellationToken cancellationToken)
    {
        Task idleTask;

        lock (_gate)
        {
            ThrowIfDisposed();
            if (_admitted == 0)
            {
                return Task.CompletedTask;
            }

            _idleWaiter ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            idleTask = _idleWaiter.Task;
        }

        return WaitForIdleAsync(idleTask, cancellationToken);
    }

    /// <summary>Waits for the supplied idle task while observing caller cancellation.</summary>
    /// <param name="idleTask">The task completed when the lane becomes idle.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous wait.</returns>
    private static Task WaitForIdleAsync(Task idleTask, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return cancellationToken.CanBeCanceled
            ? idleTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken)
            : idleTask;
    }

    /// <summary>Schedules a work item and faults it when the scheduler rejects it.</summary>
    /// <param name="item">The work item to schedule.</param>
    /// <returns><see langword="true"/> when scheduling succeeded; otherwise, <see langword="false"/>.</returns>
    private static bool TrySchedule(QueuedWorkItem item)
    {
        try
        {
            item.Schedule();
            return true;
        }
        catch (Exception exception)
        {
            return !item.TryRejectSchedule(exception);
        }
    }

    /// <summary>Admits an item and returns whether it should run immediately.</summary>
    /// <param name="item">The work item.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns><see langword="true"/> when the item should run now.</returns>
    /// <exception cref="InvalidOperationException">The lane is full.</exception>
    /// <exception cref="ObjectDisposedException">The lane has been disposed.</exception>
    private bool Admit(QueuedWorkItem item, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            if (_admitted >= _capacity)
            {
                throw new InvalidOperationException("The stream work lane is full.");
            }

            _admitted++;
            if (_running)
            {
                item.Node = _queue.AddLast(item);
                return false;
            }

            _running = true;
            return true;
        }
    }

    /// <summary>Schedules newly admitted work when no prior item is running.</summary>
    /// <param name="item">The work item.</param>
    /// <param name="runNow">Whether the item should run now.</param>
    private void ScheduleIfReady(QueuedWorkItem item, bool runNow)
    {
        if (!runNow)
        {
            return;
        }

        if (TrySchedule(item))
        {
            return;
        }

        Complete(item);
    }

    /// <summary>Copies queued work and detaches its linked-list nodes.</summary>
    /// <returns>The queued work snapshot.</returns>
    private QueuedWorkItem[] CopyQueued()
    {
        var queued = new QueuedWorkItem[_queue.Count];
        var index = 0;
        for (var node = _queue.First; node is not null; node = node.Next)
        {
            queued[index] = node.Value;
            queued[index].Node = null;
            index++;
        }

        return queued;
    }

    /// <summary>Marks a running work item complete and starts the next item when present.</summary>
    /// <param name="completed">The completed work item.</param>
    private void Complete(QueuedWorkItem completed)
    {
        var item = completed;
        while (true)
        {
            QueuedWorkItem? next = null;
            TaskCompletionSource<bool>? idleWaiter = null;

            lock (_gate)
            {
                _admitted--;
                if (_queue.First is { } first)
                {
                    next = first.Value;
                    next.Node = null;
                    _queue.RemoveFirst();
                }
                else
                {
                    _running = false;
                    idleWaiter = TakeIdleWaiter();
                }
            }

            item.DisposeRegistration();
            item.PublishSchedulerRejection();
            if (next is null)
            {
                _ = idleWaiter?.TrySetResult(true);
                return;
            }

            if (TrySchedule(next))
            {
                return;
            }

            item = next;
        }
    }

    /// <summary>Cancels a work item that has not started running.</summary>
    /// <param name="item">The queued work item.</param>
    private void CancelQueued(QueuedWorkItem item)
    {
        var removed = false;

        lock (_gate)
        {
            if (item.Node is not null)
            {
                _queue.Remove(item.Node);
                item.Node = null;
                _admitted--;
                removed = true;
            }
        }

        if (!removed)
        {
            return;
        }

        item.DisposeRegistration();
        item.CancelFromToken();
    }

    /// <summary>Returns and clears the idle waiter.</summary>
    /// <returns>The idle waiter, or null when no waiter is registered.</returns>
    private TaskCompletionSource<bool>? TakeIdleWaiter()
    {
        var idleWaiter = _idleWaiter;
        _idleWaiter = null;
        return idleWaiter;
    }

    /// <summary>Throws when the lane has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">The lane has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

    /// <summary>Stores work admitted to the serialized lane.</summary>
    private abstract class QueuedWorkItem
    {
        /// <summary>Gets or sets the linked-list node while this item is queued.</summary>
        internal abstract LinkedListNode<QueuedWorkItem>? Node { get; set; }

        /// <summary>Starts running this work item.</summary>
        internal abstract void Begin();

        /// <summary>Schedules this work item to run without chaining on the completing stack.</summary>
        internal abstract void Schedule();

        /// <summary>Registers cancellation for queued work.</summary>
        internal abstract void RegisterCancellation();

        /// <summary>Disposes the queued cancellation registration.</summary>
        internal abstract void DisposeRegistration();

        /// <summary>Cancels the item because the lane was disposed.</summary>
        internal abstract void CancelBecauseDisposed();

        /// <summary>Cancels the item with its caller token.</summary>
        internal abstract void CancelFromToken();

        /// <summary>Publishes any scheduler rejection stored while claiming the item.</summary>
        internal abstract void PublishSchedulerRejection();

        /// <summary>Tries to fault the item because the scheduler rejected it before work started.</summary>
        /// <param name="exception">The scheduler exception.</param>
        /// <returns><see langword="true"/> when the rejection claimed the item.</returns>
        internal abstract bool TryRejectSchedule(Exception exception);
    }

    /// <summary>Stores one typed work item.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    private sealed class QueuedWorkItem<T> : QueuedWorkItem
    {
        /// <summary>The item has not started and may still be rejected by the scheduler.</summary>
        private const int StartPending = 0;

        /// <summary>The item has started running.</summary>
        private const int StartRunning = 1;

        /// <summary>The item was rejected before it started running.</summary>
        private const int StartRejected = 2;

        /// <summary>Stores the owning lane.</summary>
        private readonly BoundedSerializedStreamWorkLane _owner;

        /// <summary>Stores the work delegate.</summary>
        private readonly Func<CancellationToken, ValueTask<T>> _work;

        /// <summary>Stores the caller cancellation token.</summary>
        private readonly CancellationToken _cancellationToken;

        /// <summary>Completes with the work result.</summary>
        private readonly TaskCompletionSource<T> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Stores the queued cancellation registration.</summary>
        private CancellationTokenRegistration _registration;

        /// <summary>Stores whether the work item was started or rejected by the scheduler.</summary>
        private int _startState;

        /// <summary>Stores the scheduler rejection when it claims the item before start.</summary>
        private Exception? _schedulerRejection;

        /// <summary>Initializes a new instance of the <see cref="QueuedWorkItem{T}"/> class.</summary>
        /// <param name="owner">The owning lane.</param>
        /// <param name="work">The work delegate.</param>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        internal QueuedWorkItem(
            BoundedSerializedStreamWorkLane owner,
            Func<CancellationToken, ValueTask<T>> work,
            CancellationToken cancellationToken)
        {
            _owner = owner;
            _work = work;
            _cancellationToken = cancellationToken;
        }

        /// <inheritdoc />
        internal override LinkedListNode<QueuedWorkItem>? Node { get; set; }

        /// <summary>Gets the task completed with the work result.</summary>
        internal Task<T> Task => _completion.Task;

        /// <inheritdoc />
        internal override void Begin()
        {
            if (Interlocked.CompareExchange(ref _startState, StartRunning, StartPending) != StartPending)
            {
                return;
            }

            DisposeRegistration();
            _ = RunAsync();
        }

        /// <inheritdoc />
        internal override void Schedule() => _owner._schedule(Begin);

        /// <inheritdoc />
        internal override void RegisterCancellation()
        {
            if (!_cancellationToken.CanBeCanceled)
            {
                return;
            }

#if NET8_0_OR_GREATER
            _registration = _cancellationToken.UnsafeRegister(CancelRegistered, this);
#else
            _registration = _cancellationToken.Register(CancelRegistered, this);
#endif
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal override void DisposeRegistration() => _registration.Dispose();

        /// <inheritdoc />
        internal override void CancelBecauseDisposed() =>
            _ = _completion.TrySetException(new ObjectDisposedException(nameof(BoundedSerializedStreamWorkLane)));

        /// <inheritdoc />
        internal override void CancelFromToken() => _ = _completion.TrySetCanceled(_cancellationToken);

        /// <inheritdoc />
        internal override void PublishSchedulerRejection()
        {
            if (_schedulerRejection is not { } exception)
            {
                return;
            }

            _ = _completion.TrySetException(exception);
        }

        /// <inheritdoc />
        internal override bool TryRejectSchedule(Exception exception)
        {
            if (Interlocked.CompareExchange(ref _startState, StartRejected, StartPending) != StartPending)
            {
                return false;
            }

            _schedulerRejection = exception;
            return true;
        }

        /// <summary>Cancels a queued item through its token registration.</summary>
        /// <param name="state">The queued item.</param>
        private static void CancelRegistered(object? state)
        {
            ArgumentExceptionHelper.ThrowIfNull(state);
            var item = (QueuedWorkItem<T>)state;
            item._owner.CancelQueued(item);
        }

        /// <summary>Runs the work and completes the result.</summary>
        /// <returns>The asynchronous operation.</returns>
        private async Task RunAsync()
        {
            try
            {
                _cancellationToken.ThrowIfCancellationRequested();
                var result = await _work(_cancellationToken).ConfigureAwait(false);
                _owner.Complete(this);
                _ = _completion.TrySetResult(result);
            }
            catch (OperationCanceledException exception) when (exception.CancellationToken == _cancellationToken)
            {
                _owner.Complete(this);
                _ = _completion.TrySetCanceled(_cancellationToken);
            }
            catch (Exception exception)
            {
                _owner.Complete(this);
                _ = _completion.TrySetException(exception);
            }
        }
    }
}
