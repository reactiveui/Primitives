// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Helper types for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Stores one participant registration.</summary>
    /// <param name="owner">The owning synchronization engine.</param>
    /// <param name="participant">The registered stream participant.</param>
    private sealed class ParticipantRegistration(SyncEngine owner, IOccasionallyConnectedStreamParticipant participant) : IDisposable
    {
        /// <summary>Protects receive cancellation ownership flags.</summary>
        private readonly Lock _receiveGate = new();

        /// <summary>Owns cancellation for receive work started by this registration.</summary>
        private CancellationTokenSource _receiveCancellation = new();

        /// <summary>Tracks registration disposal.</summary>
        private int _disposed;

        /// <summary>Tracks whether receive cancellation callbacks are currently executing.</summary>
        private bool _receiveCancelInProgress;

        /// <summary>Tracks whether receive cancellation disposal was requested while cancellation was in progress.</summary>
        private bool _receiveDisposeRequested;

        /// <summary>Tracks whether the receive cancellation source has been disposed.</summary>
        private bool _receiveDisposed;

        /// <summary>Tracks the current receive pump generation.</summary>
        private long _receiveGeneration;

        /// <summary>Tracks the shared-session generation currently leased by this receive pump.</summary>
        private long _activeSharedReceiveGeneration;

        /// <summary>Tracks the current receive cancellation callback drain.</summary>
        private TaskCompletionSource<bool>? _receiveCancellationDrain;

        /// <summary>Gets the participant.</summary>
        internal IOccasionallyConnectedStreamParticipant Participant { get; } = participant;

        /// <summary>Gets or sets a value indicating whether remote work is active for this stream.</summary>
        internal bool RemoteActive { get; set; } = true;

        /// <summary>Gets or sets the currently tracked receive pump task.</summary>
        internal Task? ReceiveTask { get; set; }

        /// <summary>Gets or sets a value indicating whether receive should restart after the current pump exits.</summary>
        internal bool ReceiveRestartRequested { get; set; }

        /// <summary>Gets or sets the receive generation assigned to <see cref="ReceiveTask"/>.</summary>
        internal long ReceiveTaskGeneration { get; set; }

        /// <summary>Gets the active shared receive generation, or zero for an owned retry session.</summary>
        internal long ActiveSharedReceiveGeneration => Volatile.Read(ref _activeSharedReceiveGeneration);

        /// <summary>Gets or sets the receive task being stopped by an explicit stream stop.</summary>
        internal Task? StopReceiveTask { get; set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            owner.Unregister(this);
        }

        /// <summary>Records a shared receive lease transition.</summary>
        /// <param name="generation">The shared generation, or zero after release.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetActiveSharedReceiveGeneration(long generation) => Volatile.Write(ref _activeSharedReceiveGeneration, generation);

        /// <summary>Cancels receive work owned by this registration.</summary>
        /// <returns>The cancellation callback failure, if cancellation throws.</returns>
        internal Exception? CancelReceive()
        {
            var lease = BeginCancelReceive(out _);
            return lease is null ? null : CompleteCancelReceive(lease.Value);
        }

        /// <summary>Captures the current receive cancellation source without invoking cancellation callbacks.</summary>
        /// <param name="drainTask">The active cancellation callback drain task.</param>
        /// <returns>The captured cancellation ownership, if cancellation can start.</returns>
        internal (
            long Generation,
            CancellationTokenSource Source,
            TaskCompletionSource<bool> DrainCompletion)? BeginCancelReceive(out Task? drainTask)
        {
            lock (_receiveGate)
            {
                drainTask = _receiveCancellationDrain?.Task;
                if (_receiveDisposed || _receiveCancelInProgress)
                {
                    return null;
                }

                _receiveCancelInProgress = true;
                var drainCompletion = CreateCompletion();
                _receiveCancellationDrain = drainCompletion;
                drainTask = drainCompletion.Task;
                return new(_receiveGeneration, _receiveCancellation, drainCompletion);
            }
        }

        /// <summary>Cancels a previously captured receive cancellation source.</summary>
        /// <param name="lease">The captured cancellation ownership.</param>
        /// <returns>The cancellation callback failure, if cancellation throws.</returns>
        internal Exception? CompleteCancelReceive(
            (long Generation, CancellationTokenSource Source, TaskCompletionSource<bool> DrainCompletion) lease)
        {
            Exception? failure = null;
            try
            {
                lease.Source.Cancel();
            }
            catch (Exception exception)
            {
                failure = exception;
            }

            var disposeAfterCancel = false;
            lock (_receiveGate)
            {
                // BeginReceivePump cannot replace the source while cancellation owns this generation.
                _receiveCancelInProgress = false;
                disposeAfterCancel = _receiveDisposeRequested;
                if (disposeAfterCancel)
                {
                    _receiveDisposed = true;
                }
            }

            _ = lease.DrainCompletion.TrySetResult(true);
            if (!disposeAfterCancel)
            {
                return failure;
            }

            lease.Source.Dispose();
            return failure;
        }

        /// <summary>Captures immutable cancellation ownership for a new receive pump.</summary>
        /// <param name="previousCancellation">The replaced cancellation source that must be disposed outside the engine gate.</param>
        /// <returns>The receive pump lease, if receive can start.</returns>
        internal (long Generation, CancellationToken Token)? BeginReceivePump(out CancellationTokenSource? previousCancellation)
        {
            previousCancellation = null;
            lock (_receiveGate)
            {
                if (_receiveDisposed || _receiveCancelInProgress)
                {
                    return null;
                }

                if (_receiveCancellation.IsCancellationRequested)
                {
                    previousCancellation = _receiveCancellation;
                    _receiveCancellation = new();
                    _receiveCancellationDrain = null;
                    _receiveDisposeRequested = false;
                }

                _receiveGeneration++;
                return new(_receiveGeneration, _receiveCancellation.Token);
            }
        }

        /// <summary>Releases the receive cancellation source.</summary>
        internal void DisposeReceiveCancellation()
        {
            lock (_receiveGate)
            {
                if (_receiveDisposed)
                {
                    return;
                }

                if (_receiveCancelInProgress)
                {
                    _receiveDisposeRequested = true;
                    return;
                }

                _receiveDisposed = true;
            }

            _receiveCancellation.Dispose();
        }
    }

    /// <summary>Bounds one class of retained producer work under the engine gate.</summary>
    /// <param name="maxCount">The maximum number of reservations.</param>
    /// <param name="maxRetainedBytes">The maximum retained bytes across reservations.</param>
    private sealed class CapacityBudget(int maxCount, long maxRetainedBytes)
    {
        /// <summary>The number of active reservations.</summary>
        private int _count;

        /// <summary>The retained bytes charged to active reservations.</summary>
        private long _retainedBytes;

        /// <summary>Reserves bounded retained work.</summary>
        /// <param name="retainedBytes">The retained byte charge.</param>
        /// <exception cref="QueueCapacityExceededException">The shared count or byte limit would be exceeded.</exception>
        internal void Reserve(long retainedBytes)
        {
            if (_count >= maxCount)
            {
                throw new QueueCapacityExceededException("The synchronization engine producer count limit has been reached.", true);
            }

            if (retainedBytes > maxRetainedBytes - _retainedBytes)
            {
                throw new QueueCapacityExceededException("The synchronization engine retained producer byte limit has been reached.", true);
            }

            _count++;
            _retainedBytes += retainedBytes;
        }

        /// <summary>Releases retained work exactly once under the engine gate.</summary>
        /// <param name="retainedBytes">The retained byte charge.</param>
        internal void Release(long retainedBytes)
        {
            _count--;
            _retainedBytes -= retainedBytes;
        }
    }

    /// <summary>Stores capacity-release generation and bounded waiters for one stream.</summary>
    /// <param name="budget">The waiter budget shared by all registered streams.</param>
    private sealed class CapacitySignal(CapacityBudget budget)
    {
        /// <summary>Gets the unique registration identity for this signal.</summary>
        internal Guid Id { get; } = Guid.NewGuid();

        /// <summary>Gets the waiting producers.</summary>
        internal LinkedList<CapacityWaiter> Waiters { get; } = [];

        /// <summary>Gets or sets the release generation.</summary>
        internal long Generation { get; set; }

        /// <summary>Reserves one bounded waiter.</summary>
        /// <param name="retainedBytes">The retained bytes.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReserveWaiter(long retainedBytes) => budget.Reserve(retainedBytes);

        /// <summary>Releases one waiter's retained bytes.</summary>
        /// <param name="retainedBytes">The retained bytes.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseWaiter(long retainedBytes) => budget.Release(retainedBytes);

        /// <summary>Takes and clears all waiters.</summary>
        /// <returns>The waiters.</returns>
        internal CapacityWaiter[] TakeWaiters()
        {
            var result = new CapacityWaiter[Waiters.Count];
            var index = 0;
            for (var node = Waiters.First; node is not null; node = node.Next)
            {
                result[index] = node.Value;
                result[index].Node = null;
                ReleaseWaiter(result[index].RetainedBytes);
                index++;
            }

            Waiters.Clear();
            return result;
        }
    }

    /// <summary>Stores one blocked capacity waiter.</summary>
    /// <param name="owner">The owning synchronization engine.</param>
    /// <param name="signal">The capacity signal containing this waiter.</param>
    /// <param name="retainedBytes">The retained bytes charged while waiting.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    private sealed class CapacityWaiter(
        SyncEngine owner,
        CapacitySignal signal,
        long retainedBytes,
        CancellationToken cancellationToken)
    {
        /// <summary>Completes when capacity may be available.</summary>
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Stores the cancellation registration.</summary>
        private CancellationTokenRegistration _registration;

        /// <summary>Gets or sets the linked-list node while registered.</summary>
        internal LinkedListNode<CapacityWaiter>? Node { get; set; }

        /// <summary>Gets the retained bytes.</summary>
        internal long RetainedBytes => retainedBytes;

        /// <summary>Gets the wait task.</summary>
        internal Task Task => _completion.Task;

        /// <summary>Registers caller cancellation after the waiter is linked.</summary>
        internal void RegisterCancellation()
        {
#if NET8_0_OR_GREATER
            var registration = cancellationToken.UnsafeRegister(
                static state =>
                {
                    ArgumentExceptionHelper.ThrowIfNull(state);
                    ((CapacityWaiter)state).Cancel();
                },
                this);
#else
            var registration = cancellationToken.Register(
                static state =>
                {
                    ArgumentExceptionHelper.ThrowIfNull(state);
                    ((CapacityWaiter)state).Cancel();
                },
                this);
#endif
            lock (owner._gate)
            {
                if (Node is not null)
                {
                    _registration = registration;
                    return;
                }
            }

            registration.Dispose();
        }

        /// <summary>Releases the waiter successfully.</summary>
        internal void Release()
        {
            _registration.Dispose();
            _ = _completion.TrySetResult(true);
        }

        /// <summary>Releases the waiter with an error.</summary>
        /// <param name="exception">The error.</param>
        internal void Release(Exception exception)
        {
            _registration.Dispose();
            _ = _completion.TrySetException(exception);
        }

        /// <summary>Cancels the waiter.</summary>
        private void Cancel()
        {
            lock (owner._gate)
            {
                if (Node is null)
                {
                    return;
                }

                signal.Waiters.Remove(Node);
                Node = null;
                signal.ReleaseWaiter(retainedBytes);
            }

            _ = _completion.TrySetCanceled(cancellationToken);
            _registration.Dispose();
        }
    }

    /// <summary>Maintains a finite observer list for one engine observable.</summary>
    /// <typeparam name="T">The observable value type.</typeparam>
    private sealed class BoundedObserverRegistry<T> : IObservable<T>, IDisposable
    {
        /// <summary>Bounds each observer's pending notifications.</summary>
        private static readonly ObserverNotificationSubscriptionOptions NotificationOptions =
            new(256, 4 * 1024 * 1024, ObserverNotificationOverflowMode.CoalesceLatest);

        /// <summary>Protects subscription insertion, publication order, and replay state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Delivers callbacks through isolated bounded observer queues.</summary>
        private readonly ObserverNotificationDispatcher<T> _dispatcher;

        /// <summary>Tracks configured sequencer work through execution.</summary>
        private readonly TrackingScheduler _trackingScheduler;

        /// <summary>The maximum number of active subscriptions.</summary>
        private readonly int _capacity;

        /// <summary>Charges the actual retained diagnostic value.</summary>
        private readonly Func<T, long> _sizeOf;

        /// <summary>Whether this registry holds a replayable latest state.</summary>
        private readonly bool _latestState;

        /// <summary>Holds at most one delayed drain per active subscription when all worker slots are occupied.</summary>
        private readonly Queue<Action> _deferredSchedules = new();

        /// <summary>The last state accepted by the dispatcher and its retained byte charge.</summary>
        private LatestValue? _latest;

        /// <summary>Counts unique drain reservations until both scheduling and execution finish.</summary>
        private int _outstandingSchedules;

        /// <summary>Tracks disposal.</summary>
        private bool _disposed;

        /// <summary>Initializes a new instance of the <see cref="BoundedObserverRegistry{T}"/> class.</summary>
        /// <param name="capacity">The maximum number of active subscriptions.</param>
        /// <param name="scheduler">The configured observer scheduler.</param>
        /// <param name="latestState">Whether this registry replays its latest state.</param>
        /// <param name="sizeOf">The retained byte estimator for this diagnostic type.</param>
        /// <param name="reportFault">The optional observer callback fault reporter.</param>
        internal BoundedObserverRegistry(
            int capacity,
            IObserverNotificationScheduler scheduler,
            bool latestState,
            Func<T, long> sizeOf,
            Action<Exception>? reportFault = null)
        {
            _capacity = capacity;
            _latestState = latestState;
            _sizeOf = sizeOf;
            _trackingScheduler = new(scheduler);
            _dispatcher = new(_trackingScheduler, reportFault);
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);
            IDisposable subscription;
            List<Action> schedules = [];
            lock (_gate)
            {
                ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
                if (_dispatcher.SubscriptionCount >= _capacity || _outstandingSchedules + _deferredSchedules.Count >= _capacity)
                {
                    throw new InvalidOperationException("The synchronization engine diagnostic subscription limit has been reached.");
                }

                subscription = _latestState && _latest is { } replay
                    ? _dispatcher.SubscribeDeferred(
                        observer,
                        NotificationOptions,
                        true,
                        _ => new ValueTask<T>(replay.Value),
                        replay.SizeBytes,
                        schedules)
                    : _dispatcher.Subscribe(observer, NotificationOptions);

                ReserveOrDeferSchedules(schedules);
            }

            ScheduleOffStack(schedules);

            return subscription;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
                _latest = null;
                _deferredSchedules.Clear();
            }

            _dispatcher.Dispose();
        }

        /// <summary>Publishes one value to current observers.</summary>
        /// <param name="value">The value.</param>
        internal void Publish(T value)
        {
            List<Action> schedules = [];
            lock (_gate)
            {
                if (_disposed)
                {
                    return;
                }

                var sizeBytes = _sizeOf(value);
                if (_latestState)
                {
                    _latest = new(value, sizeBytes);
                    _ = _dispatcher.PublishLatestDeferred(_ => new ValueTask<T>(value), sizeBytes, schedules);
                }
                else
                {
                    _ = _dispatcher.PublishEventDeferred(_ => new ValueTask<T>(value), sizeBytes, schedules);
                }

                ReserveOrDeferSchedules(schedules);
            }

            ScheduleOffStack(schedules);
        }

        /// <summary>Reserves available worker slots and retains excess drains in the finite subscription queues.</summary>
        /// <param name="schedules">The newly requested drain actions, reduced to those ready to start.</param>
        private void ReserveOrDeferSchedules(List<Action> schedules)
        {
            var ready = 0;
            for (var index = 0; index < schedules.Count; index++)
            {
                if (_outstandingSchedules >= _capacity)
                {
                    _deferredSchedules.Enqueue(schedules[index]);
                    continue;
                }

                _outstandingSchedules++;
                schedules[ready] = schedules[index];
                ready++;
            }

            schedules.RemoveRange(ready, schedules.Count - ready);
        }

        /// <summary>Schedules each newly active subscription drain independently after leaving the owner lock.</summary>
        /// <param name="schedules">The bounded set of new subscription drains.</param>
        private void ScheduleOffStack(List<Action> schedules)
        {
            foreach (var schedule in schedules)
            {
                var lease = new TrackingScheduler.ScheduleLease(this);
                try
                {
                    _ = Task.Run(() => TrackingScheduler.ScheduleReserved(schedule, lease));
                }
                catch
                {
                    lease.CompleteScheduleCall();
                    throw;
                }
            }
        }

        /// <summary>Starts one already reserved drain after a completed lease opens capacity.</summary>
        /// <param name="schedule">The deferred action, if one was waiting.</param>
        private void ScheduleNextDeferred(Action? schedule)
        {
            if (schedule is not null)
            {
                ScheduleOffStack([schedule]);
            }
        }

        /// <summary>Retains one reservation until scheduling and configured work execution both finish.</summary>
        /// <param name="scheduler">The configured sequencer scheduler.</param>
        /// <remarks>
        /// This registry queues only already completed value factories. The dispatcher therefore finishes each
        /// synchronous observer drain before its work item's Execute method returns.
        /// </remarks>
        private sealed class TrackingScheduler(IObserverNotificationScheduler scheduler) : IObserverNotificationScheduler
        {
            /// <summary>The lease being handed to the configured scheduler on this worker thread.</summary>
            [ThreadStatic]
            private static ScheduleLease? _currentLease;

            /// <inheritdoc />
            public void Schedule(IWorkItem item)
            {
                var lease = _currentLease ?? throw new InvalidOperationException("A diagnostic drain must own a scheduling reservation.");
                lease.MarkScheduled();
                var tracked = new TrackedWorkItem(lease, item);
                try
                {
                    scheduler.Schedule(tracked);
                }
                catch
                {
                    lease.CompleteWork();
                    throw;
                }
            }

            /// <summary>Schedules a pre-reserved dispatcher action away from the publisher stack.</summary>
            /// <param name="schedule">The deferred dispatcher action.</param>
            /// <param name="lease">The unique bounded scheduling reservation.</param>
            internal static void ScheduleReserved(Action schedule, ScheduleLease lease)
            {
                var previous = _currentLease;
                _currentLease = lease;
                try
                {
                    schedule();
                }
                finally
                {
                    _currentLease = previous;
                    lease.CompleteScheduleCall();
                }
            }

            /// <summary>Completes one bounded reservation after both halves of scheduler ownership end.</summary>
            /// <param name="owner">The owning registry.</param>
            internal sealed class ScheduleLease(BoundedObserverRegistry<T> owner)
            {
                /// <summary>Whether the deferred action entered the configured scheduler.</summary>
                private bool _scheduled;

                /// <summary>Whether the scheduling call has returned.</summary>
                private bool _scheduleReturned;

                /// <summary>Whether the configured work item has executed or been rejected.</summary>
                private bool _workFinished;

                /// <summary>Whether the registry reservation has been released.</summary>
                private bool _released;

                /// <summary>Marks the handoff to the configured scheduler.</summary>
                internal void MarkScheduled()
                {
                    lock (owner._gate)
                    {
                        _scheduled = true;
                    }
                }

                /// <summary>Marks the scheduling call complete, including a no-op or rejection.</summary>
                internal void CompleteScheduleCall()
                {
                    Action? next;
                    lock (owner._gate)
                    {
                        _scheduleReturned = true;
                        if (!_scheduled)
                        {
                            _workFinished = true;
                        }

                        next = ReleaseIfComplete();
                    }

                    owner.ScheduleNextDeferred(next);
                }

                /// <summary>Marks the configured work item complete or rejected.</summary>
                internal void CompleteWork()
                {
                    Action? next;
                    lock (owner._gate)
                    {
                        _workFinished = true;
                        next = ReleaseIfComplete();
                    }

                    owner.ScheduleNextDeferred(next);
                }

                /// <summary>Releases the single reservation after both ownership phases complete.</summary>
                /// <returns>The next deferred drain, if this release opens a slot.</returns>
                private Action? ReleaseIfComplete()
                {
                    if (_released || !_scheduleReturned || !_workFinished)
                    {
                        return null;
                    }

                    _released = true;
                    owner._outstandingSchedules--;
                    if (owner._disposed || owner._deferredSchedules.Count == 0)
                    {
                        return null;
                    }

                    owner._outstandingSchedules++;
                    return owner._deferredSchedules.Dequeue();
                }
            }

            /// <summary>Releases scheduler ownership after work executes.</summary>
            /// <param name="lease">The unique scheduling lease.</param>
            /// <param name="work">The dispatched subscription drain.</param>
            private sealed class TrackedWorkItem(ScheduleLease lease, IWorkItem work) : IWorkItem
            {
                /// <inheritdoc />
                public void Execute()
                {
                    try
                    {
                        work.Execute();
                    }
                    finally
                    {
                        lease.CompleteWork();
                    }
                }
            }
        }

        /// <summary>Stores one immutable replay value and its byte charge.</summary>
        /// <param name="Value">The replay value.</param>
        /// <param name="SizeBytes">The retained byte size.</param>
        private sealed record LatestValue(T Value, long SizeBytes);
    }
}
