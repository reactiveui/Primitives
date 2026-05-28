// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Concurrency
{
    /// <summary>
    /// ThreadPoolSequencer.
    /// </summary>
    /// <seealso cref="ISequencer" />
    [System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
    public sealed partial class ThreadPoolSequencer : ISequencer
    {
        /// <summary>
        /// Gets the shared thread-pool scheduler instance.
        /// </summary>
        public static readonly ThreadPoolSequencer Instance = new();

        /// <summary>
        /// Guards access to outstanding timers.
        /// </summary>
        internal static readonly object Gate = new();

        /// <summary>
        /// Keeps timers rooted until they fire or are cancelled.
        /// </summary>
        internal static readonly Dictionary<Timer, object> Timers = [];

        /// <summary>
        /// Initializes a new instance of the <see cref="ThreadPoolSequencer"/> class.
        /// </summary>
        private ThreadPoolSequencer()
        {
        }

        /// <summary>
        /// Gets the scheduler's notion of current time.
        /// </summary>
        public DateTimeOffset Now => Sequencer.Now;

        /// <summary>
        /// Schedules an action to be executed.
        /// </summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentNullException">action.</exception>
        public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            var workItem = new ScheduledWorkItem<TState>(this, state, action);
            workItem.Queue();
            return workItem;
        }

        /// <summary>
        /// Schedules an action to be executed after dueTime.
        /// </summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        /// <exception cref="ArgumentNullException">action.</exception>
        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            var dueTime1 = Sequencer.Normalize(dueTime);
            if (dueTime1 <= TimeSpan.Zero)
            {
                return Schedule(state, action);
            }

            var workItem = new ScheduledWorkItem<TState>(this, state, action);
            workItem.Queue(dueTime1);
            return workItem;
        }

        /// <summary>
        /// Schedules an action to be executed at dueTime.
        /// </summary>
        /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
        /// <param name="state">State passed to the action to be executed.</param>
        /// <param name="dueTime">Absolute time at which to execute the action.</param>
        /// <param name="action">Action to be executed.</param>
        /// <returns>
        /// The disposable object used to cancel the scheduled action (best effort).
        /// </returns>
        public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action) =>
            Schedule(state, Sequencer.Normalize(dueTime - Now), action);

        /// <summary>
        /// Thread-pool work item that doubles as the cancellation handle.
        /// </summary>
        /// <typeparam name="TState">The scheduled state type.</typeparam>
        private sealed class ScheduledWorkItem<TState> : IDisposable
        {
            /// <summary>
            /// Cached queue callback for immediate work.
            /// </summary>
            private static readonly WaitCallback ImmediateCallback = static state => ((ScheduledWorkItem<TState>)state!).Run();

            /// <summary>
            /// Cached timer callback for delayed work.
            /// </summary>
            private static readonly TimerCallback TimerCallback = static state => ((ScheduledWorkItem<TState>)state!).RunTimer();

            /// <summary>
            /// Owning sequencer.
            /// </summary>
            private readonly ThreadPoolSequencer _owner;

            /// <summary>
            /// Scheduled state.
            /// </summary>
            private readonly TState _state;

            /// <summary>
            /// Scheduled action.
            /// </summary>
            private readonly Func<ISequencer, TState, IDisposable> _action;

            /// <summary>
            /// Disposable returned by the scheduled action after it starts.
            /// </summary>
            private IDisposable? _disposable;

            /// <summary>
            /// Timer for delayed work.
            /// </summary>
#pragma warning disable CA2213 // Timer is disposed through RemoveTimer after an atomic exchange.
            private Timer? _timer;
#pragma warning restore CA2213

            /// <summary>
            /// Tracks cancellation.
            /// </summary>
            private int _isDisposed;

            /// <summary>
            /// Initializes a new instance of the <see cref="ScheduledWorkItem{TState}"/> class.
            /// </summary>
            /// <param name="owner">The owning sequencer.</param>
            /// <param name="state">The scheduled state.</param>
            /// <param name="action">The scheduled action.</param>
            internal ScheduledWorkItem(ThreadPoolSequencer owner, TState state, Func<ISequencer, TState, IDisposable> action)
            {
                _owner = owner;
                _state = state;
                _action = action;
            }

            /// <summary>
            /// Gets a value indicating whether the work item has been cancelled.
            /// </summary>
            private bool IsDisposed => Volatile.Read(ref _isDisposed) != 0;

            /// <summary>
            /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
            /// </summary>
            public void Dispose()
            {
                if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
                {
                    return;
                }

                Interlocked.Exchange(ref _disposable, Disposable.Empty)?.Dispose();
                RemoveTimer();
            }

            /// <summary>
            /// Queues the work item for immediate execution.
            /// </summary>
            internal void Queue() => ThreadPool.UnsafeQueueUserWorkItem(ImmediateCallback, this);

            /// <summary>
            /// Queues the work item for delayed execution.
            /// </summary>
            /// <param name="dueTime">The normalized due time.</param>
            internal void Queue(TimeSpan dueTime)
            {
                var timer = new Timer(TimerCallback, this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                _timer = timer;
                var disposeTimer = false;

                lock (Gate)
                {
                    if (IsDisposed)
                    {
                        disposeTimer = true;
                    }
                    else
                    {
                        Timers.Add(timer, this);
                    }
                }

                if (disposeTimer)
                {
                    Interlocked.CompareExchange(ref _timer, null, timer);
                    timer.Dispose();
                    return;
                }

                if (timer.Change(dueTime, Timeout.InfiniteTimeSpan))
                {
                    return;
                }

                Dispose();
            }

            /// <summary>
            /// Runs immediate work.
            /// </summary>
            private void Run()
            {
                if (IsDisposed)
                {
                    return;
                }

                var disposable = _action(_owner, _state) ?? Disposable.Empty;
                var previous = Interlocked.CompareExchange(ref _disposable, disposable, null);
                if (previous != null)
                {
                    disposable.Dispose();
                    return;
                }

                if (!IsDisposed)
                {
                    return;
                }

                disposable.Dispose();
            }

            /// <summary>
            /// Runs delayed work.
            /// </summary>
            private void RunTimer()
            {
                RemoveTimer();
                Run();
            }

            /// <summary>
            /// Unroots and disposes the delayed timer if present.
            /// </summary>
            private void RemoveTimer()
            {
                var timer = Interlocked.Exchange(ref _timer, null);
                if (timer == null)
                {
                    return;
                }

                lock (Gate)
                {
                    Timers.Remove(timer);
                }

                timer.Dispose();
            }
        }
    }
}
