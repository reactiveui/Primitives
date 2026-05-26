// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using static ReactiveUI.Primitives.Disposables.Disposable;
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

            var cancelable = new BooleanDisposable();
            ThreadPool.QueueUserWorkItem(
                _ =>
            {
                if (cancelable.IsDisposed)
                {
                    return;
                }

                action(this, state);
            },
                null);

            return cancelable;
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
            var hasAdded = false;
            var hasRemoved = false;
            Timer timer = null!;
            timer = new(
                _ =>
            {
                lock (Gate)
                {
                    if (hasAdded && timer != null)
                    {
                        Timers.Remove(timer);
                    }

                    hasRemoved = true;
                }

                timer = null!;
                action(this, state);
            },
                null,
                dueTime1,
                TimeSpan.FromMilliseconds(-1.0));
            lock (Gate)
            {
                if (!hasRemoved)
                {
                    Timers.Add(timer, null!);
                    hasAdded = true;
                }
            }

            return new AnonymousDisposable(() =>
            {
                var key = timer;
                if (key != null)
                {
                    key.Dispose();
                    lock (Gate)
                    {
                        Timers.Remove(key);
                        hasRemoved = true;
                    }
                }

                timer = null!;
            });
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
    }
}
