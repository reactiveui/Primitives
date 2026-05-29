// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// TaskPoolSequencer.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed partial class TaskPoolSequencer : ISequencer
{
    /// <summary>
    /// Task factory used to schedule asynchronous work.
    /// </summary>
    private readonly TaskFactory _taskFactory;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskPoolSequencer"/> class.
    /// </summary>
    /// <param name="taskFactory">The task factory.</param>
    public TaskPoolSequencer(TaskFactory taskFactory) => _taskFactory = taskFactory ?? throw new ArgumentNullException(nameof(taskFactory));

    /// <summary>
    /// Gets the instance.
    /// </summary>
    /// <value>
    /// The instance.
    /// </value>
    public static TaskPoolSequencer Instance { get; } = new(Task.Factory);

    /// <summary>
    /// Gets the default task-pool scheduler.
    /// </summary>
    public static TaskPoolSequencer Default => Instance;

    /// <summary>
    /// Gets or sets the unhandled exception handler used by task-pool work.
    /// </summary>
    public Action<Exception>? UnhandledExceptionHandler { get; set; }

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>
    /// Gets the scheduler's monotonic timestamp.
    /// </summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>
    /// Schedules a work item to be executed through the task factory.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

#pragma warning disable CA2008 // The caller supplied the factory; preserving its scheduler is intentional.
        _taskFactory.StartNew(static state => ((DispatchState)state!).Run(), new DispatchState(this, item));
#pragma warning restore CA2008
    }

    /// <summary>
    /// Schedules a work item to be executed through the task factory at a monotonic timestamp.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (dueTimestamp <= Timestamp)
        {
            Schedule(item);
            return;
        }

        ThreadPoolSequencer.Instance.Schedule(new DelayedDispatchWorkItem(this, item), dueTimestamp);
    }

    /// <summary>
    /// Executes a work item and routes unhandled exceptions.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    private void Execute(IWorkItem item)
    {
        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        try
        {
            item.Execute();
        }
        catch (Exception ex)
        {
            var handler = UnhandledExceptionHandler;
            if (handler != null)
            {
                handler(ex);
                return;
            }

            ExceptionDispatchInfo.Capture(ex).Throw();
        }
    }

    /// <summary>
    /// Task factory dispatch state.
    /// </summary>
    private sealed class DispatchState
    {
        /// <summary>
        /// Owning sequencer.
        /// </summary>
        private readonly TaskPoolSequencer _owner;

        /// <summary>
        /// Work item to execute.
        /// </summary>
        private readonly IWorkItem _item;

        /// <summary>
        /// Initializes a new instance of the <see cref="DispatchState"/> class.
        /// </summary>
        /// <param name="owner">Owning sequencer.</param>
        /// <param name="item">Work item to execute.</param>
        public DispatchState(TaskPoolSequencer owner, IWorkItem item)
        {
            _owner = owner;
            _item = item;
        }

        /// <summary>
        /// Runs the work item.
        /// </summary>
        public void Run() => _owner.Execute(_item);
    }

    /// <summary>
    /// Work item that switches delayed work from the thread pool onto the task factory.
    /// </summary>
    private sealed class DelayedDispatchWorkItem : IWorkItem
    {
        /// <summary>
        /// Owning sequencer.
        /// </summary>
        private readonly TaskPoolSequencer _owner;

        /// <summary>
        /// Work item to execute.
        /// </summary>
        private readonly IWorkItem _item;

        /// <summary>
        /// Initializes a new instance of the <see cref="DelayedDispatchWorkItem"/> class.
        /// </summary>
        /// <param name="owner">Owning sequencer.</param>
        /// <param name="item">Work item to execute.</param>
        public DelayedDispatchWorkItem(TaskPoolSequencer owner, IWorkItem item)
        {
            _owner = owner;
            _item = item;
        }

        /// <inheritdoc/>
        public void Execute()
        {
            if (Sequencer.IsCancelled(_item))
            {
                return;
            }

            _owner.Schedule(_item);
        }
    }
}
