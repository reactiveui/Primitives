// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Sequencer that posts work through a <see cref="SynchronizationContext"/>.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class SynchronizationContextSequencer : ISequencer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SynchronizationContextSequencer"/> class.
    /// </summary>
    /// <param name="context">The synchronization context used to schedule work.</param>
    /// <exception cref="ArgumentNullException"><paramref name="context"/> is <see langword="null"/>.</exception>
    public SynchronizationContextSequencer(SynchronizationContext context) =>
        Context = context ?? throw new ArgumentNullException(nameof(context));

    /// <summary>
    /// Gets a sequencer for the current synchronization context.
    /// </summary>
    /// <exception cref="InvalidOperationException">There is no current synchronization context.</exception>
    public static SynchronizationContextSequencer Current =>
        new(SynchronizationContext.Current ?? throw new InvalidOperationException("There is no current synchronization context."));

    /// <summary>
    /// Gets the synchronization context used to schedule work.
    /// </summary>
    public SynchronizationContext Context { get; }

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => Sequencer.Now;

    /// <summary>
    /// Gets the scheduler's monotonic timestamp.
    /// </summary>
    public long Timestamp => Sequencer.Timestamp;

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Schedule(IWorkItem item)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        Context.Post(static state => ExecutePosted((IWorkItem)state!), item);
    }

    /// <inheritdoc/>
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

        ThreadPoolSequencer.Instance.Schedule(new DelayedPostWorkItem(this, item), dueTimestamp);
    }

    /// <summary>
    /// Executes work when it has not already been cancelled.
    /// </summary>
    /// <param name="item">Work item to execute.</param>
    private static void ExecutePosted(IWorkItem item)
    {
        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }

    /// <summary>
    /// Delayed post work item.
    /// </summary>
    private sealed class DelayedPostWorkItem : IWorkItem
    {
        /// <summary>
        /// Owning sequencer.
        /// </summary>
        private readonly SynchronizationContextSequencer _owner;

        /// <summary>
        /// Scheduled item.
        /// </summary>
        private readonly IWorkItem _item;

        /// <summary>
        /// Initializes a new instance of the <see cref="DelayedPostWorkItem"/> class.
        /// </summary>
        /// <param name="owner">Owning sequencer.</param>
        /// <param name="item">Scheduled item.</param>
        public DelayedPostWorkItem(SynchronizationContextSequencer owner, IWorkItem item)
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
