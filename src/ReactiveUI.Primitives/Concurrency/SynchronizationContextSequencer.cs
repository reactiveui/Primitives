// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Sequencer that posts work through a <see cref="SynchronizationContext"/>.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class SynchronizationContextSequencer : ISequencer
{
    /// <summary>Schedules delayed dispatch without owning the scheduler lifetime.</summary>
    private readonly ISequencer _delaySequencer;

    /// <summary>Initializes a new instance of the <see cref="SynchronizationContextSequencer"/> class.</summary>
    /// <param name="context">The synchronization context used to schedule work.</param>
    /// <exception cref="ArgumentNullException"><paramref name="context"/> is <see langword="null"/>.</exception>
    public SynchronizationContextSequencer(SynchronizationContext context)
        : this(context, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SynchronizationContextSequencer"/> class that reads time and arms its delay timer through a <see cref="TimeProvider"/>.</summary>
    /// <param name="context">The synchronization context used to schedule work.</param>
    /// <param name="timeProvider">The provider supplying the current time, timestamps and the delay timer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="context"/> or <paramref name="timeProvider"/> is <see langword="null"/>.</exception>
    public SynchronizationContextSequencer(SynchronizationContext context, TimeProvider timeProvider)
        : this(context, new ThreadPoolSequencer(timeProvider))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SynchronizationContextSequencer"/> class.</summary>
    /// <param name="context">The synchronization context used to schedule work.</param>
    /// <param name="delaySequencer">The scheduler used to wait before posting delayed work.</param>
    /// <exception cref="ArgumentNullException">Either dependency is <see langword="null"/>.</exception>
    internal SynchronizationContextSequencer(SynchronizationContext context, ISequencer delaySequencer)
    {
        Context = context ?? throw new ArgumentNullException(nameof(context));
        _delaySequencer = delaySequencer ?? throw new ArgumentNullException(nameof(delaySequencer));
    }

    /// <summary>Gets a sequencer for the current synchronization context.</summary>
    /// <exception cref="InvalidOperationException">There is no current synchronization context.</exception>
    public static SynchronizationContextSequencer Current
    {
        get => new(SynchronizationContext.Current
            ?? throw new InvalidOperationException("There is no current synchronization context."));
    }

    /// <summary>Gets the synchronization context used to schedule work.</summary>
    public SynchronizationContext Context { get; }

    /// <summary>Gets the scheduler's notion of current time.</summary>
    public DateTimeOffset Now => _delaySequencer.Now;

    /// <summary>Gets the scheduler's monotonic timestamp.</summary>
    public long Timestamp => _delaySequencer.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Schedule(IWorkItem item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        Post(item);
    }

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        if (dueTimestamp <= Timestamp)
        {
            Schedule(item);
            return;
        }

        _delaySequencer.Schedule(new DelayedPostWorkItem(this, item), dueTimestamp);
    }

    /// <summary>Executes the work item unless it has been cancelled.</summary>
    /// <param name="item">Work item to execute.</param>
    internal static void ExecutePosted(IWorkItem item)
    {
        if (Sequencer.IsCancelled(item))
        {
            return;
        }

        item.Execute();
    }

    /// <summary>Posts a work item to the captured context.</summary>
    /// <param name="item">The callback state.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private void Post(IWorkItem item) => Context.Post(static state => ExecutePosted((IWorkItem)state!), item);

    /// <summary>Delayed post work item.</summary>
    /// <param name="owner">Owning sequencer.</param>
    /// <param name="item">Scheduled item.</param>
    private sealed class DelayedPostWorkItem(SynchronizationContextSequencer owner, IWorkItem item) : IWorkItem
    {
        /// <summary>Owning sequencer.</summary>
        private readonly SynchronizationContextSequencer _owner = owner;

        /// <summary>Scheduled item.</summary>
        private readonly IWorkItem _item = item;

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
