// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Efficient scheduler queue that maintains scheduled items sorted by absolute time.</summary>
/// <typeparam name="TAbsolute">Absolute time representation type.</typeparam>
/// <remarks>This type is not thread safe; users should ensure proper synchronization.</remarks>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class SequencerQueue<TAbsolute>
    where TAbsolute : IComparable<TAbsolute>
{
    /// <summary>Default initial capacity for scheduler queues.</summary>
    private const int DefaultCapacity = 4;

    /// <summary>Priority queue storing scheduled work.</summary>
    private readonly PriorityQueue<ScheduledItem<TAbsolute>> _queue;

    /// <summary>Initializes a new instance of the <see cref="SequencerQueue{TAbsolute}"/> class. Creates a new scheduler queue with a default initial capacity.</summary>
    public SequencerQueue()
        : this(DefaultCapacity)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SequencerQueue{TAbsolute}"/> class. Creates a new scheduler queue with the specified initial capacity.</summary>
    /// <param name="capacity">Initial capacity of the scheduler queue.</param>
    /// <exception cref="ArgumentOutOfRangeExceptionHelper"><paramref name="capacity"/> is less than zero.</exception>
    public SequencerQueue(int capacity)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(capacity);

        _queue = new(capacity);
    }

    /// <summary>Gets the number of scheduled items in the scheduler queue.</summary>
    public int Count => _queue.Count;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Enqueues the specified work item to be scheduled.</summary>
    /// <param name="scheduledItem">Work item to be scheduled.</param>
    public void Enqueue(ScheduledItem<TAbsolute> scheduledItem) => _queue.Enqueue(scheduledItem);

    /// <summary>Removes the specified work item from the scheduler queue.</summary>
    /// <param name="scheduledItem">Work item to be removed from the scheduler queue.</param>
    /// <returns><c>true</c> if the item was found; <c>false</c> otherwise.</returns>
    public bool Remove(ScheduledItem<TAbsolute> scheduledItem)
    {
        if (_queue.Count == 0)
        {
            return false;
        }

        if (ReferenceEquals(_queue.Peek(), scheduledItem))
        {
            _queue.Dequeue();
            return true;
        }

        return _queue.Remove(scheduledItem);
    }

    /// <summary>Dequeues the next work item from the scheduler queue.</summary>
    /// <returns>Next work item in the scheduler queue (removed).</returns>
    public ScheduledItem<TAbsolute> Dequeue() => _queue.Dequeue();

    /// <summary>Peeks the next work item in the scheduler queue.</summary>
    /// <returns>Next work item in the scheduler queue (not removed).</returns>
    public ScheduledItem<TAbsolute> Peek() => _queue.Peek();
}
