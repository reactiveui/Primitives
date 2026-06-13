// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>Binary heap priority queue that preserves insertion order for equal-priority items.</summary>
/// <typeparam name="T">The queued item type.</typeparam>
public sealed class PriorityQueue<T>
    where T : IComparable<T>
{
    /// <summary>Default queue capacity.</summary>
    private const int DefaultCapacity = 16;

    /// <summary>Number of children per heap node.</summary>
    private const int HeapBranchingFactor = 2;

    /// <summary>Offset from a node's doubled index to its left child.</summary>
    private const int LeftChildOffset = 1;

    /// <summary>Offset from a node's doubled index to its right child.</summary>
    private const int RightChildOffset = 2;

    /// <summary>Capacity divisor used to shrink sparse queues.</summary>
    private const int ShrinkDivisor = 4;

    /// <summary>Monotonic tie-breaker for equal-priority items.</summary>
    private long _count = long.MinValue;

    /// <summary>Heap storage.</summary>
    private IndexedItem[] _items;

    /// <summary>Initializes a new instance of the <see cref="PriorityQueue{T}"/> class.</summary>
    public PriorityQueue()
        : this(DefaultCapacity)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PriorityQueue{T}"/> class.</summary>
    /// <param name="capacity">Initial queue capacity.</param>
    public PriorityQueue(int capacity)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(capacity);

        _items = new IndexedItem[Math.Max(1, capacity)];
        Count = 0;
    }

    /// <summary>Gets the number of queued items.</summary>
    public int Count { get; private set; }

    /// <summary>Removes and returns the highest-priority item.</summary>
    /// <returns>The highest-priority item.</returns>
    public T Dequeue()
    {
        var result = Peek();
        RemoveAt(0);
        return result;
    }

    /// <summary>Adds an item to the queue.</summary>
    /// <param name="item">Item to enqueue.</param>
    public void Enqueue(T item)
    {
        if (Count >= _items.Length)
        {
            var temp = _items;
            _items = new IndexedItem[Math.Max(DefaultCapacity, _items.Length * HeapBranchingFactor)];
            Array.Copy(temp, _items, temp.Length);
        }

        var index = Count++;
        _items[index] = new() { Value = item, Id = ++_count };
        Percolate(index);
    }

    /// <summary>Returns the highest-priority item without removing it.</summary>
    /// <returns>The highest-priority item.</returns>
    public T Peek()
    {
        if (Count == 0)
        {
            throw new InvalidOperationException("Heap is empty.");
        }

        return _items[0].Value;
    }

    /// <summary>Removes a matching item from the queue.</summary>
    /// <param name="item">Item to remove.</param>
    /// <returns><see langword="true"/> when the item was found and removed; otherwise, <see langword="false"/>.</returns>
    public bool Remove(T item)
    {
        for (var i = 0; i < Count; ++i)
        {
            if (EqualityComparer<T>.Default.Equals(_items[i].Value, item))
            {
                RemoveAt(i);
                return true;
            }
        }

        return false;
    }

    /// <summary>Restores heap order from the supplied index downward.</summary>
    /// <param name="index">Index to heapify.</param>
    private void Heapify(int index)
    {
        if (index >= Count || index < 0)
        {
            return;
        }

        while (true)
        {
            var left = (HeapBranchingFactor * index) + LeftChildOffset;
            var right = (HeapBranchingFactor * index) + RightChildOffset;
            var first = index;

            if (left < Count && IsHigherPriority(left, first))
            {
                first = left;
            }

            if (right < Count && IsHigherPriority(right, first))
            {
                first = right;
            }

            if (first == index)
            {
                break;
            }

            // swap index and first
            (_items[first], _items[index]) = (_items[index], _items[first]);
            index = first;
        }
    }

    /// <summary>Determines whether the left index has higher priority than the right index.</summary>
    /// <param name="left">Candidate item index.</param>
    /// <param name="right">Current item index.</param>
    /// <returns><see langword="true"/> when the left item should be ordered before the right item.</returns>
    private bool IsHigherPriority(int left, int right) => _items[left].CompareTo(_items[right]) < 0;

    /// <summary>Restores heap order from the supplied index upward.</summary>
    /// <param name="index">Index to percolate.</param>
    /// <returns>The final index of the percolated item.</returns>
    private int Percolate(int index)
    {
        if (index >= Count || index < 0)
        {
            return index;
        }

        var parent = (index - 1) / HeapBranchingFactor;
        while (parent >= 0 && parent != index && IsHigherPriority(index, parent))
        {
            // swap index and parent
            (_items[parent], _items[index]) = (_items[index], _items[parent]);
            index = parent;
            parent = (index - 1) / HeapBranchingFactor;
        }

        return index;
    }

    /// <summary>Removes the item at the supplied index.</summary>
    /// <param name="index">Index to remove.</param>
    private void RemoveAt(int index)
    {
        _items[index] = _items[--Count];
        _items[Count] = default;

        if (Percolate(index) == index)
        {
            Heapify(index);
        }

        if (_items.Length <= DefaultCapacity || Count >= _items.Length / ShrinkDivisor)
        {
            return;
        }

        var temp = _items;
        _items = new IndexedItem[Math.Max(DefaultCapacity, _items.Length / HeapBranchingFactor)];
        Array.Copy(temp, 0, _items, 0, Count);
    }

    /// <summary>Heap item with an insertion-order tie-breaker.</summary>
    internal readonly record struct IndexedItem : IComparable<IndexedItem>
    {
        /// <summary>Gets or sets the insertion order id.</summary>
        public long Id { get; init; }

        /// <summary>Gets or sets the queued value.</summary>
        public T Value { get; init; }

        /// <summary>Compares two indexed items.</summary>
        /// <param name="left">The left item.</param>
        /// <param name="right">The right item.</param>
        /// <returns><see langword="true"/> when <paramref name="left"/> is lower than <paramref name="right"/>.</returns>
        public static bool operator <(IndexedItem left, IndexedItem right) => left.CompareTo(right) < 0;

        /// <summary>Compares two indexed items.</summary>
        /// <param name="left">The left item.</param>
        /// <param name="right">The right item.</param>
        /// <returns><see langword="true"/> when <paramref name="left"/> is lower than or equal to <paramref name="right"/>.</returns>
        public static bool operator <=(IndexedItem left, IndexedItem right) => left.CompareTo(right) <= 0;

        /// <summary>Compares two indexed items.</summary>
        /// <param name="left">The left item.</param>
        /// <param name="right">The right item.</param>
        /// <returns><see langword="true"/> when <paramref name="left"/> is greater than <paramref name="right"/>.</returns>
        public static bool operator >(IndexedItem left, IndexedItem right) => left.CompareTo(right) > 0;

        /// <summary>Compares two indexed items.</summary>
        /// <param name="left">The left item.</param>
        /// <param name="right">The right item.</param>
        /// <returns><see langword="true"/> when <paramref name="left"/> is greater than or equal to <paramref name="right"/>.</returns>
        public static bool operator >=(IndexedItem left, IndexedItem right) => left.CompareTo(right) >= 0;

        /// <inheritdoc/>
        public readonly int CompareTo(IndexedItem other)
        {
            var c = Value.CompareTo(other.Value);
            if (c == 0)
            {
                c = Id.CompareTo(other.Id);
            }

            return c;
        }
    }
}
