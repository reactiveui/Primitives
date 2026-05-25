// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class PriorityQueue<T>
    where T : IComparable<T>
{
    private long _count = long.MinValue;
    private IndexedItem[] _items;

    public PriorityQueue()
        : this(16)
    {
    }

    public PriorityQueue(int capacity)
    {
        _items = new IndexedItem[capacity];
        Count = 0;
    }

    public int Count { get; private set; }

    public T Dequeue()
    {
        var result = Peek();
        RemoveAt(0);
        return result;
    }

    public void Enqueue(T item)
    {
        if (Count >= _items.Length)
        {
            var temp = _items;
            _items = new IndexedItem[_items.Length * 2];
            Array.Copy(temp, _items, temp.Length);
        }

        var index = Count++;
        _items[index] = new IndexedItem { Value = item, Id = ++_count };
        Percolate(index);
    }

    public T Peek()
    {
        if (Count == 0)
        {
            throw new InvalidOperationException("Heap is empty.");
        }

        return _items[0].Value;
    }

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

    private void Heapify(int index)
    {
        if (index >= Count || index < 0)
        {
            return;
        }

        while (true)
        {
            var left = (2 * index) + 1;
            var right = (2 * index) + 2;
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

    private bool IsHigherPriority(int left, int right) => _items[left].CompareTo(_items[right]) < 0;

    private int Percolate(int index)
    {
        if (index >= Count || index < 0)
        {
            return index;
        }

        var parent = (index - 1) / 2;
        while (parent >= 0 && parent != index && IsHigherPriority(index, parent))
        {
            // swap index and parent
            (_items[parent], _items[index]) = (_items[index], _items[parent]);
            index = parent;
            parent = (index - 1) / 2;
        }

        return index;
    }

    private void RemoveAt(int index)
    {
        _items[index] = _items[--Count];
        _items[Count] = default;

        if (Percolate(index) == index)
        {
            Heapify(index);
        }

        if (Count < _items.Length / 4)
        {
            var temp = _items;
            _items = new IndexedItem[_items.Length / 2];
            Array.Copy(temp, 0, _items, 0, Count);
        }
    }

    private struct IndexedItem : IComparable<IndexedItem>
    {
        public long Id;
        public T Value;

        public int CompareTo(IndexedItem other)
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
