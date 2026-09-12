// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>A thread-safe collection of asynchronous disposables whose lifetimes are owned and released as one group.</summary>
/// <remarks>Disposal is one-way: a disposed collection holds nothing, and adding to it disposes the incoming item
/// instead of storing it. Safe for concurrent access from several threads.</remarks>
[System.Diagnostics.DebuggerDisplay("MultipleDisposableAsync: Count = {_count}, IsDisposed = {_isDisposed}")]
public sealed class MultipleDisposableAsync : IAsyncDisposable
{
    /// <summary>Capacity allocated on first <see cref="AddAsync"/>, sized so a typical composite never resizes.</summary>
    private const int DefaultCapacity = 8;

    /// <summary>Used-slot count at or below which a remove leaves the array uncompacted.</summary>
    private const int ShrinkThreshold = 16;

    /// <summary>Occupancy divisor: a remove compacts when count multiplied by this falls below the array's length.</summary>
    private const int ShrinkOccupancyDivisor = 4;

    /// <summary>Factor the backing array's capacity is multiplied by when it overflows.</summary>
    private const int GrowthFactor = 2;

    /// <summary>Divisor applied to the backing array's capacity when a sparse collection is compacted; safe because compaction only runs below quarter occupancy.</summary>
    private const int CompactionShrinkDivisor = 2;

    /// <summary>The synchronization gate protecting all mutable state in this collection.</summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Backing array, <see langword="null"/> until something is added. A removal zeroes its slot rather than shifting
    /// elements, so <see cref="_length"/> is the high-water mark and <see cref="_count"/> the non-null slots.
    /// </summary>
    private IAsyncDisposable?[]? _items;

    /// <summary>High-water mark of used slots in <see cref="_items"/>. Includes slots zeroed by Remove.</summary>
    private int _length;

    /// <summary>The number of non-<see langword="null"/> disposables in the collection.</summary>
    private int _count;

    /// <summary>Indicates whether the collection has been disposed.</summary>
    private bool _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="MultipleDisposableAsync"/> class, allocating its backing array on the first <see cref="AddAsync"/> call.</summary>
    public MultipleDisposableAsync()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MultipleDisposableAsync"/> class with the specified initial capacity.</summary>
    /// <param name="capacity">The number of elements that the collection can initially store. Must be greater than or equal to 0.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when capacity is less than 0.</exception>
    public MultipleDisposableAsync(int capacity)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(capacity, 0);

        _items = capacity == 0 ? null : new IAsyncDisposable?[capacity];
    }

    /// <summary>Initializes a new instance of the <see cref="MultipleDisposableAsync"/> class that contains the specified disposables, sizing the backing array exactly.</summary>
    /// <param name="disposables">An array of objects implementing <see cref="IAsyncDisposable"/>.</param>
    public MultipleDisposableAsync(params IAsyncDisposable[] disposables)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposables);
        if (disposables.Length == 0)
        {
            return;
        }

        _items = new IAsyncDisposable?[disposables.Length];
        Array.Copy(disposables, _items, disposables.Length);
        _length = disposables.Length;
        _count = disposables.Length;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MultipleDisposableAsync"/> class that contains the specified
    /// disposables. The backing array is sized exactly when <paramref name="disposables"/> implements
    /// <see cref="ICollection{T}"/>; otherwise it grows from the default capacity.
    /// </summary>
    /// <param name="disposables">The collection of <see cref="IAsyncDisposable"/> instances to include.</param>
    public MultipleDisposableAsync(IEnumerable<IAsyncDisposable> disposables)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposables);

        if (disposables is ICollection<IAsyncDisposable> collection)
        {
            if (collection.Count == 0)
            {
                return;
            }

            _items = new IAsyncDisposable?[collection.Count];
            var i = 0;
            foreach (var d in collection)
            {
                _items[i] = d;
                i++;
            }

            _length = collection.Count;
            _count = collection.Count;
            return;
        }

        foreach (var d in disposables)
        {
            if (d is null)
            {
                continue;
            }

            EnsureCapacityForOneMore();
            _items![_length] = d;
            _length++;
            _count++;
        }
    }

    /// <summary>Gets a value indicating whether the object has been disposed.</summary>
    public bool IsDisposed => Volatile.Read(ref _isDisposed);

    /// <summary>Gets the number of elements contained in the collection.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _count;
            }
        }
    }

    /// <summary>Takes ownership of a disposable, disposing it on the spot when this collection has been disposed.</summary>
    /// <param name="item">The item whose lifetime this collection takes over. Cannot be null.</param>
    /// <returns>A completed task when the item was stored; otherwise the task disposing it.</returns>
    public ValueTask AddAsync(IAsyncDisposable item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        lock (_gate)
        {
            if (!_isDisposed)
            {
                EnsureCapacityForOneMore();
                _items![_length] = item;
                _length++;
                _count++;
                return default;
            }
        }

        return item.DisposeAsync();
    }

    /// <summary>Removes the specified item from the collection and disposes it asynchronously.</summary>
    /// <param name="item">The item to remove and dispose. Cannot be null.</param>
    /// <returns><see langword="true"/> when the item was found, removed and disposed; otherwise,
    /// <see langword="false"/>.</returns>
    /// <remarks>An item this collection does not hold is left alone, not disposed.</remarks>
    public async ValueTask<bool> Remove(IAsyncDisposable item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        lock (_gate)
        {
            if (_isDisposed || _items is null)
            {
                return false;
            }

            var index = Array.IndexOf(_items, item, 0, _length);
            if (index < 0)
            {
                return false;
            }

            _items[index] = null;
            _count--;

            if (_count == 0)
            {
                Array.Clear(_items, 0, _length);
                _length = 0;
            }
            else if (_length > ShrinkThreshold && _count * ShrinkOccupancyDivisor < _length)
            {
                CompactInPlace();
            }
        }

        await item.DisposeAsync().ConfigureAwait(false);
        return true;
    }

    /// <summary>Empties the collection and disposes everything it held, leaving it reusable.</summary>
    /// <returns>A task that completes once every item has been disposed.</returns>
    /// <remarks>Items are disposed one after another in insertion order, outside the lock, so the collection accepts
    /// additions while the disposals are in flight.</remarks>
    public async ValueTask Clear()
    {
        IAsyncDisposable?[] rented;
        int clearLength;
        lock (_gate)
        {
            if (_isDisposed || _count == 0 || _items is null)
            {
                return;
            }

            clearLength = _length;
            rented = ArrayPool<IAsyncDisposable?>.Shared.Rent(clearLength);
            Array.Copy(_items, rented, clearLength);
            Array.Clear(_items, 0, clearLength);
            _length = 0;
            _count = 0;
        }

        try
        {
            for (var i = 0; i < clearLength; i++)
            {
                if (rented[i] is { } item)
                {
                    await item.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        finally
        {
            ArrayPool<IAsyncDisposable?>.Shared.Return(rented, true);
        }
    }

    /// <summary>Determines whether the collection contains the specified asynchronous disposable item.</summary>
    /// <param name="item">The asynchronous disposable item to locate in the collection. Can be null.</param>
    /// <returns><see langword="true"/> when the collection is live and holds the item; otherwise
    /// <see langword="false"/>.</returns>
    public bool Contains(IAsyncDisposable item)
    {
        lock (_gate)
        {
            return !_isDisposed && _items is not null && Array.IndexOf(_items, item, 0, _length) >= 0;
        }
    }

    /// <summary>Copies the elements of the collection to the specified array, starting at the given array index.</summary>
    /// <param name="array">The zero-based destination array.</param>
    /// <param name="arrayIndex">The index in <paramref name="array"/> at which copying begins.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="arrayIndex"/> falls outside
    /// <paramref name="array"/>, or the space from it to the end of the array cannot hold every item.</exception>
    /// <remarks>A disposed collection copies nothing and raises nothing.</remarks>
    public void CopyTo(IAsyncDisposable[]? array, int arrayIndex)
    {
        if (arrayIndex < 0 || arrayIndex >= array?.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(arrayIndex));
        }

        lock (_gate)
        {
            if (_isDisposed || _items is null)
            {
                return;
            }

            if (arrayIndex + _count > array?.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            }

            if (array is null)
            {
                return;
            }

            CopyToCore(array, arrayIndex);
        }
    }

    /// <summary>Asynchronously releases all resources used by the collection and disposes of each contained asynchronous disposable object.</summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    /// <remarks>Idempotent. Items are disposed one after another in insertion order.</remarks>
    public async ValueTask DisposeAsync()
    {
        IAsyncDisposable?[]? snapshot;
        int snapshotLength;

        lock (_gate)
        {
            if (_isDisposed)
            {
                return;
            }

            _isDisposed = true;
            snapshot = _items;
            snapshotLength = _length;
            _items = null;
            _length = 0;
            _count = 0;
        }

        if (snapshot is null)
        {
            return;
        }

        for (var i = 0; i < snapshotLength; i++)
        {
            if (snapshot[i] is { } item)
            {
                await item.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Returns an enumerator over a snapshot taken under the gate, so later mutations do not affect it.</summary>
    /// <returns>An enumerator over a snapshot of the collection's disposables.</returns>
    public IEnumerator<IAsyncDisposable> GetEnumerator()
    {
        IAsyncDisposable[] snapshot;
        lock (_gate)
        {
            if (_items is null || _count == 0)
            {
                return EmptyEnumerator();
            }

            snapshot = new IAsyncDisposable[_count];
            var dst = 0;
            for (var src = 0; src < _length; src++)
            {
                var item = _items[src];
                if (item is null)
                {
                    continue;
                }

                snapshot[dst] = item;
                dst++;
            }
        }

        return ((IEnumerable<IAsyncDisposable>)snapshot).GetEnumerator();
    }

    /// <summary>Returns an empty enumerator used when the composite holds nothing.</summary>
    /// <returns>An empty enumerator.</returns>
    private static IEnumerator<IAsyncDisposable> EmptyEnumerator()
    {
        yield break;
    }

    /// <summary>Performs the actual copy under the assumption that bounds have been validated.</summary>
    /// <param name="array">Destination array, guaranteed non-null by the caller.</param>
    /// <param name="arrayIndex">Destination starting index.</param>
    private void CopyToCore(IAsyncDisposable[] array, int arrayIndex)
    {
        var dst = arrayIndex;
        var src = _items!;
        for (var i = 0; i < _length; i++)
        {
            var item = src[i];
            if (item is null)
            {
                continue;
            }

            array[dst] = item;
            dst++;
        }
    }

    /// <summary>Ensures <see cref="_items"/> has at least one free slot at index <see cref="_length"/>. Allocates the default-capacity array on first use; doubles on subsequent overflow.</summary>
    private void EnsureCapacityForOneMore()
    {
        if (_items is null)
        {
            _items = new IAsyncDisposable?[DefaultCapacity];
            return;
        }

        if (_length < _items.Length)
        {
            return;
        }

        var grown = new IAsyncDisposable?[_items.Length * GrowthFactor];
        Array.Copy(_items, grown, _length);
        _items = grown;
    }

    /// <summary>Removes null gaps inside <see cref="_items"/> and shrinks the backing array to half its capacity. Caller must hold <see cref="_gate"/>.</summary>
    private void CompactInPlace()
    {
        var src = _items!;
        var fresh = new IAsyncDisposable?[src.Length / CompactionShrinkDivisor];
        var dst = 0;
        for (var i = 0; i < _length; i++)
        {
            var item = src[i];
            if (item is null)
            {
                continue;
            }

            fresh[dst] = item;
            dst++;
        }

        _items = fresh;
        _length = dst;
    }
}
