// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers;

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>
/// Represents a thread-safe collection of asynchronous disposable objects that are disposed together as a group.
/// Provides methods to add, remove, and asynchronously dispose contained resources as a single operation.
/// </summary>
/// <remarks>Use this class to manage the lifetime of multiple <see cref="IAsyncDisposable"/> resources, ensuring
/// that all are disposed when the collection is disposed. Once disposed, the collection cannot be used to add or remove
/// items. This class is not read-only and is safe for concurrent access from multiple threads.</remarks>
public sealed class MultipleDisposableAsync : IAsyncDisposable
{
    /// <summary>Capacity allocated on first <see cref="AddAsync"/>. Chosen as the typical upper bound
    /// of subscriptions a composite holds, so most lifetimes never trigger a resize.</summary>
    private const int DefaultCapacity = 8;

    /// <summary>Length threshold below which Remove no longer compacts the array.</summary>
    private const int ShrinkThreshold = 16;

    /// <summary>Divisor used to decide whether a remove triggers compaction (count * 4 &lt; length).</summary>
    private const int ShrinkOccupancyDivisor = 4;

    /// <summary>Factor the backing array's capacity is multiplied by when it overflows.</summary>
    private const int GrowthFactor = 2;

    /// <summary>Divisor applied to the backing array's capacity when a sparse collection is compacted.
    /// Compaction only runs when fewer than a quarter of the slots are occupied, so halving always leaves room.</summary>
    private const int CompactionShrinkDivisor = 2;

    /// <summary>The synchronization gate protecting all mutable state in this collection.</summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Backing array of disposables. Slots may be <see langword="null"/> after removal to avoid shifting elements;
    /// <see cref="_length"/> tracks the high-water mark of used slots and <see cref="_count"/> tracks non-null slots.
    /// <see langword="null"/> until the first <see cref="AddAsync"/>; the no-arg constructor leaves it unallocated.
    /// </summary>
    private IAsyncDisposable?[]? _items;

    /// <summary>High-water mark of used slots in <see cref="_items"/>. Includes slots zeroed by Remove.</summary>
    private int _length;

    /// <summary>The number of non-<see langword="null"/> disposables in the collection.</summary>
    private int _count;

    /// <summary>Indicates whether the collection has been disposed.</summary>
    private bool _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MultipleDisposableAsync"/> class. The backing array is allocated
    /// lazily on the first <see cref="AddAsync"/> call; an unused composite costs only its instance header + gate.
    /// </summary>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="MultipleDisposableAsync"/> class that contains the specified
    /// disposables — the backing array is sized exactly so no resize occurs.
    /// </summary>
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

    /// <summary>
    /// Adds an asynchronous disposable item to the collection, or disposes it immediately if the collection has already
    /// been disposed.
    /// </summary>
    /// <param name="item">The item to add. The item must implement <see cref="IAsyncDisposable"/> and will be disposed asynchronously if
    /// the collection is disposed.</param>
    /// <returns>A <see cref="ValueTask"/> that represents the asynchronous operation. The returned task is completed if the item
    /// was added; otherwise, it represents the asynchronous disposal of the item.</returns>
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
    /// <remarks>If the item is not found in the collection, it is not disposed. This method is
    /// thread-safe.</remarks>
    /// <param name="item">The item to remove and dispose. Cannot be null.</param>
    /// <returns>A task that represents the asynchronous remove operation. The task result is <see langword="true"/> if the item
    /// was found and removed; otherwise, <see langword="false"/>.</returns>
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

    /// <summary>Asynchronously disposes all items in the collection and removes them.</summary>
    /// <remarks>If the collection is already empty or has been disposed, this method performs no action. Each
    /// item is disposed asynchronously before being removed from the collection. This method is thread-safe.</remarks>
    /// <returns>A task that represents the asynchronous clear operation.</returns>
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
    /// <remarks>If the collection has been disposed, this method always returns false.</remarks>
    /// <param name="item">The asynchronous disposable item to locate in the collection. Can be null.</param>
    /// <returns>true if the specified item is found in the collection and the collection has not been disposed; otherwise,
    /// false.</returns>
    public bool Contains(IAsyncDisposable item)
    {
        lock (_gate)
        {
            return _isDisposed || _items is null
                ? false
                : Array.IndexOf(_items, item, 0, _length) >= 0;
        }
    }

    /// <summary>Copies the elements of the collection to the specified array, starting at the given array index.</summary>
    /// <param name="array">The one-dimensional array of IAsyncDisposable elements that is the destination of the elements copied from the
    /// collection. The array must have zero-based indexing.</param>
    /// <param name="arrayIndex">The zero-based index in the destination array at which copying begins. Must be non-negative and less than the
    /// length of the array.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when arrayIndex is less than zero, greater than or equal to the length of array, or when there is not
    /// enough space from arrayIndex to the end of array to accommodate all elements in the collection.</exception>
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

    /// <summary>
    /// Asynchronously releases all resources used by the collection and disposes of each contained asynchronous
    /// disposable object.
    /// </summary>
    /// <remarks>After calling this method, the collection is considered disposed and cannot be used. This
    /// method is thread-safe and can be called multiple times; subsequent calls after the first have no
    /// effect.</remarks>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
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

    /// <summary>
    /// Returns an enumerator that iterates a snapshot of the non-null disposables in the collection.
    /// The snapshot is taken under the gate; subsequent mutations do not affect the enumerator.
    /// </summary>
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
                if (_items[src] is { } item)
                {
                    snapshot[dst] = item;
                    dst++;
                }
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
            if (src[i] is { } item)
            {
                array[dst] = item;
                dst++;
            }
        }
    }

    /// <summary>Ensures <see cref="_items"/> has at least one free slot at index <see cref="_length"/>.
    /// Allocates the default-capacity array on first use; doubles on subsequent overflow.</summary>
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
            if (src[i] is { } item)
            {
                fresh[dst] = item;
                dst++;
            }
        }

        _items = fresh;
        _length = dst;
    }
}
