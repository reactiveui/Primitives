// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Disposables;

/// <summary>A group of disposables that are disposed together, embedded as a mutable field by the type that owns them.</summary>
/// <remarks>
/// It needs no object of its own, so a type that owns several disposables can hold them without allocating a group. Keep
/// the field non-readonly and call it in place, since a copy is a separate set. Every disposable is released outside the
/// gate, in registration order: the two inline slots first, then the overflow entries. The default value is an empty set.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("DisposableSet: IsDisposed = {_disposed}, OverflowCount = {_overflowCount}")]
public record struct DisposableSet : IDisposable
{
    /// <summary>Initial capacity for overflow disposable storage.</summary>
    private const int OverflowInitialCapacity = 2;

    /// <summary>Growth factor for overflow disposable storage.</summary>
    private const int OverflowGrowthFactor = 2;

    /// <summary>Synchronizes mutations to the disposable set, created on first use; never held while a disposable is disposed.</summary>
    private Lock? _gate;

    /// <summary>First inline disposable slot.</summary>
    private IDisposable? _slot0;

    /// <summary>Second inline disposable slot.</summary>
    private IDisposable? _slot1;

    /// <summary>Overflow disposable slots used after the inline slots are occupied.</summary>
    private IDisposable[]? _overflow;

    /// <summary>Number of active overflow disposable slots.</summary>
    private int _overflowCount;

    /// <summary>Value indicating whether the set is disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="DisposableSet"/> struct holding two disposables.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    public DisposableSet(IDisposable first, IDisposable second)
    {
        _slot0 = first;
        _slot1 = second;
    }

    /// <summary>Initializes a new instance of the <see cref="DisposableSet"/> struct holding three disposables.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <param name="third">The third disposable.</param>
    public DisposableSet(IDisposable first, IDisposable second, IDisposable third)
        : this(first, second)
    {
        _overflow = new IDisposable[OverflowInitialCapacity];
        _overflow[0] = third;
        _overflowCount = 1;
    }

    /// <summary>Initializes a new instance of the <see cref="DisposableSet"/> struct from a group of disposables, skipping null entries.</summary>
    /// <param name="disposables">Disposables that will be disposed together.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposables"/> is <see langword="null"/>.</exception>
    public DisposableSet(IDisposable[] disposables)
    {
        ArgumentExceptionHelper.ThrowIfNull(disposables);

        for (var i = 0; i < disposables.Length; i++)
        {
            if (disposables[i] is not null)
            {
                AddCore(disposables[i]);
            }
        }
    }

    /// <summary>Gets a value indicating whether the set is disposed.</summary>
    public bool IsDisposed => Volatile.Read(ref _disposed);

    /// <summary>Gets the number of held disposables, or zero after disposal.</summary>
    public int Count
    {
        get
        {
            lock (Gate)
            {
                if (_disposed)
                {
                    return 0;
                }

                var count = _overflowCount;
                if (_slot0 is not null)
                {
                    count++;
                }

                if (_slot1 is not null)
                {
                    count++;
                }

                return count;
            }
        }
    }

    /// <summary>Gets the gate, creating it on first use.</summary>
    private Lock Gate => Volatile.Read(ref _gate) ?? CreateGate(ref _gate);

    /// <summary>Adds a disposable, or disposes it immediately when the set is disposed.</summary>
    /// <param name="item">Disposable to add.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public void Add(IDisposable item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        lock (Gate)
        {
            if (!_disposed)
            {
                AddCore(item);
                return;
            }
        }

        item.Dispose();
    }

    /// <summary>Removes and disposes the requested disposable.</summary>
    /// <param name="item">Disposable to remove.</param>
    /// <returns><see langword="true"/> if the item was found and disposed; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    public bool Remove(IDisposable? item)
    {
        ArgumentExceptionHelper.ThrowIfNull(item);

        bool removed;
        lock (Gate)
        {
            removed = !_disposed && RemoveCore(item);
        }

        if (removed)
        {
            item.Dispose();
        }

        return removed;
    }

    /// <summary>Removes and disposes every disposable currently held without disposing the set itself.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear() => Release(markDisposed: false);

    /// <summary>Disposes every held disposable and marks the set disposed; repeated calls have no further effect.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Release(markDisposed: true);

    /// <summary>Determines whether the set currently holds the supplied disposable.</summary>
    /// <param name="item">Disposable to locate.</param>
    /// <returns><see langword="true"/> when the disposable is held; otherwise, <see langword="false"/>.</returns>
    public bool Contains(IDisposable? item)
    {
        if (item is null)
        {
            return false;
        }

        lock (Gate)
        {
            if (_disposed)
            {
                return false;
            }

            if (_slot0 is not null && EqualityComparer<IDisposable>.Default.Equals(_slot0, item))
            {
                return true;
            }

            if (_slot1 is not null && EqualityComparer<IDisposable>.Default.Equals(_slot1, item))
            {
                return true;
            }

            var overflow = _overflow;
            if (overflow is null)
            {
                return false;
            }

            for (var i = 0; i < _overflowCount; i++)
            {
                if (EqualityComparer<IDisposable>.Default.Equals(overflow[i], item))
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>Copies the disposables currently held into <paramref name="array"/> starting at <paramref name="arrayIndex"/>.</summary>
    /// <param name="array">Destination array.</param>
    /// <param name="arrayIndex">Zero-based index in <paramref name="array"/> at which copying begins.</param>
    /// <exception cref="ArgumentNullException"><paramref name="array"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="arrayIndex"/> is negative.</exception>
    public void CopyTo(IDisposable[] array, int arrayIndex)
    {
        ArgumentExceptionHelper.ThrowIfNull(array);

        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(arrayIndex);

        Snapshot().CopyTo(array, arrayIndex);
    }

    /// <summary>Captures the disposables currently held into a list under the gate.</summary>
    /// <returns>A snapshot list of the held disposables.</returns>
    public List<IDisposable> Snapshot()
    {
        lock (Gate)
        {
            List<IDisposable> snapshot = [];
            if (_disposed)
            {
                return snapshot;
            }

            if (_slot0 is not null)
            {
                snapshot.Add(_slot0);
            }

            if (_slot1 is not null)
            {
                snapshot.Add(_slot1);
            }

            var overflow = _overflow;
            if (overflow is not null)
            {
                for (var i = 0; i < _overflowCount; i++)
                {
                    snapshot.Add(overflow[i]);
                }
            }

            return snapshot;
        }
    }

    /// <summary>Publishes a new gate, keeping the one another thread published first.</summary>
    /// <param name="gate">The gate field.</param>
    /// <returns>The published gate.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static Lock CreateGate(ref Lock? gate)
    {
        Lock created = new();
        return Interlocked.CompareExchange(ref gate, created, null) ?? created;
    }

    /// <summary>Takes every held disposable under the gate and disposes them outside it.</summary>
    /// <param name="markDisposed">Whether the set is marked disposed, so later additions are disposed at once.</param>
    private void Release(bool markDisposed)
    {
        IDisposable? slot0;
        IDisposable? slot1;
        IDisposable[]? overflow;
        int overflowCount;
        lock (Gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = markDisposed;
            slot0 = _slot0;
            slot1 = _slot1;
            overflow = _overflow;
            overflowCount = _overflowCount;
            _slot0 = null;
            _slot1 = null;
            _overflow = null;
            _overflowCount = 0;
        }

        slot0?.Dispose();
        slot1?.Dispose();

        if (overflow is null)
        {
            return;
        }

        for (var i = 0; i < overflowCount; i++)
        {
            overflow[i].Dispose();
        }
    }

    /// <summary>Adds a disposable while the caller holds the gate.</summary>
    /// <param name="disposable">Disposable to add.</param>
    private void AddCore(IDisposable disposable)
    {
        if (_slot0 is null)
        {
            _slot0 = disposable;
            return;
        }

        if (_slot1 is null)
        {
            _slot1 = disposable;
            return;
        }

        if (_overflow is null)
        {
            _overflow = new IDisposable[OverflowInitialCapacity];
        }
        else if (_overflowCount == _overflow.Length)
        {
            var grown = new IDisposable[_overflow.Length * OverflowGrowthFactor];
            Array.Copy(_overflow, grown, _overflowCount);
            _overflow = grown;
        }

        _overflow[_overflowCount] = disposable;
        _overflowCount++;
    }

    /// <summary>Removes a disposable while the caller holds the gate.</summary>
    /// <param name="item">Disposable to remove.</param>
    /// <returns><see langword="true"/> when the item was removed; otherwise, <see langword="false"/>.</returns>
    private bool RemoveCore(IDisposable item)
    {
        if (_slot0 is not null && EqualityComparer<IDisposable>.Default.Equals(_slot0, item))
        {
            _slot0 = null;
            return true;
        }

        if (_slot1 is not null && EqualityComparer<IDisposable>.Default.Equals(_slot1, item))
        {
            _slot1 = null;
            return true;
        }

        var overflow = _overflow;
        if (overflow is null)
        {
            return false;
        }

        for (var i = 0; i < _overflowCount; i++)
        {
            if (!EqualityComparer<IDisposable>.Default.Equals(overflow[i], item))
            {
                continue;
            }

            for (var j = i + 1; j < _overflowCount; j++)
            {
                overflow[j - 1] = overflow[j];
            }

            _overflowCount--;
            overflow[_overflowCount] = null!;
            return true;
        }

        return false;
    }
}
