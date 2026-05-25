// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// A disposable pocket that contains a set of disposables and disposes them together.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public class MultipleDisposable : IsDisposed
{
    /// <summary>
    /// Initial capacity for overflow disposable storage.
    /// </summary>
    private const int OverflowInitialCapacity = 2;

    /// <summary>
    /// Growth factor for overflow disposable storage.
    /// </summary>
    private const int OverflowGrowthFactor = 2;

    /// <summary>
    /// Synchronizes mutations to the disposable set.
    /// </summary>
    private readonly object _gate = new();

    /// <summary>
    /// First inline disposable slot.
    /// </summary>
    private IDisposable? _slot0;

    /// <summary>
    /// Second inline disposable slot.
    /// </summary>
    private IDisposable? _slot1;

    /// <summary>
    /// Overflow disposable slots used after the inline slots are occupied.
    /// </summary>
    private IDisposable[]? _overflow;

    /// <summary>
    /// Number of active overflow disposable slots.
    /// </summary>
    private int _overflowCount;

    /// <summary>
    /// Value indicating whether this group is disposed.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MultipleDisposable"/> class.
    /// </summary>
    public MultipleDisposable()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MultipleDisposable"/> class.
    /// </summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    public MultipleDisposable(IDisposable first, IDisposable second)
    {
        _slot0 = first;
        _slot1 = second;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MultipleDisposable"/> class.
    /// </summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <param name="third">The third disposable.</param>
    public MultipleDisposable(IDisposable first, IDisposable second, IDisposable third)
    {
        _slot0 = first;
        _slot1 = second;
        _overflow = new IDisposable[OverflowInitialCapacity];
        _overflow[0] = third;
        _overflowCount = 1;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MultipleDisposable"/> class from a group of disposables.
    /// </summary>
    /// <param name="disposables">Disposables that will be disposed together.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposables"/> is <see langword="null"/>.</exception>
    public MultipleDisposable(params IDisposable[] disposables)
    {
        if (disposables == null)
        {
            throw new ArgumentNullException(nameof(disposables));
        }

        for (var i = 0; i < disposables.Length; i++)
        {
            if (disposables[i] != null)
            {
                AddCore(disposables[i]);
            }
        }
    }

    /// <summary>
    /// Gets a value indicating whether the object is disposed.
    /// </summary>
    public bool IsDisposed
    {
        get
        {
            return Volatile.Read(ref _disposed);
        }
    }

    /// <summary>
    /// Creates a new group of disposable resources that are disposed together.
    /// </summary>
    /// <param name="disposables">Disposable resources to add to the group.</param>
    /// <returns>Group of disposable resources that are disposed together.</returns>
    public static IDisposable Create(params IDisposable[] disposables) => new MultipleDisposableBase(disposables);

    /// <summary>
    /// Adds a disposable to the <see cref="MultipleDisposable"/> or disposes it immediately if the pocket is already disposed.
    /// </summary>
    /// <param name="disposable">Disposable to add.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposable"/> is <see langword="null"/>.</exception>
    public void Add(IDisposable disposable)
    {
        if (disposable == null)
        {
            throw new ArgumentNullException(nameof(disposable));
        }

        var shouldDispose = false;
        lock (_gate)
        {
            if (_disposed)
            {
                shouldDispose = true;
            }
            else
            {
                AddCore(disposable);
            }
        }

        if (!shouldDispose)
        {
            return;
        }

        disposable.Dispose();
    }

    /// <summary>
    /// Removes and disposes the requested disposable from the pocket.
    /// </summary>
    /// <param name="item">Disposable to remove.</param>
    /// <returns><see langword="true"/> if the item was found and disposed; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
    public bool Remove(IDisposable? item)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        var shouldDispose = false;
        lock (_gate)
        {
            shouldDispose = !_disposed && RemoveCore(item);
        }

        if (shouldDispose)
        {
            item.Dispose();
        }

        return shouldDispose;
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        IDisposable? slot0;
        IDisposable? slot1;
        IDisposable[]? overflow;
        int overflowCount;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
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

        if (overflow == null)
        {
            return;
        }

        for (var i = 0; i < overflowCount; i++)
        {
            overflow[i].Dispose();
            overflow[i] = null!;
        }
    }

    /// <summary>
    /// Adds a disposable while the caller holds the gate.
    /// </summary>
    /// <param name="disposable">Disposable to add.</param>
    private void AddCore(IDisposable disposable)
    {
        if (_slot0 == null)
        {
            _slot0 = disposable;
            return;
        }

        if (_slot1 == null)
        {
            _slot1 = disposable;
            return;
        }

        if (_overflow == null)
        {
            _overflow = new IDisposable[OverflowInitialCapacity];
        }
        else if (_overflowCount == _overflow.Length)
        {
            var grown = new IDisposable[_overflow.Length * OverflowGrowthFactor];
            Array.Copy(_overflow, grown, _overflowCount);
            _overflow = grown;
        }

        _overflow[_overflowCount++] = disposable;
    }

    /// <summary>
    /// Removes a disposable while the caller holds the gate.
    /// </summary>
    /// <param name="item">Disposable to remove.</param>
    /// <returns><see langword="true"/> when the item was removed; otherwise, <see langword="false"/>.</returns>
    private bool RemoveCore(IDisposable item)
    {
        if (_slot0 != null && EqualityComparer<IDisposable>.Default.Equals(_slot0, item))
        {
            _slot0 = null;
            return true;
        }

        if (_slot1 != null && EqualityComparer<IDisposable>.Default.Equals(_slot1, item))
        {
            _slot1 = null;
            return true;
        }

        var overflow = _overflow;
        if (overflow == null)
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

    /// <summary>
    /// Array-backed disposable group returned by the static factory.
    /// </summary>
    private sealed class MultipleDisposableBase : IDisposable
    {
        /// <summary>
        /// Disposables to release, or <see langword="null"/> after disposal.
        /// </summary>
        private IDisposable[]? _disposables;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultipleDisposableBase"/> class.
        /// </summary>
        /// <param name="disposables">Disposables owned by the group.</param>
        public MultipleDisposableBase(IDisposable[] disposables) =>
            Volatile.Write(ref _disposables, disposables ?? throw new ArgumentNullException(nameof(disposables)));

        /// <inheritdoc/>
        public void Dispose()
        {
            var disposables = Interlocked.Exchange(ref _disposables, null);
            if (disposables == null)
            {
                return;
            }

            foreach (var disposable in disposables)
            {
                disposable?.Dispose();
            }
        }
    }
}
