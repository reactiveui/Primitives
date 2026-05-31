// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// A small composite-disposable replacement specialised for the common 2-slot
/// "subscription + sink" pair found throughout this codebase. Avoids the
/// <see cref="List{T}"/> backing field of
/// <c>System.Reactive.Disposables.CompositeDisposable</c>.
/// </summary>
/// <remarks>
/// The first two added entries are stored inline. A third or later entry causes a fall-back
/// to a heap-allocated array. Disposal is idempotent and disposes every contained entry,
/// in registration order, exactly once.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class DisposableBag : IsDisposed
{
    /// <summary>Starting capacity of the overflow array once the two inline slots are taken.</summary>
    private const int OverflowInitialCapacity = 2;

    /// <summary>Growth factor for the overflow array.</summary>
    private const int OverflowGrowthFactor = 2;

    /// <summary>
    /// The synchronization object.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// The first disposable slot.
    /// </summary>
    private IDisposable? _slot0;

    /// <summary>
    /// The second disposable slot.
    /// </summary>
    private IDisposable? _slot1;

    /// <summary>
    /// The overflow array for additional disposables.
    /// </summary>
    private IDisposable[]? _overflow;

    /// <summary>
    /// The number of disposables in the overflow array.
    /// </summary>
    private int _overflowCount;

    /// <summary>
    /// Indicates whether the bag has been disposed.
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="DisposableBag"/> class.
    /// </summary>
    public DisposableBag()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DisposableBag"/> class with two pre-populated slots.
    /// </summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    public DisposableBag(IDisposable first, IDisposable second)
    {
        _slot0 = first;
        _slot1 = second;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DisposableBag"/> class with three pre-populated slots.
    /// </summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <param name="third">The third disposable.</param>
    public DisposableBag(IDisposable first, IDisposable second, IDisposable third)
    {
        _slot0 = first;
        _slot1 = second;
        _overflow = new IDisposable[OverflowInitialCapacity];
        _overflow[0] = third;
        _overflowCount = 1;
    }

    /// <summary>
    /// Gets a value indicating whether this instance has been disposed.
    /// </summary>
    public bool IsDisposed => Volatile.Read(ref _disposed);

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>
    /// Adds a disposable to the bag. If the bag is already disposed, the supplied
    /// disposable is disposed immediately.
    /// </summary>
    /// <param name="disposable">The disposable to add.</param>
    public void Add(IDisposable disposable)
    {
        if (disposable is null)
        {
            return;
        }

        var disposeNow = false;
        lock (_gate)
        {
            if (_disposed)
            {
                disposeNow = true;
            }
            else if (_slot0 is null)
            {
                _slot0 = disposable;
            }
            else if (_slot1 is null)
            {
                _slot1 = disposable;
            }
            else
            {
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

                _overflow[_overflowCount++] = disposable;
            }
        }

        if (!disposeNow)
        {
            return;
        }

        disposable.Dispose();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        IDisposable? s0;
        IDisposable? s1;
        IDisposable[]? overflow;
        int overflowCount;

        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            s0 = _slot0;
            s1 = _slot1;
            overflow = _overflow;
            overflowCount = _overflowCount;
            _slot0 = null;
            _slot1 = null;
            _overflow = null;
            _overflowCount = 0;
        }

        s0?.Dispose();
        s1?.Dispose();
        if (overflow is null)
        {
            return;
        }

        for (var i = 0; i < overflowCount; i++)
        {
            overflow[i].Dispose();
        }
    }
}
