// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Keeps one underlying subscription alive until its own handle and every lease taken from it have been disposed.</summary>
/// <remarks>
/// The handle returned to the outer subscriber is the primary disposable. Each inner sequence takes a lease while it is
/// subscribed, so disposing the outer subscription leaves the underlying subscription running for the leases that remain.
/// The operator that owns the source assigns the underlying subscription with <see cref="Attach"/> and tears it down
/// directly with <see cref="Release"/> when the source ends.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SharedSubscription: Leases = {_leases}, Primary Disposed = {_primaryDisposed}, Released = {_released}")]
public sealed class SharedSubscription : IDisposable
{
    /// <summary>Serializes lease counting and release.</summary>
    private readonly Lock _gate = new();

    /// <summary>The underlying subscription, or <see langword="null"/> before it is attached and after it is released.</summary>
    private IDisposable? _underlying;

    /// <summary>The callback each lease uses to return itself, created with the first lease.</summary>
    private Action? _giveBack;

    /// <summary>The number of leases that have not been disposed.</summary>
    private int _leases;

    /// <summary>Whether the primary handle has been disposed.</summary>
    private bool _primaryDisposed;

    /// <summary>Whether the underlying subscription has been released.</summary>
    private bool _released;

    /// <summary>Gets a value indicating whether the primary handle has been disposed.</summary>
    public bool IsDisposed
    {
        get
        {
            lock (_gate)
            {
                return _primaryDisposed;
            }
        }
    }

    /// <summary>Assigns the underlying subscription, disposing it at once when it has already been released or one is attached.</summary>
    /// <param name="underlying">The subscription released once the primary handle and every lease are disposed.</param>
    /// <exception cref="ArgumentNullException"><paramref name="underlying"/> is <see langword="null"/>.</exception>
    public void Attach(IDisposable underlying)
    {
        ArgumentExceptionHelper.ThrowIfNull(underlying);

        lock (_gate)
        {
            if (!_released && _underlying is null)
            {
                _underlying = underlying;
                return;
            }
        }

        underlying.Dispose();
    }

    /// <summary>Takes a lease that keeps the underlying subscription alive until it is disposed.</summary>
    /// <returns>The lease, or an empty disposable when the underlying subscription has already been released.</returns>
    public IDisposable Acquire()
    {
        lock (_gate)
        {
            if (_released)
            {
                return EmptyDisposable.Instance;
            }

            _leases++;
            _giveBack ??= ReturnLease;
            return new Lease(_giveBack);
        }
    }

    /// <summary>Releases the underlying subscription now, whatever leases remain.</summary>
    public void Release()
    {
        IDisposable? released;
        lock (_gate)
        {
            _released = true;
            released = _underlying;
            _underlying = null;
        }

        released?.Dispose();
    }

    /// <summary>Disposes the primary handle; the underlying subscription is released now when no lease is held.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_primaryDisposed)
            {
                return;
            }

            _primaryDisposed = true;
            if (_leases != 0)
            {
                return;
            }
        }

        Release();
    }

    /// <summary>Returns a lease and releases the underlying subscription when it was the last claim.</summary>
    private void ReturnLease()
    {
        lock (_gate)
        {
            _leases--;
            if (_leases != 0 || !_primaryDisposed)
            {
                return;
            }
        }

        Release();
    }

    /// <summary>One lease on the shared subscription.</summary>
    /// <param name="giveBack">Returns the lease to the shared subscription that issued it.</param>
    private sealed class Lease(Action giveBack) : IDisposable
    {
        /// <summary>The callback that returns the lease, cleared by the first disposal.</summary>
        private Action? _giveBack = giveBack;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Interlocked.Exchange(ref _giveBack, null)?.Invoke();
    }
}
