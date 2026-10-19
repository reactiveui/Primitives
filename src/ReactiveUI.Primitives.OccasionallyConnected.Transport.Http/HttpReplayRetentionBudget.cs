// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Threading;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Tracks the shared retained-byte budget for replay sessions and nonce cache entries.</summary>
internal sealed class HttpReplayRetentionBudget
{
    /// <summary>The synchronization gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The currently retained byte count.</summary>
    private long _retainedBytes;

    /// <summary>Initializes a new instance of the <see cref="HttpReplayRetentionBudget"/> class.</summary>
    /// <param name="maximumBytes">The maximum retained byte count.</param>
    /// <exception cref="ArgumentOutOfRangeException">The maximum byte count is not positive.</exception>
    internal HttpReplayRetentionBudget(long maximumBytes)
    {
        if (maximumBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumBytes), maximumBytes, "HTTP replay retained bytes must be positive.");
        }

        MaximumBytes = maximumBytes;
    }

    /// <summary>Gets the maximum retained byte count.</summary>
    internal long MaximumBytes { get; }

    /// <summary>Gets the currently retained byte count.</summary>
    internal long RetainedBytes
    {
        get
        {
            lock (_gate)
            {
                return _retainedBytes;
            }
        }
    }

    /// <summary>Reserves retained bytes before protected replay state is stored.</summary>
    /// <param name="bytes">The retained byte count to reserve.</param>
    /// <returns>A lease that releases the reserved bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The requested byte count is negative.</exception>
    /// <exception cref="HttpRemoteTransportException">The budget has insufficient remaining capacity.</exception>
    internal Reservation Reserve(long bytes)
    {
        if (bytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bytes), bytes, "HTTP replay retained bytes cannot be negative.");
        }

        lock (_gate)
        {
            if (bytes > MaximumBytes - _retainedBytes)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.Transient, HttpTransportStatus.TooManyRequests);
            }

            var reservation = new Reservation(new Lease(this));
            reservation.Activate(bytes);
            _retainedBytes += bytes;
            return reservation;
        }
    }

    /// <summary>Updates an existing retained-byte reservation.</summary>
    /// <param name="lease">The retained-byte lease.</param>
    /// <param name="newBytes">The replacement retained byte count.</param>
    /// <exception cref="ArgumentException">The lease is not owned by this budget.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The requested byte count is negative.</exception>
    /// <exception cref="HttpRemoteTransportException">The budget has insufficient remaining capacity.</exception>
    internal void Update(IDisposable lease, long newBytes)
    {
        ArgumentExceptionHelper.ThrowIfNull(lease);
        if (lease is not IHttpReplayRetentionBudgetLease retainedLease || !retainedLease.IsOwnedBy(this))
        {
            throw new ArgumentException("HTTP replay retained-byte lease is not owned by this budget.", nameof(lease));
        }

        retainedLease.Update(newBytes);
    }

    /// <summary>Owns a retained-byte reservation until it is either disposed or transferred.</summary>
    internal sealed class Reservation : IHttpReplayRetentionBudgetLease
    {
        /// <summary>The active retained-byte lease.</summary>
        private readonly Lease _lease;

        /// <summary>Whether this reservation has been disposed or transferred.</summary>
        private int _closed;

        /// <summary>Initializes a new instance of the <see cref="Reservation"/> class.</summary>
        /// <param name="lease">The active retained-byte lease.</param>
        internal Reservation(Lease lease) => _lease = lease;

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _closed, 1) != 0)
            {
                return;
            }

            _lease.Dispose();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsOwnedBy(HttpReplayRetentionBudget owner) => _lease.IsOwnedBy(owner);

        /// <inheritdoc />
        public void Update(long newBytes)
        {
            ObjectDisposedExceptionHelper.ThrowIf(Volatile.Read(ref _closed) != 0, this);
            _lease.Update(newBytes);
        }

        /// <summary>Transfers the active lease to retained replay state.</summary>
        /// <returns>The retained lease.</returns>
        /// <exception cref="ObjectDisposedException">The reservation has already been disposed or transferred.</exception>
        internal IDisposable Transfer()
        {
            if (Interlocked.Exchange(ref _closed, 1) == 0)
            {
                return _lease;
            }

            throw new ObjectDisposedException(GetType().FullName);
        }

        /// <summary>Activates the reservation under the owning budget gate.</summary>
        /// <param name="bytes">The retained byte count.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Activate(long bytes) => _lease.Activate(bytes);
    }

    /// <summary>Owns one retained-byte reservation.</summary>
    /// <param name="budget">The owning budget.</param>
    internal sealed class Lease(HttpReplayRetentionBudget budget) : IHttpReplayRetentionBudgetLease
    {
        /// <summary>The retained byte count.</summary>
        private long _bytes;

        /// <summary>Whether this lease was disposed.</summary>
        private int _disposed;

        /// <inheritdoc />
        public void Dispose()
        {
            lock (budget._gate)
            {
                if (_disposed != 0)
                {
                    return;
                }

                _disposed = 1;
                budget._retainedBytes -= _bytes;
                _bytes = 0;
            }
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsOwnedBy(HttpReplayRetentionBudget owner) => ReferenceEquals(budget, owner);

        /// <summary>Updates this lease reservation.</summary>
        /// <param name="newBytes">The replacement retained byte count.</param>
        /// <exception cref="ArgumentOutOfRangeException">The requested byte count is negative.</exception>
        /// <exception cref="ObjectDisposedException">This lease has already been disposed.</exception>
        /// <exception cref="HttpRemoteTransportException">The budget has insufficient remaining capacity.</exception>
        public void Update(long newBytes)
        {
            if (newBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(newBytes), newBytes, "HTTP replay retained bytes cannot be negative.");
            }

            lock (budget._gate)
            {
                ObjectDisposedExceptionHelper.ThrowIf(_disposed != 0, budget);

                var delta = newBytes - _bytes;
                if (delta > budget.MaximumBytes - budget._retainedBytes)
                {
                    throw new HttpRemoteTransportException(HttpTransportFailureKind.Transient, HttpTransportStatus.TooManyRequests);
                }

                budget._retainedBytes += delta;
                _bytes = newBytes;
            }
        }

        /// <summary>Activates the lease after capacity has been verified.</summary>
        /// <param name="bytes">The retained byte count.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Activate(long bytes) => _bytes = bytes;
    }
}
