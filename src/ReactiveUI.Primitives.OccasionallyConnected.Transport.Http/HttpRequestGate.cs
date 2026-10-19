// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Provides bounded fail-fast admission and shutdown for HTTP operations.</summary>
internal sealed class HttpRequestGate : IAsyncDisposable
{
    /// <summary>The lifecycle lock.</summary>
    private readonly Lock _gate = new();

    /// <summary>The task completed when no admitted work remains.</summary>
    private readonly TaskCompletionSource<object?> _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The maximum concurrent operation count.</summary>
    private readonly int _capacity;

    /// <summary>Whether new admission has been closed.</summary>
    private bool _closed;

    /// <summary>The admitted active operation count.</summary>
    private int _activeCount;

    /// <summary>Initializes a new instance of the <see cref="HttpRequestGate"/> class.</summary>
    /// <param name="capacity">The maximum concurrent operation count.</param>
    internal HttpRequestGate(int capacity) => _capacity = capacity;

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _closed = true;
            if (_activeCount == 0)
            {
                _ = _drained.TrySetResult(null);
            }
        }

        return new(_drained.Task);
    }

    /// <summary>Enters the request gate.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The lease that exits the gate.</returns>
    /// <exception cref="HttpRemoteTransportException">The gate has no available capacity.</exception>
    /// <exception cref="ObjectDisposedException">The gate is closed.</exception>
    internal ValueTask<Lease> EnterAsync(CancellationToken cancellationToken) => new(Enter(cancellationToken));

    /// <summary>Enters the request gate synchronously.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The lease that exits the gate.</returns>
    /// <exception cref="HttpRemoteTransportException">The gate has no available capacity.</exception>
    /// <exception cref="ObjectDisposedException">The gate is closed.</exception>
    internal Lease Enter(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Lease lease;
        lock (_gate)
        {
            if (_closed)
            {
                ObjectDisposedExceptionHelper.ThrowIf(true, this);
            }

            if (_activeCount >= _capacity)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.Transient);
            }

            _activeCount++;
            lease = new(this);
        }

        return lease;
    }

    /// <summary>Exits the gate.</summary>
    private void Exit()
    {
        lock (_gate)
        {
            _activeCount--;
            if (_closed && _activeCount == 0)
            {
                _ = _drained.TrySetResult(null);
            }
        }
    }

    /// <summary>Represents an admitted operation lease.</summary>
    internal sealed class Lease : IDisposable
    {
        /// <summary>The owning gate.</summary>
        private readonly HttpRequestGate _owner;

        /// <summary>Whether the lease has been disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="Lease"/> class.</summary>
        /// <param name="owner">The owning gate.</param>
        internal Lease(HttpRequestGate owner) => _owner = owner;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _owner.Exit();
        }
    }
}
