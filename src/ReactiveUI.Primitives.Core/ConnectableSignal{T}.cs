// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>Connectable hot signal that subscribes to its source only when connected.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// The gate only records the active connection. The source is subscribed and unsubscribed after the gate is released, so a
/// source that connects or disconnects from another thread while it is being subscribed cannot deadlock the caller.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ConnectableSignal<T> : IObservable<T>
{
    /// <summary>Synchronizes connection state; never held while the source is subscribed or unsubscribed.</summary>
    private readonly Lock _gate = new();

    /// <summary>Source sequence to connect.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Multicast hub that receives source values.</summary>
    private readonly ISignal<T> _hub;

    /// <summary>The active source connection, whose returned handle owns disposal.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "ConnectableSignal is not IDisposable; callers own the Connection returned by Connect.")]
    private Connection? _connection;

    /// <summary>Set after the source sends a terminal notification to the hub.</summary>
    private bool _terminated;

    /// <summary>Initializes a new instance of the <see cref="ConnectableSignal{T}"/> class.</summary>
    /// <param name="source">The cold or hot source sequence.</param>
    /// <param name="hub">The multicast hub.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="hub"/> is <see langword="null"/>.</exception>
    public ConnectableSignal(IObservable<T> source, ISignal<T> hub)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _hub = hub ?? throw new ArgumentNullException(nameof(hub));
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Subscribes the hub to the source, returning the live handle when a connection is open.</summary>
    /// <returns>A handle that disconnects the source subscription.</returns>
    public IDisposable Connect()
    {
        Connection connection;
        lock (_gate)
        {
            if (Volatile.Read(ref _terminated))
            {
                return Scope.Empty;
            }

            if (_connection is { } activeConnection)
            {
                return activeConnection;
            }

            connection = new(this);
            _connection = connection;
        }

        IDisposable sourceSubscription;
        try
        {
            sourceSubscription = _source.Subscribe(connection);
        }
        catch
        {
            connection.Dispose();
            throw;
        }

        connection.Attach(sourceSubscription);
        if (!Volatile.Read(ref _terminated))
        {
            return connection;
        }

        connection.Dispose();
        return Scope.Empty;
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) => _hub.Subscribe(observer);

    /// <summary>Disconnect handle and source observer for a connection, published before its source subscription arrives.</summary>
    /// <param name="parent">The owning connectable signal.</param>
    private sealed class Connection(ConnectableSignal<T> parent) : IObserver<T>, IDisposable
    {
        /// <summary>The owning connectable signal.</summary>
        private readonly ConnectableSignal<T> _parent = parent;

        /// <summary>The source subscription feeding the hub.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Disposed via SinkSubscription.Dispose.")]
        private IDisposable? _sourceSubscription;

        /// <summary>Disposal latch; non-zero once the handle has been disposed.</summary>
        private int _disposed;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _parent._hub.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            Volatile.Write(ref _parent._terminated, true);
            try
            {
                _parent._hub.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            Volatile.Write(ref _parent._terminated, true);
            try
            {
                _parent._hub.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            SinkSubscription.Dispose(ref _sourceSubscription);
            lock (_parent._gate)
            {
                if (ReferenceEquals(_parent._connection, this))
                {
                    _parent._connection = null;
                }
            }
        }

        /// <summary>Stores the source subscription, releasing it at once when the handle was disposed while subscribing.</summary>
        /// <param name="sourceSubscription">The source subscription feeding the hub.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Attach(IDisposable sourceSubscription) =>
            SinkSubscription.Set(ref _sourceSubscription, sourceSubscription);
    }
}
