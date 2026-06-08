// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>Connectable hot signal that subscribes to its source only when connected.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ConnectableSignal<T> : IObservable<T>
{
    /// <summary>Synchronizes connection state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Source sequence to connect.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Multicast hub that receives source values.</summary>
    private readonly ISignal<T> _hub;

    /// <summary>Active source connection.</summary>
    private IDisposable? _connection;

    /// <summary>Initializes a new instance of the <see cref="ConnectableSignal{T}"/> class.</summary>
    /// <param name="source">The cold or hot source sequence.</param>
    /// <param name="hub">The multicast hub.</param>
    public ConnectableSignal(IObservable<T> source, ISignal<T> hub)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _hub = hub ?? throw new ArgumentNullException(nameof(hub));
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Subscribes the hub to the source if it is not already connected.</summary>
    /// <returns>A handle that disconnects the source subscription.</returns>
    public IDisposable Connect()
    {
        lock (_gate)
        {
            // Allocate the connection only on the first connect. A dedicated disposable type
            // avoids the closure (and extra anonymous-disposable wrapper) that Scope.Create
            // would allocate.
            _connection ??= new Connection(this, _source.Subscribe(_hub));
            return _connection;
        }
    }

    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<T> observer) => _hub.Subscribe(observer);

    /// <summary>Disconnect handle for an active source connection.</summary>
    private sealed class Connection : IDisposable
    {
        /// <summary>The owning connectable signal.</summary>
        private readonly ConnectableSignal<T> _parent;

        /// <summary>The source subscription feeding the hub; nulled once on dispose.</summary>
        private IDisposable? _sourceSubscription;

        /// <summary>Initializes a new instance of the <see cref="Connection"/> class.</summary>
        /// <param name="parent">The owning connectable signal.</param>
        /// <param name="sourceSubscription">The source subscription feeding the hub.</param>
        public Connection(ConnectableSignal<T> parent, IDisposable sourceSubscription)
        {
            _parent = parent;
            _sourceSubscription = sourceSubscription;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            var sourceSubscription = Interlocked.Exchange(ref _sourceSubscription, null);
            if (sourceSubscription is null)
            {
                return;
            }

            lock (_parent._gate)
            {
                sourceSubscription.Dispose();
                if (ReferenceEquals(_parent._connection, this))
                {
                    _parent._connection = null;
                }
            }
        }
    }
}
