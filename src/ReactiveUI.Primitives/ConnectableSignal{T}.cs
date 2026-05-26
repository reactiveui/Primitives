// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>
/// Connectable hot signal that subscribes to its source only when connected.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class ConnectableSignal<T> : IObservable<T>
{
    /// <summary>
    /// Synchronizes connection state.
    /// </summary>
    private readonly object _gate = new();

    /// <summary>
    /// Source sequence to connect.
    /// </summary>
    private readonly IObservable<T> _source;

    /// <summary>
    /// Multicast hub that receives source values.
    /// </summary>
    private readonly ISignal<T> _hub;

    /// <summary>
    /// Active source connection.
    /// </summary>
    private IDisposable? _connection;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectableSignal{T}"/> class.
    /// </summary>
    /// <param name="source">The cold or hot source sequence.</param>
    /// <param name="hub">The multicast hub.</param>
    public ConnectableSignal(IObservable<T> source, ISignal<T> hub)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _hub = hub ?? throw new ArgumentNullException(nameof(hub));
    }

    /// <summary>
    /// Subscribes the hub to the source if it is not already connected.
    /// </summary>
    /// <returns>A handle that disconnects the source subscription.</returns>
    public IDisposable Connect()
    {
        lock (_gate)
        {
            if (_connection == null)
            {
                var sourceSubscription = _source.Subscribe(_hub);
                _connection = Disposable.Create(() =>
                {
                    lock (_gate)
                    {
                        sourceSubscription.Dispose();
                        _connection = null;
                    }
                });
            }

            return _connection;
        }
    }

    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<T> observer) => _hub.Subscribe(observer);
}
