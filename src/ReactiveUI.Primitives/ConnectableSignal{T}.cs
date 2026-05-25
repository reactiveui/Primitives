// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

#pragma warning disable SA1107, SA1116, SA1117, SA1204, SA1402, SA1501, SA1611, SA1615, SA1618

namespace ReactiveUI.Primitives;

/// <summary>
/// Connectable hot signal that subscribes to its source only when connected.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
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

/// <summary>
/// Hot-sharing operators for Primitives connectable signals.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public static class ConnectableSignalMixins
{
    /// <summary>
    /// Multicasts source values through the supplied hub.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to multicast.</param>
    /// <param name="hub">Hub that receives source values.</param>
    /// <returns>A connectable signal.</returns>
    public static ConnectableSignal<T> Multicast<T>(this IObservable<T> source, ISignal<T> hub)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (hub == null)
        {
            throw new ArgumentNullException(nameof(hub));
        }

        return new ConnectableSignal<T>(source, hub);
    }

    /// <summary>
    /// Publishes source values through a live signal hub.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to publish.</param>
    /// <returns>A connectable live signal.</returns>
    public static ConnectableSignal<T> PublishLive<T>(this IObservable<T> source) =>
        source.Multicast(new Signal<T>());

    /// <summary>
    /// Replays source values through a bounded replay hub.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to replay.</param>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <returns>A connectable replay signal.</returns>
    public static ConnectableSignal<T> ReplayLive<T>(this IObservable<T> source, int bufferSize) =>
        source.Multicast(new ReplaySignal<T>(bufferSize));

    /// <summary>
    /// Replays source values through a replay hub constrained by count and time.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to replay.</param>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <param name="window">Maximum replay window.</param>
    /// <returns>A connectable replay signal.</returns>
    public static ConnectableSignal<T> ReplayLive<T>(this IObservable<T> source, int bufferSize, TimeSpan window) =>
        source.Multicast(new ReplaySignal<T>(bufferSize, window));

    /// <summary>
    /// Shares one live source subscription while at least one observer is subscribed.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to share.</param>
    /// <returns>A reference-counted live sequence.</returns>
    public static IObservable<T> ShareLive<T>(this IObservable<T> source) => source.PublishLive().RefCount();

    /// <summary>
    /// Connects on first subscriber and disconnects when the last subscriber disposes.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Connectable signal to reference count.</param>
    /// <returns>A reference-counted sequence.</returns>
    public static IObservable<T> RefCount<T>(this ConnectableSignal<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        var gate = RefCountGate<T>.For(source);
        return ReactiveUI.Primitives.Signals.Signal.Create<T>(gate.Subscribe);
    }

    /// <summary>
    /// Connects on the first observer subscription.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Connectable signal to connect.</param>
    /// <returns>A sequence that connects after the first subscription.</returns>
    public static IObservable<T> AutoConnect<T>(this ConnectableSignal<T> source) =>
        AutoConnect(source, 1);

    /// <summary>
    /// Connects after <paramref name="subscriberCount"/> observers have subscribed.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Connectable signal to connect.</param>
    /// <param name="subscriberCount">Number of observers required before connecting.</param>
    /// <returns>A sequence that connects after the requested number of subscriptions.</returns>
    public static IObservable<T> AutoConnect<T>(this ConnectableSignal<T> source, int subscriberCount)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (subscriberCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(subscriberCount));
        }

        var gate = AutoConnectGate<T>.For(source, subscriberCount);
        return ReactiveUI.Primitives.Signals.Signal.Create<T>(gate.Subscribe);
    }

    /// <summary>
    /// Tracks reference-counted connection state.
    /// </summary>
    /// <typeparam name="TValue">The value type.</typeparam>
    private sealed class RefCountGate<TValue>
    {
        /// <summary>
        /// Synchronizes reference-count state.
        /// </summary>
        private readonly object _gate = new();

        /// <summary>
        /// Connectable signal being reference-counted.
        /// </summary>
        private readonly ConnectableSignal<TValue> _source;

        /// <summary>
        /// Active subscriber count.
        /// </summary>
        private int _count;

        /// <summary>
        /// Active source connection.
        /// </summary>
        private IDisposable? _connection;

        /// <summary>
        /// Initializes a new instance of the <see cref="RefCountGate{TValue}"/> class.
        /// </summary>
        /// <param name="source">Connectable signal being reference-counted.</param>
        private RefCountGate(ConnectableSignal<TValue> source) => _source = source;

        /// <summary>
        /// Creates a reference-count gate for a connectable signal.
        /// </summary>
        /// <param name="source">Connectable signal being reference-counted.</param>
        /// <returns>A reference-count gate.</returns>
        public static RefCountGate<TValue> For(ConnectableSignal<TValue> source) => new(source);

        /// <summary>
        /// Subscribes an observer and manages the shared connection lifetime.
        /// </summary>
        /// <param name="observer">Observer to subscribe.</param>
        /// <returns>A disposable that removes the observer and may disconnect the source.</returns>
        public IDisposable Subscribe(IObserver<TValue> observer)
        {
            IDisposable subscription;
            lock (_gate)
            {
                subscription = _source.Subscribe(observer);
                _count++;
                _connection ??= _source.Connect();
            }

            return Disposable.Create(() =>
            {
                subscription.Dispose();
                lock (_gate)
                {
                    _count--;
                    if (_count == 0)
                    {
                        _connection?.Dispose();
                        _connection = null;
                    }
                }
            });
        }
    }

    /// <summary>
    /// Tracks auto-connect subscription state.
    /// </summary>
    /// <typeparam name="TValue">The value type.</typeparam>
    private sealed class AutoConnectGate<TValue>
    {
        /// <summary>
        /// Synchronizes auto-connect state.
        /// </summary>
        private readonly object _gate = new();

        /// <summary>
        /// Connectable signal being auto-connected.
        /// </summary>
        private readonly ConnectableSignal<TValue> _source;

        /// <summary>
        /// Number of observers required before connecting.
        /// </summary>
        private readonly int _subscriberCount;

        /// <summary>
        /// Current subscriber count.
        /// </summary>
        private int _count;

        /// <summary>
        /// Value indicating whether the source has connected.
        /// </summary>
        private bool _connected;

        /// <summary>
        /// Initializes a new instance of the <see cref="AutoConnectGate{TValue}"/> class.
        /// </summary>
        /// <param name="source">Connectable signal being auto-connected.</param>
        /// <param name="subscriberCount">Number of observers required before connecting.</param>
        private AutoConnectGate(ConnectableSignal<TValue> source, int subscriberCount)
        {
            _source = source;
            _subscriberCount = subscriberCount;
        }

        /// <summary>
        /// Creates an auto-connect gate for a connectable signal.
        /// </summary>
        /// <param name="source">Connectable signal being auto-connected.</param>
        /// <param name="subscriberCount">Number of observers required before connecting.</param>
        /// <returns>An auto-connect gate.</returns>
        public static AutoConnectGate<TValue> For(ConnectableSignal<TValue> source, int subscriberCount) =>
            new(source, subscriberCount);

        /// <summary>
        /// Subscribes an observer and connects when the threshold is reached.
        /// </summary>
        /// <param name="observer">Observer to subscribe.</param>
        /// <returns>A disposable that removes the observer subscription.</returns>
        public IDisposable Subscribe(IObserver<TValue> observer)
        {
            var subscription = _source.Subscribe(observer);
            lock (_gate)
            {
                _count++;
                if (!_connected && _count >= _subscriberCount)
                {
                    _connected = true;
                    _source.Connect();
                }
            }

            return subscription;
        }
    }
}
