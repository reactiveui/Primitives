// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>
/// Hot-sharing operators for Primitives connectable signals.
/// </summary>
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

        return new(source, hub);
    }

    /// <summary>
    /// Shares source values through a live signal hub.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to share.</param>
    /// <returns>A connectable live signal.</returns>
    public static ConnectableSignal<T> ShareLive<T>(this IObservable<T> source) =>
        source.Multicast(new Signal<T>());

    /// <summary>
    /// Shares source values through a live signal hub.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to share.</param>
    /// <returns>A connectable live signal.</returns>
    public static ConnectableSignal<T> Share<T>(this IObservable<T> source) =>
        source.ShareLive();

    /// <summary>
    /// Replays source values through a bounded replay hub.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to replay.</param>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <returns>A connectable replay signal.</returns>
    public static ConnectableSignal<T> ReplayLive<T>(this IObservable<T> source, int bufferSize) =>
        source.Multicast(new HistorySignal<T>(bufferSize));

    /// <summary>
    /// Replays source values through a replay hub constrained by count and time.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to replay.</param>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <param name="window">Maximum replay window.</param>
    /// <returns>A connectable replay signal.</returns>
    public static ConnectableSignal<T> ReplayLive<T>(this IObservable<T> source, int bufferSize, TimeSpan window) =>
        source.Multicast(new HistorySignal<T>(bufferSize, window));

    /// <summary>
    /// Replays source values through a bounded replay hub.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to replay.</param>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <returns>A connectable replay signal.</returns>
    public static ConnectableSignal<T> Replay<T>(this IObservable<T> source, int bufferSize) =>
        source.ReplayLive(bufferSize);

    /// <summary>
    /// Replays source values through a replay hub constrained by count and time.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to replay.</param>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <param name="window">Maximum replay window.</param>
    /// <returns>A connectable replay signal.</returns>
    public static ConnectableSignal<T> Replay<T>(this IObservable<T> source, int bufferSize, TimeSpan window) =>
        source.ReplayLive(bufferSize, window);

    /// <summary>
    /// Shares one live source subscription while at least one observer is subscribed.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to share.</param>
    /// <returns>A reference-counted live sequence.</returns>
    public static IObservable<T> ShareLatest<T>(this IObservable<T> source) => source.ShareLive().AutoShare();

    /// <summary>
    /// Connects on first subscriber and disconnects when the last subscriber disposes.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Connectable signal to reference count.</param>
    /// <returns>A reference-counted sequence.</returns>
    public static IObservable<T> AutoShare<T>(this ConnectableSignal<T> source)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        return AutoShareGate<T>.For(source);
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

        return AutoConnectGate<T>.For(source, subscriberCount);
    }

    /// <summary>
    /// Tracks reference-counted connection state.
    /// </summary>
    /// <typeparam name="TValue">The value type.</typeparam>
    private sealed class AutoShareGate<TValue> : IObservable<TValue>
    {
        /// <summary>
        /// Synchronizes reference-count state.
        /// </summary>
        private readonly Lock _gate = new();

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
        /// Initializes a new instance of the <see cref="AutoShareGate{TValue}"/> class.
        /// </summary>
        /// <param name="source">Connectable signal being reference-counted.</param>
        private AutoShareGate(ConnectableSignal<TValue> source) => _source = source;

        /// <summary>
        /// Creates a reference-count gate for a connectable signal.
        /// </summary>
        /// <param name="source">Connectable signal being reference-counted.</param>
        /// <returns>A reference-count gate.</returns>
        public static AutoShareGate<TValue> For(ConnectableSignal<TValue> source) => new(source);

        /// <summary>
        /// Subscribes an observer and manages the shared connection lifetime.
        /// </summary>
        /// <param name="observer">Observer to subscribe.</param>
        /// <returns>A disposable that removes the observer and may disconnect the source.</returns>
        public IDisposable Subscribe(IObserver<TValue> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            IDisposable subscription;
            lock (_gate)
            {
                subscription = _source.Subscribe(observer);
                _count++;
                _connection ??= _source.Connect();
            }

            return new Subscription(this, subscription);
        }

        /// <summary>
        /// Releases an observer subscription and disconnects the source when the last one leaves.
        /// </summary>
        /// <param name="subscription">The inner subscription to release.</param>
        private void Release(IDisposable subscription)
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
        }

        /// <summary>
        /// Reference-counted subscription handle. A dedicated type avoids the closure that
        /// <c>Disposable.Create</c> would allocate per subscription.
        /// </summary>
        private sealed class Subscription : IDisposable
        {
            /// <summary>The inner source subscription.</summary>
            private readonly IDisposable _subscription;

            /// <summary>The owning gate; nulled once on dispose.</summary>
            private AutoShareGate<TValue>? _owner;

            /// <summary>Initializes a new instance of the <see cref="Subscription"/> class.</summary>
            /// <param name="owner">The owning gate.</param>
            /// <param name="subscription">The inner source subscription.</param>
            public Subscription(AutoShareGate<TValue> owner, IDisposable subscription)
            {
                _owner = owner;
                _subscription = subscription;
            }

            /// <inheritdoc/>
            public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Release(_subscription);
        }
    }

    /// <summary>
    /// Tracks auto-connect subscription state.
    /// </summary>
    /// <typeparam name="TValue">The value type.</typeparam>
    private sealed class AutoConnectGate<TValue> : IObservable<TValue>
    {
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
        /// Connect-once latch; 0 before connecting, 1 once connected.
        /// </summary>
        private int _connected;

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
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var subscription = _source.Subscribe(observer);

            // Auto-connect only ever counts up and connects once, so no lock is needed: a single
            // CompareExchange latches the connect once the threshold is reached.
            var count = Interlocked.Increment(ref _count);
            if (count >= _subscriberCount && Interlocked.CompareExchange(ref _connected, 1, 0) == 0)
            {
                _source.Connect();
            }

            return subscription;
        }
    }
}
