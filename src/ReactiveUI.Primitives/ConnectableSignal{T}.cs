// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
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
    private readonly object _gate = new();
    private readonly IObservable<T> _source;
    private readonly ISignal<T> _hub;
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
    public static ConnectableSignal<T> PublishLive<T>(this IObservable<T> source) =>
        source.Multicast(new Signal<T>());

    /// <summary>
    /// Replays source values through a bounded replay hub.
    /// </summary>
    public static ConnectableSignal<T> ReplayLive<T>(this IObservable<T> source, int bufferSize) =>
        source.Multicast(new ReplaySignal<T>(bufferSize));

    /// <summary>
    /// Replays source values through a replay hub constrained by count and time.
    /// </summary>
    public static ConnectableSignal<T> ReplayLive<T>(this IObservable<T> source, int bufferSize, TimeSpan window) =>
        source.Multicast(new ReplaySignal<T>(bufferSize, window));

    /// <summary>
    /// Shares one live source subscription while at least one observer is subscribed.
    /// </summary>
    public static IObservable<T> ShareLive<T>(this IObservable<T> source) => source.PublishLive().RefCount();

    /// <summary>
    /// Connects on first subscriber and disconnects when the last subscriber disposes.
    /// </summary>
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
    /// Connects after <paramref name="subscriberCount"/> observers have subscribed.
    /// </summary>
    public static IObservable<T> AutoConnect<T>(this ConnectableSignal<T> source, int subscriberCount = 1)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (subscriberCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(subscriberCount));
        }

        var gate = new object();
        var count = 0;
        var connected = false;
        return ReactiveUI.Primitives.Signals.Signal.Create<T>(observer =>
        {
            var subscription = source.Subscribe(observer);
            lock (gate)
            {
                count++;
                if (!connected && count >= subscriberCount)
                {
                    connected = true;
                    source.Connect();
                }
            }

            return subscription;
        });
    }

    private sealed class RefCountGate<TValue>
    {
        private readonly object _gate = new();
        private readonly ConnectableSignal<TValue> _source;
        private int _count;
        private IDisposable? _connection;

        private RefCountGate(ConnectableSignal<TValue> source) => _source = source;

        public static RefCountGate<TValue> For(ConnectableSignal<TValue> source) => new(source);

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
}
