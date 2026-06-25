// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Tracks auto-connect subscription state.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class AutoConnectSignal<T> : IObservable<T>
{
    /// <summary>Current subscriber count.</summary>
    private int _count;

    /// <summary>Connect-once latch; 0 before connecting, 1 once connected.</summary>
    private int _connected;

    /// <summary>Initializes a new instance of the <see cref="AutoConnectSignal{T}"/> class.</summary>
    /// <param name="source">Connectable signal being auto-connected.</param>
    /// <param name="subscriberCount">Number of observers required before connecting.</param>
    public AutoConnectSignal(ConnectableSignal<T> source, int subscriberCount)
        : this(source, subscriberCount, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="AutoConnectSignal{T}"/> class.</summary>
    /// <param name="source">Connectable signal being auto-connected.</param>
    /// <param name="subscriberCount">Number of observers required before connecting.</param>
    /// <param name="onConnect">Action invoked with the connection disposable when the source connects.</param>
    public AutoConnectSignal(ConnectableSignal<T> source, int subscriberCount, Action<IDisposable>? onConnect)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(subscriberCount);

        Source = source;
        SubscriberCount = subscriberCount;
        OnConnect = onConnect;
    }

    /// <summary>Gets the connectable signal being auto-connected.</summary>
    private ConnectableSignal<T> Source { get; }

    /// <summary>Gets the number of observers required before connecting.</summary>
    private int SubscriberCount { get; }

    /// <summary>Gets the callback invoked with the connection disposable.</summary>
    private Action<IDisposable>? OnConnect { get; }

    /// <summary>Subscribes an observer and connects when the threshold is reached.</summary>
    /// <param name="observer">Observer to subscribe.</param>
    /// <returns>A disposable that removes the observer subscription.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var subscription = Source.Subscribe(observer);

        // Auto-connect only ever counts up and connects once, so no lock is needed: a single
        // CompareExchange latches the connect once the threshold is reached.
        var count = Interlocked.Increment(ref _count);
        if (count >= SubscriberCount && Interlocked.CompareExchange(ref _connected, 1, 0) == 0)
        {
            var connection = Source.Connect();
            OnConnect?.Invoke(connection);
        }

        return subscription;
    }
}
