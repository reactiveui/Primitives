// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Tracks reference-counted connection state.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class AutoShareSignal<T> : IObservable<T>
{
    /// <summary>Synchronizes reference-count state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Active subscriber count.</summary>
    private int _count;

    /// <summary>Active source connection.</summary>
    private IDisposable? _connection;

    /// <summary>Initializes a new instance of the <see cref="AutoShareSignal{T}"/> class.</summary>
    /// <param name="source">Connectable signal being reference-counted.</param>
    public AutoShareSignal(ConnectableSignal<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the connectable signal being reference-counted.</summary>
    private ConnectableSignal<T> Source { get; }

    /// <summary>Subscribes an observer and manages the shared connection lifetime.</summary>
    /// <param name="observer">Observer to subscribe.</param>
    /// <returns>A disposable that removes the observer and may disconnect the source.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        IDisposable subscription;
        lock (_gate)
        {
            subscription = Source.Subscribe(observer);
            _count++;
            _connection ??= Source.Connect();
        }

        return new AutoShareSubscription<T>(this, subscription);
    }

    /// <summary>Releases an observer subscription and disconnects the source when the last one leaves.</summary>
    /// <param name="subscription">The inner subscription to release.</param>
    internal void Release(IDisposable subscription)
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
}
