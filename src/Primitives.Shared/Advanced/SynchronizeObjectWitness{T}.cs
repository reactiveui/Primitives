// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer that serializes notifications using an object gate.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SynchronizeObjectWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The gate that serializes every forwarded notification.</summary>
    private readonly object _gate;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="SynchronizeObjectWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="gate">The gate shared with other synchronized observers.</param>
    public SynchronizeObjectWitness(IObserver<T> observer, object gate)
    {
        _observer = observer;
        _gate = gate;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        lock (_gate)
        {
            _observer.OnNext(value);
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        lock (_gate)
        {
            _observer.OnError(error);
        }
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        lock (_gate)
        {
            _observer.OnCompleted();
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
