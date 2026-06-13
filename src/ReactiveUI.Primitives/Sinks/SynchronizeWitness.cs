// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Observer that serializes notifications behind a gate so downstream operators always observe the
/// single-threaded <c>OnNext*</c> then <c>OnError</c>|<c>OnCompleted</c> grammar they depend on, even when the
/// upstream source delivers concurrently. Stateful sinks (counting, distinct, buffering) rely on that
/// grammar; placing one of these ahead of them is the supported way to consume a non-conformant source.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SynchronizeWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The gate that serializes every forwarded notification.</summary>
    private readonly Lock _gate;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="SynchronizeWitness{T}"/> class with a private gate.</summary>
    /// <param name="observer">The downstream observer.</param>
    public SynchronizeWitness(IObserver<T> observer)
        : this(observer, new())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SynchronizeWitness{T}"/> class sharing the supplied gate.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="gate">The gate shared with other synchronized observers.</param>
    public SynchronizeWitness(IObserver<T> observer, Lock gate)
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

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
