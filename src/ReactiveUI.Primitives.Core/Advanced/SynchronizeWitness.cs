// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Serializes concurrent observer notifications so values precede a single terminal notification.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SynchronizeWitness: Observer = {_observer}, Subscription = {_subscription}")]
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

    /// <summary>Gets the gate serializing downstream notifications.</summary>
    internal Lock Gate => _gate;

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

    /// <summary>Assigns the upstream subscription, disposing the incoming one when this sink holds a subscription or has been disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
