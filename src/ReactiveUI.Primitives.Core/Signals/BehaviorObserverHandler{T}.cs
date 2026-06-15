// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Subscription handle that removes its observer from the owning signal exactly once when disposed.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal sealed class BehaviorObserverHandler<T> : IDisposable
{
    /// <summary>The owning signal, cleared after the first disposal.</summary>
    private IObserverRemovable<T>? _owner;

    /// <summary>The subscribed observer, cleared after the first disposal.</summary>
    private IObserver<T>? _observer;

    /// <summary>Initializes a new instance of the <see cref="BehaviorObserverHandler{T}"/> class.</summary>
    /// <param name="owner">The owning signal.</param>
    /// <param name="observer">The subscribed observer.</param>
    public BehaviorObserverHandler(IObserverRemovable<T> owner, IObserver<T> observer)
    {
        _owner = owner;
        _observer = observer;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        var owner = Interlocked.Exchange(ref _owner, null);
        var observer = Interlocked.Exchange(ref _observer, null);
        if (owner is null || observer is null)
        {
            return;
        }

        owner.RemoveObserver(observer);
    }
}
