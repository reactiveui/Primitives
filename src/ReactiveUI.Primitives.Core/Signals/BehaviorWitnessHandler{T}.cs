// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Subscription handle that removes its observer from the owning signal exactly once when disposed.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <param name="owner">The owning signal.</param>
/// <param name="observer">The subscribed observer.</param>
internal sealed class BehaviorWitnessHandler<T>(IWitnessRemovable<T> owner, IObserver<T> observer) : IDisposable
{
    /// <summary>The owning signal, cleared after the first disposal.</summary>
    private IWitnessRemovable<T>? _owner = owner;

    /// <summary>The subscribed observer, cleared after the first disposal.</summary>
    private IObserver<T>? _observer = observer;

    /// <inheritdoc/>
    public void Dispose()
    {
        var ownerState = Interlocked.Exchange(ref _owner, null);
        var observerState = Interlocked.Exchange(ref _observer, null);
        if (ownerState is null || observerState is null)
        {
            return;
        }

        ownerState.RemoveObserver(observerState);
    }
}
