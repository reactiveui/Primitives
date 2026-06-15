// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that can remove a previously subscribed observer when its subscription handle is disposed.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal interface IObserverRemovable<out T>
{
    /// <summary>Removes an observer previously registered via subscription.</summary>
    /// <param name="observer">The observer to remove.</param>
    void RemoveObserver(IObserver<T> observer);
}
