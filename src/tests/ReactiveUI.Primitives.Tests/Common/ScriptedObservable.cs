// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>An observable that replays a scripted sequence of observer callbacks on subscribe.</summary>
/// <typeparam name="T">The type of the elements produced by the observable.</typeparam>
/// <param name="script">The scripted callback to invoke on each subscription.</param>
internal sealed class ScriptedObservable<T>(Action<IObserver<T>> script) : IObservable<T>
{
    /// <summary>Subscribes the observer and replays the scripted callback.</summary>
    /// <param name="observer">The observer to drive with the script.</param>
    /// <returns>An empty disposable subscription.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        script(observer);
        return EmptyDisposable.Instance;
    }
}
