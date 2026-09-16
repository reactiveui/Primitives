// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>An observable that replays a scripted sequence of observer callbacks on subscribe.</summary>
/// <typeparam name="T">The type of the elements produced by the observable.</typeparam>
/// <param name="script">The scripted callback to invoke on each subscription.</param>
/// <param name="subscription">The subscription returned once the script has run.</param>
internal sealed class ScriptedObservable<T>(Action<IObserver<T>> script, IDisposable subscription) : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="ScriptedObservable{T}"/> class that returns an empty subscription.</summary>
    /// <param name="script">The scripted callback to invoke on each subscription.</param>
    internal ScriptedObservable(Action<IObserver<T>> script)
        : this(script, EmptyDisposable.Instance)
    {
    }

    /// <summary>Subscribes the observer and replays the scripted callback.</summary>
    /// <param name="observer">The observer to drive with the script.</param>
    /// <returns>The supplied subscription.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        script(observer);
        return subscription;
    }
}
