// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>Observer that forwards notifications to an immutable observer list.</summary>
/// <typeparam name="T">The observed value type.</typeparam>
internal sealed class ListWitness<T> : IObserver<T>
{
    /// <summary>Immutable observer snapshot.</summary>
    private readonly CopyOnWriteList<IObserver<T>> _observers;

    /// <summary>Initializes a new instance of the <see cref="ListWitness{T}"/> class.</summary>
    /// <param name="observers">Observers that receive forwarded notifications.</param>
    public ListWitness(CopyOnWriteList<IObserver<T>> observers) => _observers = observers;

    /// <summary>Gets a value indicating whether the list contains observers.</summary>
    public bool HasObservers => _observers.Items.Length > 0;

    /// <inheritdoc/>
    public void OnCompleted()
    {
        var targetObservers = _observers.Items;
        for (var i = 0; i < targetObservers.Length; i++)
        {
            targetObservers[i].OnCompleted();
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        var targetObservers = _observers.Items;
        for (var i = 0; i < targetObservers.Length; i++)
        {
            targetObservers[i].OnError(error);
        }
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        var targetObservers = _observers.Items;
        for (var i = 0; i < targetObservers.Length; i++)
        {
            targetObservers[i].OnNext(value);
        }
    }

    /// <summary>Returns a witness with the observer added.</summary>
    /// <param name="observer">Observer to add.</param>
    /// <returns>The updated observer list witness.</returns>
    internal IObserver<T> Add(IObserver<T> observer) => new ListWitness<T>(_observers.Add(observer));

    /// <summary>Returns a witness with the observer removed.</summary>
    /// <param name="observer">Observer to remove.</param>
    /// <returns>The updated observer list witness.</returns>
    internal IObserver<T> Remove(IObserver<T> observer)
    {
        var i = Array.IndexOf(_observers.Items, observer);
        if (i < 0)
        {
            return this;
        }

        if (_observers.Items.Length == 1)
        {
            return EmptyWitness<T>.Instance;
        }

        return new ListWitness<T>(_observers.Remove(observer));
    }
}
