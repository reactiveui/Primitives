// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

internal sealed class ListWitness<T> : IObserver<T>
{
    private readonly ImmutableList<IObserver<T>> _observers;

    public ListWitness(ImmutableList<IObserver<T>> observers) => _observers = observers;

    public bool HasObservers => _observers.Items.Length > 0;

    public void OnCompleted()
    {
        var targetObservers = _observers.Items;
        for (var i = 0; i < targetObservers.Length; i++)
        {
            targetObservers[i].OnCompleted();
        }
    }

    public void OnError(Exception error)
    {
        var targetObservers = _observers.Items;
        for (var i = 0; i < targetObservers.Length; i++)
        {
            targetObservers[i].OnError(error);
        }
    }

    public void OnNext(T value)
    {
        var targetObservers = _observers.Items;
        for (var i = 0; i < targetObservers.Length; i++)
        {
            targetObservers[i].OnNext(value);
        }
    }

    internal IObserver<T> Add(IObserver<T> observer) => new ListWitness<T>(_observers.Add(observer));

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
