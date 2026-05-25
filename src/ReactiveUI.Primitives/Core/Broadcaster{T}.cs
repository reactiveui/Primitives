// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Copy-on-write observer broadcaster optimized for zero-allocation single-subscriber delivery.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal struct Broadcaster<T>
{
    private object? _observers;

    public bool HasObservers => Volatile.Read(ref _observers) is not null;

    public void Add(IObserver<T> observer)
    {
        if (_observers is IObserver<T>[] many)
        {
            var copy = new IObserver<T>[many.Length + 1];
            Array.Copy(many, copy, many.Length);
            copy[many.Length] = observer;
            Volatile.Write(ref _observers, copy);
            return;
        }

        if (_observers is IObserver<T> single)
        {
            Volatile.Write(ref _observers, new[] { single, observer });
            return;
        }

        Volatile.Write(ref _observers, observer);
    }

    public void Clear() => Volatile.Write(ref _observers, null);

    public void Remove(IObserver<T> observer)
    {
        if (ReferenceEquals(_observers, observer))
        {
            Volatile.Write(ref _observers, null);
            return;
        }

        if (_observers is not IObserver<T>[] many)
        {
            return;
        }

        var index = Array.IndexOf(many, observer);
        if (index < 0)
        {
            return;
        }

        if (many.Length == 2)
        {
            Volatile.Write(ref _observers, many[index == 0 ? 1 : 0]);
            return;
        }

        var copy = new IObserver<T>[many.Length - 1];
        for (var i = 0; i < index; i++)
        {
            copy[i] = many[i];
        }

        for (var i = index + 1; i < many.Length; i++)
        {
            copy[i - 1] = many[i];
        }

        Volatile.Write(ref _observers, copy);
    }

    public void Next(T value)
    {
        var snapshot = Volatile.Read(ref _observers);
        if (snapshot is IObserver<T> single)
        {
            single.OnNext(value);
            return;
        }

        if (snapshot is not IObserver<T>[] many)
        {
            return;
        }

        for (var i = 0; i < many.Length; i++)
        {
            many[i].OnNext(value);
        }
    }

    public void Error(Exception error)
    {
        var snapshot = Volatile.Read(ref _observers);
        if (snapshot is IObserver<T> single)
        {
            single.OnError(error);
            return;
        }

        if (snapshot is not IObserver<T>[] many)
        {
            return;
        }

        for (var i = 0; i < many.Length; i++)
        {
            many[i].OnError(error);
        }
    }

    public void Completed()
    {
        var snapshot = Volatile.Read(ref _observers);
        if (snapshot is IObserver<T> single)
        {
            single.OnCompleted();
            return;
        }

        if (snapshot is not IObserver<T>[] many)
        {
            return;
        }

        for (var i = 0; i < many.Length; i++)
        {
            many[i].OnCompleted();
        }
    }
}
