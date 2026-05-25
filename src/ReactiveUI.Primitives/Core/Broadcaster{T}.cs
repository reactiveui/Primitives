// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Core;

/// <summary>
/// Copy-on-write observer broadcaster optimized for zero-allocation single-subscriber delivery.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal struct Broadcaster<T> : IEquatable<Broadcaster<T>>
{
    /// <summary>
    /// Stores either a single observer, an observer array, or <see langword="null"/>.
    /// </summary>
    private object? _observers;

    /// <summary>
    /// Gets a value indicating whether at least one observer is registered.
    /// </summary>
    public bool HasObservers => Volatile.Read(ref _observers) is not null;

    /// <summary>
    /// Adds an observer to the broadcaster.
    /// </summary>
    /// <param name="observer">Observer to add.</param>
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

    /// <summary>
    /// Removes all observers from the broadcaster.
    /// </summary>
    public void Clear() => Volatile.Write(ref _observers, null);

    /// <summary>
    /// Removes an observer from the broadcaster.
    /// </summary>
    /// <param name="observer">Observer to remove.</param>
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

    /// <summary>
    /// Broadcasts a value to the current observers.
    /// </summary>
    /// <param name="value">Value to broadcast.</param>
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

    /// <summary>
    /// Broadcasts an error to the current observers.
    /// </summary>
    /// <param name="exception">Error to broadcast.</param>
    public void Error(Exception exception)
    {
        var snapshot = Volatile.Read(ref _observers);
        if (snapshot is IObserver<T> single)
        {
            single.OnError(exception);
            return;
        }

        if (snapshot is not IObserver<T>[] many)
        {
            return;
        }

        for (var i = 0; i < many.Length; i++)
        {
            many[i].OnError(exception);
        }
    }

    /// <summary>
    /// Broadcasts completion to the current observers.
    /// </summary>
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

    /// <inheritdoc/>
    public readonly bool Equals(Broadcaster<T> other) =>
        ReferenceEquals(_observers, other._observers);

    /// <inheritdoc/>
    public override readonly bool Equals(object? obj) =>
        obj is Broadcaster<T> other && Equals(other);

    /// <inheritdoc/>
    public override readonly int GetHashCode() =>
        _observers is null ? 0 : System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(_observers);
}
