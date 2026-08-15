// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Copy-on-write observer broadcaster optimized for zero-allocation single-subscriber delivery.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Observers = {_observers}")]
public struct Broadcaster<T> : IEquatable<Broadcaster<T>>
{
    /// <summary>The starting value the observer hashes are folded into.</summary>
    private const int ObserverHashSeed = 17;

    /// <summary>The multiplier applied between observer hashes, so order is part of the result.</summary>
    private const int ObserverHashStep = 31;

    /// <summary>Stores either a single observer, an observer array, or <see langword="null"/>.</summary>
    private object? _observers;

    /// <summary>Gets a value indicating whether at least one observer is registered.</summary>
    public bool HasObservers => Volatile.Read(ref _observers) is not null;

    /// <summary>Determines whether two broadcasters reference the same observer set.</summary>
    /// <param name="left">The left broadcaster.</param>
    /// <param name="right">The right broadcaster.</param>
    /// <returns><see langword="true"/> when both reference the same observer set; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool operator ==(Broadcaster<T> left, Broadcaster<T> right) => left.Equals(right);

    /// <summary>Determines whether two broadcasters reference different observer sets.</summary>
    /// <param name="left">The left broadcaster.</param>
    /// <param name="right">The right broadcaster.</param>
    /// <returns><see langword="true"/> when the broadcasters reference different observer sets; otherwise, <see langword="false"/>.</returns>
    public static bool operator !=(Broadcaster<T> left, Broadcaster<T> right) => !left.Equals(right);

    /// <summary>
    /// Adds an observer to the broadcaster. The update is a lock-free compare-and-swap, so the
    /// broadcaster is self-contained and does not rely on an external lock for correctness.
    /// </summary>
    /// <param name="observer">Observer to add.</param>
    public void Add(IObserver<T> observer)
    {
        while (true)
        {
            var current = Volatile.Read(ref _observers);
            object next;
            if (current is IObserver<T>[] many)
            {
                var copy = new IObserver<T>[many.Length + 1];
                Array.Copy(many, copy, many.Length);
                copy[many.Length] = observer;
                next = copy;
            }
            else if (current is IObserver<T> single)
            {
                next = new[] { single, observer };
            }
            else if (Interlocked.CompareExchange(ref _observers, observer, null) is null)
            {
                return;
            }
            else
            {
                continue;
            }

            if (ReferenceEquals(Interlocked.CompareExchange(ref _observers, next, current), current))
            {
                return;
            }
        }
    }

    /// <summary>Removes all observers from the broadcaster.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear() => Volatile.Write(ref _observers, null);

    /// <summary>Removes an observer from the broadcaster using a lock-free compare-and-swap.</summary>
    /// <param name="observer">Observer to remove.</param>
    public void Remove(IObserver<T> observer)
    {
        while (true)
        {
            var current = Volatile.Read(ref _observers);
            if (!TryComputeRemoval(current, observer, out var next))
            {
                return;
            }

            if (ReferenceEquals(Interlocked.CompareExchange(ref _observers, next, current), current))
            {
                return;
            }
        }
    }

    /// <summary>Broadcasts a value to the current observers.</summary>
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

    /// <summary>Broadcasts an error to the current observers.</summary>
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

    /// <summary>Broadcasts completion to the current observers.</summary>
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly bool Equals(Broadcaster<T> other) =>
        ReferenceEquals(_observers, other._observers);

    /// <inheritdoc/>
    public override readonly bool Equals(object? obj) =>
        obj is Broadcaster<T> other && Equals(other);

    /// <inheritdoc/>
    /// <remarks>
    /// The hash is taken over the observers the broadcaster is holding, not over the object it happens to
    /// hold them in. That keeps it agreeing with <see cref="Equals(Broadcaster{T})"/> — two broadcasters
    /// sharing a set share its observers — while saying something about the value rather than about which
    /// array the copy-on-write path last allocated. No observers hashes to zero.
    /// <para>
    /// It moves as observers come and go, which is inherent: equality here is the observer set, and the
    /// set is what changes. A broadcaster is compared, never filed in a hash table.
    /// </para>
    /// </remarks>
    [SuppressMessage(
        "Maintainability",
        "SST1482:GetHashCode reads mutable state",
        Justification = "Equality is the observer set, so the hash must follow it; a Broadcaster is compared, never used as a hash key.")]
    public override readonly int GetHashCode()
    {
        var snapshot = _observers;
        if (snapshot is IObserver<T> single)
        {
            return RuntimeHelpers.GetHashCode(single);
        }

        if (snapshot is not IObserver<T>[] many)
        {
            return 0;
        }

        var hash = ObserverHashSeed;
        for (var i = 0; i < many.Length; i++)
        {
            hash = unchecked((hash * ObserverHashStep) + RuntimeHelpers.GetHashCode(many[i]));
        }

        return hash;
    }

    /// <summary>Computes the observer-set value that results from removing an observer.</summary>
    /// <param name="current">The current observer-set snapshot.</param>
    /// <param name="observer">The observer to remove.</param>
    /// <param name="next">The replacement observer-set value when the observer is present.</param>
    /// <returns><c>true</c> when the observer was found and a replacement was produced; otherwise, <c>false</c>.</returns>
    private static bool TryComputeRemoval(object? current, IObserver<T> observer, out object? next)
    {
        next = null;
        if (ReferenceEquals(current, observer))
        {
            return true;
        }

        if (current is not IObserver<T>[] many)
        {
            return false;
        }

        var index = Array.IndexOf(many, observer);
        if (index < 0)
        {
            return false;
        }

        if (many.Length == 2)
        {
            next = many[index == 0 ? 1 : 0];
            return true;
        }

        var copy = new IObserver<T>[many.Length - 1];
        Array.Copy(many, 0, copy, 0, index);
        Array.Copy(many, index + 1, copy, index, many.Length - index - 1);
        next = copy;
        return true;
    }
}
