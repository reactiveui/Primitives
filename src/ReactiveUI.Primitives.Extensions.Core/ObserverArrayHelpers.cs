// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Helpers for broadcasting to, and removing from, swap-on-write <see cref="IObserver{T}"/> arrays.</summary>
public static class ObserverArrayHelpers
{
    /// <summary>Fans <paramref name="value"/> out to every observer in <paramref name="observers"/> in order; an empty array emits nothing.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="observers">The observer array snapshot.</param>
    /// <param name="value">The value to broadcast.</param>
    public static void Broadcast<T>(IObserver<T>[] observers, T value)
    {
        if (observers.Length == 0)
        {
            return;
        }

        for (var i = 0; i < observers.Length; i++)
        {
            observers[i].OnNext(value);
        }
    }

    /// <summary>Copies <paramref name="current"/> without <paramref name="observer"/>, reporting absence so a caller can skip the swap entirely.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="current">The current observer array snapshot.</param>
    /// <param name="observer">The observer to remove.</param>
    /// <param name="empty">The sentinel empty array.</param>
    /// <returns>
    /// The shortened array, <paramref name="empty"/> when the removed observer was the last one, or
    /// <see langword="null"/> when the observer is absent from <paramref name="current"/>.
    /// </returns>
    public static IObserver<T>[]? RemoveOrNull<T>(
        IObserver<T>[] current,
        IObserver<T> observer,
        IObserver<T>[] empty)
    {
        var idx = Array.IndexOf(current, observer);
        if (idx < 0)
        {
            return null;
        }

        if (current.Length == 1)
        {
            return empty;
        }

        var copy = new IObserver<T>[current.Length - 1];
        for (var i = 0; i < idx; i++)
        {
            copy[i] = current[i];
        }

        for (var i = idx + 1; i < current.Length; i++)
        {
            copy[i - 1] = current[i];
        }

        return copy;
    }
}
