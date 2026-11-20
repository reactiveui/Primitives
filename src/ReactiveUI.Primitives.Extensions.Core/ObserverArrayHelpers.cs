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

    /// <summary>
    /// Emits every element of <paramref name="source"/> to <paramref name="observer"/> in order through
    /// <see cref="IObserver{T}.OnNext(T)"/>, indexing arrays, <see cref="List{T}"/> and the list interfaces directly
    /// and enumerating anything else.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source collection.</typeparam>
    /// <param name="observer">The observer to emit elements to.</param>
    /// <param name="source">The source collection to iterate.</param>
    public static void FastForEach<T>(IObserver<T> observer, IEnumerable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (source is T[] array)
        {
            for (var i = 0; i < array.Length; i++)
            {
                observer.OnNext(array[i]);
            }

            return;
        }

        if (source is List<T> fullList)
        {
            for (var i = 0; i < fullList.Count; i++)
            {
                observer.OnNext(fullList[i]);
            }

            return;
        }

        if (source is IList<T> list)
        {
            for (var i = 0; i < list.Count; i++)
            {
                observer.OnNext(list[i]);
            }

            return;
        }

        if (source is IReadOnlyList<T> readOnlyList)
        {
            for (var i = 0; i < readOnlyList.Count; i++)
            {
                observer.OnNext(readOnlyList[i]);
            }

            return;
        }

        foreach (var item in source)
        {
            observer.OnNext(item);
        }
    }

    /// <summary>Pushes multiple <paramref name="events"/> to <paramref name="observer"/> in order.</summary>
    /// <typeparam name="T">The type of elements in the array.</typeparam>
    /// <param name="observer">The observer to emit elements to.</param>
    /// <param name="events">The values to push.</param>
    public static void OnNext<T>(IObserver<T> observer, params T[] events)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        ArgumentExceptionHelper.ThrowIfNull(events);

        for (var i = 0; i < events.Length; i++)
        {
            observer.OnNext(events[i]);
        }
    }
}
