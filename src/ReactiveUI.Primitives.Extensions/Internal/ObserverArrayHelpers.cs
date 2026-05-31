// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Pure-plumbing helpers for swap-on-write <see cref="IObserver{T}"/> arrays. Centralizes the
/// empty-array short-circuit on broadcast and the not-present short-circuit on remove so the
/// operator hot paths stay branchless on the steady state. Every branch is a pure function
/// over its inputs and is directly RxVoid-testable through this class.
/// </summary>
internal static class ObserverArrayHelpers
{
    /// <summary>
    /// Snapshots the supplied observer array and fans the value out to every observer in order.
    /// Returns silently if the array is empty (which happens during the race between the last
    /// unsubscribe and an already-scheduled broadcast).
    /// </summary>
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

    /// <summary>
    /// Returns a new observer array with <paramref name="observer"/> removed, or
    /// <see langword="null"/> if the observer was not in the array (which happens during
    /// the race between an idempotent subscription dispose and a previous successful remove).
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="current">The current observer array snapshot.</param>
    /// <param name="observer">The observer to remove.</param>
    /// <param name="empty">The sentinel empty array.</param>
    /// <returns>
    /// The new array (possibly the empty sentinel), or <see langword="null"/> if the observer
    /// was not present.
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
