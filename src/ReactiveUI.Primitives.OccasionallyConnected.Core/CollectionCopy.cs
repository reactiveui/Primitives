// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Creates defensive read-only copies of caller-owned collections.</summary>
/// <remarks>
/// These helpers run at the in-process protocol boundary after transport or builder validation has already enforced
/// batch and payload bounds, so the count-based allocations do not declare a runtime resource guarantee.
/// </remarks>
internal static class CollectionCopy
{
    /// <summary>Copies a dictionary into a read-only dictionary.</summary>
    /// <param name="source">The source dictionary.</param>
    /// <returns>A read-only dictionary containing the copied entries.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    internal static ReadOnlyDictionary<string, string> Dictionary(IReadOnlyDictionary<string, string> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        var copy = new Dictionary<string, string>(source.Count, StringComparer.Ordinal);
        foreach (var pair in source)
        {
            copy.Add(pair.Key, pair.Value);
        }

        return new(copy);
    }

    /// <summary>Copies a list into a read-only collection.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="source">The source list.</param>
    /// <returns>A read-only collection containing the copied items.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    internal static ReadOnlyCollection<T> List<T>(IReadOnlyList<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        var copy = new T[source.Count];
        for (var index = 0; index < source.Count; index++)
        {
            copy[index] = source[index];
        }

        return new(copy);
    }

    /// <summary>Copies a collection into a read-only collection.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="source">The source collection.</param>
    /// <returns>A read-only collection containing the copied items.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    internal static ReadOnlyCollection<T> Collection<T>(IReadOnlyCollection<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        var copy = new T[source.Count];
        var index = 0;
        foreach (var item in source)
        {
            copy[index] = item;
            index++;
        }

        return new(copy);
    }
}
