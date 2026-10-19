// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Copies public server collections into immutable owned containers.</summary>
internal static class ServerCollectionCopy
{
    /// <summary>The empty string dictionary instance.</summary>
    internal static readonly IReadOnlyDictionary<string, string> EmptyDictionary = new ReadOnlyDictionary<string, string>(
        new Dictionary<string, string>(0, StringComparer.Ordinal));

    /// <summary>The maximum retained list length for public server configuration and proposals.</summary>
    private const int MaximumRetainedListItems = 512;

    /// <summary>The maximum retained dictionary length for public server metadata proposals.</summary>
    private const int MaximumRetainedDictionaryEntries = 32;

    /// <summary>Copies a public list before retaining it.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="source">The source list.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <returns>The immutable copy.</returns>
    internal static IReadOnlyList<T> List<T>(IReadOnlyList<T> source, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(source, parameterName);
        var count = source.Count;
        ThrowIfCountOutsideBounds(count, MaximumRetainedListItems, parameterName);
        var copy = new T[count];
        for (var index = 0; index < copy.Length; index++)
        {
            copy[index] = source[index];
        }

        return Array.AsReadOnly(copy);
    }

    /// <summary>Copies a public string dictionary before retaining it.</summary>
    /// <param name="source">The source dictionary.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <returns>The immutable copy.</returns>
    internal static IReadOnlyDictionary<string, string> Dictionary(
        IReadOnlyDictionary<string, string> source,
        string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(source, parameterName);
        var count = source.Count;
        ThrowIfCountOutsideBounds(count, MaximumRetainedDictionaryEntries, parameterName);
        if (count == 0)
        {
            return EmptyDictionary;
        }

        var copy = new Dictionary<string, string>(count, StringComparer.Ordinal);
        foreach (var item in source)
        {
            copy.Add(item.Key, item.Value);
        }

        return new ReadOnlyDictionary<string, string>(copy);
    }

    /// <summary>Throws when an incoming collection count cannot be safely retained.</summary>
    /// <param name="count">The incoming count.</param>
    /// <param name="maximumCount">The maximum supported count.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The count is negative or exceeds the maximum.</exception>
    private static void ThrowIfCountOutsideBounds(int count, int maximumCount, string parameterName)
    {
        if (count >= 0 && count <= maximumCount)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, count, "The retained server collection count is outside the supported bounds.");
    }
}
