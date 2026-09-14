// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates bounded owned copies for internal snapshot recovery server views.</summary>
internal static class ServerSnapshotRecoveryCollectionCopy
{
    /// <summary>The fixed ownership ceiling for internal server snapshot recovery lists.</summary>
    private const int MaximumOwnedItems = 4096;

    /// <summary>Copies a list after capturing its count once.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="source">The source list.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <returns>A read-only owned copy.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The collection exceeds the ownership limit.</exception>
    internal static ReadOnlyCollection<T> List<T>(IReadOnlyList<T>? source, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        var count = source.Count;
        if (count > MaximumOwnedItems)
        {
            throw new ArgumentOutOfRangeException(parameterName, count, "The server snapshot recovery collection exceeds the fixed ownership limit.");
        }

        var copy = new T[count];
        for (var index = 0; index < count; index++)
        {
            copy[index] = source[index];
        }

        return new(copy);
    }
}
