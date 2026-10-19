// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Creates bounded owned collection copies for snapshot recovery contracts.</summary>
internal static class SnapshotRecoveryCollectionCopy
{
    /// <summary>The maximum items copied by a snapshot recovery DTO before caller-specific validation.</summary>
    internal const int MaximumOwnedItems = 4096;

    /// <summary>Copies a list after capturing its count once and checking the fixed ownership ceiling.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="source">The source list.</param>
    /// <param name="parameterName">The public parameter or property name.</param>
    /// <returns>A read-only copy.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="source"/> exceeds the fixed ownership ceiling.</exception>
    internal static ReadOnlyCollection<T> List<T>(IReadOnlyList<T>? source, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        var count = source.Count;
        if (count > MaximumOwnedItems)
        {
            throw new ArgumentOutOfRangeException(parameterName, count, "The snapshot recovery collection exceeds the fixed ownership limit.");
        }

        var copy = new T[count];
        for (var index = 0; index < count; index++)
        {
            copy[index] = source[index];
        }

        return new(copy);
    }
}
