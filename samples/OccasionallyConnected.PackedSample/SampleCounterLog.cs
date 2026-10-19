// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace OccasionallyConnected.PackedSample;

/// <summary>Records diagnostic counts and formats an atomic snapshot.</summary>
internal sealed class SampleCounterLog
{
    /// <summary>The counts updated by request and observer callbacks.</summary>
    private readonly ConcurrentDictionary<string, int> _counts = new(StringComparer.Ordinal);

    /// <summary>Records one occurrence of a diagnostic key.</summary>
    /// <param name="key">The key to count.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Record(string key) => _counts.AddOrUpdate(key, 1, static (_, count) => count + 1);

    /// <summary>Formats a snapshot without racing dictionary count and copy operations.</summary>
    /// <param name="empty">The result when the snapshot contains no entries.</param>
    /// <returns>The sorted diagnostic counts.</returns>
    internal string Describe(string empty = "")
    {
        var snapshot = _counts.ToArray();
        if (snapshot.Length == 0)
        {
            return empty;
        }

        Array.Sort(snapshot, static (left, right) => StringComparer.Ordinal.Compare(left.Key, right.Key));
        var entries = new string[snapshot.Length];
        for (var index = 0; index < snapshot.Length; index++)
        {
            entries[index] = $"{snapshot[index].Key} x{snapshot[index].Value}";
        }

        return string.Join(", ", entries);
    }
}
