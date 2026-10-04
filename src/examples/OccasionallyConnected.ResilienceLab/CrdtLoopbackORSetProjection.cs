// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Projects OR-set authoritative state into lab display values and observed remove dots.</summary>
internal static class CrdtLoopbackORSetProjection
{
    /// <summary>Gets the active OR-set element display as text.</summary>
    /// <param name="state">The OR-set state.</param>
    /// <returns>The active element text.</returns>
    internal static string GetElementDisplay(CrdtState state)
    {
        var elements = state.Value.Elements;
        return elements.Count == 1 ? FromBytes(elements[0]) : string.Join(",", ElementsToStrings(elements));
    }

    /// <summary>Finds an observed OR-set dot from a received authoritative state.</summary>
    /// <param name="state">The authoritative OR-set state.</param>
    /// <param name="element">The element text.</param>
    /// <returns>The observed dot.</returns>
    /// <exception cref="InvalidOperationException">The element was not observed.</exception>
    internal static CrdtDot FindObservedDot(CrdtState state, string element)
    {
        for (var index = 0; index < state.DotBindings.Count; index++)
        {
            var binding = state.DotBindings[index];
            if (string.Equals(FromBytes(binding.Element), element, StringComparison.Ordinal))
            {
                return binding.Dot;
            }
        }

        throw new InvalidOperationException($"The OR-set element '{element}' was not observed.");
    }

    /// <summary>Gets the active element strings.</summary>
    /// <param name="elements">The element bytes.</param>
    /// <returns>The element strings.</returns>
    private static List<string> ElementsToStrings(IReadOnlyList<ReadOnlyMemory<byte>> elements)
    {
        List<string> values = [];
        for (var index = 0; index < elements.Count; index++)
        {
            values.Add(FromBytes(elements[index]));
        }

        values.Sort(StringComparer.Ordinal);
        return values;
    }

    /// <summary>Converts UTF-8 bytes to text.</summary>
    /// <param name="value">The byte value.</param>
    /// <returns>The text value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FromBytes(ReadOnlyMemory<byte> value) =>
        Encoding.UTF8.GetString(value.Span);
}
