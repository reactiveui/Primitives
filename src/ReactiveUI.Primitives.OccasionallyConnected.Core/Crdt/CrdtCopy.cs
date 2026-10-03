// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Creates owned CRDT collection snapshots.</summary>
internal static class CrdtCopy
{
    /// <summary>The largest collection capacity allocated from caller-provided counter component counts.</summary>
    private const int MaximumOwnedCounterComponents = CrdtBounds.MaximumOwnedCounterComponents;

    /// <summary>The largest collection capacity allocated from caller-provided retained dot counts.</summary>
    private const int MaximumOwnedDots = CrdtBounds.MaximumOwnedRetainedDots;

    /// <summary>The largest collection capacity allocated from caller-provided OR-set element counts.</summary>
    private const int MaximumOwnedElements = CrdtBounds.MaximumOwnedElements;

    /// <summary>The largest byte array allocated from caller-provided payload lengths.</summary>
    private const int MaximumOwnedBytes = CrdtBounds.MaximumOwnedRegisterBytes;

    /// <summary>Gets the largest byte array accepted for mutation payload ownership.</summary>
    internal static int MaximumOwnedMutationBytes => MaximumOwnedBytes;

    /// <summary>Copies bytes after enforcing a finite pre-allocation ceiling.</summary>
    /// <param name="source">The source bytes.</param>
    /// <param name="maximumLength">The maximum accepted length.</param>
    /// <returns>The copied bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    internal static byte[] Bytes(ReadOnlyMemory<byte> source, int maximumLength)
    {
        var effectiveMaximumLength = Math.Min(maximumLength, MaximumOwnedBytes);
        if (source.Length <= effectiveMaximumLength)
        {
            return source.ToArray();
        }

        throw new InvalidOperationException("The CRDT byte value exceeds configured ownership bounds.");
    }

    /// <summary>Copies register bytes after enforcing a finite pre-allocation ceiling.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The copied bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte[] RegisterBytes(ReadOnlyMemory<byte> source) =>
        Bytes(source, CrdtBounds.MaximumOwnedRegisterBytes);

    /// <summary>Copies element bytes after enforcing a finite pre-allocation ceiling.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The copied bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte[] ElementBytes(ReadOnlyMemory<byte> source) =>
        Bytes(source, CrdtBounds.MaximumOwnedElementBytes);

    /// <summary>Copies a string-to-long dictionary with ordinal keys.</summary>
    /// <param name="source">The source dictionary.</param>
    /// <returns>The copied dictionary.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    internal static ReadOnlyDictionary<string, long> StringLongDictionary(IReadOnlyDictionary<string, long> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        var count = source.Count;
        ThrowIfCountOutOfRange(count, MaximumOwnedCounterComponents);
        Dictionary<string, long> copy = [with(capacity: count, comparer: StringComparer.Ordinal)];
        foreach (var pair in source)
        {
            ThrowIfNextItemExceedsLimit(copy.Count, MaximumOwnedCounterComponents);
            copy.Add(pair.Key, pair.Value);
        }

        return new(copy);
    }

    /// <summary>Copies a list of byte memories.</summary>
    /// <param name="source">The source list.</param>
    /// <returns>The copied list.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    internal static ReadOnlyCollection<ReadOnlyMemory<byte>> MemoryList(IReadOnlyList<ReadOnlyMemory<byte>> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        var count = source.Count;
        ThrowIfCountOutOfRange(count, MaximumOwnedElements);
        List<ReadOnlyMemory<byte>> copy = [with(capacity: count)];
        foreach (var item in source)
        {
            ThrowIfNextItemExceedsLimit(copy.Count, MaximumOwnedElements);
            copy.Add(ElementBytes(item));
        }

        return new(copy);
    }

    /// <summary>Copies a list of dot element bindings.</summary>
    /// <param name="source">The source list.</param>
    /// <returns>The copied list.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    internal static ReadOnlyCollection<CrdtDotElement> DotElementList(IReadOnlyList<CrdtDotElement> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        var count = source.Count;
        ThrowIfCountOutOfRange(count, MaximumOwnedDots);
        List<CrdtDotElement> copy = [with(capacity: count)];
        foreach (var item in source)
        {
            ThrowIfNextItemExceedsLimit(copy.Count, MaximumOwnedDots);
            copy.Add(item);
        }

        return new(copy);
    }

    /// <summary>Copies a list of observed dots.</summary>
    /// <param name="source">The source list.</param>
    /// <returns>The copied list.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    internal static ReadOnlyCollection<CrdtDot> DotList(IReadOnlyList<CrdtDot> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        var count = source.Count;
        ThrowIfCountOutOfRange(count, MaximumOwnedDots);
        List<CrdtDot> copy = [with(capacity: count)];
        foreach (var item in source)
        {
            ThrowIfNextItemExceedsLimit(copy.Count, MaximumOwnedDots);
            copy.Add(item);
        }

        return new(copy);
    }

    /// <summary>Throws when a caller-provided count is outside the ownership ceiling.</summary>
    /// <param name="count">The caller-provided count.</param>
    /// <param name="maximumCount">The maximum accepted count.</param>
    /// <exception cref="InvalidOperationException">Thrown when the count is invalid.</exception>
    private static void ThrowIfCountOutOfRange(int count, int maximumCount)
    {
        if (count >= 0 && count <= maximumCount)
        {
            return;
        }

        throw new InvalidOperationException("The CRDT collection count exceeds configured ownership bounds.");
    }

    /// <summary>Throws when adding another item would exceed the ownership ceiling.</summary>
    /// <param name="currentCount">The copied item count.</param>
    /// <param name="maximumCount">The maximum accepted count.</param>
    /// <exception cref="InvalidOperationException">Thrown when the count is invalid.</exception>
    private static void ThrowIfNextItemExceedsLimit(int currentCount, int maximumCount)
    {
        if (currentCount < maximumCount)
        {
            return;
        }

        throw new InvalidOperationException("The CRDT collection count exceeds configured ownership bounds.");
    }
}
