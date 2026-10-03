// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>
/// A deterministic SplitMix64 generator for bounded protocol fuzz tests. It produces the same sequence for the same seed
/// on every runtime, so a failing case can be replayed from its seed and case index.
/// </summary>
internal sealed class ProtocolFuzzRandom
{
    /// <summary>The SplitMix64 state increment.</summary>
    private const ulong GoldenGamma = 0x9E3779B97F4A7C15UL;

    /// <summary>The first SplitMix64 finalizer multiplier.</summary>
    private const ulong FirstMixMultiplier = 0xBF58476D1CE4E5B9UL;

    /// <summary>The second SplitMix64 finalizer multiplier.</summary>
    private const ulong SecondMixMultiplier = 0x94D049BB133111EBUL;

    /// <summary>The first SplitMix64 finalizer shift.</summary>
    private const int FirstMixShift = 30;

    /// <summary>The second SplitMix64 finalizer shift.</summary>
    private const int SecondMixShift = 27;

    /// <summary>The third SplitMix64 finalizer shift.</summary>
    private const int ThirdMixShift = 31;

    /// <summary>The FNV-1a offset basis.</summary>
    private const ulong FnvOffsetBasis = 0xCBF29CE484222325UL;

    /// <summary>The FNV-1a prime.</summary>
    private const ulong FnvPrime = 0x100000001B3UL;

    /// <summary>The current generator state.</summary>
    private ulong _state;

    /// <summary>Initializes a new instance of the <see cref="ProtocolFuzzRandom"/> class.</summary>
    /// <param name="seed">The case seed.</param>
    internal ProtocolFuzzRandom(ulong seed) => _state = seed;

    /// <summary>Derives a stable per-case seed from a base seed, a target name and a case index.</summary>
    /// <param name="baseSeed">The fixed base seed of the fuzz test.</param>
    /// <param name="target">The fuzz target name.</param>
    /// <param name="caseIndex">The case index.</param>
    /// <returns>The case seed.</returns>
    internal static ulong DeriveSeed(ulong baseSeed, string target, int caseIndex)
    {
        var hash = FnvOffsetBasis;
        for (var index = 0; index < target.Length; index++)
        {
            hash = unchecked((hash ^ target[index]) * FnvPrime);
        }

        return Mix(unchecked(baseSeed ^ hash ^ ((ulong)caseIndex * GoldenGamma)));
    }

    /// <summary>Formats the reproduction coordinates of a fuzz case.</summary>
    /// <param name="baseSeed">The fixed base seed of the fuzz test.</param>
    /// <param name="target">The fuzz target name.</param>
    /// <param name="caseIndex">The case index.</param>
    /// <returns>The reproduction text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string Describe(ulong baseSeed, string target, int caseIndex) =>
        string.Create(CultureInfo.InvariantCulture, $"seed=0x{baseSeed:X16} target={target} case={caseIndex} caseSeed=0x{DeriveSeed(baseSeed, target, caseIndex):X16}");

    /// <summary>Returns the next 64 random bits.</summary>
    /// <returns>The random value.</returns>
    internal ulong NextUInt64()
    {
        _state = unchecked(_state + GoldenGamma);
        return Mix(_state);
    }

    /// <summary>Returns a random value in <c>[0, maxExclusive)</c>.</summary>
    /// <param name="maxExclusive">The exclusive upper bound; must be positive.</param>
    /// <returns>The random value.</returns>
    internal int Next(int maxExclusive) => (int)(NextUInt64() % (ulong)maxExclusive);

    /// <summary>Returns a random value in <c>[minInclusive, maxExclusive)</c>.</summary>
    /// <param name="minInclusive">The inclusive lower bound.</param>
    /// <param name="maxExclusive">The exclusive upper bound; must exceed <paramref name="minInclusive"/>.</param>
    /// <returns>The random value.</returns>
    internal int Next(int minInclusive, int maxExclusive) => minInclusive + Next(maxExclusive - minInclusive);

    /// <summary>Returns <see langword="true"/> with probability one in <paramref name="odds"/>.</summary>
    /// <param name="odds">The odds denominator.</param>
    /// <returns>The random decision.</returns>
    internal bool OneIn(int odds) => Next(odds) == 0;

    /// <summary>Picks one item from a list.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="items">The non-empty items.</param>
    /// <returns>The picked item.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal T Pick<T>(IReadOnlyList<T> items) => items[Next(items.Count)];

    /// <summary>Fills a new byte array with random bytes.</summary>
    /// <param name="length">The array length.</param>
    /// <returns>The random bytes.</returns>
    internal byte[] NextBytes(int length)
    {
        var bytes = new byte[length];
        for (var index = 0; index < length; index++)
        {
            bytes[index] = (byte)NextUInt64();
        }

        return bytes;
    }

    /// <summary>Applies the SplitMix64 finalizer.</summary>
    /// <param name="value">The value to mix.</param>
    /// <returns>The mixed value.</returns>
    private static ulong Mix(ulong value)
    {
        var mixed = unchecked((value ^ (value >> FirstMixShift)) * FirstMixMultiplier);
        mixed = unchecked((mixed ^ (mixed >> SecondMixShift)) * SecondMixMultiplier);
        return mixed ^ (mixed >> ThirdMixShift);
    }
}
