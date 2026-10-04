// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if NET8_0_OR_GREATER
using System.Security.Cryptography;
#endif

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Provides replay cryptography helpers across supported target frameworks.</summary>
internal static class HttpReplayCryptography
{
    /// <summary>Clears retained secret material.</summary>
    /// <param name="buffer">The buffer to clear.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ZeroMemory(byte[] buffer)
    {
#if NET8_0_OR_GREATER
        CryptographicOperations.ZeroMemory(buffer);
#else
        ZeroMemoryFallback(buffer);
#endif
    }

    /// <summary>Compares equal-length byte sequences without branching on byte values.</summary>
    /// <param name="left">The first sequence.</param>
    /// <param name="right">The second sequence.</param>
    /// <returns>Whether the two sequences have the same length and content.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool FixedTimeEquals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
#if NET8_0_OR_GREATER
        return CryptographicOperations.FixedTimeEquals(left, right);
#else
        return FixedTimeEqualsFallback(left, right);
#endif
    }

#if !NET8_0_OR_GREATER
    /// <summary>Clears retained secret material on frameworks without <c>CryptographicOperations</c>.</summary>
    /// <param name="buffer">The buffer to clear.</param>
    [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
    private static void ZeroMemoryFallback(byte[] buffer) => Array.Clear(buffer, 0, buffer.Length);

    /// <summary>Compares same-length byte sequences with work that depends on length, not byte values.</summary>
    /// <param name="left">The first sequence.</param>
    /// <param name="right">The second sequence.</param>
    /// <returns>Whether the two sequences have the same length and content.</returns>
    [MethodImpl(MethodImplOptions.NoInlining | MethodImplOptions.NoOptimization)]
    private static bool FixedTimeEqualsFallback(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        var length = left.Length;
        var difference = 0;
        for (var index = 0; index < length; index++)
        {
            difference |= left[index] - right[index];
        }

        return difference == 0;
    }
#endif
}
