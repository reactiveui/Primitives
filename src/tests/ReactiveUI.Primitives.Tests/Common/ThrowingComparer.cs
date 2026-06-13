// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>An equality comparer that throws when comparing values.</summary>
internal sealed class ThrowingComparer : IEqualityComparer<int>
{
    /// <summary>Defers to a faulting comparison so the equality comparison throws when invoked.</summary>
    /// <param name="x">The first value to compare.</param>
    /// <param name="y">The second value to compare.</param>
    /// <returns>This method never returns; the faulting comparison always throws.</returns>
    public bool Equals(int x, int y) => Fail();

    /// <summary>Returns the hash code for the specified value.</summary>
    /// <param name="obj">The value to hash.</param>
    /// <returns>The hash code for the value.</returns>
    public int GetHashCode(int obj) => obj.GetHashCode();

    /// <summary>Throws to simulate a faulting comparison.</summary>
    /// <returns>This method never returns; it always throws.</returns>
    private static bool Fail() => throw new InvalidOperationException("comparer-fault");
}
