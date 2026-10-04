// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Compares four independently projected received-history values against one expected value.</summary>
/// <typeparam name="T">The projected value type.</typeparam>
/// <param name="Expected">The expected received-history value.</param>
/// <param name="ClientAForward">The client A forward-merge projected value.</param>
/// <param name="ClientAReverse">The client A reverse-merge projected value.</param>
/// <param name="ClientBForward">The client B forward-merge projected value.</param>
/// <param name="ClientBReverse">The client B reverse-merge projected value.</param>
internal sealed record CrdtLoopbackHistoryComparison<T>(
    T Expected,
    T ClientAForward,
    T ClientAReverse,
    T ClientBForward,
    T ClientBReverse)
{
    /// <summary>Gets the expected value when every projection matches, or the first divergent projection.</summary>
    /// <param name="comparer">The value comparer.</param>
    /// <returns>The actual value to publish in a case result.</returns>
    internal T GetActual(IEqualityComparer<T> comparer)
    {
        if (!comparer.Equals(ClientAForward, Expected))
        {
            return ClientAForward;
        }

        if (!comparer.Equals(ClientAReverse, Expected))
        {
            return ClientAReverse;
        }

        if (!comparer.Equals(ClientBForward, Expected))
        {
            return ClientBForward;
        }

        return comparer.Equals(ClientBReverse, Expected)
            ? Expected
            : ClientBReverse;
    }
}
