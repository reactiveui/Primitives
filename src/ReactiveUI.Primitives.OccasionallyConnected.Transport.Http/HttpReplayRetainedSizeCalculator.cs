// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Calculates retained HTTP replay byte charges from already measured components.</summary>
internal static class HttpReplayRetainedSizeCalculator
{
    /// <summary>Calculates the sum of four retained text byte counts.</summary>
    /// <param name="firstByteCount">The first retained byte count.</param>
    /// <param name="secondByteCount">The second retained byte count.</param>
    /// <param name="thirdByteCount">The third retained byte count.</param>
    /// <param name="fourthByteCount">The fourth retained byte count.</param>
    /// <returns>The total retained byte count.</returns>
    internal static long SumTextBytes(
        int firstByteCount,
        int secondByteCount,
        int thirdByteCount,
        int fourthByteCount) =>
        (long)firstByteCount + secondByteCount + thirdByteCount + fourthByteCount;
}
