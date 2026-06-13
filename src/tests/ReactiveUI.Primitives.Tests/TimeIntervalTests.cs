// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="TimeInterval{T}"/> equality and hashing contracts.</summary>
public class TimeIntervalTests
{
    /// <summary>Verifies a time interval with a null value uses a deterministic null hash code.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NullValueTimeIntervalUsesDeterministicNullHashCode()
    {
        const int NullHashSeed = 1963;
        var interval = TimeSpan.FromMilliseconds(123);
        TimeInterval<string?> timeInterval = new(null, interval);
        await Assert.That(timeInterval.GetHashCode()).IsEqualTo(interval.GetHashCode() ^ NullHashSeed);
    }
}
