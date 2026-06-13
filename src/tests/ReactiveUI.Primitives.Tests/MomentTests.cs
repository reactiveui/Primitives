// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Moment{T}"/> equality and hashing contracts.</summary>
public class MomentTests
{
    /// <summary>Verifies a moment with a null value uses a deterministic null hash code.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NullValueMomentUsesDeterministicNullHashCode()
    {
        const int NullHashSeed = 1963;
        DateTimeOffset timestamp = new(2026, 5, 24, 22, 52, 0, TimeSpan.Zero);
        Moment<string?> moment = new(null, timestamp);
        await Assert.That(moment.GetHashCode()).IsEqualTo(timestamp.GetHashCode() ^ NullHashSeed);
    }
}
