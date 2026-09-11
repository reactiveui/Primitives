// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="CompactionResult"/>.</summary>
public sealed class CompactionResultTests
{
    /// <summary>The reclaimed byte count.</summary>
    private const long BytesReclaimed = 512;

    /// <summary>The compacted record count.</summary>
    private const int RecordsCompacted = 3;

    /// <summary>Verifies reclaimed bytes are retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsReclaimedBytes()
    {
        var result = new CompactionResult(RecordsCompacted, BytesReclaimed);

        await Assert.That(result.RecordsRemoved).IsEqualTo(RecordsCompacted);
        await Assert.That(result.BytesReclaimed).IsEqualTo(BytesReclaimed);
    }
}
