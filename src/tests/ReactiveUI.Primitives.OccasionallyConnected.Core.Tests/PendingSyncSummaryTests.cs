// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="PendingSyncSummary"/>.</summary>
public sealed class PendingSyncSummaryTests
{
    /// <summary>The pending byte count.</summary>
    private const long PendingBytes = 64;

    /// <summary>Verifies the pending byte count is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsBytes()
    {
        var oldestOperation = DateTimeOffset.UnixEpoch;
        var summary = new PendingSyncSummary(1, PendingBytes, oldestOperation);

        await Assert.That(summary.OperationCount).IsEqualTo(1);
        await Assert.That(summary.Bytes).IsEqualTo(PendingBytes);
        await Assert.That(summary.OldestOperationUtc).IsEqualTo(oldestOperation);
    }
}
