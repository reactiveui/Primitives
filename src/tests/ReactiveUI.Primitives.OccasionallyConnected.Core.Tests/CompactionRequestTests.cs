// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="CompactionRequest"/>.</summary>
public sealed class CompactionRequestTests
{
    /// <summary>The requested target byte count.</summary>
    private const long TargetBytes = 1024;

    /// <summary>Verifies the target size is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsTargetBytes()
    {
        StreamId streamId = new("sensor/temperature");
        var retainAfter = DateTimeOffset.UnixEpoch;
        var request = new CompactionRequest(streamId, retainAfter, TargetBytes);

        await Assert.That(request.StreamId).IsEqualTo(streamId);
        await Assert.That(request.RetainTerminalRecordsAfter).IsEqualTo(retainAfter);
        await Assert.That(request.TargetBytes).IsEqualTo(TargetBytes);
    }
}
