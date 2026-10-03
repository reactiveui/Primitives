// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SyncState"/>.</summary>
public sealed class SyncStateTests
{
    /// <summary>The pending byte count.</summary>
    private const long PendingBytes = 64;

    /// <summary>The retry delay in seconds.</summary>
    private const int RetrySeconds = 30;

    /// <summary>Verifies the lifecycle status is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsStatus()
    {
        var changedAt = DateTimeOffset.UnixEpoch;
        var lastSuccessfulSync = DateTimeOffset.UnixEpoch.AddMinutes(1);
        var retryAfter = TimeSpan.FromSeconds(RetrySeconds);
        var state = new SyncState(
            SyncLifecycleStatus.Online,
            true,
            1,
            PendingBytes,
            changedAt,
            lastSuccessfulSync,
            retryAfter,
            "OC.Online");

        await Assert.That(state.Status).IsEqualTo(SyncLifecycleStatus.Online);
        await Assert.That(state.NetworkAvailable).IsTrue();
        await Assert.That(state.PendingOperations).IsEqualTo(1);
        await Assert.That(state.PendingBytes).IsEqualTo(PendingBytes);
        await Assert.That(state.ChangedAtUtc).IsEqualTo(changedAt);
        await Assert.That(state.LastSuccessfulSyncUtc).IsEqualTo(lastSuccessfulSync);
        await Assert.That(state.RetryAfter).IsEqualTo(retryAfter);
        await Assert.That(state.ReasonCode).IsEqualTo("OC.Online");
    }
}
