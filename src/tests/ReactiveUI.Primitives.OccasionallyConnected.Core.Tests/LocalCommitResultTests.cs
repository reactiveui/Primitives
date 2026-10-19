// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalCommitResult"/>.</summary>
public sealed class LocalCommitResultTests
{
    /// <summary>The snapshot revision.</summary>
    private const long SnapshotRevision = 8;

    /// <summary>Verifies supplied values are retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsValues()
    {
        var operationId = OperationId.New();
        var committedAt = DateTimeOffset.UnixEpoch;
        var result = new LocalCommitResult(operationId, 1, SnapshotRevision, committedAt);

        await Assert.That(result.OperationId).IsEqualTo(operationId);
        await Assert.That(result.ClientSequence).IsEqualTo(1);
        await Assert.That(result.SnapshotRevision).IsEqualTo(SnapshotRevision);
        await Assert.That(result.CommittedAtUtc).IsEqualTo(committedAt);
    }

    /// <summary>Verifies the compatibility constructor defaults the snapshot revision.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompatibilityConstructorDefaultsSnapshotRevision() =>
        await Assert.That(new LocalCommitResult(OperationId.New(), 1, DateTimeOffset.UnixEpoch).SnapshotRevision).IsEqualTo(0);
}
