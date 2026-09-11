// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteApplyResult"/>.</summary>
public sealed class RemoteApplyResultTests
{
    /// <summary>The next remote cursor.</summary>
    private const string NextCursor = "cursor-2";

    /// <summary>The duplicate event count.</summary>
    private const int DuplicateCount = 2;

    /// <summary>The snapshot revision.</summary>
    private const long SnapshotRevision = 8;

    /// <summary>Verifies supplied values are retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsValues()
    {
        var result = new RemoteApplyResult(NextCursor, 1, DuplicateCount, SnapshotRevision);

        await Assert.That(result.NextCursor).IsEqualTo(NextCursor);
        await Assert.That(result.AppliedCount).IsEqualTo(1);
        await Assert.That(result.DuplicateCount).IsEqualTo(DuplicateCount);
        await Assert.That(result.SnapshotRevision).IsEqualTo(SnapshotRevision);
    }

    /// <summary>Verifies the compatibility constructor defaults the snapshot revision.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompatibilityConstructorDefaultsSnapshotRevision() =>
        await Assert.That(new RemoteApplyResult(NextCursor, 1, 0).SnapshotRevision).IsEqualTo(0);
}
