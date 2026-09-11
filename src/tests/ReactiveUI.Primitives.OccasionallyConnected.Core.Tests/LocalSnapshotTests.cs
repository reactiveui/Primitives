// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalSnapshot"/>.</summary>
public sealed class LocalSnapshotTests
{
    /// <summary>Verifies the compatibility constructor defaults the revision.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompatibilityConstructorDefaultsRevision()
    {
        var snapshot = new LocalSnapshot(
            new("sensor/temperature"),
            1,
            null,
            new("reading", 1, "application/json", ReadOnlyMemory<byte>.Empty, "sha256-empty"),
            DateTimeOffset.UnixEpoch);

        await Assert.That(snapshot.Revision).IsEqualTo(0);
    }
}
