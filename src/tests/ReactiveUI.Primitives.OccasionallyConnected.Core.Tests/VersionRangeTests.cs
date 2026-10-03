// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="VersionRange"/>.</summary>
public sealed class VersionRangeTests
{
    /// <summary>Verifies the maximum supported version is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsMaximum() =>
        await Assert.That(new VersionRange(new(1, 0), new(1, 1)).Maximum).IsEqualTo(new(1, 1));
}
