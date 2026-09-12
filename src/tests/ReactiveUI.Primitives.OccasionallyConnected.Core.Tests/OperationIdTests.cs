// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OperationId"/>.</summary>
public sealed class OperationIdTests
{
    /// <summary>Verifies new operation identifiers are non-empty.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NewCreatesNonEmptyIdentifier()
    {
        var operationId = OperationId.New();

        await Assert.That(operationId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies new operation identifiers are unique across calls.</summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Test]
    public async Task NewCreatesDistinctIdentifiers()
    {
        var first = OperationId.New();
        var second = OperationId.New();

        await Assert.That(first).IsNotEqualTo(second);
    }
}
