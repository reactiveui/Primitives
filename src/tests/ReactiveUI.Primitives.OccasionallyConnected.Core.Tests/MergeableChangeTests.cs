// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests the factory-constrained mergeable cases.</summary>
public sealed class MergeableChangeTests
{
    /// <summary>The test client identifier.</summary>
    private const string ClientId = "client";

    /// <summary>Verifies a counter factory rejects invalid components.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CounterFactoryRejectsNegativeComponents()
    {
        await Assert.That(static () => MergeableChange.IncrementOnlyCounterSet(ClientId, -1)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => MergeableChange.CounterSet(ClientId, -1, 0)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => MergeableChange.CounterSet(ClientId, 0, -1)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a counter factory rejects missing actor identity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CounterFactoryRejectsMissingActor()
    {
        await Assert.That(static () => MergeableChange.IncrementOnlyCounterSet(string.Empty, 0)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => MergeableChange.CounterSet(string.Empty, 0, 0)).ThrowsExactly<InvalidOperationException>();
    }
}
