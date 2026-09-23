// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Checks missing durable snapshot evidence in the HTTP lost-ACK workflow.</summary>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>An uninitialized store snapshot has no counter value.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MissingSnapshotHasNoCounter()
    {
        var counter = await DurableHttpLostAckScenario.ReadSnapshotCounterAsync(null, CancellationToken.None);
        await Assert.That(counter.HasValue).IsFalse();
    }
}
