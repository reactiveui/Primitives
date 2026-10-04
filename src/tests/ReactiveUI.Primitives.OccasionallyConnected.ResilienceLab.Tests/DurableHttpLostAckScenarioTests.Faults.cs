// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Checks which typed public stream faults invalidate the durable HTTP proof.</summary>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>Only nontransient error or critical faults are terminal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TerminalFaultClassificationUsesSeverityAndRetryability()
    {
        var fault = new OccasionallyConnectedFault("test.fault", "typed fault", DateTimeOffset.UnixEpoch, null, null, null);
        await Assert.That(DurableHttpLostAckScenario.IsTerminalFault(fault)).IsTrue();
        await Assert.That(DurableHttpLostAckScenario.IsTerminalFault(fault with { Severity = FaultSeverity.Critical })).IsTrue();
        await Assert.That(DurableHttpLostAckScenario.IsTerminalFault(fault with { Severity = FaultSeverity.Warning })).IsFalse();
        await Assert.That(DurableHttpLostAckScenario.IsTerminalFault(fault with { Severity = FaultSeverity.Information })).IsFalse();
        await Assert.That(DurableHttpLostAckScenario.IsTerminalFault(fault with { IsTransient = true })).IsFalse();
        await Assert.That(DurableHttpLostAckScenario.IsTerminalFault(fault with { Severity = FaultSeverity.Critical, IsTransient = true })).IsFalse();
    }
}
