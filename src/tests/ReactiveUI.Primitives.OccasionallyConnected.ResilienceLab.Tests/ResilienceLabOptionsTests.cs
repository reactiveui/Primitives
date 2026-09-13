// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="ResilienceLabOptions"/>.</summary>
public sealed class ResilienceLabOptionsTests
{
    /// <summary>The explicitly requested scenario name.</summary>
    private const string RequestedScenario = "custom-lab";

    /// <summary>Verifies the parser uses an explicit scenario switch value.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseUsesScenarioSwitchValue()
    {
        var options = ResilienceLabOptions.Parse(["--scenario", RequestedScenario]);

        await Assert.That(options.Scenario).IsEqualTo(RequestedScenario);
    }

    /// <summary>Verifies the parser defaults to the CRDT loopback scenario when no arguments are supplied.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseDefaultsToCrdtLoopbackWhenNoArgumentsSupplied()
    {
        var options = ResilienceLabOptions.Parse([]);

        await Assert.That(options.Scenario).IsEqualTo(CrdtLoopbackScenarioShape.ScenarioName);
    }

    /// <summary>Verifies the parser defaults to the CRDT loopback scenario when the scenario switch has no value.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseDefaultsToCrdtLoopbackWhenScenarioSwitchHasNoValue()
    {
        var options = ResilienceLabOptions.Parse(["--scenario"]);

        await Assert.That(options.Scenario).IsEqualTo(CrdtLoopbackScenarioShape.ScenarioName);
    }

    /// <summary>Verifies the parser defaults to the CRDT loopback scenario when the scenario switch is missing.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ParseDefaultsToCrdtLoopbackWhenScenarioSwitchIsMissing()
    {
        var options = ResilienceLabOptions.Parse(["--other", RequestedScenario]);

        await Assert.That(options.Scenario).IsEqualTo(CrdtLoopbackScenarioShape.ScenarioName);
    }
}
