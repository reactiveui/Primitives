// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Defines command-line options for one resilience lab invocation.</summary>
/// <param name="Scenario">The requested scenario name.</param>
[System.Diagnostics.DebuggerDisplay("{Scenario,nq}")]
public sealed record ResilienceLabOptions(string Scenario)
{
    /// <summary>Parses supported command-line arguments.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <returns>The parsed options.</returns>
    public static ResilienceLabOptions Parse(IReadOnlyList<string> args)
    {
        const string scenarioSwitch = "--scenario";
        for (var index = 0; index < args.Count - 1; index++)
        {
            if (string.Equals(args[index], scenarioSwitch, StringComparison.Ordinal))
            {
                return new(args[index + 1]);
            }
        }

        return new(CrdtLoopbackScenarioShape.ScenarioName);
    }
}
