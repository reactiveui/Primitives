// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Describes the result of one resilience lab scenario run.</summary>
[System.Diagnostics.DebuggerDisplay("{Scenario,nq}; Passed={Succeeded,nq}; Cases={Cases.Count,nq}")]
public sealed class ResilienceLabRunResult
{
    /// <summary>Initializes a new instance of the <see cref="ResilienceLabRunResult"/> class.</summary>
    /// <param name="scenario">The scenario name.</param>
    /// <param name="cases">The invariant case results.</param>
    public ResilienceLabRunResult(string scenario, IReadOnlyList<ResilienceLabCaseResult> cases)
    {
        Scenario = scenario;
        Cases = new ReadOnlyCollection<ResilienceLabCaseResult>([.. cases]);
    }

    /// <summary>Gets the scenario name.</summary>
    public string Scenario { get; }

    /// <summary>Gets the invariant case results.</summary>
    public IReadOnlyList<ResilienceLabCaseResult> Cases { get; }

    /// <summary>Gets a value indicating whether every invariant passed.</summary>
    public bool Succeeded
    {
        get
        {
            for (var index = 0; index < Cases.Count; index++)
            {
                if (!Cases[index].Succeeded)
                {
                    return false;
                }
            }

            return true;
        }
    }

    /// <summary>Gets the process exit code for this run.</summary>
    public int ExitCode => Succeeded ? 0 : 1;
}
