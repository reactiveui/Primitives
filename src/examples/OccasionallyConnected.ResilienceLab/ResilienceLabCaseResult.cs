// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Describes one expected-versus-actual lab invariant.</summary>
/// <param name="Name">The stable invariant name.</param>
/// <param name="Expected">The expected outcome.</param>
/// <param name="Actual">The observed outcome.</param>
/// <param name="Succeeded">Whether the invariant passed.</param>
[System.Diagnostics.DebuggerDisplay("{Name,nq}; Passed={Succeeded,nq}")]
public sealed record ResilienceLabCaseResult(
    string Name,
    object Expected,
    object Actual,
    bool Succeeded)
{
    /// <summary>Creates a failed invariant result.</summary>
    /// <param name="name">The invariant name.</param>
    /// <param name="expected">The expected outcome.</param>
    /// <param name="actual">The actual outcome.</param>
    /// <returns>The failed invariant result.</returns>
    public static ResilienceLabCaseResult Fail(string name, object expected, object actual) =>
        new(name, expected, actual, false);
}
