// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using CiCoverage = global::OccasionallyConnected.Ci.Coverage;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Tests coverage gate build selection arguments.</content>
public sealed partial class CoverageTests
{
    /// <summary>Verifies build reuse cannot bypass complete CI argument validation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NoBuildRequiresCompleteCiArguments()
    {
        await using var error = new StringWriter(CultureInfo.InvariantCulture);
        var result = CiCoverage.Run(["--no-build"], error);
        await Assert.That(result).IsEqualTo(1);
        await Assert.That(error.ToString()).Contains("Full CI invocation requires");
    }

    /// <summary>Verifies report-only validation rejects an irrelevant build option.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NoBuildRejectsStandaloneReports()
    {
        await using var error = new StringWriter(CultureInfo.InvariantCulture);
        var result = CiCoverage.Run(["--no-build", "--report-path", "unused.xml", "--package-name", PackageName], error);
        await Assert.That(result).IsEqualTo(1);
        await Assert.That(error.ToString()).Contains("--no-build is only supported for a full CI invocation");
    }
}
