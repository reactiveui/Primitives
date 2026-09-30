// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using CiCoverage = global::OccasionallyConnected.Ci.Coverage;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Tests evaluated framework discovery for the coverage gate.</content>
public sealed partial class CoverageTests
{
    /// <summary>Verifies conditional assignments do not admit inactive platform frameworks.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FrameworkDiscoveryEvaluatesConditionalAssignments()
    {
        using var directory = TestDirectory.Create();
        var project = Path.Combine(directory.Path, "Conditional.csproj");
        await File.WriteAllTextAsync(
            project,
            """
            <Project>
              <PropertyGroup>
                <DesktopFrameworks>net10.0;net11.0</DesktopFrameworks>
                <TargetFrameworks>$(DesktopFrameworks)</TargetFrameworks>
                <TargetFrameworks Condition="'$(EnableLegacy)' == 'true'">net8.0;net9.0</TargetFrameworks>
              </PropertyGroup>
            </Project>
            """);

        await Assert.That(CiCoverage.TargetsFramework(project, "net10.0")).IsTrue();
        await Assert.That(CiCoverage.TargetsFramework(project, "net8.0")).IsFalse();
    }

    /// <summary>Verifies imported framework properties use their evaluated values.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FrameworkDiscoveryEvaluatesImportedProperties()
    {
        using var directory = TestDirectory.Create();
        var imported = Path.Combine(directory.Path, "Frameworks.props");
        var project = Path.Combine(directory.Path, "Imported.csproj");
        await File.WriteAllTextAsync(imported, "<Project><PropertyGroup><BrowserFrameworks>net10.0;net11.0</BrowserFrameworks></PropertyGroup></Project>");
        await File.WriteAllTextAsync(
            project,
            """
            <Project>
              <Import Project="Frameworks.props" />
              <PropertyGroup><TargetFrameworks>$(BrowserFrameworks)</TargetFrameworks></PropertyGroup>
            </Project>
            """);

        await Assert.That(CiCoverage.TargetsFramework(project, "net11.0")).IsTrue();
        await Assert.That(CiCoverage.TargetsFramework(project, "net9.0")).IsFalse();
    }

    /// <summary>Verifies a single framework is matched exactly rather than by prefix.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FrameworkDiscoveryMatchesSingleFrameworkExactly()
    {
        using var directory = TestDirectory.Create();
        var project = Path.Combine(directory.Path, "Single.csproj");
        await File.WriteAllTextAsync(project, "<Project><PropertyGroup><TargetFramework>net10.0</TargetFramework></PropertyGroup></Project>");

        await Assert.That(CiCoverage.TargetsFramework(project, "net10.0")).IsTrue();
        await Assert.That(CiCoverage.TargetsFramework(project, "net10.0-android")).IsFalse();
    }
}
