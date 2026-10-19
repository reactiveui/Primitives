// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Xml.Linq;
using OccasionallyConnected.Ci;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests release filter membership and stable package validation.</summary>
public sealed class PackagesTests
{
    /// <summary>The number of packages in the release graph.</summary>
    private const int PackageCount = 20;

    /// <summary>The package under validation.</summary>
    private const string PackageName = "Example";

    /// <summary>The stable version under validation.</summary>
    private const string StableVersion = "0.1.0";

    /// <summary>Checks the release filter contains the complete local package graph.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReleaseFilterContainsEveryProductionPackageAndDependency()
    {
        var content = await File.ReadAllTextAsync(Path.Combine(FindRepository(), "src", "ReactiveUI.Primitives.slnf"));
        using var filter = System.Text.Json.JsonDocument.Parse(content);
        var projects = filter.RootElement.GetProperty("solution").GetProperty("projects")
            .EnumerateArray().Select(static value => value.GetString()!);
        await Assert.That(Packages.SelectReleaseProjects(projects)).Count().IsEqualTo(PackageCount);
    }

    /// <summary>Checks missing release filter entries fail the gate.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReleaseFilterRejectsMissingProductionPackages() =>
        await Assert.That(static () => Packages.SelectReleaseProjects([])).Throws<InvalidOperationException>();

    /// <summary>Checks stable packages cannot carry preview binary assets.</summary>
    /// <param name="asset">The preview binary asset.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("lib/net11.0/Example.dll")]
    [Arguments("ref/net11.0-android/Example.dll")]
    public async Task StablePackageRejectsPreviewAssets(string asset) =>
        await Assert.That(() => Packages.ValidateReleasePackage(PackageName, StableVersion, [asset], XDocument.Parse("<package/>"), "."))
            .Throws<InvalidOperationException>();

    /// <summary>Checks stable packages cannot carry preview dependency groups or versions.</summary>
    /// <param name="xml">The package manifest.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("<package><dependencies><group targetFramework=\"net11.0\"/></dependencies></package>")]
    [Arguments("<package><dependencies><dependency id=\"Microsoft.Extensions.Options\" version=\"11.0.0-rc.1\"/></dependencies></package>")]
    public async Task StablePackageRejectsPreviewDependencies(string xml) =>
        await Assert.That(() => Packages.ValidateReleasePackage(PackageName, StableVersion, [], XDocument.Parse(xml), "."))
            .Throws<InvalidOperationException>();

    /// <summary>Checks prereleases can carry preview binary assets and dependencies.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PrereleasePackageRetainsPreviewAssetsAndDependencies() =>
        await Assert.That(static () => Packages.ValidateReleasePackage(
            PackageName,
            "0.1.0-alpha.1",
            ["lib/net11.0/Example.dll"],
            XDocument.Parse("<package><dependencies><dependency id=\"Microsoft.Extensions.Options\" version=\"11.0.0-rc.1\"/></dependencies></package>"),
            "."))
            .ThrowsNothing();

    /// <summary>Checks stable binary assets and dependencies pass validation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StablePackageAcceptsStableAssetsAndDependencies() =>
        await Assert.That(static () => Packages.ValidateReleasePackage(
            PackageName,
            StableVersion,
            ["lib/net10.0/Example.dll"],
            XDocument.Parse("<package><dependencies><dependency id=\"Microsoft.Extensions.Options\" version=\"10.0.12\"/></dependencies></package>"),
            "."))
            .ThrowsNothing();

    /// <summary>Checks local dependencies must exist in the release feed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PackageRejectsMissingLocalDependency() =>
        await Assert.That(static () => Packages.ValidateReleasePackage(
            PackageName,
            StableVersion,
            [],
            XDocument.Parse("<package><dependencies><dependency id=\"ReactiveUI.Primitives.OccasionallyConnected.Core\" version=\"0.1.0\"/></dependencies></package>"),
            "."))
            .Throws<InvalidOperationException>();

    /// <summary>Locates the checkout used by the test build.</summary>
    /// <returns>The repository directory.</returns>
    /// <exception cref="InvalidOperationException">The checkout cannot be located.</exception>
    private static string FindRepository()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "src", "ReactiveUI.Primitives.slnf")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("Repository root not found.");
    }
}
