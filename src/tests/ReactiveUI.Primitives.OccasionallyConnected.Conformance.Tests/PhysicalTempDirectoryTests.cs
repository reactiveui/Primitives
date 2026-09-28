// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Tests SQLite fixture directory creation.</summary>
public sealed class PhysicalTempDirectoryTests
{
    /// <summary>Checks that SQLite receives a physical path when macOS maps /var to /private/var.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateResolvesMacOSTemporaryRootAlias()
    {
        var rawRoot = Path.GetFullPath(Path.GetTempPath()).Replace('\\', '/');
        var directory = PhysicalTempDirectory.Create("rxui-oc-conformance-alias-");
        try
        {
            await Assert.That(Path.IsPathFullyQualified(directory.FullName)).IsTrue();
            if (OperatingSystem.IsMacOS() && rawRoot.StartsWith("/var/", StringComparison.Ordinal))
            {
                await Assert.That(directory.FullName.Replace('\\', '/').StartsWith("/private/var/", StringComparison.Ordinal)).IsTrue();
            }
        }
        finally
        {
            directory.Delete();
        }
    }
}
