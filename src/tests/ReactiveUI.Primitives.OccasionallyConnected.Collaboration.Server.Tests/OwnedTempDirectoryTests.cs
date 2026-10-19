// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for owned temporary directory path creation.</summary>
public sealed class OwnedTempDirectoryTests
{
    /// <summary>Verifies owned temp paths resolve existing configured-root aliases before SQLite receives them.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreatePathResolvesConfiguredTempRootAliasWhenPresent()
    {
        var rawTempRoot = System.IO.Path.GetFullPath(System.IO.Path.GetTempPath()).Replace('\\', '/');
        var directory = OwnedTempDirectory.CreatePath("rxui-oc-alias-audit-");
        var normalizedDirectory = directory.Replace('\\', '/');

        await Assert.That(System.IO.Path.IsPathFullyQualified(directory)).IsTrue();
        if (!OperatingSystem.IsMacOS() || !rawTempRoot.StartsWith("/var/", StringComparison.Ordinal))
        {
            return;
        }

        await Assert.That(normalizedDirectory.StartsWith("/private/var/", StringComparison.Ordinal)).IsTrue();
    }
}
