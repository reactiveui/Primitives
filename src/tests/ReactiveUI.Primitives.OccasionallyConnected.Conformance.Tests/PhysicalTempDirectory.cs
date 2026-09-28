// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Creates temporary database directories without aliases in their parent paths.</summary>
internal static class PhysicalTempDirectory
{
    /// <summary>Creates a temporary directory at its physical path.</summary>
    /// <param name="prefix">The directory name prefix.</param>
    /// <returns>The physical directory.</returns>
    internal static DirectoryInfo Create(string prefix)
    {
        var root = ResolveDirectory(new(Path.GetTempPath()));
        return Directory.CreateDirectory(Path.Combine(root, $"{prefix}{Guid.NewGuid():N}"));
    }

    /// <summary>Resolves links in an existing directory and each of its parents.</summary>
    /// <param name="directory">The directory to resolve.</param>
    /// <returns>The physical directory path.</returns>
    private static string ResolveDirectory(DirectoryInfo directory)
    {
        if (directory.Parent is not { } parent)
        {
            return directory.FullName;
        }

        var resolved = new DirectoryInfo(Path.Combine(ResolveDirectory(parent), directory.Name));
        return resolved.ResolveLinkTarget(returnFinalTarget: true)?.FullName ?? resolved.FullName;
    }
}
