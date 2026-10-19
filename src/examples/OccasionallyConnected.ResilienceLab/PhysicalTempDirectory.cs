// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Resolves operating-system aliases in the temporary directory path.</summary>
internal static class PhysicalTempDirectory
{
    /// <summary>Gets the physical temporary directory.</summary>
    /// <returns>The directory without aliases in its parent path.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal static string GetRoot() => ResolveDirectory(new(Path.GetTempPath()));

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
