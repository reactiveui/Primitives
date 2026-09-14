// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Creates temporary roots without operating-system directory aliases in existing parents.</summary>
internal static class TemporaryDirectory
{
    /// <summary>Gets a physical temporary directory without operating-system directory aliases.</summary>
    /// <returns>The temporary directory with existing parent links resolved.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string GetTemporaryDirectory() =>
        ResolveExistingParents(Path.GetTempPath());

    /// <summary>Resolves existing directory aliases before creating a child path.</summary>
    /// <param name="path">The directory path whose existing parents may contain operating-system aliases.</param>
    /// <returns>The path with existing parent aliases resolved.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string ResolveExistingParents(string path) =>
        ResolveDirectory(new(Path.GetFullPath(path)));

    /// <summary>Resolves existing aliases in a directory path.</summary>
    /// <param name="directory">The directory whose parents may contain an operating-system alias.</param>
    /// <returns>The physical directory path.</returns>
    private static string ResolveDirectory(DirectoryInfo directory)
    {
        if (directory.Parent is not { } parent)
        {
            return directory.FullName;
        }

        var resolved = new DirectoryInfo(Path.Combine(ResolveDirectory(parent), directory.Name));
        return Path.TrimEndingDirectorySeparator(
            resolved.Exists
                ? resolved.ResolveLinkTarget(returnFinalTarget: true)?.FullName ?? resolved.FullName
                : resolved.FullName);
    }
}
