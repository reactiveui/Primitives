// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Creates physical temporary directories for SQLite ownership tests.</summary>
internal static class SqliteTestDirectory
{
    /// <summary>Creates a temporary directory and resolves existing operating-system aliases.</summary>
    /// <param name="prefix">The temporary directory name prefix.</param>
    /// <returns>The owned directory at its physical path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static DirectoryInfo Create(string prefix) =>
        new(ResolveDirectory(Directory.CreateTempSubdirectory(prefix)));

    /// <summary>Resolves aliases in an existing directory and its parents.</summary>
    /// <param name="directory">The existing directory.</param>
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
