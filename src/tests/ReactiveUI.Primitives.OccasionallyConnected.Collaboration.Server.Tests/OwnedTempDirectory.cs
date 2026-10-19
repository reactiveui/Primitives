// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Creates owned temporary directories after resolving existing parent aliases.</summary>
internal static class OwnedTempDirectory
{
    /// <summary>The SQLite database file name used by server example fixtures.</summary>
    private const string JournalFileName = "journal.db";

    /// <summary>Creates an owned directory below the canonical process temp root.</summary>
    /// <param name="prefix">The directory name prefix.</param>
    /// <returns>The created directory path.</returns>
    internal static string Create(string prefix)
    {
        var path = CreatePath(prefix);
        _ = Directory.CreateDirectory(path);
        return path;
    }

    /// <summary>Creates an owned directory path below the canonical process temp root without creating it.</summary>
    /// <param name="prefix">The directory name prefix.</param>
    /// <returns>The directory path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string CreatePath(string prefix) =>
        System.IO.Path.Combine(GetCanonicalTempRoot(), $"{prefix}{Guid.NewGuid():N}");

    /// <summary>Creates an owned SQLite database path below the canonical process temp root.</summary>
    /// <param name="prefix">The directory name prefix.</param>
    /// <returns>The database path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string CreateDatabasePath(string prefix) =>
        System.IO.Path.Combine(CreatePath(prefix), JournalFileName);

    /// <summary>Resolves the process temp root through existing symlinked parent directories.</summary>
    /// <returns>The canonical temporary root path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string GetCanonicalTempRoot() =>
        ResolveExistingDirectoryPath(System.IO.Path.GetTempPath());

    /// <summary>Resolves an existing directory path through symlinked parents.</summary>
    /// <param name="path">The existing directory path.</param>
    /// <returns>The resolved directory path.</returns>
    private static string ResolveExistingDirectoryPath(string path)
    {
        var directory = Directory.CreateDirectory(System.IO.Path.GetFullPath(path));
        var root = System.IO.Path.GetPathRoot(directory.FullName) ?? string.Empty;
        var current = root.Length == 0 ? directory.FullName : root;
        var remainder = directory.FullName[root.Length..];
        foreach (var segment in remainder.Split(System.IO.Path.DirectorySeparatorChar, System.IO.Path.AltDirectorySeparatorChar))
        {
            if (string.IsNullOrEmpty(segment))
            {
                continue;
            }

            current = ResolveExistingSegment(System.IO.Path.Combine(current, segment));
        }

        return current;
    }

    /// <summary>Resolves one existing directory segment when it is a symlink.</summary>
    /// <param name="path">The directory segment path.</param>
    /// <returns>The resolved path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string ResolveExistingSegment(string path)
    {
        var directory = new DirectoryInfo(path);
        var target = directory.ResolveLinkTarget(returnFinalTarget: true);
        return target?.FullName ?? directory.FullName;
    }
}
