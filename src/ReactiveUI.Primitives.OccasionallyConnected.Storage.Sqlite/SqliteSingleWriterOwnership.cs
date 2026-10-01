// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Owns one initialized SQLite writer lifetime for a normalized database path.</summary>
internal sealed class SqliteSingleWriterOwnership : IDisposable
{
    /// <summary>The suffix used for sidecar ownership handles.</summary>
    private const string OwnershipSuffix = ".rxui-owner";

    /// <summary>The exclusive sidecar handle.</summary>
    private readonly FileStream _stream;

    /// <summary>Initializes a new instance of the <see cref="SqliteSingleWriterOwnership"/> class.</summary>
    /// <param name="stream">The exclusive sidecar stream.</param>
    private SqliteSingleWriterOwnership(FileStream stream) => _stream = stream;

    /// <inheritdoc/>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _stream.Dispose();

    /// <summary>Acquires exclusive writer ownership for a normalized SQLite database path.</summary>
    /// <param name="databasePath">The full SQLite database path captured by the adapter.</param>
    /// <returns>The ownership handle.</returns>
    /// <exception cref="ArgumentException">The database path is not a rooted real file path.</exception>
    /// <exception cref="InvalidOperationException">Another initialized writer owns the database path.</exception>
    internal static SqliteSingleWriterOwnership Acquire(string databasePath)
    {
        SqliteLocalCommitValidation.ThrowIfBlank(databasePath, nameof(databasePath), "The SQLite database path cannot be empty.");
        SqliteLocalCommitValidation.ThrowIfUnsupportedPath(databasePath);
        ThrowIfUnsupportedRoot(databasePath);
        databasePath = ResolveDatabasePath(databasePath);
        var directory = Path.GetDirectoryName(databasePath);
        ArgumentExceptionHelper.ThrowIfNull(directory);
        _ = Directory.CreateDirectory(directory);
        ThrowIfUnsupportedRoot(databasePath);
        ThrowIfExistingReparsePoint(databasePath);
        ThrowIfExistingReparsePoint(databasePath + OwnershipSuffix);
        ThrowIfExistingReparsePoint($"{databasePath}-wal");
        ThrowIfExistingReparsePoint($"{databasePath}-shm");
        try
        {
            var stream = new FileStream(databasePath + OwnershipSuffix, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            return new(stream);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException("The SQLite local store is already owned by another initialized writer.", exception);
        }
        catch (UnauthorizedAccessException exception)
        {
            throw new InvalidOperationException("The SQLite local store writer ownership handle could not be acquired.", exception);
        }
    }

    /// <summary>Captures a physical directory path without following a final database file link.</summary>
    /// <param name="databasePath">The supplied database path.</param>
    /// <returns>The full database path with existing directory aliases resolved.</returns>
    /// <remarks>Unsupported directory aliases remain unchanged until Acquire rejects them during initialization.</remarks>
    internal static string NormalizeDatabasePath(string databasePath)
    {
        try
        {
            return ResolveDatabasePath(databasePath);
        }
        catch (NotSupportedException)
        {
            return Path.GetFullPath(databasePath);
        }
    }

    /// <summary>Selects the longest mounted filesystem root containing a database.</summary>
    /// <param name="databasePath">The full database path.</param>
    /// <param name="selected">The current mount root.</param>
    /// <param name="candidate">The candidate mount root.</param>
    /// <returns>The mount root for the most specific filesystem.</returns>
    internal static string SelectMountRoot(string databasePath, string selected, string candidate)
    {
        if (candidate.Length <= selected.Length)
        {
            return selected;
        }

        var comparison = Path.DirectorySeparatorChar == '\\' ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        var boundary = candidate.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        return string.Equals(databasePath, candidate, comparison) || databasePath.StartsWith(boundary, comparison)
            ? candidate
            : selected;
    }

    /// <summary>Rejects drive types that cannot make a local sidecar ownership claim.</summary>
    /// <param name="driveType">The drive type reported by the runtime.</param>
    /// <exception cref="NotSupportedException">The drive type is a known unsupported network location.</exception>
    internal static void ThrowIfUnsupportedDriveType(DriveType driveType)
    {
        if (driveType != DriveType.Network)
        {
            return;
        }

        throw new NotSupportedException("SQLite single-writer ownership is not supported on network drives.");
    }

    /// <summary>Rejects path roots where a local exclusive sidecar is a known unsafe coordination claim.</summary>
    /// <param name="databasePath">The full SQLite database path.</param>
    /// <exception cref="ArgumentException">The path root is missing.</exception>
    /// <exception cref="NotSupportedException">The path root is a known unsupported network location.</exception>
    private static void ThrowIfUnsupportedRoot(string databasePath)
    {
        if (databasePath.StartsWith(@"\\", StringComparison.Ordinal) || databasePath.StartsWith("//", StringComparison.Ordinal))
        {
            throw new NotSupportedException("SQLite single-writer ownership is not supported for UNC database paths.");
        }

        var root = Path.GetPathRoot(databasePath);
        ArgumentExceptionHelper.ThrowIfNull(root);
        var selected = root;
        foreach (var mounted in DriveInfo.GetDrives())
        {
            selected = SelectMountRoot(databasePath, selected, mounted.Name);
        }

        ThrowIfUnsupportedDriveType(new DriveInfo(selected).DriveType);
    }

    /// <summary>Rejects existing reparse points that could give the same database more than one sidecar path.</summary>
    /// <param name="databasePath">The full SQLite database path.</param>
    /// <exception cref="NotSupportedException">An existing path segment is a reparse point.</exception>
    private static void ThrowIfExistingReparsePoint(string databasePath)
    {
        var attributes = ReadExistingAttributes(databasePath);
        if (attributes is null || (attributes.Value & FileAttributes.ReparsePoint) == 0)
        {
            return;
        }

        throw new NotSupportedException("SQLite single-writer ownership is not supported for reparse-point database files.");
    }

    /// <summary>Resolves directory aliases without deferring unsupported-path failures.</summary>
    /// <param name="databasePath">The supplied database path.</param>
    /// <returns>The captured database path.</returns>
    private static string ResolveDatabasePath(string databasePath)
    {
        if (databasePath.StartsWith(@"\\", StringComparison.Ordinal) || databasePath.StartsWith("//", StringComparison.Ordinal))
        {
            return databasePath;
        }

        var fullPath = Path.GetFullPath(databasePath);
        var directory = Path.GetDirectoryName(fullPath);
        ArgumentExceptionHelper.ThrowIfNull(directory);
        return Path.Combine(ResolveDirectory(new(directory)), Path.GetFileName(fullPath));
    }

    /// <summary>Resolves directory aliases so all names for a database use the same ownership sidecar.</summary>
    /// <param name="directory">The directory to inspect.</param>
    /// <returns>The physical directory path.</returns>
    /// <exception cref="NotSupportedException">A directory alias cannot be resolved.</exception>
    private static string ResolveDirectory(DirectoryInfo directory)
    {
        var path = directory.Parent is null
            ? directory.FullName
            : Path.Combine(ResolveDirectory(directory.Parent), directory.Name);
        var attributes = ReadExistingAttributes(path);
        if (attributes is null || (attributes.Value & FileAttributes.ReparsePoint) == 0)
        {
            return path;
        }

#if NETFRAMEWORK
        throw new NotSupportedException("Directory aliases require a modern .NET target for safe SQLite path normalization.");
#else
        var physical = new DirectoryInfo(path);
        var target = physical.ResolveLinkTarget(true) as DirectoryInfo
            ?? throw new NotSupportedException("The SQLite directory alias cannot be resolved to a real directory.");
        return ResolveDirectory(target);
#endif
    }

    /// <summary>Reads path attributes while treating an absent path as a normal creation case.</summary>
    /// <param name="path">The path to inspect.</param>
    /// <returns>The existing attributes, or null when the path has not been created.</returns>
    private static FileAttributes? ReadExistingAttributes(string path)
    {
        try
        {
            return File.GetAttributes(path);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }
}
