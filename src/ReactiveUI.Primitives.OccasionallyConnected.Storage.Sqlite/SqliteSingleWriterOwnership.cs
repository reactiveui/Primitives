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
        ThrowIfExistingReparsePoint(databasePath);
        ThrowIfExistingReparsePoint(databasePath + OwnershipSuffix);

        var directory = Path.GetDirectoryName(databasePath);
        ArgumentExceptionHelper.ThrowIfNull(directory);

        _ = Directory.CreateDirectory(directory);
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
        var drive = new DriveInfo(root);
        ThrowIfUnsupportedDriveType(drive.DriveType);
    }

    /// <summary>Rejects existing reparse points that could give the same database more than one sidecar path.</summary>
    /// <param name="databasePath">The full SQLite database path.</param>
    /// <exception cref="NotSupportedException">An existing path segment is a reparse point.</exception>
    private static void ThrowIfExistingReparsePoint(string databasePath)
    {
        var directory = Path.GetDirectoryName(databasePath);
        if (!string.IsNullOrEmpty(directory))
        {
            ThrowIfExistingDirectoryReparsePoint(new(directory));
        }

        if (!File.Exists(databasePath))
        {
            return;
        }

        if ((File.GetAttributes(databasePath) & FileAttributes.ReparsePoint) == 0)
        {
            return;
        }

        throw new NotSupportedException("SQLite single-writer ownership is not supported for reparse-point database files.");
    }

    /// <summary>Rejects existing reparse-point directories in a parent chain.</summary>
    /// <param name="directory">The directory to inspect.</param>
    /// <exception cref="NotSupportedException">An existing directory is a reparse point.</exception>
    private static void ThrowIfExistingDirectoryReparsePoint(DirectoryInfo directory)
    {
        if (directory.Parent is not null)
        {
            ThrowIfExistingDirectoryReparsePoint(directory.Parent);
        }

        if (!directory.Exists)
        {
            return;
        }

        if ((directory.Attributes & FileAttributes.ReparsePoint) == 0)
        {
            return;
        }

        throw new NotSupportedException("SQLite single-writer ownership is not supported through reparse-point directories.");
    }
}
