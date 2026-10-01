// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using Microsoft.Maui.Storage;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Binds durable sequence state to one secure identity and serializes installation provisioning.</summary>
internal static class MobileInstallation
{
    /// <summary>The non-secret identity marker suffix.</summary>
    private const string MarkerSuffix = ".rxui-installation";

    /// <summary>Acquires a lifetime provisioning handle without following redirected final files.</summary>
    /// <param name="path">The app-data database path.</param>
    /// <returns>The exclusive installation handle.</returns>
    /// <exception cref="InvalidOperationException">Another bundle owns this installation.</exception>
    internal static FileStream Acquire(string path)
    {
        RejectLink(path);
        RejectLink($"{path}.rxui-owner");
        RejectLink(path + MarkerSuffix);
        var ownerPath = $"{path}{MarkerSuffix}-owner";
        RejectLink(ownerPath);
        try
        {
            return new(ownerPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException("Another mobile storage bundle owns this installation.", exception);
        }
    }

    /// <summary>Loads existing keys or starts a fresh installation when durable sequence state is absent.</summary>
    /// <param name="storage">The platform secure storage.</param>
    /// <param name="storageKey">The secure entry name.</param>
    /// <param name="path">The database path.</param>
    /// <param name="cancellationToken">The admission cancellation token.</param>
    /// <returns>The installation's secure state.</returns>
    /// <exception cref="InvalidOperationException">The marker is missing, malformed or belongs to another identity.</exception>
    internal static async ValueTask<MobileSecureState> OpenAsync(
        ISecureStorage storage,
        string storageKey,
        string path,
        CancellationToken cancellationToken)
    {
        var exists = File.Exists(path);
        if (!exists && (File.Exists($"{path}-wal") || File.Exists($"{path}-shm")))
        {
            throw new InvalidOperationException(
                "SQLite recovery sidecars exist without their database. Restore or quarantine the complete database "
                + "and its original secure key ring before starting a new installation.");
        }

        var marker = path + MarkerSuffix;
        ThrowIfMissingMarker(exists, marker);

        var keys = exists
            ? await MobileSecureState.OpenAsync(storage, storageKey, false, cancellationToken).ConfigureAwait(false)
            : await MobileSecureState.CreateInstallationAsync(storage, storageKey, cancellationToken).ConfigureAwait(false);
        if (exists)
        {
            if (new FileInfo(marker).Length != 32)
            {
                throw new InvalidOperationException("The installation marker is malformed; restore the matching original marker.");
            }

            var identity = await File.ReadAllTextAsync(marker, cancellationToken).ConfigureAwait(false);
            if (!string.Equals(identity, keys.Identity.ClientId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "The installation marker and secure identity do not match. Restore the matching database, marker "
                    + "and original secure key ring together; no identity or encrypted data was replaced.");
            }
        }
        else
        {
            WriteMarker(marker, keys.Identity.ClientId);
        }

        if (!exists)
        {
            // Reserve durable sequence storage before returning; a later factory call must not reuse a deleted database's ID.
            ReserveDatabase(path);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return keys;
    }

    /// <summary>Captures the physical app-data directory before provisioning any installation files.</summary>
    /// <param name="directory">The existing app-data directory.</param>
    /// <returns>The physical directory path.</returns>
    /// <exception cref="NotSupportedException">An app-data alias cannot be resolved.</exception>
    internal static string ResolveDirectory(DirectoryInfo directory)
    {
        var path = directory.Parent is null
            ? directory.FullName
            : Path.Combine(ResolveDirectory(directory.Parent), directory.Name);
        var physical = new DirectoryInfo(path);
        if ((physical.Attributes & FileAttributes.ReparsePoint) == 0)
        {
            return path;
        }

        var target = physical.ResolveLinkTarget(true) as DirectoryInfo
            ?? throw new NotSupportedException("The platform app-data alias cannot be resolved to a real directory.");
        return ResolveDirectory(target);
    }

    /// <summary>Rejects recovery without the original installation binding.</summary>
    /// <param name="databaseExists">Whether durable sequence state already exists.</param>
    /// <param name="marker">The original installation marker path.</param>
    /// <exception cref="InvalidOperationException">The existing database has no marker.</exception>
    private static void ThrowIfMissingMarker(bool databaseExists, string marker)
    {
        if (databaseExists && !File.Exists(marker))
        {
            throw new InvalidOperationException(
                "The existing database is missing its installation marker. Restore the matching database, marker "
                + "and original secure key ring together; no identity or encrypted data was replaced.");
        }
    }

    /// <summary>Writes and flushes the identity marker before creating the database.</summary>
    /// <param name="path">The marker path.</param>
    /// <param name="identity">The non-secret client identity.</param>
    private static void WriteMarker(string path, string identity)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        stream.Write(Encoding.UTF8.GetBytes(identity));
        stream.Flush(true);
    }

    /// <summary>Reserves the sequence database after secure provisioning succeeds.</summary>
    /// <param name="path">The new database path.</param>
    private static void ReserveDatabase(string path)
    {
        using var database = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        database.Flush(true);
    }

    /// <summary>Rejects final-file links including dangling links.</summary>
    /// <param name="path">The final file path.</param>
    /// <exception cref="NotSupportedException">A final file is redirected by a link.</exception>
    private static void RejectLink(string path)
    {
        var file = new FileInfo(path);
        if (file.LinkTarget is not null || (file.Exists && (file.Attributes & FileAttributes.ReparsePoint) != 0))
        {
            throw new NotSupportedException("Mobile storage cannot follow a redirected database or installation file.");
        }
    }
}
