// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Maui.Storage;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Composes app-data SQLite storage with a securely provisioned identity and encryption key provider.</summary>
/// <remarks>
/// This type owns only its SQLite adapter. The host supplies platform Essentials services, initializes the adapter
/// through the normal core builder, and owns the resulting context. Native SQLite availability remains a host requirement.
/// </remarks>
[DebuggerDisplay("Encrypted mobile SQLite storage")]
public sealed class MobileSqliteStorage : IAsyncDisposable
{
    /// <summary>Rejects path separators across supported host platforms.</summary>
    private static readonly SearchValues<char> PathSeparators = SearchValues.Create("/\\:");

    /// <summary>Initializes a new instance of the <see cref="MobileSqliteStorage"/> class.</summary>
    /// <param name="databasePath">The app-data database path.</param>
    /// <param name="keys">The loaded secure state.</param>
    /// <param name="store">The owned SQLite adapter.</param>
    private MobileSqliteStorage(string databasePath, MobileSecureState keys, SqliteLocalStoreAdapter store)
    {
        DatabasePath = databasePath;
        Keys = keys;
        Store = store;
    }

    /// <summary>Gets the absolute SQLite database path in the platform app-data directory.</summary>
    public string DatabasePath { get; }

    /// <summary>Gets the secure identity and retained key provider.</summary>
    public MobileSecureState Keys { get; }

    /// <summary>Gets the securely provisioned client identity to pass to core initialization.</summary>
    public ClientIdentity Identity => Keys.Identity;

    /// <summary>Gets the owned adapter with authenticated record encryption enabled.</summary>
    public SqliteLocalStoreAdapter Store { get; }

    /// <summary>Creates the standard SQLite adapter under the platform app-data directory.</summary>
    /// <param name="secureStorage">The platform secure storage service.</param>
    /// <param name="fileSystem">The platform app-data service.</param>
    /// <param name="fileName">A simple database file name, not a path.</param>
    /// <param name="secureStorageKey">The app-specific identity and key-ring entry name.</param>
    /// <param name="options">The SQLite bounds, retention and clock; its key provider must be null.</param>
    /// <param name="cancellationToken">The provisioning cancellation token.</param>
    /// <returns>The composed storage bundle, not yet initialized.</returns>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    /// <exception cref="ArgumentException">The file name, app-data path, or key provider is unsuitable.</exception>
    /// <exception cref="InvalidOperationException">Secure state required by an existing database is missing or corrupt.</exception>
    /// <exception cref="OperationCanceledException">Provisioning admission or waiting was cancelled.</exception>
    public static async ValueTask<MobileSqliteStorage> CreateAsync(
        ISecureStorage secureStorage,
        IFileSystem fileSystem,
        string fileName,
        string secureStorageKey,
        SqliteLocalStoreAdapterOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(secureStorage);
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);
        ArgumentException.ThrowIfNullOrWhiteSpace(secureStorageKey);
        if (fileName[^1] is '.' or ' ' || fileName.AsSpan().IndexOfAny(PathSeparators) >= 0
            || fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 || options.KeyProvider is not null)
        {
            throw new ArgumentException("Use a simple file name and let mobile storage supply its key provider.", nameof(fileName));
        }

        var directory = fileSystem.AppDataDirectory;
        if (string.IsNullOrWhiteSpace(directory) || !Path.IsPathFullyQualified(directory))
        {
            throw new ArgumentException("The platform app-data directory must be an absolute path.", nameof(fileSystem));
        }

        cancellationToken.ThrowIfCancellationRequested();
        directory = Path.GetFullPath(directory);
        var path = Path.Combine(directory, fileName);
        var keys = await MobileSecureState.OpenAsync(
            secureStorage,
            secureStorageKey,
            !File.Exists(path),
            cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        _ = Directory.CreateDirectory(directory);
        var adapter = new SqliteLocalStoreAdapter(path, options with { KeyProvider = keys });
        return new(path, keys, adapter);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => Store.DisposeAsync();
}
