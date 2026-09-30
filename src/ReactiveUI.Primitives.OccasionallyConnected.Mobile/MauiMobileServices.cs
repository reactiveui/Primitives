// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if ANDROID || IOS || MACCATALYST || WINDOWS
using Microsoft.Maui.Networking;
using Microsoft.Maui.Storage;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Creates mobile convenience adapters using the actual MAUI Essentials platform services.</summary>
/// <remarks>Available only on platform target frameworks. Configure the platform's permissions, entitlements and backups.</remarks>
public static class MauiMobileServices
{
    /// <summary>Creates a connectivity hint backed by the current platform network service.</summary>
    /// <returns>The disposable platform connectivity subscription.</returns>
    public static MauiConnectivityHint CreateConnectivityHint() => new(Connectivity.Current);

    /// <summary>Creates encrypted app-data SQLite storage backed by platform secure storage.</summary>
    /// <param name="fileName">The simple database file name.</param>
    /// <param name="secureStorageKey">The app-specific secure storage entry name.</param>
    /// <param name="options">The SQLite adapter options without a key provider.</param>
    /// <param name="cancellationToken">The provisioning cancellation token.</param>
    /// <returns>The composed uninitialized SQLite storage bundle.</returns>
    public static ValueTask<MobileSqliteStorage> CreateSqliteStorageAsync(
        string fileName,
        string secureStorageKey,
        SqliteLocalStoreAdapterOptions options,
        CancellationToken cancellationToken) =>
        MobileSqliteStorage.CreateAsync(
            SecureStorage.Default,
            FileSystem.Current,
            fileName,
            secureStorageKey,
            options,
            cancellationToken);
}
#endif
