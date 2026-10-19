// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using Microsoft.Maui.Storage;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Provides a project-local app-data directory for SQLite integration tests.</summary>
internal sealed class MobileTestFileSystem : IFileSystem, IDisposable
{
    /// <summary>The fixture-owned directory, independent of a deliberately invalid reported path.</summary>
    private readonly string _ownedDirectory =
        Path.Combine(AppContext.BaseDirectory, "MobileTestData", Guid.NewGuid().ToString("N"));

    /// <summary>Initializes a new instance of the <see cref="MobileTestFileSystem"/> class.</summary>
    internal MobileTestFileSystem() => AppDataDirectory = _ownedDirectory;

    /// <inheritdoc/>
    public string AppDataDirectory { get; internal set; }

    /// <inheritdoc/>
    public string CacheDirectory => AppDataDirectory;

    /// <inheritdoc/>
    public Task<Stream> OpenAppPackageFileAsync(string filename) => throw new NotSupportedException();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task<bool> AppPackageFileExistsAsync(string filename) => Task.FromResult(false);

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Directory.Exists(_ownedDirectory))
        {
            Directory.Delete(_ownedDirectory, recursive: true);
        }
    }
}
