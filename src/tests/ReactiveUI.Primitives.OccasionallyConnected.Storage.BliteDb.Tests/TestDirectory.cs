// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb.Tests;

/// <summary>Creates and cleans up a unique test directory.</summary>
internal sealed class TestDirectory : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="TestDirectory"/> class.</summary>
    public TestDirectory()
    {
        RootPath = System.IO.Path.Combine(AppContext.BaseDirectory, "blitedb-test-data", Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(RootPath);
    }

    /// <summary>Gets the root directory path.</summary>
    public string RootPath { get; }

    /// <summary>Gets the store database path.</summary>
    public string DatabasePath => System.IO.Path.Combine(RootPath, "store.db");

    /// <inheritdoc />
    public void Dispose()
    {
        if (Directory.Exists(RootPath))
        {
            Directory.Delete(RootPath, recursive: true);
        }
    }
}
