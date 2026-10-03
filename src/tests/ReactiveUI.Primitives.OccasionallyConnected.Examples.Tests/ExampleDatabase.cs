// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using OccasionallyConnected.DurableOutbox;

namespace ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests;

/// <summary>Creates an owned temporary SQLite database path for example tests.</summary>
internal sealed class ExampleDatabase : IDisposable
{
    /// <summary>The test root directory name.</summary>
    private const string TestRootName = "oc-durable-outbox-tests";

    /// <summary>The owned temporary directory.</summary>
    private readonly string _directory;

    /// <summary>Initializes a new instance of the <see cref="ExampleDatabase"/> class.</summary>
    /// <param name="directory">The owned temporary directory.</param>
    /// <param name="path">The SQLite database path.</param>
    private ExampleDatabase(string directory, string path)
    {
        _directory = directory;
        Path = path;
    }

    /// <summary>Gets the SQLite database path.</summary>
    internal string Path { get; }

    /// <summary>Deletes the owned temporary directory.</summary>
    public void Dispose()
    {
        if (!Directory.Exists(_directory))
        {
            return;
        }

        Directory.Delete(_directory, recursive: true);
    }

    /// <summary>Creates a new owned temporary database path.</summary>
    /// <returns>The owned database.</returns>
    internal static ExampleDatabase Create()
    {
        var directory = System.IO.Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(directory);
        return new(directory, System.IO.Path.Combine(directory, "outbox.db"));
    }
}
