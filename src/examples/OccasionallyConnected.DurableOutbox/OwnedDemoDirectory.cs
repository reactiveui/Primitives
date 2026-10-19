// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Owns the lifecycle and cleanup guard for a generated demo directory.</summary>
internal sealed class OwnedDemoDirectory
{
    /// <summary>The marker file proving a generated directory is owned by this demo.</summary>
    internal const string MarkerFileName = ".owned-by-oc-durable-outbox-demo";

    /// <summary>The marker file contents.</summary>
    private const string MarkerContents = "owned";

    /// <summary>A value indicating whether this instance created the directory.</summary>
    private bool _directoryCreated;

    /// <summary>A value indicating whether this instance wrote the ownership marker.</summary>
    private bool _markerWritten;

    /// <summary>Initializes a new instance of the <see cref="OwnedDemoDirectory"/> class.</summary>
    /// <param name="root">The configured root directory.</param>
    /// <exception cref="ArgumentException">Thrown when the root is empty.</exception>
    internal OwnedDemoDirectory(string root)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(root);

        var normalizedRoot = TemporaryDirectory.ResolveExistingParents(root);
        DirectoryPath = Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.Combine(normalizedRoot, Guid.NewGuid().ToString("N"))));
        MarkerPath = Path.Combine(DirectoryPath, MarkerFileName);
        DatabasePath = Path.Combine(DirectoryPath, "outbox.db");
    }

    /// <summary>Gets the generated directory path.</summary>
    internal string DirectoryPath { get; }

    /// <summary>Gets the ownership marker path.</summary>
    internal string MarkerPath { get; }

    /// <summary>Gets the demo SQLite database path.</summary>
    internal string DatabasePath { get; }

    /// <summary>Creates the generated directory.</summary>
    /// <exception cref="IOException">Thrown when the generated directory already exists.</exception>
    internal void CreateDirectory()
    {
        if (Directory.Exists(DirectoryPath))
        {
            throw new IOException("The generated demo directory already exists.");
        }

        _ = Directory.CreateDirectory(DirectoryPath);
        _directoryCreated = true;
    }

    /// <summary>Writes the ownership marker into the generated directory.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous marker write.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the generated directory was not created.</exception>
    internal async ValueTask MarkOwnedAsync(CancellationToken cancellationToken)
    {
        if (!_directoryCreated)
        {
            throw new InvalidOperationException("Create the generated demo directory before marking ownership.");
        }

        await File.WriteAllTextAsync(MarkerPath, MarkerContents, cancellationToken).ConfigureAwait(false);
        _markerWritten = true;
    }

    /// <summary>Cleans the generated directory when ownership and containment can be proved.</summary>
    internal void Cleanup()
    {
        if (!_directoryCreated)
        {
            return;
        }

        if (!_markerWritten)
        {
            DeleteEmptyDirectory();
            return;
        }

        if (!File.Exists(MarkerPath))
        {
            return;
        }

        Directory.Delete(DirectoryPath, recursive: true);
    }

    /// <summary>Deletes the generated directory only when it is still empty.</summary>
    private void DeleteEmptyDirectory()
    {
        if (!Directory.Exists(DirectoryPath)
            || Directory.GetFiles(DirectoryPath).Length != 0
            || Directory.GetDirectories(DirectoryPath).Length != 0)
        {
            return;
        }

        Directory.Delete(DirectoryPath);
    }
}
