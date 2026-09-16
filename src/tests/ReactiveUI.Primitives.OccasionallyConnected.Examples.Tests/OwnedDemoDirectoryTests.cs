// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using OccasionallyConnected.DurableOutbox;

namespace ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests;

/// <summary>Tests for <see cref="OwnedDemoDirectory"/>.</summary>
public sealed class OwnedDemoDirectoryTests
{
    /// <summary>The root directory name used by component tests.</summary>
    private const string TestRootName = "oc-durable-outbox-owned-directory-tests";

    /// <summary>The user file name used by ownership safety tests.</summary>
    private const string ExistingUserFileName = "user-data.txt";

    /// <summary>Verifies a volume root produces an immediate child without requiring permission to write there.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRootIsVolumeRoot_ThenGeneratedPathIsAnImmediateChild()
    {
        var root = Path.GetPathRoot(Path.GetFullPath(AppContext.BaseDirectory));
        ArgumentNullException.ThrowIfNull(root);
        OwnedDemoDirectory directory = new(root);

        await Assert.That(Path.GetDirectoryName(directory.DirectoryPath)).IsEqualTo(root);
        await Assert.That(Guid.TryParseExact(Path.GetFileName(directory.DirectoryPath), "N", out _)).IsTrue();
        await Assert.That(Directory.Exists(directory.DirectoryPath)).IsFalse();
    }

    /// <summary>Verifies cleanup removes an empty directory created before the ownership marker is written.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnedDirectoryIsUnmarkedAndEmpty_ThenCleanupRemovesIt()
    {
        var root = CreateTestRoot();
        OwnedDemoDirectory directory = new(root);

        try
        {
            directory.CreateDirectory();

            directory.Cleanup();

            await Assert.That(Directory.Exists(directory.DirectoryPath)).IsFalse();
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    /// <summary>Verifies cleanup does not delete an unmarked directory that gained unexpected content.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnedDirectoryIsUnmarkedAndNonEmpty_ThenCleanupPreservesIt()
    {
        var root = CreateTestRoot();
        OwnedDemoDirectory directory = new(root);
        var userFile = Path.Combine(directory.DirectoryPath, ExistingUserFileName);

        try
        {
            directory.CreateDirectory();
            await File.WriteAllTextAsync(userFile, "keep").ConfigureAwait(false);

            directory.Cleanup();

            await Assert.That(Directory.Exists(directory.DirectoryPath)).IsTrue();
            await Assert.That(File.Exists(userFile)).IsTrue();
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    /// <summary>Verifies cleanup does not recursively delete content after the ownership marker disappears.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnedDirectoryLosesMarker_ThenCleanupPreservesIt()
    {
        var root = CreateTestRoot();
        OwnedDemoDirectory directory = new(root);

        try
        {
            directory.CreateDirectory();
            await directory.MarkOwnedAsync(CancellationToken.None);
            File.Delete(directory.MarkerPath);

            directory.Cleanup();

            await Assert.That(Directory.Exists(directory.DirectoryPath)).IsTrue();
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    /// <summary>Verifies relative roots are normalized before generated paths are exposed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRootIsRelative_ThenGeneratedDirectoryIsAnchoredToNormalizedRoot()
    {
        var root = Path.Combine(".", TestRootName, Guid.NewGuid().ToString("N"));
        var expectedRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        OwnedDemoDirectory directory = new(root);
        var generatedParent = Path.TrimEndingDirectorySeparator(
            Path.GetDirectoryName(directory.DirectoryPath) ?? string.Empty);

        try
        {
            await Assert.That(generatedParent).IsEqualTo(expectedRoot);
            await Assert.That(Path.IsPathRooted(directory.DirectoryPath)).IsTrue();
            await Assert.That(Directory.Exists(directory.DirectoryPath)).IsFalse();
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    /// <summary>Verifies existing directory aliases are resolved before generated child paths are created.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenConfiguredRootUsesExistingDirectoryAlias_ThenGeneratedDirectoryUsesResolvedParent()
    {
        var physicalRoot = CreateTestRoot();
        var aliasContainer = CreateTestRoot();
        var aliasRoot = Path.Combine(aliasContainer, "alias");
        var aliasCreated = false;

        try
        {
            _ = Directory.CreateDirectory(physicalRoot);
            _ = Directory.CreateDirectory(aliasContainer);
            await Assert.That(Directory.Exists(physicalRoot)).IsTrue();
            var aliasCreationFailure = TryCreateDirectoryAlias(aliasRoot, physicalRoot);
            await Assert.That(aliasCreationFailure).IsNull();
            aliasCreated = true;
            var resolvedAlias = new DirectoryInfo(aliasRoot).ResolveLinkTarget(returnFinalTarget: true)?.FullName;
            await Assert.That(resolvedAlias).IsEqualTo(physicalRoot);

            var configuredRoot = Path.Combine(aliasRoot, "configured");
            var expectedRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.Combine(physicalRoot, "configured")));
            OwnedDemoDirectory directory = new(configuredRoot);
            var generatedParent = Path.TrimEndingDirectorySeparator(
                Path.GetDirectoryName(directory.DirectoryPath) ?? string.Empty);

            await Assert.That(generatedParent).IsEqualTo(expectedRoot);
            await Assert.That(directory.DatabasePath).StartsWith(directory.DirectoryPath);
        }
        finally
        {
            if (aliasCreated)
            {
                Directory.Delete(aliasRoot);
            }

            DeleteDirectoryIfExists(aliasContainer);
            DeleteDirectoryIfExists(physicalRoot);
        }
    }

    /// <summary>Verifies ownership cannot be marked before the generated directory is created.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOwnershipIsMarkedBeforeCreate_ThenItIsRejectedBeforeFilesystemMutation()
    {
        var root = CreateTestRoot();
        OwnedDemoDirectory directory = new(root);

        try
        {
            await Assert.That(async () => await directory.MarkOwnedAsync(CancellationToken.None))
                .ThrowsExactly<InvalidOperationException>()
                .WithMessageContaining("Create the generated demo directory before marking ownership.");
            await Assert.That(Directory.Exists(directory.DirectoryPath)).IsFalse();
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    /// <summary>Verifies cleanup does not claim and recursively delete an existing directory.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenGeneratedDirectoryAlreadyExists_ThenItIsNotClaimedOrDeleted()
    {
        var root = CreateTestRoot();
        OwnedDemoDirectory directory = new(root);
        var userFile = Path.Combine(directory.DirectoryPath, ExistingUserFileName);

        try
        {
            _ = Directory.CreateDirectory(directory.DirectoryPath);
            await File.WriteAllTextAsync(userFile, "existing user data").ConfigureAwait(false);

            await Assert.That(() => directory.CreateDirectory())
                .ThrowsExactly<IOException>()
                .WithMessageContaining("The generated demo directory already exists.");

            await Assert.That(File.Exists(userFile)).IsTrue();
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    /// <summary>Creates a unique test root directory path.</summary>
    /// <returns>The unique path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateTestRoot() =>
        Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));

    /// <summary>Creates a real directory alias when the current platform allows it.</summary>
    /// <param name="aliasRoot">The alias path to create.</param>
    /// <param name="physicalRoot">The physical directory path targeted by the alias.</param>
    /// <returns>The alias creation failure, or <see langword="null"/> when the alias was created.</returns>
    private static string? TryCreateDirectoryAlias(string aliasRoot, string physicalRoot)
    {
        try
        {
            _ = Directory.CreateSymbolicLink(aliasRoot, physicalRoot);
            return null;
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            return $"{exception.GetType().Name}: {exception.Message}";
        }
    }

    /// <summary>Deletes a directory when it exists.</summary>
    /// <param name="path">The directory path.</param>
    private static void DeleteDirectoryIfExists(string path)
    {
        if (!Directory.Exists(path))
        {
            return;
        }

        Directory.Delete(path, recursive: true);
    }
}
