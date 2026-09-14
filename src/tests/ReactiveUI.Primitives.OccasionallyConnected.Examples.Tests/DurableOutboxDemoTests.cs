// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using OccasionallyConnected.DurableOutbox;

namespace ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests;

/// <summary>Tests for <see cref="DurableOutboxDemo"/>.</summary>
public sealed class DurableOutboxDemoTests
{
    /// <summary>The root directory name used by demo tests.</summary>
    private const string TestRootName = "oc-durable-outbox-demo-tests";

    /// <summary>The non-zero exit code returned by failing sample stages.</summary>
    private const int FailingStageExitCode = 2;

    /// <summary>The maximum accepted stage count for the demo workflow.</summary>
    private const int MaximumStageCount = 16;

    /// <summary>Verifies the demo owns a snapshot of its configured stages.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCallerMutatesStageListAfterConstruction_ThenOriginalStagesRun()
    {
        var root = Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));
        List<DemoStage> stages =
        [
            static (_, _) => ValueTask.FromResult(
                new OutboxCommandResult(0, $"original stage{Environment.NewLine}", string.Empty)),
        ];
        DurableOutboxDemo demo = new(root, stages);
        stages.Clear();
        stages.Add(
            static (_, _) => ValueTask.FromResult(
                new OutboxCommandResult(FailingStageExitCode, string.Empty, $"mutated stage{Environment.NewLine}")));

        try
        {
            var result = await demo.RunAsync(CancellationToken.None);

            await Assert.That(result.ExitCode).IsEqualTo(0);
            await Assert.That(result.StandardOutput).Contains("original stage");
            await Assert.That(result.StandardError).DoesNotContain("mutated stage");
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    /// <summary>Verifies cancellation during owned directory creation does not leak owned scratch directories.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRunIsCanceledBeforeMarkerIsWritten_ThenOwnedTemporaryDirectoryIsCleaned()
    {
        var root = Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));
        DurableOutboxDemo demo = new(
            root,
            [
                static (_, _) => ValueTask.FromResult(new OutboxCommandResult(0, string.Empty, string.Empty)),
            ]);
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();

        try
        {
            await Assert.That(async () => await demo.RunAsync(cancellation.Token))
                .ThrowsExactly<OperationCanceledException>();
            await Assert.That(CountOwnedChildren(root)).IsEqualTo(0);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    /// <summary>Verifies demo failure stages propagate their exit code while preserving cleanup.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStageFails_ThenExitCodeIsPropagatedAndOwnedDirectoryIsCleaned()
    {
        var root = Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));
        DurableOutboxDemo demo = new(
            root,
            [
                static (_, _) => ValueTask.FromResult(
                    new OutboxCommandResult(FailingStageExitCode, string.Empty, $"forced demo failure{Environment.NewLine}")),
            ]);

        try
        {
            var result = await demo.RunAsync(CancellationToken.None);

            await Assert.That(result.ExitCode).IsEqualTo(FailingStageExitCode);
            await Assert.That(result.StandardError).Contains("forced demo failure");
            await Assert.That(result.StandardOutput).Contains("owned directory removed: True");
            await Assert.That(Directory.Exists(root)).IsTrue();
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    /// <summary>Verifies an owned immediate child under a volume root is cleaned safely.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRootIsVolumeRoot_ThenOwnedImmediateChildIsCleaned()
    {
        var volumeRoot = Path.GetPathRoot(Path.GetFullPath(AppContext.BaseDirectory)) ?? TemporaryDirectory.GetTemporaryDirectory();
        var generatedDirectory = string.Empty;
        DurableOutboxDemo demo = new(
            volumeRoot,
            [
                (databasePath, _) =>
                {
                    generatedDirectory = Path.GetDirectoryName(databasePath) ?? string.Empty;
                    return ValueTask.FromResult(
                        new OutboxCommandResult(0, $"volume-root stage{Environment.NewLine}", string.Empty));
                },
            ]);

        try
        {
            var result = await demo.RunAsync(CancellationToken.None);

            await Assert.That(result.ExitCode).IsEqualTo(0);
            await Assert.That(IsGeneratedImmediateChild(volumeRoot, generatedDirectory)).IsTrue();
            await Assert.That(result.StandardOutput).Contains("owned directory removed: True");
            await Assert.That(Directory.Exists(generatedDirectory)).IsFalse();
        }
        finally
        {
            DeleteGeneratedChildIfLeaked(volumeRoot, generatedDirectory);
        }
    }

    /// <summary>Verifies the demo rejects an empty workflow at construction.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDemoHasNoStages_ThenConstructorRejectsConfiguration()
    {
        var root = Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));

        await Assert.That(() => new DurableOutboxDemo(root, []))
            .ThrowsExactly<ArgumentException>()
            .WithMessageContaining("At least one demo stage is required.");
    }

    /// <summary>Verifies the demo rejects unbounded workflow configuration.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDemoHasTooManyStages_ThenConstructorRejectsConfiguration()
    {
        var root = Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));
        var stages = Enumerable.Repeat<DemoStage>(
            static (_, _) => ValueTask.FromResult(new OutboxCommandResult(0, string.Empty, string.Empty)),
            MaximumStageCount + 1).ToArray();

        await Assert.That(() => new DurableOutboxDemo(root, stages))
            .ThrowsExactly<ArgumentException>()
            .WithMessageContaining("The demo has too many configured stages.");
    }

    /// <summary>Verifies cleanup is safe when directory creation fails before ownership can be marked.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDemoDirectoryCannotBeCreated_ThenCreationFailureIsPropagated()
    {
        var rootFile = Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, $"{Guid.NewGuid():N}.tmp");
        _ = Directory.CreateDirectory(Path.GetDirectoryName(rootFile) ?? TemporaryDirectory.GetTemporaryDirectory());
        await File.WriteAllTextAsync(rootFile, "not a directory").ConfigureAwait(false);
        DurableOutboxDemo demo = new(
            rootFile,
            [
                static (_, _) => ValueTask.FromResult(new OutboxCommandResult(0, string.Empty, string.Empty)),
            ]);

        try
        {
            await Assert.That(async () => await demo.RunAsync(CancellationToken.None))
                .Throws<IOException>();
        }
        finally
        {
            File.Delete(rootFile);
        }
    }

    /// <summary>Verifies cleanup refuses recursive deletion when the ownership marker is removed.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStageRemovesOwnershipMarker_ThenDemoLeavesDirectoryForInspection()
    {
        var root = Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), TestRootName, Guid.NewGuid().ToString("N"));
        DurableOutboxDemo demo = new(
            root,
            [
                static (databasePath, _) =>
                {
                    var directory = Path.GetDirectoryName(databasePath);
                    if (directory is not null)
                    {
                        File.Delete(Path.Combine(directory, OwnedDemoDirectory.MarkerFileName));
                    }

                    return ValueTask.FromResult(new OutboxCommandResult(0, "marker removed\n", string.Empty));
                },
            ]);

        try
        {
            var result = await demo.RunAsync(CancellationToken.None);

            await Assert.That(result.ExitCode).IsEqualTo(0);
            await Assert.That(result.StandardOutput).Contains("owned directory removed: False");
            await Assert.That(CountOwnedChildren(root)).IsEqualTo(1);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    /// <summary>Counts generated children under a root directory.</summary>
    /// <param name="root">The root directory.</param>
    /// <returns>The number of generated children.</returns>
    private static int CountOwnedChildren(string root) =>
        Directory.Exists(root) ? Directory.GetDirectories(root).Length : 0;

    /// <summary>Deletes only the exact generated child directory if a regression leaks it.</summary>
    /// <param name="root">The expected root directory.</param>
    /// <param name="generatedDirectory">The generated child directory.</param>
    private static void DeleteGeneratedChildIfLeaked(string root, string generatedDirectory)
    {
        if (!IsGeneratedImmediateChild(root, generatedDirectory) || !Directory.Exists(generatedDirectory))
        {
            return;
        }

        Directory.Delete(generatedDirectory, recursive: true);
    }

    /// <summary>Determines whether a path is a GUID-named immediate child of the expected root.</summary>
    /// <param name="root">The expected root directory.</param>
    /// <param name="generatedDirectory">The generated child directory.</param>
    /// <returns><see langword="true"/> when the path is a generated immediate child.</returns>
    private static bool IsGeneratedImmediateChild(string root, string generatedDirectory)
    {
        if (generatedDirectory.Length == 0)
        {
            return false;
        }

        var fullRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        var fullDirectory = Path.TrimEndingDirectorySeparator(Path.GetFullPath(generatedDirectory));
        var childName = Path.GetFileName(fullDirectory);
        var parent = Path.GetDirectoryName(fullDirectory);
        var comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        return Guid.TryParseExact(childName, "N", out _)
            && parent is not null
            && string.Equals(Path.TrimEndingDirectorySeparator(parent), fullRoot, comparison);
    }
}
