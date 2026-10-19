// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Tests atomic replacement and bounded Windows handle contention.</summary>
public sealed class FileSystemJournalReplacementTests
{
    /// <summary>The file holding the new contents.</summary>
    private const string ReplacementName = "replacement";

    /// <summary>The file holding the committed contents.</summary>
    private const string JournalName = "journal";

    /// <summary>The replacement contents.</summary>
    private const string NextContents = "next";

    /// <summary>The committed contents.</summary>
    private const string OldContents = "old";

    /// <summary>The lock observation period in milliseconds.</summary>
    private const int ObservationMilliseconds = 100;

    /// <summary>The upper bound including test scheduling overhead.</summary>
    private const int DeadlineSeconds = 5;

    /// <summary>Verifies permanent failures are never classified as transient handle contention.</summary>
    /// <param name="code">The native error represented by the exception.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(5)]
    [Arguments(23)]
    [Arguments(1175)]
    [Arguments(1176)]
    public async Task PermanentNativeErrorsDoNotRetry(int code) =>
        await Assert.That(FileSystemJournalReplacement.IsSharingViolation(
            new("Permanent native failure", unchecked((int)0x80070000) | code))).IsFalse();

    /// <summary>Verifies a permanent Windows permission failure preserves both files.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReadOnlyDestinationFailsWithoutWaitingForSharingDeadline()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var directory = CreateDirectory();
        var destination = Path.Combine(directory, JournalName);
        try
        {
            var source = Path.Combine(directory, ReplacementName);
            await File.WriteAllTextAsync(source, NextContents);
            await File.WriteAllTextAsync(destination, OldContents);
            File.SetAttributes(destination, FileAttributes.ReadOnly);
            var started = Stopwatch.GetTimestamp();
            await Assert.That(() => FileSystemJournalReplacement.ReplaceAsync(source, destination, CancellationToken.None).AsTask())
                .Throws<IOException>();
            await Assert.That(Stopwatch.GetElapsedTime(started)).IsLessThan(FileSystemJournalReplacement.SharingRetryTimeout);
            await Assert.That(await File.ReadAllTextAsync(source)).IsEqualTo(NextContents);
            await Assert.That(await File.ReadAllTextAsync(destination)).IsEqualTo(OldContents);
        }
        finally
        {
            File.SetAttributes(destination, FileAttributes.Normal);
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies replacement succeeds after an actual Windows sharing handle closes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WindowsSharingViolationRetriesUntilHandleCloses()
    {
        var directory = CreateDirectory();
        try
        {
            var source = Path.Combine(directory, ReplacementName);
            var destination = Path.Combine(directory, JournalName);
            await File.WriteAllTextAsync(source, NextContents);
            await File.WriteAllTextAsync(destination, OldContents);
            await using var handle = File.Open(destination, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var replacement = FileSystemJournalReplacement.ReplaceAsync(source, destination, CancellationToken.None).AsTask();
            if (OperatingSystem.IsWindows())
            {
                await Task.Delay(ObservationMilliseconds);
                await Assert.That(replacement.IsCompleted).IsFalse();
            }

            await handle.DisposeAsync();
            await replacement;
            FileSystemJournalReplacement.FlushDirectory(directory);
            await Assert.That(await File.ReadAllTextAsync(destination)).IsEqualTo(NextContents);
            await Assert.That(File.Exists(source)).IsFalse();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies a permanently shared Windows journal stops within a finite deadline.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WindowsPermanentSharingViolationPreservesBothFilesWithinDeadline()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var directory = CreateDirectory();
        try
        {
            var source = Path.Combine(directory, ReplacementName);
            var destination = Path.Combine(directory, JournalName);
            await File.WriteAllTextAsync(source, NextContents);
            await File.WriteAllTextAsync(destination, OldContents);
            await using var handle = File.Open(destination, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var started = Stopwatch.GetTimestamp();
            await Assert.That(() => FileSystemJournalReplacement.ReplaceAsync(source, destination, CancellationToken.None).AsTask())
                .Throws<IOException>();
            await Assert.That(Stopwatch.GetElapsedTime(started)).IsLessThan(TimeSpan.FromSeconds(DeadlineSeconds));
            await Assert.That(await File.ReadAllTextAsync(source)).IsEqualTo(NextContents);
            await Assert.That(await File.ReadAllTextAsync(destination)).IsEqualTo(OldContents);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Verifies cancellation interrupts a sharing retry and leaves both names untouched.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationPreservesOriginalAndReplacement()
    {
        var directory = CreateDirectory();
        try
        {
            var source = Path.Combine(directory, ReplacementName);
            var destination = Path.Combine(directory, JournalName);
            await File.WriteAllTextAsync(source, NextContents);
            await File.WriteAllTextAsync(destination, OldContents);
            await using var handle = File.Open(destination, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var cancellation = new CancellationTokenSource();
            if (OperatingSystem.IsWindows())
            {
                cancellation.CancelAfter(TimeSpan.FromMilliseconds(ObservationMilliseconds));
            }
            else
            {
                await cancellation.CancelAsync();
            }

            await Assert.That(() => FileSystemJournalReplacement.ReplaceAsync(source, destination, cancellation.Token).AsTask())
                .Throws<OperationCanceledException>();
            await Assert.That(await File.ReadAllTextAsync(source)).IsEqualTo(NextContents);
            await Assert.That(await File.ReadAllTextAsync(destination)).IsEqualTo(OldContents);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Creates a unique directory for the replacement pair.</summary>
    /// <returns>The test directory.</returns>
    private static string CreateDirectory()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"journal-replacement-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(directory);
        return directory;
    }
}
