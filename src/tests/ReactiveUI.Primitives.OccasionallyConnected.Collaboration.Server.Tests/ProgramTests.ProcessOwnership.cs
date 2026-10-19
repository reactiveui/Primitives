// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <content>Checks the process test waits for file ownership before cleanup.</content>
public sealed partial class ProgramTests
{
    /// <summary>Checks an exit notification alone cannot release a held database file.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task ProcessOwnershipProbeWaitsForReleasedDatabaseHandle()
    {
        Skip.Unless(OperatingSystem.IsWindows(), "Windows file sharing ownership probe.");
        using var lease = new ProcessDatabaseLease();
        await using var held = new FileStream(lease.Path, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(StopTimeoutSeconds));
        var waiting = WaitForReleasedDatabaseFilesAsync(System.IO.Path.GetDirectoryName(lease.Path)!, timeout.Token);
        await Assert.That(waiting.IsCompleted).IsFalse();
        await held.DisposeAsync();
        await waiting;
        await Assert.That(waiting.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Checks a persistent owner fails within the shutdown deadline.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task ProcessOwnershipProbePropagatesCancellation()
    {
        Skip.Unless(OperatingSystem.IsWindows(), "Windows file sharing ownership probe.");
        using var lease = new ProcessDatabaseLease();
        await using var held = new FileStream(lease.Path, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
        using CancellationTokenSource timeout = new();
        var waiting = WaitForReleasedDatabaseFilesAsync(System.IO.Path.GetDirectoryName(lease.Path)!, timeout.Token);
        await Assert.That(waiting.IsCompleted).IsFalse();
        await timeout.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => waiting);
    }

    /// <summary>Checks failures other than Windows sharing violations are not retried.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task ProcessOwnershipProbePropagatesMissingDirectory()
    {
        var lease = new ProcessDatabaseLease();
        var directory = System.IO.Path.GetDirectoryName(lease.Path)!;
        ((IDisposable)lease).Dispose();
        await Assert.ThrowsAsync<DirectoryNotFoundException>(
            () => WaitForReleasedDatabaseFilesAsync(directory, CancellationToken.None));
    }

    /// <summary>Checks that terminated process handles no longer own any fixture file.</summary>
    /// <param name="directory">The owned database directory.</param>
    /// <param name="cancellationToken">The shared shutdown deadline.</param>
    /// <returns>The bounded exclusive-access barrier.</returns>
    private static async Task WaitForReleasedDatabaseFilesAsync(string directory, CancellationToken cancellationToken)
    {
        using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(ReadyPollMilliseconds));
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                foreach (var file in Directory.EnumerateFiles(directory))
                {
                    await using var probe = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                }

                return;
            }
            catch (IOException error) when (
                OperatingSystem.IsWindows()
                && error.HResult is unchecked((int)0x80070020) or unchecked((int)0x80070021))
            {
                _ = await poll.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
