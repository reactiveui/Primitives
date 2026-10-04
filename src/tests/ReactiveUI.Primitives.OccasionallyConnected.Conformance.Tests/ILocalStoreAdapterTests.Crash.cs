// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <content>Process-kill verification of acknowledged native store commits.</content>
public sealed partial class ILocalStoreAdapterTests
{
    /// <summary>The child provider environment variable.</summary>
    private const string ChildProviderVariable = "RXUI_STORE_CONFORMANCE_PROVIDER";

    /// <summary>The child durable location environment variable.</summary>
    private const string ChildDirectoryVariable = "RXUI_STORE_CONFORMANCE_DIRECTORY";

    /// <summary>The signal file containing the committed identities.</summary>
    private const string ChildSignalFile = "committed.signal";

    /// <summary>The compiled conformance test assembly.</summary>
    private const string TestAssemblyFile = "ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests.dll";

    /// <summary>The three durable identifiers and owned writer process identifier.</summary>
    private const int SignalIdentityCount = 4;

    /// <summary>The signal observation interval.</summary>
    private static readonly TimeSpan SignalPoll = TimeSpan.FromMilliseconds(50);

    /// <summary>The maximum process startup and join interval.</summary>
    private static readonly TimeSpan ChildGuard = TimeSpan.FromSeconds(30);

    /// <summary>Checks the shutdown probe waits for actual exclusive file access.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task WriterOwnershipProbeWaitsForReleasedHandle()
    {
        Skip.Unless(OperatingSystem.IsWindows(), "Windows sharing-violation shutdown probe.");
        await using var fixture = new StoreFixture(0);
        await using var held = new FileStream(
            Path.Combine(fixture.DirectoryPath, "owned-file.bin"),
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None);
        using var timeout = new CancellationTokenSource(ChildGuard);
        var waiting = WaitForReleasedFilesAsync(fixture.DirectoryPath, timeout.Token);
        await Assert.That(waiting.IsCompleted).IsFalse();
        await held.DisposeAsync();
        await waiting;
    }

    /// <summary>Checks a persistent sharing violation cannot exceed the shutdown deadline.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task WriterOwnershipProbeHonorsCancellation()
    {
        Skip.Unless(OperatingSystem.IsWindows(), "Windows sharing-violation shutdown probe.");
        await using var fixture = new StoreFixture(0);
        await using var held = new FileStream(
            Path.Combine(fixture.DirectoryPath, "owned-file.bin"),
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None);
        using CancellationTokenSource timeout = new();
        var waiting = WaitForReleasedFilesAsync(fixture.DirectoryPath, timeout.Token);
        await Assert.That(waiting.IsCompleted).IsFalse();
        await timeout.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => waiting);
    }

    /// <summary>Checks shutdown probing propagates failures other than sharing violations.</summary>
    /// <returns>The assertions.</returns>
    [Test]
    public async Task WriterOwnershipProbePropagatesMissingDirectory()
    {
        var fixture = new StoreFixture(0);
        await fixture.DisposeAsync();
        await Assert.ThrowsAsync<DirectoryNotFoundException>(
            () => WaitForReleasedFilesAsync(fixture.DirectoryPath, CancellationToken.None));
    }

    /// <summary>Checks all acknowledged state survives abrupt process termination without disposal.</summary>
    /// <param name="provider">The native durable provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(1)]
    [Arguments(FileSystemProvider)]
    [Arguments(LiteDbProvider)]
    [Arguments(BliteDbProvider)]
    [Arguments(EncryptedSqliteProvider)]
    public async Task AcknowledgedCommitsSurviveProcessKill(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        using var child = StartStoreChild(provider, fixture.DirectoryPath);
        var signal = Path.Combine(fixture.DirectoryPath, ChildSignalFile);
        using var timeout = new CancellationTokenSource(ChildGuard);
        using var poll = new PeriodicTimer(SignalPoll);
        try
        {
            while (!File.Exists(signal))
            {
                await Assert.That(child.HasExited).IsFalse();
                _ = await poll.WaitForNextTickAsync(timeout.Token);
            }

            Func<Task> competingWriter = async () =>
            {
                await using var second = await fixture.OpenAsync();
            };
            var ownershipFailure = await Assert.ThrowsAsync<Exception>(competingWriter);
            await Assert.That(ownershipFailure is TimeoutException or OperationCanceledException).IsFalse();
            var proof = (await File.ReadAllTextAsync(signal, timeout.Token)).Split('|');
            await Assert.That(proof.Length).IsEqualTo(SignalIdentityCount);
            using var writer = Process.GetProcessById(int.Parse(proof[3], CultureInfo.InvariantCulture));
            await Assert.That(writer.Id).IsNotEqualTo(Environment.ProcessId);
            child.Kill(entireProcessTree: true);
            await Task.WhenAll(
                WaitForChildExitAsync(child, timeout.Token),
                WaitForChildExitAsync(writer, timeout.Token));
            await WaitForReleasedFilesAsync(fixture.DirectoryPath, timeout.Token);
            await using var reopened = await fixture.OpenAsync();
            await LocalStoreConformance.AssertDurableAsync(
                reopened,
                new(Guid.Parse(proof[0])),
                new(Guid.Parse(proof[1])),
                Guid.Parse(proof[2]));
        }
        catch (Exception error)
        {
            await TestContext.Current!.OutputWriter.WriteLineAsync(error.ToString());
            throw;
        }
        finally
        {
            if (!child.HasExited)
            {
                child.Kill(entireProcessTree: true);
            }

            using var join = new CancellationTokenSource(ChildGuard);
            await WaitForChildExitAsync(child, join.Token);
        }
    }

    /// <summary>Writes acknowledged state in the process that the parent terminates.</summary>
    /// <returns>The child operation or a completed non-child test.</returns>
    [Test]
    public async Task StoreChildPublishesCommittedIdentities()
    {
        var selector = Environment.GetEnvironmentVariable(ChildProviderVariable);
        if (selector is null)
        {
            return;
        }

        var provider = int.Parse(selector, CultureInfo.InvariantCulture);
        var directory = new DirectoryInfo(Environment.GetEnvironmentVariable(ChildDirectoryVariable)!);
        await using var fixture = new StoreFixture(provider, directory, false);
        await using var store = await fixture.OpenAsync();
        var subscription = await store.GetOrCreateSubscriptionIdAsync(LocalStoreConformance.Stream, null, CancellationToken.None);
        var committed = await LocalStoreConformance.SeedDurableAsync(store);
        var signal = Path.Combine(directory.FullName, ChildSignalFile);
        var temporary = $"{signal}.tmp";
        await File.WriteAllTextAsync(
            temporary,
            $"{subscription.Value}|{committed.Operation.Value}|{committed.Event}|{Environment.ProcessId}");
        File.Move(temporary, signal);
        await Task.Delay(Timeout.InfiniteTimeSpan);
    }

    /// <summary>Starts only the child store test without inheriting a parent test selector.</summary>
    /// <param name="provider">The native store provider.</param>
    /// <param name="directory">The durable directory.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">The child cannot be started.</exception>
    private static Process StartStoreChild(int provider, string directory)
    {
        var executable = Environment.ProcessPath ?? throw new InvalidOperationException("The test host path is unavailable.");
        ProcessStartInfo start = new(executable) { UseShellExecute = false, WorkingDirectory = AppContext.BaseDirectory };
        if (string.Equals(Path.GetFileNameWithoutExtension(executable), "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, TestAssemblyFile));
        }

        start.ArgumentList.Add("--treenode-filter");
        start.ArgumentList.Add($"/*/*/ILocalStoreAdapterTests/{nameof(StoreChildPublishesCommittedIdentities)}");
        start.Environment[ChildProviderVariable] = provider.ToString(CultureInfo.InvariantCulture);
        start.Environment[ChildDirectoryVariable] = directory;
        return Process.Start(start) ?? throw new InvalidOperationException("The conformance child did not start.");
    }

    /// <summary>Joins an owned child process before the cancellation deadline.</summary>
    /// <param name="process">The owned child.</param>
    /// <param name="cancellationToken">The join deadline.</param>
    /// <returns>The join task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task WaitForChildExitAsync(Process process, CancellationToken cancellationToken) =>
        process.WaitForExitAsync(cancellationToken);

    /// <summary>Waits for Windows to release terminated writer handles before reopening.</summary>
    /// <param name="directory">The owned fixture directory.</param>
    /// <param name="cancellationToken">The shutdown deadline.</param>
    /// <returns>The bounded ownership probe.</returns>
    private static async Task WaitForReleasedFilesAsync(string directory, CancellationToken cancellationToken)
    {
        using var poll = new PeriodicTimer(SignalPoll);
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
                _ = await poll.WaitForNextTickAsync(cancellationToken);
            }
        }
    }
}
