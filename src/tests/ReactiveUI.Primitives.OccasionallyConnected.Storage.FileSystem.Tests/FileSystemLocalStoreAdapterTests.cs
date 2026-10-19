// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests;

/// <summary>Tests for the append-only filesystem store.</summary>
public sealed partial class FileSystemLocalStoreAdapterTests
{
    /// <summary>The default client identity used by store tests.</summary>
    private const string TestClientId = "client";

    /// <summary>The payload used for initial snapshots.</summary>
    private const string SnapshotPayload = "snapshot";

    /// <summary>The stream name used by recovery tests.</summary>
    private const string TemperatureStreamName = "sensor/temperature";

    /// <summary>The journal file name.</summary>
    private const string JournalFileName = "journal.log";

    /// <summary>The event cursor used by replay tests.</summary>
    private const string TestCursor = "cursor-1";

    /// <summary>The first operation sequence.</summary>
    private const int FirstOperationSequence = 0;

    /// <summary>The next operation sequence.</summary>
    private const int NextOperationSequence = 1;

    /// <summary>The initial snapshot revision.</summary>
    private const int InitialSnapshotRevision = 1;

    /// <summary>The second snapshot revision.</summary>
    private const int SecondSnapshotRevision = 2;

    /// <summary>The byte count used for the incomplete journal tail.</summary>
    private const int IncompleteTailLength = 4;

    /// <summary>The byte limit used by standard lease tests.</summary>
    private const int StandardLeaseByteLimit = 1024;

    /// <summary>The byte limit used to reject an oversized operation.</summary>
    private const int UndersizedLeaseByteLimit = 8;

    /// <summary>The initial compaction record threshold.</summary>
    private const int NoCompactionRecords = 0;

    /// <summary>The number of records expected after compaction.</summary>
    private const int OneCompactedRecord = 1;

    /// <summary>The expected pending operation count after recovery.</summary>
    private const int TwoPendingOperations = 2;

    /// <summary>The environment variable carrying a child compaction crash case.</summary>
    private const string CompactionCrashCaseVariable = "RXUI_FILESYSTEM_COMPACTION_CRASH_CASE";

    /// <summary>The child test tree node filter used by the compaction crash case.</summary>
    private const string CompactionCrashChildFilter = $"/*/*/*/{nameof(WhenCompactionCrashChildReachesCheckpoint_ThenSignalsParent)}";

    /// <summary>The number of fields encoded in a compaction crash case.</summary>
    private const int CompactionCrashCaseFieldCount = 4;

    /// <summary>The duration of the original lease.</summary>
    private static readonly TimeSpan OriginalLeaseDuration = TimeSpan.FromSeconds(2);

    /// <summary>The delay that takes the original lease past its expiry.</summary>
    private static readonly TimeSpan OriginalLeaseExpiryDelay = TimeSpan.FromSeconds(2.1);

    /// <summary>A deterministic clock for timestamp values in store tests.</summary>
    private static readonly TimeProvider TestTimeProvider = new FixedTimeProvider(
        new DateTimeOffset(2032, 4, 5, 6, 7, 8, TimeSpan.Zero));

    /// <summary>The incomplete record bytes used by recovery tests.</summary>
    private static readonly byte[] IncompleteTailBytes = [1, 2, 3, 4];

    /// <summary>The interval between child crash signal checks.</summary>
    private static readonly TimeSpan CompactionCrashPollInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>The maximum wait for a child crash signal.</summary>
    private static readonly TimeSpan CompactionCrashSignalTimeout = TimeSpan.FromSeconds(25);

    /// <summary>The maximum wait for a killed child process to exit.</summary>
    private static readonly TimeSpan CompactionCrashExitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies committed state survives closing and reopening the adapter.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CommitIsRecoveredAfterReopen()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId(TemperatureStreamName);
            var operation = CreateOperation(stream, FirstOperationSequence);
            var mutation = new SnapshotMutation(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscriptionId;

            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                var result = await adapter.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
                await Assert.That(result.OperationId).IsEqualTo(operation.OperationId);
                await Assert.That(adapter.Capabilities).IsEqualTo(
                    LocalStoreCapabilities.AtomicLocalCommit
                    | LocalStoreCapabilities.AtomicRemoteApply
                    | LocalStoreCapabilities.DurableInbox
                    | LocalStoreCapabilities.LeasedOutbox
                    | LocalStoreCapabilities.DurableLocalCommit
                    | LocalStoreCapabilities.ClientIdentityBinding);
            }

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
            await Assert.That(recovered.PendingOperations).HasSingleItem();
            await Assert.That(recovered.PendingOperations[0].StreamId).IsEqualTo(stream);
            await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(recovered.Snapshot).IsNotNull();
            await Assert.That(recovered.Snapshot!.Revision).IsEqualTo(InitialSnapshotRevision);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies durable timestamps use the configured time provider.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TimeProviderControlsDurableTimestamps()
    {
        var directory = CreateJournalTestDirectory();
        var expectedTime = new DateTimeOffset(2032, 4, 5, 6, 7, 8, TimeSpan.Zero);
        try
        {
            var stream = new StreamId(TemperatureStreamName);
            var operation = CreateOperation(stream, FirstOperationSequence);
            var mutation = new SnapshotMutation(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);

            await using var adapter = new FileSystemLocalStoreAdapter(directory, new FixedTimeProvider(expectedTime));
            await adapter.InitializeAsync(initialization, CancellationToken.None);
            var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var result = await adapter.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
            var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
            var recovered = await adapter.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);

            await Assert.That(result.CommittedAtUtc).IsEqualTo(expectedTime);
            await Assert.That(status!.ChangedAtUtc).IsEqualTo(expectedTime);
            await Assert.That(recovered.Snapshot!.SavedAtUtc).IsEqualTo(expectedTime);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies concurrent and repeated disposal completes safely.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentDisposeAsyncCallsComplete()
    {
        var directory = CreateJournalTestDirectory();
        var adapter = new FileSystemLocalStoreAdapter(directory);
        try
        {
            await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            await Task.WhenAll(adapter.DisposeAsync().AsTask(), adapter.DisposeAsync().AsTask());
            await adapter.DisposeAsync();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies an incomplete crash tail is ignored and truncated.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IncompleteTailIsDiscardedDuringRecovery()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId(TemperatureStreamName);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscriptionId;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await adapter.CommitLocalOperationAsync(
                    CreateOperation(stream, FirstOperationSequence),
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    CancellationToken.None);
            }

            await using (var tail = new FileStream(Path.Combine(directory, JournalFileName), FileMode.Append, FileAccess.Write, FileShare.Read))
            {
                await tail.WriteAsync(IncompleteTailBytes);
            }

            var nextOperation = CreateOperation(stream, NextOperationSequence);
            await using (var reopened = new FileSystemLocalStoreAdapter(directory))
            {
                await reopened.InitializeAsync(initialization, CancellationToken.None);
                var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
                await Assert.That(recovered.PendingOperations).HasSingleItem();
                await Assert.That(new FileInfo(Path.Combine(directory, JournalFileName)).Length).IsGreaterThan(IncompleteTailLength);

                _ = await reopened.CommitLocalOperationAsync(
                    nextOperation,
                    new(stream, CreatePayload("next-snapshot"), 1, 1),
                    CancellationToken.None);
            }

            await using var recoveredAgain = new FileSystemLocalStoreAdapter(directory);
            await recoveredAgain.InitializeAsync(initialization, CancellationToken.None);
            var appendedAfterTruncation = await recoveredAgain.RecoverStreamAsync(
                stream,
                subscriptionId,
                CancellationToken.None);
            await Assert.That(appendedAfterTruncation.PendingOperations).Count().IsEqualTo(TwoPendingOperations);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies an orphaned compaction temporary file cannot hide the committed journal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OrphanedCompactionTemporaryFileDoesNotHideCommittedJournal()
    {
        var directory = CreateJournalTestDirectory();
        try
        {
            var stream = new StreamId("interrupted-compaction");
            var operation = CreateOperation(stream, FirstOperationSequence);
            var initialization = new LocalStoreInitialization(TestClientId, 1, false);
            SubscriptionId subscriptionId;
            await using (var adapter = new FileSystemLocalStoreAdapter(directory))
            {
                await adapter.InitializeAsync(initialization, CancellationToken.None);
                subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
                await adapter.CommitLocalOperationAsync(
                    operation,
                    new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
                    CancellationToken.None);
            }

            var temporaryPath = Path.Combine(directory, $"journal.log.{Guid.NewGuid():N}.compact");
            await File.WriteAllBytesAsync(temporaryPath, IncompleteTailBytes);

            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(initialization, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);

            await Assert.That(recovered.PendingOperations).HasSingleItem();
            await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
            await Assert.That(File.Exists(temporaryPath)).IsTrue();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies a process crash around atomic journal replacement leaves a recoverable journal.</summary>
    /// <param name="checkpoint">The journal replacement checkpoint.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [NotInParallel("filesystem-compaction-process-crash")]
    [Arguments(nameof(FileSystemJournalCheckpoint.BeforeCompactionJournalReplace))]
    [Arguments(nameof(FileSystemJournalCheckpoint.AfterCompactionJournalReplace))]
    public async Task WhenProcessDiesAtCompactionCheckpoint_ThenReopenUsesCompleteJournal(string checkpoint)
    {
        var directory = CreateJournalTestDirectory();
        var signalPath = Path.Combine(directory, "compaction.signal");
        var operationId = Guid.NewGuid();
        try
        {
            await RunCompactionCrashChildAsync(
                string.Join('\n', directory, signalPath, operationId.ToString("D"), checkpoint),
                signalPath);
            await Assert.That(await File.ReadAllTextAsync(signalPath)).IsEqualTo(checkpoint);

            var stream = new StreamId("compaction-process-crash");
            await using var reopened = new FileSystemLocalStoreAdapter(directory);
            await reopened.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
            var subscriptionId = await reopened.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);

            await Assert.That(recovered.PendingOperations).HasSingleItem();
            await Assert.That(recovered.PendingOperations[0].OperationId.Value).IsEqualTo(operationId);
            await Assert.That(recovered.Snapshot?.Revision).IsEqualTo(1);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Runs the child writer and blocks at the selected journal replacement checkpoint.</summary>
    /// <returns>The child task, or a passing no-op for the normal parent test run.</returns>
    /// <exception cref="InvalidOperationException">The child case is malformed or the checkpoint is not reached.</exception>
    [Test]
    public async Task WhenCompactionCrashChildReachesCheckpoint_ThenSignalsParent()
    {
        var encoded = Environment.GetEnvironmentVariable(CompactionCrashCaseVariable);
        if (encoded is null)
        {
            await Assert.That(encoded).IsNull();
            return;
        }

        var fields = encoded.Split('\n');
        if (fields.Length != CompactionCrashCaseFieldCount
            || !Guid.TryParse(fields[2], out var operationGuid)
            || !Enum.TryParse(fields[3], ignoreCase: false, out FileSystemJournalCheckpoint checkpoint))
        {
            throw new InvalidOperationException("The FileSystem compaction crash case is malformed.");
        }

        var stream = new StreamId("compaction-process-crash");
        var appendCase = checkpoint is FileSystemJournalCheckpoint.AfterAppendHeader
            or FileSystemJournalCheckpoint.AfterAppendPayload
            or FileSystemJournalCheckpoint.AfterAppendChecksum
            or FileSystemJournalCheckpoint.AfterAppendFlush;
        var armed = !appendCase;
        Action<FileSystemJournalCheckpoint> reached = current =>
        {
            if (armed && current == checkpoint)
            {
                SignalAndBlockAtCompactionCheckpoint(fields[1], current.ToString());
            }
        };
        await using var adapter = new FileSystemLocalStoreAdapter(fields[0], reached);
        await adapter.InitializeAsync(new(TestClientId, 1, false), CancellationToken.None);
        var operation = CreateOperation(stream, FirstOperationSequence) with { OperationId = new(operationGuid) };
        await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        await adapter.CommitLocalOperationAsync(
            operation,
            new(stream, CreatePayload(SnapshotPayload), InitialSnapshotRevision),
            CancellationToken.None);
        if (appendCase)
        {
            armed = true;
            await adapter.CommitLocalOperationAsync(
                CreateOperation(stream, NextOperationSequence),
                new(stream, CreatePayload("next"), 1, 1),
                CancellationToken.None);
            throw new InvalidOperationException($"The append child completed without reaching {checkpoint}.");
        }

        await adapter.CompactAsync(new(stream, DateTimeOffset.MaxValue, NoCompactionRecords), CancellationToken.None);
        throw new InvalidOperationException($"The compaction child completed without reaching {checkpoint}.");
    }

    /// <summary>Runs the compaction operation in a child process until it reaches the selected checkpoint.</summary>
    /// <param name="encodedCase">The newline-separated child case fields.</param>
    /// <param name="signalPath">The child signal path.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or reach its checkpoint.</exception>
    private static async Task RunCompactionCrashChildAsync(string encodedCase, string signalPath)
    {
        var assembly = Path.Combine(AppContext.BaseDirectory, "ReactiveUI.Primitives.OccasionallyConnected.Storage.FileSystem.Tests.dll");
        ProcessStartInfo start = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false, };
        start.ArgumentList.Add(assembly);
        start.ArgumentList.Add("--treenode-filter");
        start.ArgumentList.Add(CompactionCrashChildFilter);
        start.Environment[CompactionCrashCaseVariable] = encodedCase;
        using var child = Process.Start(start) ?? throw new InvalidOperationException("The FileSystem compaction crash child did not start.");
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        var began = Stopwatch.GetTimestamp();
        while (!File.Exists(signalPath)
               && !child.HasExited
               && Stopwatch.GetElapsedTime(began) < CompactionCrashSignalTimeout)
        {
            await Task.Delay(CompactionCrashPollInterval);
        }

        var signaled = File.Exists(signalPath);
        if (!child.HasExited)
        {
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(CompactionCrashExitTimeout);
        }

        var output = await standardOutput.WaitAsync(CompactionCrashExitTimeout);
        var error = await standardError.WaitAsync(CompactionCrashExitTimeout);
        if (!signaled)
        {
            throw new InvalidOperationException(
                string.Join(Environment.NewLine, "The FileSystem compaction crash child did not signal.", output, error));
        }
    }

    /// <summary>Publishes a crash checkpoint signal and blocks until the parent kills this process.</summary>
    /// <param name="signalPath">The signal file path.</param>
    /// <param name="checkpoint">The reached checkpoint.</param>
    private static void SignalAndBlockAtCompactionCheckpoint(string signalPath, string checkpoint)
    {
        var temporaryPath = $"{signalPath}.{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}.tmp";
        File.WriteAllText(temporaryPath, checkpoint);
        File.Move(temporaryPath, signalPath);
        using var never = new ManualResetEventSlim(false);
        never.Wait();
    }
}
