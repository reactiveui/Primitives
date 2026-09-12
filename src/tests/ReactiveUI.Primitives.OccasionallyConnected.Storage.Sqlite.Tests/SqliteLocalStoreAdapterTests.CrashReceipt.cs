// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Crash receipt tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The child process mode marker environment variable.</summary>
    private const string CrashReceiptChildModeVariable = "RXUI_SQLITE_CRASH_RECEIPT_CHILD";

    /// <summary>The child database path environment variable.</summary>
    private const string CrashReceiptDatabasePathVariable = "RXUI_SQLITE_CRASH_RECEIPT_DATABASE";

    /// <summary>The child signal path environment variable.</summary>
    private const string CrashReceiptSignalPathVariable = "RXUI_SQLITE_CRASH_RECEIPT_SIGNAL";

    /// <summary>The child operation identifier environment variable.</summary>
    private const string CrashReceiptOperationIdVariable = "RXUI_SQLITE_CRASH_RECEIPT_OPERATION";

    /// <summary>The child subscription identifier environment variable.</summary>
    private const string CrashReceiptSubscriptionIdVariable = "RXUI_SQLITE_CRASH_RECEIPT_SUBSCRIPTION";

    /// <summary>The marker value that enables child crash receipt mode.</summary>
    private const string CrashReceiptChildMode = "1";

    /// <summary>The signal file polling interval in milliseconds.</summary>
    private const int SignalPollIntervalMilliseconds = 100;

    /// <summary>The test assembly file name used by direct MTP execution.</summary>
    private const string TestAssemblyFileName = "ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests.dll";

    /// <summary>The child test tree node filter.</summary>
    private const string ChildTestTreeNodeFilter = $"/*/*/*/{nameof(WhenCrashReceiptChildCommitsAndWaits_ThenSignalIsPublished)}";

    /// <summary>The maximum time to wait for the child to publish the acknowledged receipt signal.</summary>
    private static readonly TimeSpan SignalWaitTimeout = TimeSpan.FromSeconds(20);

    /// <summary>The maximum time to wait for the killed child process to exit.</summary>
    private static readonly TimeSpan ChildExitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Verifies an acknowledged local commit survives abrupt writer process termination.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start, fails to signal, or publishes a malformed signal.</exception>
    [Test]
    public async Task WhenWriterProcessDiesAfterAcknowledgedLocalCommit_ThenReopenRecoversReceiptWithoutDuplicateOptimism()
    {
        using var database = TempDatabase.Create();
        var signalPath = System.IO.Path.ChangeExtension(database.Path, $"commit-{Guid.NewGuid():N}.signal");
        var operationId = OperationId.New();
        var subscriptionId = SubscriptionId.New();
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, subscriptionId, CancellationToken.None);
        }

        await RunCrashReceiptChildUntilSignalAsync(database.Path, signalPath, operationId, subscriptionId);
        var receipt = await ReadCrashReceiptAsync(signalPath);
        await Assert.That(receipt.OperationId).IsEqualTo(operationId.Value);
        await Assert.That(receipt.SubscriptionId).IsEqualTo(subscriptionId.Value);
        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(receipt.SnapshotRevision).IsEqualTo(1);

        await VerifyCrashReceiptRecoveryAsync(database.Path, operationId, subscriptionId, receipt);
    }

    /// <summary>Child workflow used by the parent process crash receipt test.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The child crash receipt environment is incomplete.</exception>
    [Test]
    public async Task WhenCrashReceiptChildCommitsAndWaits_ThenSignalIsPublished()
    {
        var childContext = ReadCrashReceiptChildContext();
        if (childContext is null)
        {
            await Assert.That(Environment.GetEnvironmentVariable(CrashReceiptChildModeVariable)).IsNull();
            return;
        }

        await using var adapter = CreateAdapter(childContext.DatabasePath);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, childContext.SubscriptionId, CancellationToken.None);
        var operation = CreateCrashReceiptOperation(childContext.OperationId);
        var receipt = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);

        await PublishCrashReceiptSignalAsync(childContext.SignalPath, new(
            receipt.OperationId.Value,
            subscriptionId.Value,
            receipt.ClientSequence,
            receipt.SnapshotRevision,
            receipt.CommittedAtUtc));

        await Task.Delay(Timeout.InfiniteTimeSpan);
    }

    /// <summary>Verifies recovered state and idempotent duplicate commit behavior after child process termination.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="receipt">The acknowledged receipt written by the child process.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    private static async Task VerifyCrashReceiptRecoveryAsync(
        string databasePath,
        OperationId operationId,
        SubscriptionId subscriptionId,
        CrashReceiptSignal receipt)
    {
        var operation = CreateCrashReceiptOperation(operationId);
        var snapshotMutation = CreateSnapshotMutation(expectedRevision: 0);
        await using var reopened = CreateAdapter(databasePath);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovery = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var status = await reopened.GetOperationStatusAsync(operationId, CancellationToken.None);
        var duplicate = await reopened.CommitLocalOperationAsync(operation, snapshotMutation, CancellationToken.None);
        var afterDuplicate = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(recovery.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(recovery.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(recovery.PendingOperations[0].ClientSequence).IsEqualTo(FirstClientSequence);
        await Assert.That(recovery.PendingOperations[0].Payload.PayloadHash).IsEqualTo(operation.Payload.PayloadHash);
        await Assert.That(recovery.Snapshot?.Revision).IsEqualTo(receipt.SnapshotRevision);
        await Assert.That(recovery.Snapshot?.State.PayloadHash).IsEqualTo(snapshotMutation.State.PayloadHash);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        await Assert.That(duplicate.OperationId).IsEqualTo(operationId);
        await Assert.That(duplicate.ClientSequence).IsEqualTo(receipt.ClientSequence);
        await Assert.That(duplicate.SnapshotRevision).IsEqualTo(receipt.SnapshotRevision);
        await Assert.That(duplicate.CommittedAtUtc).IsEqualTo(receipt.CommittedAtUtc);
        await Assert.That(afterDuplicate.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(afterDuplicate.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(afterDuplicate.Snapshot?.Revision).IsEqualTo(receipt.SnapshotRevision);
        await Assert.That((reopened.Capabilities & LocalStoreCapabilities.DurableLocalCommit) != 0).IsTrue();
    }

    /// <summary>Starts the owned child process that commits and waits to be killed.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">The child test process did not start.</exception>
    private static Process StartCrashReceiptChild(
        string databasePath,
        string signalPath,
        OperationId operationId,
        SubscriptionId subscriptionId)
    {
        var testAssembly = System.IO.Path.Combine(AppContext.BaseDirectory, TestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(ChildTestTreeNodeFilter);
        startInfo.ArgumentList.Add("--output");
        startInfo.ArgumentList.Add("Detailed");
        startInfo.Environment[CrashReceiptChildModeVariable] = CrashReceiptChildMode;
        startInfo.Environment[CrashReceiptDatabasePathVariable] = databasePath;
        startInfo.Environment[CrashReceiptSignalPathVariable] = signalPath;
        startInfo.Environment[CrashReceiptOperationIdVariable] = operationId.Value.ToString("D");
        startInfo.Environment[CrashReceiptSubscriptionIdVariable] = subscriptionId.Value.ToString("D");

        var child = Process.Start(startInfo);
        return child ?? throw new InvalidOperationException("The child test process did not start.");
    }

    /// <summary>Runs the child process until it publishes an acknowledged receipt signal, then kills the owned child tree.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or fails to publish a signal.</exception>
    private static async Task RunCrashReceiptChildUntilSignalAsync(
        string databasePath,
        string signalPath,
        OperationId operationId,
        SubscriptionId subscriptionId)
    {
        using var child = StartCrashReceiptChild(databasePath, signalPath, operationId, subscriptionId);
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        CrashReceiptChildOutput? output = null;
        try
        {
            var signaled = await WaitForSignalAsync(signalPath, child, SignalWaitTimeout);
            if (!signaled)
            {
                output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
                throw new InvalidOperationException(CreateSignalTimeoutMessage(output));
            }

            output = await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
        }
        finally
        {
            output ??= await StopAndDrainCrashReceiptChildAsync(child, standardOutput, standardError);
        }
    }

    /// <summary>Waits for the child signal file to appear.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="child">The child process.</param>
    /// <param name="timeout">The bounded wait timeout.</param>
    /// <returns>A value indicating whether the signal appeared.</returns>
    private static async Task<bool> WaitForSignalAsync(string signalPath, Process child, TimeSpan timeout)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        while (Stopwatch.GetElapsedTime(startTimestamp) < timeout && !child.HasExited)
        {
            if (File.Exists(signalPath))
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(SignalPollIntervalMilliseconds));
        }

        return File.Exists(signalPath);
    }

    /// <summary>Kills an owned child if needed, waits for exit, and drains redirected output.</summary>
    /// <param name="child">The child process.</param>
    /// <param name="standardOutput">The standard output read task.</param>
    /// <param name="standardError">The standard error read task.</param>
    /// <returns>The child process output.</returns>
    private static async Task<CrashReceiptChildOutput> StopAndDrainCrashReceiptChildAsync(
        Process child,
        Task<string> standardOutput,
        Task<string> standardError)
    {
        if (!child.HasExited)
        {
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(ChildExitTimeout);
        }

        return new(
            child.HasExited,
            await standardOutput.WaitAsync(GuardTimeout),
            await standardError.WaitAsync(GuardTimeout));
    }

    /// <summary>Creates a diagnostic timeout message from child process output.</summary>
    /// <param name="output">The child process output.</param>
    /// <returns>The timeout message.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateSignalTimeoutMessage(CrashReceiptChildOutput output) =>
        string.Join(
            Environment.NewLine,
            "The child process did not publish the acknowledged commit signal.",
            $"HasExited: {output.HasExited.ToString(CultureInfo.InvariantCulture)}",
            "StandardOutput:",
            output.StandardOutput,
            "StandardError:",
            output.StandardError);

    /// <summary>Reads the crash receipt signal.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <returns>The deserialized receipt.</returns>
    private static async Task<CrashReceiptSignal> ReadCrashReceiptAsync(string signalPath)
    {
        var text = await File.ReadAllTextAsync(signalPath);
        return CrashReceiptSignal.Parse(text);
    }

    /// <summary>Atomically publishes the child commit receipt signal.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="signal">The signal payload.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task PublishCrashReceiptSignalAsync(string signalPath, CrashReceiptSignal signal)
    {
        var temporaryPath = $"{signalPath}.{Environment.ProcessId}.tmp";
        await File.WriteAllTextAsync(temporaryPath, signal.ToSignalText());
        File.Move(temporaryPath, signalPath);
    }

    /// <summary>Reads child process settings from environment variables.</summary>
    /// <returns>The child context, or null during a normal test run.</returns>
    /// <exception cref="InvalidOperationException">The child crash receipt environment is incomplete.</exception>
    private static CrashReceiptChildContext? ReadCrashReceiptChildContext()
    {
        if (!string.Equals(Environment.GetEnvironmentVariable(CrashReceiptChildModeVariable), CrashReceiptChildMode, StringComparison.Ordinal))
        {
            return null;
        }

        var databasePath = Environment.GetEnvironmentVariable(CrashReceiptDatabasePathVariable);
        var signalPath = Environment.GetEnvironmentVariable(CrashReceiptSignalPathVariable);
        var operationText = Environment.GetEnvironmentVariable(CrashReceiptOperationIdVariable);
        var subscriptionText = Environment.GetEnvironmentVariable(CrashReceiptSubscriptionIdVariable);
        if (string.IsNullOrWhiteSpace(databasePath)
            || string.IsNullOrWhiteSpace(signalPath)
            || !Guid.TryParse(operationText, out var operationId)
            || !Guid.TryParse(subscriptionText, out var subscriptionId))
        {
            throw new InvalidOperationException("The child crash receipt environment is incomplete.");
        }

        return new(databasePath, signalPath, new(operationId), new(subscriptionId));
    }

    /// <summary>Creates the deterministic operation shared by the parent and child process.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateCrashReceiptOperation(OperationId operationId) =>
        CreateOperation(FirstClientSequence) with { OperationId = operationId };

    /// <summary>The child process crash receipt context.</summary>
    /// <param name="DatabasePath">The SQLite database path.</param>
    /// <param name="SignalPath">The atomic signal path.</param>
    /// <param name="OperationId">The operation identifier.</param>
    /// <param name="SubscriptionId">The subscription identifier.</param>
    private sealed record CrashReceiptChildContext(
        string DatabasePath,
        string SignalPath,
        OperationId OperationId,
        SubscriptionId SubscriptionId);

    /// <summary>The drained child process output.</summary>
    /// <param name="HasExited">A value indicating whether the child process exited.</param>
    /// <param name="StandardOutput">The child process standard output.</param>
    /// <param name="StandardError">The child process standard error.</param>
    private sealed record CrashReceiptChildOutput(bool HasExited, string StandardOutput, string StandardError);

    /// <summary>The acknowledged local commit receipt published by the child process.</summary>
    /// <param name="OperationId">The operation identifier.</param>
    /// <param name="SubscriptionId">The subscription identifier.</param>
    /// <param name="ClientSequence">The committed client sequence.</param>
    /// <param name="SnapshotRevision">The committed snapshot revision.</param>
    /// <param name="CommittedAtUtc">The commit timestamp.</param>
    private sealed record CrashReceiptSignal(
        Guid OperationId,
        Guid SubscriptionId,
        long ClientSequence,
        long SnapshotRevision,
        DateTimeOffset CommittedAtUtc)
    {
        /// <summary>Parses a crash receipt from signal text.</summary>
        /// <param name="text">The signal text.</param>
        /// <returns>The parsed signal.</returns>
        /// <exception cref="InvalidOperationException">The signal text is malformed.</exception>
        public static CrashReceiptSignal Parse(string text)
        {
            var lines = text.Split('\n', StringSplitOptions.TrimEntries);
            if (lines.Length != 5
                || !Guid.TryParse(lines[0], out var operationId)
                || !Guid.TryParse(lines[1], out var subscriptionId)
                || !long.TryParse(lines[2], NumberStyles.None, CultureInfo.InvariantCulture, out var clientSequence)
                || !long.TryParse(lines[3], NumberStyles.None, CultureInfo.InvariantCulture, out var snapshotRevision)
                || !DateTimeOffset.TryParse(lines[4], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var committedAtUtc))
            {
                throw new InvalidOperationException("The crash receipt signal is malformed.");
            }

            return new(operationId, subscriptionId, clientSequence, snapshotRevision, committedAtUtc);
        }

        /// <summary>Formats a crash receipt as invariant signal text.</summary>
        /// <returns>The signal text.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ToSignalText() =>
            string.Join(
                '\n',
                OperationId.ToString("D"),
                SubscriptionId.ToString("D"),
                ClientSequence.ToString(CultureInfo.InvariantCulture),
                SnapshotRevision.ToString(CultureInfo.InvariantCulture),
                CommittedAtUtc.ToString("O", CultureInfo.InvariantCulture));
    }
}
