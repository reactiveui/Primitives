// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Crash matrix tests that stop a child server writer at named <see cref="SqliteServerCommitCheckpoint"/> values.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>The child crash matrix environment variable that carries the encoded case.</summary>
    private const string ServerCrashMatrixCaseVariable = "RXUI_SERVER_SQLITE_CRASH_MATRIX_CASE";

    /// <summary>The number of encoded crash matrix case fields.</summary>
    private const int ServerCrashMatrixCaseFieldCount = 3;

    /// <summary>The child crash matrix test tree node filter.</summary>
    private const string ServerCrashMatrixChildTreeNodeFilter = $"/*/*/*/{nameof(ServerCrashMatrixChildReachesCheckpointAndWaits)}";

    /// <summary>Verifies a server writer killed at a named journal checkpoint applies a client resend exactly once.</summary>
    /// <param name="checkpoint">The <see cref="SqliteServerCommitCheckpoint"/> name where the child blocks.</param>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    [Test]
    [NotInParallel("sqlite-server-crash-matrix")]
    [Arguments(nameof(SqliteServerCommitCheckpoint.TryCommitBeforeCommit))]
    [Arguments(nameof(SqliteServerCommitCheckpoint.TryCommitAfterCommit))]
    public async Task WhenServerWriterDiesAtCommitCheckpoint_ThenResendAppliesExactlyOnce(string checkpoint)
    {
        using var database = new TemporaryDatabase();
        var signalPath = System.IO.Path.ChangeExtension(database.Path, $"{checkpoint}-{Guid.NewGuid():N}.signal");
        using (var initialized = CreateJournal(database.Path))
        {
            await Assert.That(initialized.StreamCount).IsEqualTo(0);
        }

        var childOutput = await RunServerCrashMatrixChildAsync(string.Join('\n', checkpoint, database.Path, signalPath), signalPath);
        await Assert.That(await File.ReadAllTextAsync(signalPath)).IsEqualTo(checkpoint);

        var key = OperationKey(FirstOperationSeed);
        using var reopened = ReopenJournalAfterCrash(database.Path, childOutput);
        var committedBeforeCrash = checkpoint == nameof(SqliteServerCommitCheckpoint.TryCommitAfterCommit);
        var expectedCount = committedBeforeCrash ? SingleEntryCount : 0;
        var afterCrash = reopened.Read(StreamKey(), [key]);
        await Assert.That(reopened.LedgerEntryCount).IsEqualTo(expectedCount);
        await Assert.That(reopened.EventCount).IsEqualTo(expectedCount);
        await Assert.That(afterCrash.Revision).IsEqualTo(expectedCount);
        await Assert.That(afterCrash.Entries).Count().IsEqualTo(expectedCount);

        var resend = reopened.TryCommit(CreateServerCrashMatrixPlan());
        var duplicate = reopened.TryCommit(CreateServerCrashMatrixPlan());
        var afterResend = reopened.Read(StreamKey(), [key]);

        await Assert.That(resend.Status).IsEqualTo(committedBeforeCrash ? ServerCommitStatus.StaleRevision : ServerCommitStatus.Committed);
        await Assert.That(duplicate.Status).IsEqualTo(ServerCommitStatus.StaleRevision);
        await Assert.That(reopened.LedgerEntryCount).IsEqualTo(SingleEntryCount);
        await Assert.That(reopened.EventCount).IsEqualTo(SingleEntryCount);
        await Assert.That(afterResend.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(afterResend.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(afterResend.Entries[0].OperationKey).IsEqualTo(key);
    }

    /// <summary>Child workflow used by the server crash matrix parent tests.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The child environment is malformed or the checkpoint was not reached.</exception>
    [Test]
    public async Task ServerCrashMatrixChildReachesCheckpointAndWaits()
    {
        var encoded = Environment.GetEnvironmentVariable(ServerCrashMatrixCaseVariable);
        if (encoded is null)
        {
            await Assert.That(encoded).IsNull();
            return;
        }

        var fields = encoded.Split('\n');
        if (fields.Length != ServerCrashMatrixCaseFieldCount
            || !Enum.TryParse<SqliteServerCommitCheckpoint>(fields[0], ignoreCase: false, out var checkpoint))
        {
            throw new InvalidOperationException("The server crash matrix environment is malformed.");
        }

        using var journal = new SqliteServerCommitJournal(
            fields[1],
            CreateServerCrashMatrixOptions(),
            new BlockingServerCommitFaultPoint(checkpoint, fields[2]));
        _ = journal.TryCommit(CreateServerCrashMatrixPlan());
        throw new InvalidOperationException($"The server crash matrix child finished without reaching {checkpoint}.");
    }

    /// <summary>Creates the deterministic plan sent by the parent and the child.</summary>
    /// <returns>The commit plan.</returns>
    private static ServerCommitPlan CreateServerCrashMatrixPlan()
    {
        var key = OperationKey(FirstOperationSeed);
        return Plan(0, State(FirstVersion), Stamp(key), Entry(key, OperationResultKind.Accepted, FirstOperationSeed));
    }

    /// <summary>Creates journal options identical to <see cref="CreateJournal"/> defaults.</summary>
    /// <returns>The journal options.</returns>
    private static ServerCommitJournalOptions CreateServerCrashMatrixOptions() => new()
    {
        MaximumStreams = DefaultMaximumStreams,
        MaximumLedgerEntries = DefaultMaximumLedgerEntries,
        MaximumEvents = DefaultMaximumEvents,
        MaximumLogicalBytes = DefaultMaximumLogicalBytes,
        OperationRetention = TimeSpan.FromMinutes(DefaultRetentionMinutes),
        TimeProvider = new ManualTimeProvider(Start),
    };

    /// <summary>Starts the crash matrix child, waits for its signal, and kills it.</summary>
    /// <param name="encodedCase">The encoded child case.</param>
    /// <param name="signalPath">The signal path.</param>
    /// <returns>The drained child output.</returns>
    /// <exception cref="InvalidOperationException">The child did not start or signal.</exception>
    private static async Task<CrashChildOutput> RunServerCrashMatrixChildAsync(string encodedCase, string signalPath)
    {
        var testAssembly = System.IO.Path.Combine(AppContext.BaseDirectory, TestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(ServerCrashMatrixChildTreeNodeFilter);
        startInfo.Environment[ServerCrashMatrixCaseVariable] = encodedCase;
        using var child = Process.Start(startInfo) ?? throw new InvalidOperationException("The server crash matrix child did not start.");
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        var signaled = await WaitForSignalAsync(signalPath, child, SignalWaitTimeout);
        var output = await StopAndDrainCrashChildAsync(child, standardOutput, standardError);
        return signaled ? output : throw new InvalidOperationException(CreateSignalTimeoutMessage(output));
    }

    /// <summary>Blocks the journal writer at one named checkpoint after publishing an atomic signal file.</summary>
    /// <param name="target">The checkpoint that blocks.</param>
    /// <param name="signalPath">The signal file path.</param>
    private sealed class BlockingServerCommitFaultPoint(SqliteServerCommitCheckpoint target, string signalPath) : ISqliteServerCommitFaultPoint
    {
        /// <inheritdoc/>
        public void Reached(SqliteServerCommitCheckpoint checkpoint)
        {
            if (checkpoint != target)
            {
                return;
            }

            var temporaryPath = $"{signalPath}.{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}.tmp";
            File.WriteAllText(temporaryPath, checkpoint.ToString());
            File.Move(temporaryPath, signalPath);
            using var never = new ManualResetEventSlim(false);
            never.Wait();
        }
    }
}
