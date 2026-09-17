// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Durability and corruption tests for <see cref="SqliteServerCommitJournal"/>.</summary>
public sealed partial class SqliteServerCommitJournalTests
{
    /// <summary>Verifies separate SQLite instances serialize concurrent compare-and-swap attempts.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConcurrentCompetingInstancesCommitOnlyOnePreparedRevision()
    {
        using var database = new TemporaryDatabase();
        using (var initialized = CreateJournal(database.Path))
        {
            await Assert.That(initialized.StreamCount).IsEqualTo(0);
        }

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = 0;
        var firstKey = OperationKey(FirstOperationSeed);
        var secondKey = OperationKey(SecondOperationSeed);
        var firstPlan = Plan(
            0,
            State(FirstVersion),
            Stamp(firstKey),
            Entry(firstKey, OperationResultKind.Accepted, FirstOperationSeed));
        var secondPlan = Plan(
            0,
            State(SecondVersion),
            Stamp(secondKey),
            Entry(secondKey, OperationResultKind.Accepted, SecondOperationSeed));
        var firstTask = RunReleasedCommitAsync(database.Path, firstPlan, release.Task, () => Interlocked.Increment(ref ready));
        var secondTask = RunReleasedCommitAsync(database.Path, secondPlan, release.Task, () => Interlocked.Increment(ref ready));

        while (Volatile.Read(ref ready) < DoubleEntryCount)
        {
            await Task.Delay(ReadyPollIntervalMilliseconds);
        }

        release.SetResult();
        var results = await Task.WhenAll(firstTask, secondTask);
        using var reader = CreateJournal(database.Path);
        var snapshot = reader.Read(StreamKey(), [firstKey, secondKey]);

        await Assert.That(results.Count(static result => result.Status == ServerCommitStatus.Committed)).IsEqualTo(SingleEntryCount);
        await Assert.That(results.Count(static result => result.Status == ServerCommitStatus.StaleRevision)).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(snapshot.Entries[0].Result.Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Verifies one durable commit can retain a zero-event acceptance and a multi-event replay.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReopenReconstructsZeroEventAcceptanceAndMultiEventLedger()
    {
        using var database = new TemporaryDatabase();
        var zeroEventKey = OperationKey(FirstOperationSeed);
        var multiEventKey = OperationKey(SecondOperationSeed);
        var secondEvent = Event(multiEventKey.OperationId, SecondCursor, "event-b");
        var thirdEvent = Event(multiEventKey.OperationId, ThirdCursor, "event-c");
        using (var journal = CreateJournal(database.Path))
        {
            var result = journal.TryCommit(new(
                StreamKey(),
                0,
                State(SecondVersion),
                Stamp(multiEventKey),
                [
                    Entry(zeroEventKey, OperationResultKind.Accepted, FirstOperationSeed, events: []),
                    Entry(multiEventKey, OperationResultKind.Accepted, SecondOperationSeed, events: [secondEvent, thirdEvent]),
                ]));
            await Assert.That(result.Status).IsEqualTo(ServerCommitStatus.Committed);
        }

        using var reopened = CreateJournal(database.Path);
        var replay = reopened.Read(StreamKey(), [zeroEventKey, multiEventKey]);

        await Assert.That(reopened.LedgerEntryCount).IsEqualTo(DoubleEntryCount);
        await Assert.That(reopened.EventCount).IsEqualTo(DoubleEntryCount);
        await Assert.That(replay.Revision).IsEqualTo(SingleEntryCount);
        await Assert.That(replay.LastCursor).IsEqualTo(ThirdCursor);
        await Assert.That(replay.LastEventSequence).IsEqualTo(DoubleEntryCount);
        await Assert.That(replay.Entries).Count().IsEqualTo(DoubleEntryCount);
        await Assert.That(replay.Entries[0].Events).Count().IsEqualTo(0);
        await Assert.That(replay.Entries[1].Events).Count().IsEqualTo(DoubleEntryCount);
        await Assert.That(replay.Entries[1].Events[0].ServerCursor).IsEqualTo(SecondCursor);
        await Assert.That(replay.Entries[1].Events[1].ServerCursor).IsEqualTo(ThirdCursor);
    }

    /// <summary>Verifies tenant, stream and client boundaries do not share terminal replay rows.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReopenKeepsTenantStreamAndClientReplayIsolated()
    {
        using var database = new TemporaryDatabase();
        var operationId = OperationKey(FirstOperationSeed).OperationId;
        var tenantKey = new ServerStreamKey("tenant-b", Stream);
        var streamKey = new ServerStreamKey(Tenant, new("stream-b"));
        var clientKey = new ServerOperationKey(OtherClient, operationId);
        var defaultKey = new ServerOperationKey(Client, operationId);
        using (var journal = CreateJournal(database.Path))
        {
            _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(defaultKey), Entry(defaultKey, OperationResultKind.Accepted, FirstOperationSeed)));
            _ = journal.TryCommit(new(
                tenantKey,
                0,
                new(Stream, SecondVersion, Payload(SecondVersion)),
                Stamp(clientKey),
                [Entry(clientKey, OperationResultKind.Accepted, SecondOperationSeed, events: [])]));
            _ = journal.TryCommit(new(
                streamKey,
                0,
                new(streamKey.StreamId, ThirdCursor, Payload(ThirdCursor)),
                Stamp(clientKey),
                [Entry(clientKey, OperationResultKind.Accepted, ThirdOperationSeed, events: [])]));
        }

        using var reopened = CreateJournal(database.Path);
        var defaultReplay = reopened.Read(StreamKey(), [defaultKey, clientKey]);
        var tenantReplay = reopened.Read(tenantKey, [defaultKey, clientKey]);
        var streamReplay = reopened.Read(streamKey, [defaultKey, clientKey]);

        await Assert.That(defaultReplay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(defaultReplay.Entries[0].OperationKey).IsEqualTo(defaultKey);
        await Assert.That(tenantReplay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(tenantReplay.Entries[0].OperationKey).IsEqualTo(clientKey);
        await Assert.That(streamReplay.Entries).Count().IsEqualTo(SingleEntryCount);
        await Assert.That(streamReplay.Entries[0].OperationKey).IsEqualTo(clientKey);
        await Assert.That(reopened.StreamCount).IsEqualTo(ThirdOperationSeed);
        await Assert.That(reopened.LedgerEntryCount).IsEqualTo(ThirdOperationSeed);
    }

    /// <summary>Runs a commit after all concurrent callers are released.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="plan">The prepared commit plan.</param>
    /// <param name="release">The release signal.</param>
    /// <param name="markReady">Marks the task as ready.</param>
    /// <returns>The commit task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<ServerCommitResult> RunReleasedCommitAsync(
        string path,
        ServerCommitPlan plan,
        Task release,
        Action markReady) =>
        Task.Run(async () =>
        {
            markReady();
            await release.ConfigureAwait(false);
            using var journal = CreateJournal(path);
            return journal.TryCommit(plan);
        });

    /// <summary>Seeds one durable replay row.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The seeded operation key.</returns>
    /// <exception cref="InvalidOperationException">The seed commit was not accepted.</exception>
    private static ServerOperationKey SeedReplayRow(string path)
    {
        var key = OperationKey(FirstOperationSeed);
        var conflict = new ResolvedConflict(key.OperationId, "merge", Payload("resolved"));
        using var journal = CreateJournal(path);
        var result = journal.TryCommit(Plan(
            0,
            State(FirstVersion),
            Stamp(key),
            Entry(key, OperationResultKind.Conflict, FirstOperationSeed, [conflict], [Event(key.OperationId, FirstCursor, EventPayload)])));
        if (result.Status != ServerCommitStatus.Committed)
        {
            throw new InvalidOperationException("The corruption seed commit was not accepted.");
        }

        return key;
    }

    /// <summary>Creates replay row corruptions that should fail during reconstruction.</summary>
    /// <returns>The corruption writers.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReplayCorruption[] CreateReplayCorruptions() =>
        Enum.GetValues<ReplayCorruption>();

    /// <summary>Creates metric row corruptions that should fail during retained counter reads.</summary>
    /// <returns>The corruption writers.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MetricCorruption[] CreateMetricCorruptions() =>
        Enum.GetValues<MetricCorruption>();

    /// <summary>Applies one replay corruption.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="corruption">The corruption to apply.</param>
    /// <exception cref="InvalidOperationException">The corruption value is unsupported.</exception>
    private static void ApplyReplayCorruption(string path, ReplayCorruption corruption)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        DisableForeignKeys(command);
        SetReplayCorruptionCommand(command, corruption);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Sets one replay corruption command.</summary>
    /// <param name="command">The command.</param>
    /// <param name="corruption">The corruption to apply.</param>
    /// <exception cref="InvalidOperationException">The corruption value is unsupported.</exception>
    private static void SetReplayCorruptionCommand(SqliteCommand command, ReplayCorruption corruption)
    {
        if (corruption <= ReplayCorruption.BlobClientId)
        {
            SetLedgerCorruptionCommand(command, corruption);
            return;
        }

        if (corruption <= ReplayCorruption.PartialConflictPayload)
        {
            SetConflictCorruptionCommand(command, corruption);
            return;
        }

        if (corruption <= ReplayCorruption.PartialEventOrigin)
        {
            SetEventCorruptionCommand(command, corruption);
            return;
        }

        SetStreamCorruptionCommand(command, corruption);
    }

    /// <summary>Sets one ledger-row corruption command.</summary>
    /// <param name="command">The command.</param>
    /// <param name="corruption">The corruption to apply.</param>
    /// <exception cref="InvalidOperationException">The corruption value is unsupported.</exception>
    private static void SetLedgerCorruptionCommand(SqliteCommand command, ReplayCorruption corruption)
    {
        switch (corruption)
        {
            case ReplayCorruption.InvalidResultKind:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET result_kind = 99;";
                break;
            }

            case ReplayCorruption.FractionalResultKind:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET result_kind = 0.5;";
                break;
            }

            case ReplayCorruption.TextResultKind:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET result_kind = 'accepted';";
                break;
            }

            case ReplayCorruption.OutOfRangeResultKind:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET result_kind = 9223372036854775807;";
                break;
            }

            case ReplayCorruption.ShortFingerprint:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET fingerprint = zeroblob(1);";
                break;
            }

            case ReplayCorruption.InvalidCommitTimestamp:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET committed_at_utc = 'not-a-date';";
                break;
            }

            case ReplayCorruption.EmptyOperationId:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET operation_id = '00000000-0000-0000-0000-000000000000';";
                break;
            }

            case ReplayCorruption.BlankClientId:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET client_id = ' ';";
                break;
            }

            case ReplayCorruption.BlobClientId:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET client_id = x'313233';";
                break;
            }

            default:
            {
                throw new InvalidOperationException("The ledger corruption is unknown.");
            }
        }
    }

    /// <summary>Sets one conflict-row corruption command.</summary>
    /// <param name="command">The command.</param>
    /// <param name="corruption">The corruption to apply.</param>
    /// <exception cref="InvalidOperationException">The corruption value is unsupported.</exception>
    private static void SetConflictCorruptionCommand(SqliteCommand command, ReplayCorruption corruption)
    {
        switch (corruption)
        {
            case ReplayCorruption.BlankConflictResolution:
            {
                command.CommandText = "UPDATE oc_server_journal_conflicts SET resolution_code = ' ';";
                break;
            }

            case ReplayCorruption.PartialConflictPayload:
            {
                command.CommandText = "UPDATE oc_server_journal_conflicts SET resolved_payload_contract_id = NULL;";
                break;
            }

            default:
            {
                throw new InvalidOperationException("The conflict corruption is unknown.");
            }
        }
    }

    /// <summary>Sets one event-row corruption command.</summary>
    /// <param name="command">The command.</param>
    /// <param name="corruption">The corruption to apply.</param>
    /// <exception cref="InvalidOperationException">The corruption value is unsupported.</exception>
    private static void SetEventCorruptionCommand(SqliteCommand command, ReplayCorruption corruption)
    {
        switch (corruption)
        {
            case ReplayCorruption.EmptyEventId:
            {
                command.CommandText = "UPDATE oc_server_journal_events SET event_id = '00000000-0000-0000-0000-000000000000';";
                break;
            }

            case ReplayCorruption.BlankEventCursor:
            {
                command.CommandText = "UPDATE oc_server_journal_events SET server_cursor = ' ';";
                break;
            }

            case ReplayCorruption.InvalidEventPayloadSchema:
            {
                command.CommandText = "UPDATE oc_server_journal_events SET payload_schema_version = 0;";
                break;
            }

            case ReplayCorruption.FractionalEventPayloadSchema:
            {
                command.CommandText = "UPDATE oc_server_journal_events SET payload_schema_version = 0.5;";
                break;
            }

            case ReplayCorruption.TextEventPayloadSchema:
            {
                command.CommandText = "UPDATE oc_server_journal_events SET payload_schema_version = 'one';";
                break;
            }

            case ReplayCorruption.NonBlobEventPayload:
            {
                command.CommandText = "UPDATE oc_server_journal_events SET payload = 'not-a-blob';";
                break;
            }

            case ReplayCorruption.PartialEventOrigin:
            {
                command.CommandText = "UPDATE oc_server_journal_events SET origin_client_id = NULL;";
                break;
            }

            default:
            {
                throw new InvalidOperationException("The event corruption is unknown.");
            }
        }
    }

    /// <summary>Sets one stream-row corruption command.</summary>
    /// <param name="command">The command.</param>
    /// <param name="corruption">The corruption to apply.</param>
    /// <exception cref="InvalidOperationException">The corruption value is unsupported.</exception>
    private static void SetStreamCorruptionCommand(SqliteCommand command, ReplayCorruption corruption)
    {
        switch (corruption)
        {
            case ReplayCorruption.PartialStreamState:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET state_version = NULL;";
                break;
            }

            case ReplayCorruption.BlankStreamCursor:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET last_cursor = ' ';";
                break;
            }

            case ReplayCorruption.PartialWriteStamp:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET write_stamp_committed_at_utc = NULL;";
                break;
            }

            default:
            {
                throw new InvalidOperationException("The stream corruption is unknown.");
            }
        }
    }

    /// <summary>Applies one metric corruption.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="corruption">The corruption to apply.</param>
    /// <exception cref="InvalidOperationException">The corruption value is unsupported.</exception>
    private static void ApplyMetricCorruption(string path, MetricCorruption corruption)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        DisableForeignKeys(command);
        switch (corruption)
        {
            case MetricCorruption.BlankTenantId:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET tenant_id = ' ';";
                break;
            }

            case MetricCorruption.InvalidStreamId:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET stream_id = '../bad';";
                break;
            }

            case MetricCorruption.NegativeStateBytes:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET state_bytes = -1;";
                break;
            }

            case MetricCorruption.FractionalStateBytes:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET state_bytes = 0.5;";
                break;
            }

            case MetricCorruption.TextStateBytes:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET state_bytes = 'zero';";
                break;
            }

            case MetricCorruption.NegativeCursorBytes:
            {
                command.CommandText = "UPDATE oc_server_journal_streams SET last_cursor_bytes = -1;";
                break;
            }

            case MetricCorruption.NegativeLedgerBytes:
            {
                command.CommandText = "UPDATE oc_server_journal_ledger SET logical_bytes = -1;";
                break;
            }

            default:
            {
                throw new InvalidOperationException("The metric corruption is unknown.");
            }
        }

        _ = command.ExecuteNonQuery();
    }

    /// <summary>Allows raw tests to create corrupted parent-key rows.</summary>
    /// <param name="command">The command.</param>
    private static void DisableForeignKeys(SqliteCommand command)
    {
        command.CommandText = "PRAGMA foreign_keys = OFF;";
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Starts raw writes inside an uncommitted transaction and keeps the process alive.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The held uncommitted write.</returns>
    private static UncommittedRawWrite BeginUncommittedRawWrite(string path)
    {
        var connection = OpenRawConnection(path);
        var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_streams
                (tenant_id, stream_id, revision, state_version, state_payload_contract_id, state_payload_schema_version,
                 state_payload_content_type, state_payload, state_payload_hash, write_stamp_committed_at_utc, write_stamp_client_id,
                 write_stamp_operation_id, last_cursor, last_event_sequence, state_bytes, last_cursor_bytes,
                 last_group_sequence, receive_history_incomplete)
            VALUES
                ('tenant', 'stream', 99, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0);
            """;
        _ = command.ExecuteNonQuery();
        return new(connection, transaction);
    }

    /// <summary>Reopens the crashed writer's journal with native SQLite failure details.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The recovered journal.</returns>
    /// <exception cref="InvalidOperationException">SQLite could not reopen the journal after the child exited.</exception>
    private static SqliteServerCommitJournal ReopenJournalAfterCrash(string databasePath)
    {
        try
        {
            return CreateJournal(databasePath);
        }
        catch (SqliteException exception)
        {
            throw new InvalidOperationException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"Crash recovery failed: SQLite code {exception.SqliteErrorCode}, "
                    + $"extended code {exception.SqliteExtendedErrorCode}, database '{databasePath}'."),
                exception);
        }
    }

    /// <summary>Runs a crash child until it publishes its signal, then kills it.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="mode">The child mode.</param>
    /// <returns>The asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The child process did not publish a valid signal.</exception>
    private static async Task RunCrashChildUntilSignalAsync(string databasePath, string signalPath, Guid operationId, string mode)
    {
        using var child = StartCrashChild(databasePath, signalPath, operationId, mode);
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        CrashChildOutput? output = null;
        try
        {
            var signaled = await WaitForSignalAsync(signalPath, child, SignalWaitTimeout);
            if (!signaled)
            {
                output = await StopAndDrainCrashChildAsync(child, standardOutput, standardError);
                throw new InvalidOperationException(CreateSignalTimeoutMessage(output));
            }

            output = await StopAndDrainCrashChildAsync(child, standardOutput, standardError);
        }
        finally
        {
            output ??= await StopAndDrainCrashChildAsync(child, standardOutput, standardError);
        }
    }

    /// <summary>Starts the owned child process.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="mode">The child mode.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">The child process did not start.</exception>
    private static Process StartCrashChild(string databasePath, string signalPath, Guid operationId, string mode)
    {
        var testAssembly = System.IO.Path.Combine(AppContext.BaseDirectory, TestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(ChildTestTreeNodeFilter);
        startInfo.ArgumentList.Add("--output");
        startInfo.ArgumentList.Add("Detailed");
        startInfo.Environment[CrashChildModeVariable] = mode;
        startInfo.Environment[CrashDatabasePathVariable] = databasePath;
        startInfo.Environment[CrashSignalPathVariable] = signalPath;
        startInfo.Environment[CrashOperationIdVariable] = operationId.ToString("D");

        var child = Process.Start(startInfo);
        return child ?? throw new InvalidOperationException("The child test process did not start.");
    }

    /// <summary>Waits for a child signal file.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="child">The child process.</param>
    /// <param name="timeout">The timeout.</param>
    /// <returns>Whether the signal appeared.</returns>
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
    /// <param name="standardOutput">The standard output task.</param>
    /// <param name="standardError">The standard error task.</param>
    /// <returns>The child process output.</returns>
    private static async Task<CrashChildOutput> StopAndDrainCrashChildAsync(Process child, Task<string> standardOutput, Task<string> standardError)
    {
        if (!child.HasExited)
        {
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(ChildExitTimeout);
        }

        return new(child.HasExited, await standardOutput.WaitAsync(ChildExitTimeout), await standardError.WaitAsync(ChildExitTimeout));
    }

    /// <summary>Creates a diagnostic timeout message from child process output.</summary>
    /// <param name="output">The child process output.</param>
    /// <returns>The timeout message.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateSignalTimeoutMessage(CrashChildOutput output) =>
        string.Join(
            Environment.NewLine,
            "The child process did not publish the server journal signal.",
            $"HasExited: {output.HasExited.ToString(CultureInfo.InvariantCulture)}",
            "StandardOutput:",
            output.StandardOutput,
            "StandardError:",
            output.StandardError);

    /// <summary>Atomically publishes the child signal.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async Task PublishCrashSignalAsync(string signalPath, Guid operationId)
    {
        var temporaryPath = $"{signalPath}.{Environment.ProcessId}.tmp";
        await File.WriteAllTextAsync(temporaryPath, operationId.ToString("D"));
        File.Move(temporaryPath, signalPath);
    }

    /// <summary>Reads child process settings from environment variables.</summary>
    /// <returns>The child context, or null during a normal test run.</returns>
    /// <exception cref="InvalidOperationException">The child crash environment is incomplete.</exception>
    private static CrashChildContext? ReadCrashChildContext()
    {
        var mode = Environment.GetEnvironmentVariable(CrashChildModeVariable);
        if (mode is null)
        {
            return null;
        }

        var databasePath = Environment.GetEnvironmentVariable(CrashDatabasePathVariable);
        var signalPath = Environment.GetEnvironmentVariable(CrashSignalPathVariable);
        var operationText = Environment.GetEnvironmentVariable(CrashOperationIdVariable);
        if ((mode != CrashAfterCommitMode && mode != CrashBeforeCommitMode)
            || string.IsNullOrWhiteSpace(databasePath)
            || string.IsNullOrWhiteSpace(signalPath)
            || !Guid.TryParse(operationText, out var operationId))
        {
            throw new InvalidOperationException("The child crash environment is incomplete.");
        }

        return new(mode, databasePath, signalPath, operationId);
    }

    /// <summary>Retains an uncommitted SQLite transaction until the child process is killed.</summary>
    private sealed class UncommittedRawWrite : IDisposable
    {
        /// <summary>The held connection.</summary>
        private readonly SqliteConnection _connection;

        /// <summary>The held transaction.</summary>
        private readonly SqliteTransaction _transaction;

        /// <summary>Initializes a new instance of the <see cref="UncommittedRawWrite"/> class.</summary>
        /// <param name="connection">The held connection.</param>
        /// <param name="transaction">The held transaction.</param>
        internal UncommittedRawWrite(SqliteConnection connection, SqliteTransaction transaction)
        {
            _connection = connection;
            _transaction = transaction;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _transaction.Dispose();
            _connection.Dispose();
        }
    }

    /// <summary>The child process crash context.</summary>
    /// <param name="Mode">The child mode.</param>
    /// <param name="DatabasePath">The SQLite database path.</param>
    /// <param name="SignalPath">The atomic signal path.</param>
    /// <param name="OperationId">The operation identifier.</param>
    private sealed record CrashChildContext(string Mode, string DatabasePath, string SignalPath, Guid OperationId);

    /// <summary>The drained child process output.</summary>
    /// <param name="HasExited">A value indicating whether the child exited.</param>
    /// <param name="StandardOutput">The standard output.</param>
    /// <param name="StandardError">The standard error.</param>
    private sealed record CrashChildOutput(bool HasExited, string StandardOutput, string StandardError);
}
