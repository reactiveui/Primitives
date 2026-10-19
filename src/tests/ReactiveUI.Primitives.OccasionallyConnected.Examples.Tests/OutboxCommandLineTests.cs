// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using OccasionallyConnected.DurableOutbox;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests;

/// <summary>Tests for <see cref="OutboxCommandLine"/>.</summary>
public sealed class OutboxCommandLineTests
{
    /// <summary>The append command name.</summary>
    private const string AppendReadingCommandName = "append-reading";

    /// <summary>The database option name.</summary>
    private const string DatabaseOption = "--database";

    /// <summary>The device option name.</summary>
    private const string DeviceOption = "--device";

    /// <summary>The value option name.</summary>
    private const string ValueOption = "--value";

    /// <summary>The guarantee option name.</summary>
    private const string GuaranteeOption = "--guarantee";

    /// <summary>The operation option name.</summary>
    private const string OperationOption = "--operation";

    /// <summary>The outcome option name.</summary>
    private const string OutcomeOption = "--outcome";

    /// <summary>The simulate-attempt command name.</summary>
    private const string SimulateAttemptCommandName = "simulate-attempt";

    /// <summary>The status command name.</summary>
    private const string StatusCommandName = "status";

    /// <summary>The lost-response outcome name.</summary>
    private const string LostResponseOutcomeName = "lost-response";

    /// <summary>The subscribe command name.</summary>
    private const string SubscribeCommandName = "subscribe";

    /// <summary>The reused device identifier.</summary>
    private const string DeviceId = "device-a";

    /// <summary>The ignored database path used by parser-only tests.</summary>
    private const string IgnoredDatabase = "ignored.db";

    /// <summary>The finite reading value text used by parser-only tests.</summary>
    private const string FiniteReadingText = "22.0";

    /// <summary>The reading value used by command workflow tests.</summary>
    private const double CommandReading = 20.0;

    /// <summary>The invalid command exit code.</summary>
    private const int InvalidCommandExitCode = 2;

    /// <summary>The unsupported command name used by parser tests.</summary>
    private const string UnknownCommandName = "unknown-command";

    /// <summary>The maximum time allowed for the real process demo test.</summary>
    private const int ProcessTimeoutSeconds = 30;

    /// <summary>The maximum time allowed to stop and drain a canceled real process demo test.</summary>
    private const int ProcessCleanupTimeoutSeconds = 5;

    /// <summary>The sample app executable assembly name.</summary>
    private const string SampleAppAssemblyName = "ReactiveUI.Primitives.OccasionallyConnected.Examples.DurableOutbox.dll";

    /// <summary>Verifies the command-line runner reports usage for invalid input.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRequiredDatabaseIsMissing_ThenUsageExplainsTheCommand()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [AppendReadingCommandName, DeviceOption, DeviceId, ValueOption, FiniteReadingText],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("Usage:");
        await Assert.That(error.ToString()).Contains("append-reading --database <path> --device <id> --value <number>");
    }

    /// <summary>Verifies the guarantee command names supported and rejected delivery examples honestly.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenGuaranteesArePrinted_ThenAllThreeModesAreExplained()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(["guarantees"], output, error, CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(0);
        await Assert.That(output.ToString()).Contains("AtMostOnce: one durable local attempt barrier");
        await Assert.That(output.ToString()).Contains("AtLeastOnce: retained with the same OperationId");
        await Assert.That(output.ToString()).Contains("ExactlyOnce: rejected by this sample");
        await Assert.That(error.ToString()).IsEmpty();
    }

    /// <summary>Verifies commands without options reject trailing arguments.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenGuaranteesReceiveExtraArguments_ThenCommandShapeIsRejected()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(["guarantees", "--extra", "true"], output, error, CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("The guarantees command does not accept options.");
    }

    /// <summary>Verifies subscribe requires a bounded take count.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSubscribeTakeIsMissing_ThenUsageExplainsTheBound()
    {
        using var database = ExampleDatabase.Create();
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [SubscribeCommandName, DatabaseOption, database.Path],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("Missing required option: --take");
        await Assert.That(error.ToString()).Contains("subscribe --database <path> --take <n>");
    }

    /// <summary>Verifies commands reject unknown options before opening SQLite.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenUnknownOptionIsPassed_ThenCommandShapeIsRejected()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [
                AppendReadingCommandName,
                DatabaseOption,
                IgnoredDatabase,
                DeviceOption,
                DeviceId,
                ValueOption,
                FiniteReadingText,
                "--surprise",
                "true",
            ],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("Unknown option: --surprise");
    }

    /// <summary>Verifies duplicate options are rejected before the command touches the filesystem.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOptionIsDuplicated_ThenCommandShapeIsRejected()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [
                AppendReadingCommandName,
                DatabaseOption,
                IgnoredDatabase,
                DatabaseOption,
                "other.db",
                DeviceOption,
                DeviceId,
                ValueOption,
                FiniteReadingText,
            ],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("Duplicate option: --database");
    }

    /// <summary>Verifies missing option values are rejected instead of consuming the next option name.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOptionValueIsMissing_ThenCommandShapeIsRejected()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [AppendReadingCommandName, DatabaseOption, DeviceOption, DeviceId, ValueOption, FiniteReadingText],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("Missing value for option: --database");
    }

    /// <summary>Verifies non-finite readings are rejected before local persistence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenReadingValueIsNotFinite_ThenCommandShapeIsRejected()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [AppendReadingCommandName, DatabaseOption, IgnoredDatabase, DeviceOption, DeviceId, ValueOption, "NaN"],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("The reading value must be finite.");

        output = new();
        error = new();
        exitCode = await OutboxCommandLine.RunAsync(
            [AppendReadingCommandName, DatabaseOption, IgnoredDatabase, DeviceOption, DeviceId, ValueOption, "Infinity"],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(error.ToString()).Contains("The reading value must be finite.");
    }

    /// <summary>Verifies terminal receipt status can be inspected by operation id through the CLI.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenStatusReceivesOperationId_ThenTerminalReceiptIsPrinted()
    {
        using var database = ExampleDatabase.Create();
        var append = await DurableOutboxApplication.RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, CommandReading, DeliveryGuarantee.AtLeastOnce),
            CancellationToken.None);
        _ = await DurableOutboxApplication.RunAsync(
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted),
            CancellationToken.None);
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [StatusCommandName, DatabaseOption, database.Path, OperationOption, append.OperationId.Value.ToString()],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(0);
        await Assert.That(output.ToString()).Contains($"operation-status: operation={append.OperationId.Value}");
        await Assert.That(output.ToString()).Contains("state=Synchronized");
    }

    /// <summary>Verifies rejected terminal receipts remain inspectable by operation id through the CLI.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRejectedStatusReceivesOperationId_ThenTerminalReceiptIsPrinted()
    {
        using var database = ExampleDatabase.Create();
        var append = await DurableOutboxApplication.RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, CommandReading, DeliveryGuarantee.AtLeastOnce),
            CancellationToken.None);
        _ = await DurableOutboxApplication.RunAsync(
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Rejected),
            CancellationToken.None);
        var output = new StringWriter();
        var error = new StringWriter();

        var exitCode = await OutboxCommandLine.RunAsync(
            [StatusCommandName, DatabaseOption, database.Path, OperationOption, append.OperationId.Value.ToString()],
            output,
            error,
            CancellationToken.None);

        await Assert.That(exitCode).IsEqualTo(0);
        await Assert.That(output.ToString()).Contains($"operation-status: operation={append.OperationId.Value}");
        await Assert.That(output.ToString()).Contains("state=Rejected");
        await Assert.That(output.ToString()).Contains("reason=OC.SampleRejected");
        await Assert.That(error.ToString()).IsEmpty();
    }

    /// <summary>Verifies the CLI dispatches every durable inspection command shape.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInspectCommandsRunThroughCli_ThenEachViewPrintsRecoveredState()
    {
        using var database = ExampleDatabase.Create();
        _ = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            database.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText);

        var pending = await RunCliAsync("pending", DatabaseOption, database.Path);
        var snapshot = await RunCliAsync("snapshot", DatabaseOption, database.Path);
        var subscription = await RunCliAsync("subscription", DatabaseOption, database.Path);

        await Assert.That(pending.ExitCode).IsEqualTo(0);
        await Assert.That(pending.StandardOutput).Contains("pending-operation:");
        await Assert.That(snapshot.StandardOutput).Contains("reading-count: 1");
        await Assert.That(subscription.StandardOutput).Contains("subscription:");
    }

    /// <summary>Verifies the CLI dispatches bounded subscribe with a positive take count.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSubscribeRunsThroughCli_ThenBoundedSubscriptionEntriesArePrinted()
    {
        using var database = ExampleDatabase.Create();
        _ = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            database.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText);

        var result = await RunCliAsync(SubscribeCommandName, DatabaseOption, database.Path, "--take", "1");

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.StandardOutput).Contains("subscription-entry: 1");
    }

    /// <summary>Verifies the CLI can parse simulated accepted and rejected outcomes.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSimulateAttemptRunsThroughCli_ThenOutcomeNamesAreParsed()
    {
        using var acceptedDatabase = ExampleDatabase.Create();
        var acceptedAppend = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            acceptedDatabase.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText);
        using var rejectedDatabase = ExampleDatabase.Create();
        var rejectedAppend = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            rejectedDatabase.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText);

        var accepted = await RunCliAsync(
            SimulateAttemptCommandName,
            DatabaseOption,
            acceptedDatabase.Path,
            OperationOption,
            ExtractOperationId(acceptedAppend.StandardOutput).Value.ToString(),
            OutcomeOption,
            "accepted");
        var rejected = await RunCliAsync(
            SimulateAttemptCommandName,
            DatabaseOption,
            rejectedDatabase.Path,
            OperationOption,
            ExtractOperationId(rejectedAppend.StandardOutput).Value.ToString(),
            OutcomeOption,
            "rejected");

        await Assert.That(accepted.ExitCode).IsEqualTo(0);
        await Assert.That(accepted.StandardOutput).Contains("local simulation: Accepted");
        await Assert.That(rejected.ExitCode).IsEqualTo(0);
        await Assert.That(rejected.StandardOutput).Contains("local simulation: Rejected");
    }

    /// <summary>Verifies omitting an operation id selects the next pending operation through the CLI.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSimulateAttemptOmitsOperationId_ThenCliSelectsNextPendingOperation()
    {
        using var database = ExampleDatabase.Create();
        var earlierAppend = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            database.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText);
        var laterAppend = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            database.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            "23.0");
        var earlierOperation = ExtractOperationId(earlierAppend.StandardOutput);
        var laterOperation = ExtractOperationId(laterAppend.StandardOutput);

        var accepted = await RunCliAsync(
            SimulateAttemptCommandName,
            DatabaseOption,
            database.Path,
            OutcomeOption,
            "accepted");
        var laterStatus = await RunCliAsync(
            StatusCommandName,
            DatabaseOption,
            database.Path,
            OperationOption,
            laterOperation.Value.ToString());

        await Assert.That(accepted.ExitCode).IsEqualTo(0);
        await Assert.That(accepted.StandardOutput).Contains($"operation: {earlierOperation.Value}");
        await Assert.That(laterStatus.StandardOutput).Contains("state=QueuedForUpload");
    }

    /// <summary>Verifies invalid top-level command shapes print usage without touching durable state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenTopLevelCommandShapeIsInvalid_ThenUsageIsPrinted()
    {
        var missing = await RunCliAsync();
        var help = await RunCliAsync("--help");
        var unknown = await RunCliAsync(UnknownCommandName);
        var bareValue = await RunCliAsync(AppendReadingCommandName, "bare-value");

        await Assert.That(missing.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(missing.StandardError).Contains("A command is required.");
        await Assert.That(help.StandardError).Contains("Usage:");
        await Assert.That(unknown.StandardError).Contains($"Unknown command: {UnknownCommandName}");
        await Assert.That(bareValue.StandardError).Contains("Unexpected value: bare-value");
    }

    /// <summary>Verifies nonnumeric reading values are rejected before persistence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenReadingValueIsNotNumeric_ThenCommandShapeIsRejected()
    {
        var result = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            IgnoredDatabase,
            DeviceOption,
            DeviceId,
            ValueOption,
            "not-a-number");

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.StandardError).Contains("The reading value must be a number.");
    }

    /// <summary>Verifies non-positive subscribe counts are rejected before command execution.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSubscribeTakeIsNotPositive_ThenCommandShapeIsRejected()
    {
        using var database = ExampleDatabase.Create();

        var result = await RunCliAsync(SubscribeCommandName, DatabaseOption, database.Path, "--take", "0");

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.StandardError).Contains("The subscription take count must be a positive integer.");
    }

    /// <summary>Verifies unsupported guarantee and simulated outcome names are rejected.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenEnumNamesAreUnknown_ThenCommandShapeIsRejected()
    {
        var badGuarantee = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            IgnoredDatabase,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText,
            GuaranteeOption,
            "server-magic");
        var badOutcome = await RunCliAsync(
            SimulateAttemptCommandName,
            DatabaseOption,
            IgnoredDatabase,
            OutcomeOption,
            "teleported");

        await Assert.That(badGuarantee.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(badGuarantee.StandardError).Contains("Unknown delivery guarantee: server-magic");
        await Assert.That(badOutcome.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(badOutcome.StandardError).Contains("Unknown simulated outcome: teleported");
    }

    /// <summary>Verifies all delivery guarantee names are parsed through the CLI.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenGuaranteeNamesArePassed_ThenParserMapsThemToSampleSemantics()
    {
        using var atMostOnceDatabase = ExampleDatabase.Create();
        using var exactlyOnceDatabase = ExampleDatabase.Create();
        var atMostOnce = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            atMostOnceDatabase.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText,
            GuaranteeOption,
            "at-most-once");
        var exactlyOnce = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            exactlyOnceDatabase.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText,
            GuaranteeOption,
            "exactly-once");

        await Assert.That(atMostOnce.ExitCode).IsEqualTo(0);
        await Assert.That(atMostOnce.StandardOutput).Contains("state: QueuedForUpload");
        await Assert.That(exactlyOnce.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(exactlyOnce.StandardError).Contains("ExactlyOnce effect requires");
    }

    /// <summary>Verifies the CLI parses demo and default lost-response simulation commands.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDemoAndDefaultSimulationRunThroughCli_ThenDefaultBranchesAreUsed()
    {
        using var database = ExampleDatabase.Create();
        var append = await RunCliAsync(
            AppendReadingCommandName,
            DatabaseOption,
            database.Path,
            DeviceOption,
            DeviceId,
            ValueOption,
            FiniteReadingText);

        var demo = await RunCliAsync("--demo");
        var lost = await RunCliAsync(
            SimulateAttemptCommandName,
            DatabaseOption,
            database.Path,
            OperationOption,
            ExtractOperationId(append.StandardOutput).Value.ToString(),
            OutcomeOption,
            LostResponseOutcomeName);

        await Assert.That(demo.ExitCode).IsEqualTo(0);
        await Assert.That(demo.StandardOutput).Contains("demo database cleaned:");
        await Assert.That(lost.ExitCode).IsEqualTo(0);
        await Assert.That(lost.StandardOutput).Contains("local simulation: response lost");
    }

    /// <summary>Verifies the built sample process propagates the demo exit code and standard output.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDemoRunsAsProcess_ThenExitCodeAndOutputArePropagated()
    {
        var appPath = Path.Combine(AppContext.BaseDirectory, SampleAppAssemblyName);
        await Assert.That(File.Exists(appPath)).IsTrue();
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(appPath);
        startInfo.ArgumentList.Add("--demo");
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(ProcessTimeoutSeconds));
        var output = await RunProcessAsync(startInfo, timeout.Token);

        await Assert.That(output.ExitedNormally).IsTrue();
        await Assert.That(output.ExitCode).IsEqualTo(0);
        await Assert.That(output.StandardOutput).Contains("demo database cleaned:");
        await Assert.That(output.StandardOutput).Contains("owned directory removed: True");
        await Assert.That(output.StandardError).IsEmpty();
    }

    /// <summary>Runs command-line arguments and captures process-style output.</summary>
    /// <param name="args">The arguments to run.</param>
    /// <returns>The command result.</returns>
    private static async Task<OutboxCommandResult> RunCliAsync(params string[] args)
    {
        await using StringWriter output = new();
        await using StringWriter error = new();
        var exitCode = await OutboxCommandLine.RunAsync(args, output, error, CancellationToken.None);
        return new(exitCode, output.ToString(), error.ToString());
    }

    /// <summary>Runs the sample app process and captures its standard output streams.</summary>
    /// <param name="startInfo">The configured process start information.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The captured process output.</returns>
    /// <exception cref="InvalidOperationException">The process could not be started.</exception>
    /// <exception cref="OperationCanceledException">The process did not exit before cancellation.</exception>
    private static async Task<CapturedProcessOutput> RunProcessAsync(
        ProcessStartInfo startInfo,
        CancellationToken cancellationToken)
    {
        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("The sample process could not be started.");
        var standardOutput = ReadToEndAsync(process.StandardOutput);
        var standardError = ReadToEndAsync(process.StandardError);

        try
        {
#if NET11_0_OR_GREATER
            var exitStatus = await process.WaitForExitStatusAsync(cancellationToken).ConfigureAwait(false);
            if (exitStatus.Canceled)
            {
                throw new OperationCanceledException(cancellationToken);
            }

            return new(
                exitStatus.ExitCode,
                exitStatus.Signal is null,
                await standardOutput.ConfigureAwait(false),
                await standardError.ConfigureAwait(false));
#else
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
            return new(
                process.ExitCode,
                true,
                await standardOutput.ConfigureAwait(false),
                await standardError.ConfigureAwait(false));
#endif
        }
        catch (OperationCanceledException)
        {
            await StopAndDrainProcessAsync(process, standardOutput, standardError).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Stops an owned process after timeout and observes redirected output drains.</summary>
    /// <param name="process">The owned process.</param>
    /// <param name="standardOutput">The standard output drain task.</param>
    /// <param name="standardError">The standard error drain task.</param>
    /// <returns>A task that represents the asynchronous cleanup.</returns>
    private static async Task StopAndDrainProcessAsync(
        Process process,
        Task<string> standardOutput,
        Task<string> standardError)
    {
        var cleanupTimeout = TimeSpan.FromSeconds(ProcessCleanupTimeoutSeconds);
        if (!process.HasExited)
        {
            process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(CancellationToken.None).WaitAsync(cleanupTimeout, CancellationToken.None).ConfigureAwait(false);
        }

        _ = await standardOutput.WaitAsync(cleanupTimeout, CancellationToken.None).ConfigureAwait(false);
        _ = await standardError.WaitAsync(cleanupTimeout, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Reads a redirected text stream without linking it to the process timeout token.</summary>
    /// <param name="reader">The text reader to drain.</param>
    /// <returns>The drained text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<string> ReadToEndAsync(TextReader reader) =>
        reader.ReadToEndAsync(CancellationToken.None);

    /// <summary>Extracts the printed operation id from command output.</summary>
    /// <param name="output">The command output.</param>
    /// <returns>The operation id.</returns>
    private static OperationId ExtractOperationId(string output)
    {
        var operationLine = output.Split(Environment.NewLine)[0];
        var value = operationLine["operation: ".Length..];
        return new(Guid.Parse(value));
    }

    /// <summary>Captured sample process output.</summary>
    /// <param name="ExitCode">The process exit code.</param>
    /// <param name="ExitedNormally">A value indicating whether the process exited normally.</param>
    /// <param name="StandardOutput">The captured standard output text.</param>
    /// <param name="StandardError">The captured standard error text.</param>
    private readonly record struct CapturedProcessOutput(
        int ExitCode,
        bool ExitedNormally,
        string StandardOutput,
        string StandardError);
}
