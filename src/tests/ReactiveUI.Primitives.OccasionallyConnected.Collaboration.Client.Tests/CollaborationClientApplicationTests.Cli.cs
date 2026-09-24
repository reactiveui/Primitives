// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Builder;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>CLI observability tests for <see cref="CollaborationClientApplication"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>Text that must not appear in sanitized diagnostic output.</summary>
    private const string RawExceptionMarker = "Exception";

    /// <summary>The redacted error prefix for invalid command arguments.</summary>
    private const string InvalidArgumentsOutput = "error: InvalidArguments";

    /// <summary>An enum value that does not name a client command.</summary>
    private const int UnknownCommandKind = 42;

    /// <summary>The exit code for a canceled command.</summary>
    private const int CanceledCommandExitCode = 2;

    /// <summary>The publish command-line verb.</summary>
    private const string PublishCommandName = "publish";

    /// <summary>The server command-line option.</summary>
    private const string ServerOptionName = "--server";

    /// <summary>The database command-line option.</summary>
    private const string DatabaseOptionName = "--database";

    /// <summary>The token command-line option.</summary>
    private const string TokenOptionName = "--token";

    /// <summary>The client command-line option.</summary>
    private const string ClientOptionName = "--client";

    /// <summary>Verifies online publish output includes bounded sync and operation diagnostics.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncPublishPrintsSyncOperationAndFaultSummary()
    {
        using var lease = new CollaborationClientDatabaseLease();
        var app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
        try
        {
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            await using var output = new StringWriter(CultureInfo.InvariantCulture);
            var command = CreatePublishCommand(
                boundUri,
                lease.ClientAPath,
                TokenA,
                ClientA,
                OnlineStatus,
                OfflineTitle);

            var exitCode = await CollaborationClientApplication.RunAsync(command, output, CancellationToken.None)
                .ConfigureAwait(false);

            var text = output.ToString();
            var operationId = ReadQueuedOperationId(text);
            await Assert.That(exitCode).IsEqualTo(0).Because(CreateCliFailureContext(text));
            var context = CreateCliFailureContext(text);
            await Assert.That(text.Contains($"status: {OnlineStatus}", StringComparison.Ordinal))
                .IsTrue()
                .Because(context);
            await Assert.That(text.Contains($"title: {OfflineTitle}", StringComparison.Ordinal))
                .IsTrue()
                .Because(context);
            await Assert.That(text.Contains("sync: Online", StringComparison.Ordinal))
                .IsTrue()
                .Because(context);
            var operationSummary = $"operation: {operationId} Synchronized";
            await Assert.That(text.Contains(operationSummary, StringComparison.Ordinal))
                .IsTrue()
                .Because(context);
            await Assert.That(text.Contains("faults: 0", StringComparison.Ordinal))
                .IsTrue()
                .Because(context);
            await AssertNoInfrastructureLeakAsync(text, lease).ConfigureAwait(false);
        }
        finally
        {
            await StopAndDisposeServerAsync(app).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies offline publish output immediately reports the saved local operation status.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncOfflinePublishPrintsSavedLocalOperationSummary()
    {
        using var lease = new CollaborationClientDatabaseLease();
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        var serverUri = new Uri("http://127.0.0.1:0");
        var command = CreatePublishCommand(serverUri, lease.ClientAPath, TokenA, ClientA, OfflineStatus, OfflineTitle)
            with
            {
                Options = CreateClientOptions(serverUri, lease.ClientAPath, TokenA, ClientA) with
                {
                    AutoStart = false,
                },
            };

        var exitCode = await CollaborationClientApplication.RunAsync(command, output, CancellationToken.None)
            .ConfigureAwait(false);

        var text = output.ToString();
        var operationId = ReadQueuedOperationId(text);
        await Assert.That(exitCode).IsEqualTo(0);
        await Assert.That(text.Contains($"operation: {operationId} SavedLocally", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains("faults: 0", StringComparison.Ordinal)).IsTrue();
        await AssertNoInfrastructureLeakAsync(text, lease).ConfigureAwait(false);
    }

    /// <summary>Verifies authentication failures print structured diagnostics without raw exception data.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncPublishAuthenticationFailurePrintsStructuredFaultWithoutSecrets()
    {
        using var lease = new CollaborationClientDatabaseLease();
        var app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
        try
        {
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            await using var output = new StringWriter(CultureInfo.InvariantCulture);
            var command = CreatePublishCommand(
                boundUri,
                lease.ClientAPath,
                InvalidToken,
                ClientA,
                OnlineStatus,
                OfflineTitle);
            using var cancellation = CreateWaitCancellation();

            var exitCode = await ProgramRunner.RunAsync(CreatePublishArguments(command), output, cancellation.Token)
                .ConfigureAwait(false);

            var text = output.ToString();
            await Assert.That(exitCode).IsNotEqualTo(0);
            await Assert.That(text.Contains("faults: 1", StringComparison.Ordinal)).IsTrue();
            await Assert.That(text.Contains("fault: Authentication", StringComparison.Ordinal)).IsTrue();
            await Assert.That(text.Contains("status=401", StringComparison.Ordinal)).IsTrue();
            await Assert.That(text.Contains("HttpRemoteTransportException", StringComparison.Ordinal)).IsFalse();
            await Assert.That(text.Contains(RawExceptionMarker, StringComparison.Ordinal)).IsFalse();
            await AssertNoInfrastructureLeakAsync(text, lease).ConfigureAwait(false);
        }
        finally
        {
            await StopAndDisposeServerAsync(app).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies watch startup failures print structured diagnostics without raw exception data.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncWatchAuthenticationFailurePrintsStructuredFaultWithoutSecrets()
    {
        using var lease = new CollaborationClientDatabaseLease();
        var app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
        try
        {
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            await using var output = new StringWriter(CultureInfo.InvariantCulture);
            using var cancellation = CreateWaitCancellation();

            var exitCode = await ProgramRunner.RunAsync(
                    CreateWatchArguments(boundUri, lease.ClientAPath, InvalidToken, ClientA),
                    output,
                    cancellation.Token)
                .ConfigureAwait(false);

            var text = output.ToString();
            await Assert.That(exitCode).IsNotEqualTo(0);
            await Assert.That(text.Contains("faults: 1", StringComparison.Ordinal)).IsTrue();
            await Assert.That(text.Contains("fault: Authentication", StringComparison.Ordinal)).IsTrue();
            await Assert.That(text.Contains("status=401", StringComparison.Ordinal)).IsTrue();
            await Assert.That(text.Contains("HttpRemoteTransportException", StringComparison.Ordinal)).IsFalse();
            await Assert.That(text.Contains(RawExceptionMarker, StringComparison.Ordinal)).IsFalse();
            await AssertNoInfrastructureLeakAsync(text, lease).ConfigureAwait(false);
        }
        finally
        {
            await StopAndDisposeServerAsync(app).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies invalid argument diagnostics do not echo raw argument values.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncInvalidArgumentsPrintRedactedUsageWithoutSecrets()
    {
        const string rawSecretArgument = "secret-extra-value";
        using var lease = new CollaborationClientDatabaseLease();
        await using var output = new StringWriter(CultureInfo.InvariantCulture);

        var exitCode = await ProgramRunner.RunAsync(
                [PublishCommandName, DatabaseOptionName, lease.ClientAPath, "--unknown", rawSecretArgument],
                output,
                CancellationToken.None)
            .ConfigureAwait(false);

        var text = output.ToString();
        await Assert.That(exitCode).IsNotEqualTo(0);
        await Assert.That(text.Contains(InvalidArgumentsOutput, StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains(rawSecretArgument, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(lease.ClientAPath, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(RawExceptionMarker, StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Verifies malformed URI diagnostics do not echo raw argument values.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncMalformedServerUriPrintsRedactedUsageWithoutSecrets()
    {
        const string malformedServer = "http://[invalid-token.example";
        using var lease = new CollaborationClientDatabaseLease();
        await using var output = new StringWriter(CultureInfo.InvariantCulture);

        var exitCode = await ProgramRunner.RunAsync(
                [
                    PublishCommandName,
                    ServerOptionName,
                    malformedServer,
                    DatabaseOptionName,
                    lease.ClientAPath,
                    TokenOptionName,
                    InvalidToken,
                    ClientOptionName,
                    ClientA,
                ],
                output,
                CancellationToken.None)
            .ConfigureAwait(false);

        var text = output.ToString();
        await Assert.That(exitCode).IsNotEqualTo(0);
        await Assert.That(text.Contains(InvalidArgumentsOutput, StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains(malformedServer, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(InvalidToken, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(lease.ClientAPath, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(RawExceptionMarker, StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Verifies missing, help, and unknown commands produce redacted usage.</summary>
    /// <param name="commandName">The initial command argument, or empty for no arguments.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("")]
    [Arguments("--help")]
    [Arguments("-h")]
    [Arguments("unknown")]
    public async Task RunAsyncInvalidCommandPrintsUsage(string commandName)
    {
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        string[] arguments = commandName.Length == 0 ? [] : [commandName];

        var exitCode = await ProgramRunner.RunAsync(arguments, output, CancellationToken.None)
            .ConfigureAwait(false);

        await Assert.That(exitCode).IsEqualTo(1);
        await Assert.That(output.ToString().Contains(InvalidArgumentsOutput, StringComparison.Ordinal)).IsTrue();
        await Assert.That(output.ToString().Contains("Usage:", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies named switches are consumed and unknown arguments are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncOfflineSwitchAndUnknownArgumentAreHandled()
    {
        using var lease = new CollaborationClientDatabaseLease();
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        var arguments = new[]
        {
            PublishCommandName,
            "--offline",
            DatabaseOptionName, lease.ClientAPath,
            TokenOptionName, TokenA,
            ClientOptionName, ClientA,
            "--unexpected",
        };

        var exitCode = await ProgramRunner.RunAsync(arguments, output, CancellationToken.None)
            .ConfigureAwait(false);

        await Assert.That(exitCode).IsEqualTo(1);
        await Assert.That(output.ToString().Contains(InvalidArgumentsOutput, StringComparison.Ordinal)).IsTrue();
        await Assert.That(output.ToString().Contains(lease.ClientAPath, StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Verifies canceled watch startup returns the CLI cancellation code and safe text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncCanceledWatchReturnsCancellationCode()
    {
        using var lease = new CollaborationClientDatabaseLease();
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(false);
        var arguments = CreateWatchArguments(new("http://127.0.0.1:5088"), lease.ClientAPath, TokenA, ClientA);

        var exitCode = await ProgramRunner.RunAsync(arguments, output, cancellation.Token).ConfigureAwait(false);

        await Assert.That(exitCode).IsEqualTo(CanceledCommandExitCode);
        await Assert.That(output.ToString().Contains("timed out or was canceled", StringComparison.Ordinal)).IsTrue();
        await AssertNoInfrastructureLeakAsync(output.ToString(), lease).ConfigureAwait(false);
    }

    /// <summary>Verifies an unsupported command is rejected before opening local resources.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncRejectsUnsupportedCommandBeforeOpen()
    {
        using var lease = new CollaborationClientDatabaseLease();
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        var command = CreatePublishCommand(
            new("http://127.0.0.1:5088"),
            lease.ClientAPath,
            TokenA,
            ClientA,
            OnlineStatus,
            OfflineTitle) with { Kind = (CollaborationClientCommandKind)UnknownCommandKind };

        _ = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            CollaborationClientApplication.RunAsync(command, output, CancellationToken.None));
        await Assert.That(File.Exists(lease.ClientAPath)).IsFalse();
    }

    /// <summary>Verifies a watch already active ends with its exact cancellation token.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WatchLifetimeCancelsAfterRegistration()
    {
        using var cancellation = new CancellationTokenSource();
        var watch = CollaborationClientApplication.WaitForWatchCancellationAsync(cancellation.Token);

        await Assert.That(watch.IsCompleted).IsFalse();
        await cancellation.CancelAsync().ConfigureAwait(false);
        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => watch);

        await Assert.That(exception?.CancellationToken).IsEqualTo(cancellation.Token);
    }

    /// <summary>Verifies a canceled token ends the watch during registration.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WatchLifetimeHonorsCancellationBeforeRegistration()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync().ConfigureAwait(false);

        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() =>
            CollaborationClientApplication.WaitForWatchCancellationAsync(cancellation.Token));

        await Assert.That(exception?.CancellationToken).IsEqualTo(cancellation.Token);
    }

    /// <summary>Asserts diagnostic output does not expose tokens or local storage paths.</summary>
    /// <param name="text">The command output.</param>
    /// <param name="lease">The database lease.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertNoInfrastructureLeakAsync(string text, CollaborationClientDatabaseLease lease)
    {
        await Assert.That(text.Contains(TokenA, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(TokenB, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(InvalidToken, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(lease.ClientAPath, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(lease.ClientBPath, StringComparison.Ordinal)).IsFalse();
        await Assert.That(text.Contains(lease.ServerPath, StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Creates a bounded assertion context from already-redacted CLI output.</summary>
    /// <param name="output">The command output.</param>
    /// <returns>The assertion context.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateCliFailureContext(string output) =>
        $"CLI output: {output.ReplaceLineEndings("\\n")}";

    /// <summary>Creates command-line arguments for one publish command.</summary>
    /// <param name="command">The command.</param>
    /// <returns>The command-line arguments.</returns>
    private static string[] CreatePublishArguments(CollaborationClientCommand command) =>
        [
            PublishCommandName,
            ServerOptionName,
            command.Options.ServerUri.AbsoluteUri,
            DatabaseOptionName,
            command.Options.DatabasePath,
            TokenOptionName,
            command.Options.Token,
            ClientOptionName,
            command.Options.ClientId,
            "--status",
            command.Update.Status,
            "--title",
            command.Update.Title ?? string.Empty,
        ];

    /// <summary>Creates command-line arguments for one watch command.</summary>
    /// <param name="serverUri">The server URI.</param>
    /// <param name="databasePath">The client database path.</param>
    /// <param name="token">The development token.</param>
    /// <param name="clientId">The client id.</param>
    /// <returns>The command-line arguments.</returns>
    private static string[] CreateWatchArguments(
        Uri serverUri,
        string databasePath,
        string token,
        string clientId) =>
        [
            "watch",
            ServerOptionName,
            serverUri.AbsoluteUri,
            DatabaseOptionName,
            databasePath,
            TokenOptionName,
            token,
            ClientOptionName,
            clientId,
        ];
}
