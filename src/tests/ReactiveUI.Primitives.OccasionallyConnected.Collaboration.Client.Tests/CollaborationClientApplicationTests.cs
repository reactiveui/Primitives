// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Tests for <see cref="CollaborationClientApplication"/>.</summary>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>The token for client A.</summary>
    private const string TokenA = "token-a";

    /// <summary>The token for client B.</summary>
    private const string TokenB = "token-b";

    /// <summary>A token rejected by the development server.</summary>
    private const string InvalidToken = "invalid-token";

    /// <summary>The tenant used by both clients.</summary>
    private const string Tenant = "tenant-a";

    /// <summary>The first client identity.</summary>
    private const string ClientA = "client-a";

    /// <summary>The second client identity.</summary>
    private const string ClientB = "client-b";

    /// <summary>The earlier status seeded before the publish command test.</summary>
    private const string SeedStatus = "seeded";

    /// <summary>The earlier title seeded before the publish command test.</summary>
    private const string SeedTitle = "Already confirmed";

    /// <summary>The offline status used by the first client.</summary>
    private const string OfflineStatus = "draft";

    /// <summary>The online status used by the second client.</summary>
    private const string OnlineStatus = "approved";

    /// <summary>The offline title value.</summary>
    private const string OfflineTitle = "Launch checklist";

    /// <summary>The online details value.</summary>
    private const string OnlineDetails = "Reviewed by client B";

    /// <summary>The live outage status used by the first client.</summary>
    private const string OutageStatus = "outage-draft";

    /// <summary>The live outage details value.</summary>
    private const string OutageDetails = "Queued while the live endpoint is down";

    /// <summary>The maximum number of events retained by test telemetry.</summary>
    private const int TelemetryCapacity = 256;

    /// <summary>The server long-poll timeout in milliseconds.</summary>
    private const int ServerLongPollMilliseconds = 200;

    /// <summary>The empty poll delay in milliseconds.</summary>
    private const int EmptyPollDelayMilliseconds = 10;

    /// <summary>The finite wait for live HTTP and SQLite convergence.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(15);

    /// <summary>Verifies two SQLite clients converge through the real ASP.NET HTTP collaboration server.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OpenPersistsOfflinePublishReconnectsAndConvergesTwoSqliteClients()
    {
        using var lease = new CollaborationClientDatabaseLease();
        var serverUri = new Uri("http://127.0.0.1:0");
        var offline = await PublishOfflineActivityAsync(lease, serverUri).ConfigureAwait(false);
        await Assert.That(offline.PendingView.Status).IsEqualTo(OfflineStatus);
        await using var app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
        await app.StartAsync().ConfigureAwait(false);
        var boundUri = new Uri(GetBoundAddress(app.Services));
        await using var clientA = await CollaborationClientApplication.OpenAsync(
            CreateClientOptions(boundUri, lease.ClientAPath, TokenA, ClientA) with { AutoStart = false }).ConfigureAwait(false);
        await using var clientB = await CollaborationClientApplication.OpenAsync(
            CreateClientOptions(boundUri, lease.ClientBPath, TokenB, ClientB) with { AutoStart = false }).ConfigureAwait(false);
        var observerA = new ActivityViewObserver();
        var observerB = new ActivityViewObserver();
        using var subscriptionA = clientA.Activity.Local.Subscribe(observerA);
        using var subscriptionB = clientB.Activity.Local.Subscribe(observerB);

        await StartClientsAndAssertSubscriptionAsync(clientA, clientB, offline.SubscriptionId).ConfigureAwait(false);
        var accepted = await WaitForOfflineAcceptanceAsync(observerA, observerB).ConfigureAwait(false);
        await Assert.That(accepted.ClientA.AcceptedClientId).IsEqualTo(ClientA);
        await Assert.That(accepted.ClientB.AcceptedVersion).IsEqualTo(accepted.ClientA.AcceptedVersion);
        await PublishOnlineActivityAndAssertConvergenceAsync(clientB, observerA, observerB).ConfigureAwait(false);
        await clientA.StopAsync(CancellationToken.None).ConfigureAwait(false);
        await clientB.StopAsync(CancellationToken.None).ConfigureAwait(false);
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Verifies a durable publish made during a live outage synchronizes after same-endpoint restart.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LiveEndpointRestartSynchronizesDurablePublishMadeDuringOutage()
    {
        using var lease = new CollaborationClientDatabaseLease();
        WebApplication? app = null;
        try
        {
            app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            await using var clientA = await OpenClientAsync(boundUri, lease.ClientAPath, TokenA, ClientA).ConfigureAwait(false);
            await using var clientB = await OpenClientAsync(boundUri, lease.ClientBPath, TokenB, ClientB).ConfigureAwait(false);
            using var telemetryA = new ActivityTelemetry(clientA.Activity);
            using var telemetryB = new ActivityTelemetry(clientB.Activity);

            await StartClientsAsync(clientA, clientB).ConfigureAwait(false);
            await PublishInitialConnectedActivityAndAssertConvergenceAsync(clientB, telemetryA.Local, telemetryB.Local).ConfigureAwait(false);
            await StopAndDisposeServerAsync(app).ConfigureAwait(false);
            app = null;

            var receipt = await PublishOutageActivityAsync(clientA, telemetryA).ConfigureAwait(false);
            app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath, boundUri));
            await StartServerAsync(app).ConfigureAwait(false);

            var finalA = await WaitForAcceptedOperationAsync(telemetryA.Local, receipt).ConfigureAwait(false);
            var finalB = await WaitForAcceptedOperationAsync(telemetryB.Local, receipt).ConfigureAwait(false);
            await WaitForReopenedOperationSynchronizedAsync(
                    lease,
                    clientA,
                    clientB,
                    telemetryA,
                    telemetryB,
                    receipt)
                .ConfigureAwait(false);
            _ = await telemetryB.Remote.WaitForAsync(IsRemoteOutageUpdate, WaitTimeout).ConfigureAwait(false);
            await AssertConvergedCanonicalViewsAsync(finalA, finalB, receipt).ConfigureAwait(false);
            await StopClientsAsync(clientA, clientB).ConfigureAwait(false);
            await AssertNoTerminalStreamFailuresAsync(telemetryA, telemetryB).ConfigureAwait(false);
            await Assert.That(telemetryA.Operations.Count(IsSynchronized(receipt))).IsEqualTo(1);
            await Assert.That(telemetryA.Operations.Count(IsConflict(receipt))).IsEqualTo(0);
            await Assert.That(telemetryB.Remote.Count(IsRemoteOutageUpdate)).IsEqualTo(1);
        }
        finally
        {
            if (app is not null)
            {
                await StopAndDisposeServerAsync(app).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a durable outage publish resumes from SQLite after the client is reopened.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReopenedClientResumesDurableOutagePublishWithoutDuplicateRemoteEffect()
    {
        using var lease = new CollaborationClientDatabaseLease();
        WebApplication? app = null;
        try
        {
            app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            PublishReceipt receipt;
            SubscriptionId subscriptionId;
            await using (var clientA = await OpenClientAsync(boundUri, lease.ClientAPath, TokenA, ClientA).ConfigureAwait(false))
            await using (var clientB = await OpenClientAsync(boundUri, lease.ClientBPath, TokenB, ClientB).ConfigureAwait(false))
            {
                using var telemetryA = new ActivityTelemetry(clientA.Activity);
                using var telemetryB = new ActivityTelemetry(clientB.Activity);
                await StartClientsAsync(clientA, clientB).ConfigureAwait(false);
                _ = await PublishInitialConnectedActivityAndAssertConvergenceAsync(
                    clientB,
                    telemetryA.Local,
                    telemetryB.Local).ConfigureAwait(false);
                await StopClientsAsync(clientA, clientB).ConfigureAwait(false);
                await AssertNoTerminalStreamFailuresAsync(telemetryA, telemetryB).ConfigureAwait(false);
                subscriptionId = clientA.Activity.SubscriptionId;
            }

            var proof = await ReadActualClientResumeProofAsync(lease.ClientAPath, subscriptionId).ConfigureAwait(false);
            await StopAndDisposeServerAsync(app).ConfigureAwait(false);
            app = null;
            await using (var offlineClient = await OpenClientAsync(boundUri, lease.ClientAPath, TokenA, ClientA).ConfigureAwait(false))
            {
                using var offlineTelemetry = new ActivityTelemetry(offlineClient.Activity);
                receipt = await PublishOutageActivityAsync(offlineClient, offlineTelemetry).ConfigureAwait(false);
                await AssertNoTerminalStreamFailuresAsync(offlineTelemetry).ConfigureAwait(false);
            }

            var offlineProof = await ReadActualClientResumeProofAsync(lease.ClientAPath, proof.ClientSubscriptionId).ConfigureAwait(false);
            await Assert.That(offlineProof.ServerCursor).IsEqualTo(proof.ServerCursor);
            app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath, boundUri));
            await StartServerAsync(app).ConfigureAwait(false);
            await VerifyPersistedResumeAndNoDuplicatesAsync(lease, boundUri, receipt, proof).ConfigureAwait(false);
        }
        finally
        {
            if (app is not null)
            {
                await StopAndDisposeServerAsync(app).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies rejected development credentials fail without producing a remote effect.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvalidDevelopmentTokenReportsAuthenticationFailureWithoutPublishing()
    {
        using var lease = new CollaborationClientDatabaseLease();
        await using var app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
        await StartServerAsync(app).ConfigureAwait(false);
        var boundUri = new Uri(GetBoundAddress(app.Services));
        await using var verifier = await OpenClientAsync(boundUri, lease.ClientBPath, TokenB, ClientB).ConfigureAwait(false);
        await using var rejected = await OpenClientAsync(boundUri, lease.ClientAPath, InvalidToken, ClientA).ConfigureAwait(false);
        using var verifierTelemetry = new ActivityTelemetry(verifier.Activity);
        using var rejectedTelemetry = new ActivityTelemetry(rejected.Activity);

        await StartSingleClientAsync(verifier).ConfigureAwait(false);
        var receipt = await PublishOutageActivityAsync(rejected, rejectedTelemetry).ConfigureAwait(false);

        var exception = await CaptureHttpExceptionAsync(
            async () => await StartSingleClientAsync(rejected).ConfigureAwait(false)).ConfigureAwait(false);

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.Authentication);
        await Assert.That(exception.StatusCode).IsEqualTo((HttpStatusCode?)HttpStatusCode.Unauthorized);
        await StopSingleClientAsync(verifier).ConfigureAwait(false);
        await AssertNoTerminalStreamFailuresAsync(verifierTelemetry).ConfigureAwait(false);
        await Assert.That(rejectedTelemetry.Operations.Count(IsSynchronized(receipt))).IsEqualTo(0);
        await Assert.That(verifierTelemetry.Remote.Count(IsRemoteOutageUpdate)).IsEqualTo(0);
        using var cancellation = CreateWaitCancellation();
        await app.StopAsync(cancellation.Token).ConfigureAwait(false);
    }

    /// <summary>Verifies the publish command waits for the current operation instead of an earlier confirmed local view.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncPublishWaitsForCurrentOperationConfirmation()
    {
        using var lease = new CollaborationClientDatabaseLease();
        await using var app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
        await app.StartAsync().ConfigureAwait(false);
        var boundUri = new Uri(GetBoundAddress(app.Services));
        await SeedConfirmedActivityAsync(boundUri, lease.ClientAPath).ConfigureAwait(false);
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        var command = CreatePublishCommand(boundUri, lease.ClientAPath, TokenA, ClientA, OnlineStatus, OfflineTitle);

        var exitCode = await CollaborationClientApplication.RunAsync(command, output, CancellationToken.None).ConfigureAwait(false);

        var text = output.ToString();
        var queuedOperationId = ReadQueuedOperationId(text);
        await Assert.That(exitCode).IsEqualTo(0).Because(CreateCliFailureContext(text));
        await Assert.That(text.Contains($"status: {OnlineStatus}", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains($"title: {OfflineTitle}", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains($"accepted: {ClientA}/", StringComparison.Ordinal)).IsTrue();
        await VerifyServerAcceptedPublishedOperationAsync(boundUri, lease.ClientBPath, queuedOperationId).ConfigureAwait(false);
        await app.StopAsync().ConfigureAwait(false);
    }

    /// <summary>Opens one stopped collaboration client against the supplied endpoint.</summary>
    /// <param name="serverUri">The server URI.</param>
    /// <param name="databasePath">The client database path.</param>
    /// <param name="token">The development token.</param>
    /// <param name="clientId">The client id.</param>
    /// <returns>The opened client.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<CollaborationClientSession> OpenClientAsync(
        Uri serverUri,
        string databasePath,
        string token,
        string clientId) =>
        CollaborationClientApplication.OpenAsync(
            CreateClientOptions(serverUri, databasePath, token, clientId) with { AutoStart = false });

    /// <summary>Starts both clients.</summary>
    /// <param name="clientA">The first client.</param>
    /// <param name="clientB">The second client.</param>
    /// <returns>The assertion task.</returns>
    private static async Task StartClientsAsync(
        CollaborationClientSession clientA,
        CollaborationClientSession clientB)
    {
        using var cancellation = CreateWaitCancellation();
        await clientA.StartAsync(cancellation.Token).ConfigureAwait(false);
        await clientB.StartAsync(cancellation.Token).ConfigureAwait(false);
    }

    /// <summary>Starts one client with a bounded wait.</summary>
    /// <param name="client">The client.</param>
    /// <returns>The assertion task.</returns>
    private static async Task StartSingleClientAsync(CollaborationClientSession client)
    {
        using var cancellation = CreateWaitCancellation();
        await client.StartAsync(cancellation.Token).ConfigureAwait(false);
    }

    /// <summary>Stops both clients with a bounded wait.</summary>
    /// <param name="clientA">The first client.</param>
    /// <param name="clientB">The second client.</param>
    /// <returns>The assertion task.</returns>
    private static async Task StopClientsAsync(
        CollaborationClientSession clientA,
        CollaborationClientSession clientB)
    {
        using var cancellation = CreateWaitCancellation();
        await clientA.StopAsync(cancellation.Token).ConfigureAwait(false);
        await clientB.StopAsync(cancellation.Token).ConfigureAwait(false);
    }

    /// <summary>Stops one client with a bounded wait.</summary>
    /// <param name="client">The client.</param>
    /// <returns>The assertion task.</returns>
    private static async Task StopSingleClientAsync(CollaborationClientSession client)
    {
        using var cancellation = CreateWaitCancellation();
        await client.StopAsync(cancellation.Token).ConfigureAwait(false);
    }

    /// <summary>Publishes the initial connected update for the live reconnect scenario.</summary>
    /// <param name="clientB">The second client.</param>
    /// <param name="observerA">The first client observer.</param>
    /// <param name="observerB">The second client observer.</param>
    /// <returns>The assertion task.</returns>
    private static async Task<PublishReceipt> PublishInitialConnectedActivityAndAssertConvergenceAsync(
        CollaborationClientSession clientB,
        RecordingObserver<ActivityView> observerA,
        RecordingObserver<ActivityView> observerB)
    {
        using var cancellation = CreateWaitCancellation();
        var receipt = await clientB.PublishAsync(
                new() { Status = OnlineStatus, Title = OfflineTitle, TitleSpecified = true, Details = OnlineDetails, DetailsSpecified = true },
                cancellation.Token)
            .ConfigureAwait(false);
        var finalA = await WaitForOnlineActivityAsync(observerA).ConfigureAwait(false);
        var finalB = await WaitForOnlineActivityAsync(observerB).ConfigureAwait(false);
        await Assert.That(finalB.AcceptedVersion).IsEqualTo(finalA.AcceptedVersion);
        await Assert.That(finalB.AcceptedClientId).IsEqualTo(ClientB);
        return receipt;
    }

    /// <summary>Publishes while the live endpoint is down and verifies local durable observable state.</summary>
    /// <param name="clientA">The first client.</param>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <returns>The local publish receipt.</returns>
    private static async Task<PublishReceipt> PublishOutageActivityAsync(
        CollaborationClientSession clientA,
        ActivityTelemetry telemetryA)
    {
        using var cancellation = CreateWaitCancellation();
        var receipt = await clientA.PublishAsync(
                new() { Status = OutageStatus, Details = OutageDetails, DetailsSpecified = true },
                cancellation.Token)
            .ConfigureAwait(false);
        _ = await telemetryA.Local.WaitForAsync(value => IsPendingOutageView(value, receipt), WaitTimeout).ConfigureAwait(false);
        _ = await telemetryA.Operations.WaitForAsync(value => IsNonTerminal(value, receipt), WaitTimeout).ConfigureAwait(false);
        return receipt;
    }

    /// <summary>Verifies reopened clients resume one durable outage publish without duplicate remote effects.</summary>
    /// <param name="lease">The database lease.</param>
    /// <param name="boundUri">The restarted server URI.</param>
    /// <param name="receipt">The receipt for the durable publish.</param>
    /// <param name="proof">The durable client resume state recovered before the outage publish.</param>
    /// <returns>The assertion task.</returns>
    private static async Task VerifyPersistedResumeAndNoDuplicatesAsync(
        CollaborationClientDatabaseLease lease,
        Uri boundUri,
        PublishReceipt receipt,
        ResumeProof proof)
    {
        await using (var clientA = await OpenClientAsync(boundUri, lease.ClientAPath, TokenA, ClientA).ConfigureAwait(false))
        await using (var clientB = await OpenClientAsync(boundUri, lease.ClientBPath, TokenB, ClientB).ConfigureAwait(false))
        {
            using var telemetryA = new ActivityTelemetry(clientA.Activity);
            using var telemetryB = new ActivityTelemetry(clientB.Activity);
            await Assert.That(proof.ServerCursor).IsNotEmpty();
            await StartClientsAsync(clientA, clientB).ConfigureAwait(false);
            await Assert.That(clientA.Activity.SubscriptionId).IsEqualTo(proof.ClientSubscriptionId);
            var finalA = await WaitForAcceptedOperationAsync(telemetryA.Local, receipt).ConfigureAwait(false);
            var finalB = await WaitForAcceptedOperationAsync(telemetryB.Local, receipt).ConfigureAwait(false);
            await WaitForReopenedOperationSynchronizedAsync(
                    lease,
                    clientA,
                    clientB,
                    telemetryA,
                    telemetryB,
                    receipt)
                .ConfigureAwait(false);
            _ = await telemetryB.Remote.WaitForAsync(IsRemoteOutageUpdate, WaitTimeout).ConfigureAwait(false);
            await AssertConvergedCanonicalViewsAsync(finalA, finalB, receipt).ConfigureAwait(false);
            await StopClientsAsync(clientA, clientB).ConfigureAwait(false);
            await AssertNoTerminalStreamFailuresAsync(telemetryA, telemetryB).ConfigureAwait(false);
            await Assert.That(telemetryA.Remote.Count(IsRemoteOnlineUpdate)).IsEqualTo(0);
            await Assert.That(telemetryA.Operations.Count(IsSynchronized(receipt))).IsEqualTo(1);
            await Assert.That(telemetryA.Operations.Count(IsConflict(receipt))).IsEqualTo(0);
            await Assert.That(telemetryB.Remote.Count(IsRemoteOutageUpdate)).IsEqualTo(1);
        }

        var resumed = await ReadActualClientResumeProofAsync(lease.ClientAPath, proof.ClientSubscriptionId).ConfigureAwait(false);
        await Assert.That(string.Equals(resumed.ServerCursor, proof.ServerCursor, StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Waits for a canonical local view accepted for the supplied receipt.</summary>
    /// <param name="observer">The local observer.</param>
    /// <param name="receipt">The expected receipt.</param>
    /// <returns>The canonical view.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<ActivityView> WaitForAcceptedOperationAsync(
        RecordingObserver<ActivityView> observer,
        PublishReceipt receipt) =>
        observer.WaitForAsync(value => IsAcceptedOutageView(value, receipt), WaitTimeout);

    /// <summary>Verifies both final views describe the same accepted server state.</summary>
    /// <param name="clientA">The first client view.</param>
    /// <param name="clientB">The second client view.</param>
    /// <param name="receipt">The expected receipt.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertConvergedCanonicalViewsAsync(
        ActivityView clientA,
        ActivityView clientB,
        PublishReceipt receipt)
    {
        var operationId = ToOperationText(receipt);
        await Assert.That(clientA.IsPending).IsFalse();
        await Assert.That(clientB.IsPending).IsFalse();
        await Assert.That(clientA.Status).IsEqualTo(OutageStatus);
        await Assert.That(clientB.Status).IsEqualTo(OutageStatus);
        await Assert.That(clientA.Title).IsEqualTo(OfflineTitle);
        await Assert.That(clientB.Title).IsEqualTo(OfflineTitle);
        await Assert.That(clientA.Details).IsEqualTo(OutageDetails);
        await Assert.That(clientB.Details).IsEqualTo(OutageDetails);
        await Assert.That(clientA.AcceptedOperationId).IsEqualTo(operationId);
        await Assert.That(clientB.AcceptedOperationId).IsEqualTo(operationId);
        await Assert.That(clientB.AcceptedVersion).IsEqualTo(clientA.AcceptedVersion);
        await Assert.That(clientB.AcceptedClientId).IsEqualTo(ClientA);
    }

    /// <summary>Verifies none of the subscribed telemetry streams ended with an observer terminal error.</summary>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <param name="telemetryB">The second client telemetry.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertNoTerminalStreamFailuresAsync(ActivityTelemetry telemetryA, ActivityTelemetry telemetryB)
    {
        await Assert.That(telemetryA.HasTerminalError).IsFalse();
        await Assert.That(telemetryB.HasTerminalError).IsFalse();
    }

    /// <summary>Verifies a subscribed telemetry stream did not end with an observer terminal error.</summary>
    /// <param name="telemetry">The telemetry.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertNoTerminalStreamFailuresAsync(ActivityTelemetry telemetry) =>
        await Assert.That(telemetry.HasTerminalError).IsFalse();

    /// <summary>Publishes the first client update while disconnected.</summary>
    /// <param name="lease">The database lease.</param>
    /// <param name="serverUri">The placeholder server URI.</param>
    /// <returns>The pending view and durable subscription id.</returns>
    private static async Task<(ActivityView PendingView, SubscriptionId SubscriptionId)> PublishOfflineActivityAsync(
        CollaborationClientDatabaseLease lease,
        Uri serverUri)
    {
        var clientAOptions = CreateClientOptions(serverUri, lease.ClientAPath, TokenA, ClientA) with { AutoStart = false };
        await using var offlineClient = await CollaborationClientApplication.OpenAsync(clientAOptions).ConfigureAwait(false);
        var observer = new ActivityViewObserver();
        using var subscription = offlineClient.Activity.Local.Subscribe(observer);
        await offlineClient.PublishAsync(
                new() { Status = OfflineStatus, Title = OfflineTitle, TitleSpecified = true },
                CancellationToken.None)
            .ConfigureAwait(false);
        var pendingView = await observer.WaitForAsync(
                static value => value.IsPending && string.Equals(value.Title, OfflineTitle, StringComparison.Ordinal),
                WaitTimeout)
            .ConfigureAwait(false);
        return (pendingView, offlineClient.Activity.SubscriptionId);
    }

    /// <summary>Starts both clients and verifies client A reused its persisted subscription identity.</summary>
    /// <param name="clientA">The first client.</param>
    /// <param name="clientB">The second client.</param>
    /// <param name="expectedSubscriptionId">The expected client A subscription id.</param>
    /// <returns>The assertion task.</returns>
    private static async Task StartClientsAndAssertSubscriptionAsync(
        CollaborationClientSession clientA,
        CollaborationClientSession clientB,
        SubscriptionId expectedSubscriptionId)
    {
        await clientA.StartAsync(CancellationToken.None).ConfigureAwait(false);
        await clientB.StartAsync(CancellationToken.None).ConfigureAwait(false);
        await Assert.That(clientA.Activity.SubscriptionId).IsEqualTo(expectedSubscriptionId);
    }

    /// <summary>Waits for both clients to observe the accepted offline publish.</summary>
    /// <param name="observerA">The first client observer.</param>
    /// <param name="observerB">The second client observer.</param>
    /// <returns>The accepted views.</returns>
    private static async Task<(ActivityView ClientA, ActivityView ClientB)> WaitForOfflineAcceptanceAsync(
        ActivityViewObserver observerA,
        ActivityViewObserver observerB)
    {
        var acceptedA = await observerA.WaitForAsync(
                static value => !value.IsPending && string.Equals(value.Title, OfflineTitle, StringComparison.Ordinal),
                WaitTimeout)
            .ConfigureAwait(false);
        var acceptedB = await observerB.WaitForAsync(
                static value => !value.IsPending && string.Equals(value.Title, OfflineTitle, StringComparison.Ordinal),
                WaitTimeout)
            .ConfigureAwait(false);
        return (acceptedA, acceptedB);
    }

    /// <summary>Publishes from client B and verifies both public views converge.</summary>
    /// <param name="clientB">The second client.</param>
    /// <param name="observerA">The first client observer.</param>
    /// <param name="observerB">The second client observer.</param>
    /// <returns>The assertion task.</returns>
    private static async Task PublishOnlineActivityAndAssertConvergenceAsync(
        CollaborationClientSession clientB,
        ActivityViewObserver observerA,
        ActivityViewObserver observerB)
    {
        await clientB.PublishAsync(
                new() { Status = OnlineStatus, Details = OnlineDetails, DetailsSpecified = true },
                CancellationToken.None)
            .ConfigureAwait(false);
        var finalA = await WaitForOnlineActivityAsync(observerA).ConfigureAwait(false);
        var finalB = await WaitForOnlineActivityAsync(observerB).ConfigureAwait(false);
        await Assert.That(finalA.Status).IsEqualTo(OnlineStatus);
        await Assert.That(finalB.Status).IsEqualTo(OnlineStatus);
        await Assert.That(finalB.AcceptedVersion).IsEqualTo(finalA.AcceptedVersion);
        await Assert.That(finalB.AcceptedClientId).IsEqualTo(ClientB);
    }

    /// <summary>Waits for a public activity stream to observe the online update.</summary>
    /// <param name="observer">The activity observer.</param>
    /// <returns>The matching view.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<ActivityView> WaitForOnlineActivityAsync(ActivityViewObserver observer) =>
        observer.WaitForAsync(
            static value => !value.IsPending
                && string.Equals(value.Title, OfflineTitle, StringComparison.Ordinal)
                && string.Equals(value.Details, OnlineDetails, StringComparison.Ordinal),
            WaitTimeout);

    /// <summary>Waits for a public activity stream to observe the online update.</summary>
    /// <param name="observer">The activity observer.</param>
    /// <returns>The matching view.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<ActivityView> WaitForOnlineActivityAsync(RecordingObserver<ActivityView> observer) =>
        observer.WaitForAsync(
            static value => !value.IsPending
                && string.Equals(value.Title, OfflineTitle, StringComparison.Ordinal)
                && string.Equals(value.Details, OnlineDetails, StringComparison.Ordinal),
            WaitTimeout);

    /// <summary>Seeds an older accepted local view in the same client database.</summary>
    /// <param name="serverUri">The bound server URI.</param>
    /// <param name="databasePath">The client database path.</param>
    /// <returns>The assertion task.</returns>
    private static async Task SeedConfirmedActivityAsync(Uri serverUri, string databasePath)
    {
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        var command = CreatePublishCommand(serverUri, databasePath, TokenA, ClientA, SeedStatus, SeedTitle);

        var exitCode = await CollaborationClientApplication.RunAsync(command, output, CancellationToken.None).ConfigureAwait(false);

        var text = output.ToString();
        await Assert.That(exitCode).IsEqualTo(0).Because(CreateCliFailureContext(text));
        await Assert.That(text.Contains($"status: {SeedStatus}", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies another SQLite client can observe the operation accepted by the server.</summary>
    /// <param name="serverUri">The bound server URI.</param>
    /// <param name="databasePath">The verification client database path.</param>
    /// <param name="operationId">The expected operation id.</param>
    /// <returns>The assertion task.</returns>
    private static async Task VerifyServerAcceptedPublishedOperationAsync(
        Uri serverUri,
        string databasePath,
        string operationId)
    {
        await using var session = await CollaborationClientApplication.OpenAsync(
            CreateClientOptions(serverUri, databasePath, TokenB, ClientB) with { AutoStart = false }).ConfigureAwait(false);
        var observer = new ActivityViewObserver();
        using var subscription = session.Activity.Local.Subscribe(observer);
        await session.StartAsync(CancellationToken.None).ConfigureAwait(false);
        var view = await observer.WaitForAsync(
                value => !value.IsPending && string.Equals(value.AcceptedOperationId, operationId, StringComparison.Ordinal),
                WaitTimeout)
            .ConfigureAwait(false);
        await Assert.That(view.Status).IsEqualTo(OnlineStatus);
        await Assert.That(view.Title).IsEqualTo(OfflineTitle);
        await Assert.That(view.AcceptedClientId).IsEqualTo(ClientA);
        await Assert.That(view.AcceptedOperationId).IsEqualTo(operationId);
        await session.StopAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Creates a publish command for the CLI application.</summary>
    /// <param name="serverUri">The bound server URI.</param>
    /// <param name="databasePath">The client database path.</param>
    /// <param name="token">The development token.</param>
    /// <param name="clientId">The client id.</param>
    /// <param name="status">The status to publish.</param>
    /// <param name="title">The title to publish.</param>
    /// <returns>The publish command.</returns>
    private static CollaborationClientCommand CreatePublishCommand(
        Uri serverUri,
        string databasePath,
        string token,
        string clientId,
        string status,
        string title)
    {
        var update = new ActivityUpdate { Status = status, Title = title, TitleSpecified = true };
        return new() { Kind = CollaborationClientCommandKind.Publish, Options = CreateClientOptions(serverUri, databasePath, token, clientId), Update = update };
    }

    /// <summary>Reads the queued operation id from CLI output.</summary>
    /// <param name="text">The CLI output.</param>
    /// <returns>The queued operation id.</returns>
    /// <exception cref="InvalidOperationException">The CLI output does not include a queued operation.</exception>
    private static string ReadQueuedOperationId(string text)
    {
        using var reader = new StringReader(text);
        var line = reader.ReadLine() ?? throw new InvalidOperationException("The CLI did not report a queued operation.");
        const string prefix = "queued ";
        if (!line.StartsWith(prefix, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The CLI did not report a queued operation.");
        }

        return line[prefix.Length..];
    }

    /// <summary>Creates server options for the real HTTP fixture.</summary>
    /// <param name="databasePath">The server SQLite database path.</param>
    /// <returns>The server options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CollaborationServerOptions CreateServerOptions(string databasePath) =>
        CreateServerOptions(databasePath, new("http://127.0.0.1:0"));

    /// <summary>Creates server options for the real HTTP fixture.</summary>
    /// <param name="databasePath">The server SQLite database path.</param>
    /// <param name="listenUri">The listen URI.</param>
    /// <returns>The server options.</returns>
    private static CollaborationServerOptions CreateServerOptions(string databasePath, Uri listenUri)
    {
        var longPollTimeout = TimeSpan.FromMilliseconds(ServerLongPollMilliseconds);
        var emptyPollDelay = TimeSpan.FromMilliseconds(EmptyPollDelayMilliseconds);
        DevelopmentCredential[] credentials = [new(TokenA, Tenant, ClientA), new(TokenB, Tenant, ClientB)];
        return new() { ListenUri = listenUri, DatabasePath = databasePath, Credentials = credentials, LongPollTimeout = longPollTimeout, EmptyPollDelay = emptyPollDelay };
    }

    /// <summary>Starts one ASP.NET server fixture with a bounded wait.</summary>
    /// <param name="app">The server application.</param>
    /// <returns>The startup task.</returns>
    private static async Task StartServerAsync(WebApplication app)
    {
        using var cancellation = CreateWaitCancellation();
        await app.StartAsync(cancellation.Token).ConfigureAwait(false);
    }

    /// <summary>Stops and disposes one ASP.NET server fixture.</summary>
    /// <param name="app">The server application.</param>
    /// <returns>The cleanup task.</returns>
    private static async Task StopAndDisposeServerAsync(WebApplication app)
    {
        Exception? stopFailure = null;
        try
        {
            using var cancellation = CreateWaitCancellation();
            await app.StopAsync(cancellation.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            stopFailure = exception;
        }

        Exception? disposeFailure = null;
        try
        {
            await app.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            disposeFailure = exception;
        }

        ThrowCleanupFailure(stopFailure, disposeFailure);
    }

    /// <summary>Throws the cleanup failure after disposal has been attempted.</summary>
    /// <param name="stopFailure">The optional stop failure.</param>
    /// <param name="disposeFailure">The optional disposal failure.</param>
    /// <exception cref="AggregateException">Stopping and disposing the server both failed.</exception>
    private static void ThrowCleanupFailure(Exception? stopFailure, Exception? disposeFailure)
    {
        if (stopFailure is not null && disposeFailure is not null)
        {
            throw new AggregateException("Stopping the server failed, and disposal also failed.", stopFailure, disposeFailure);
        }

        if (stopFailure is not null)
        {
            ExceptionDispatchInfo.Capture(stopFailure).Throw();
        }

        if (disposeFailure is not null)
        {
            ExceptionDispatchInfo.Capture(disposeFailure).Throw();
        }
    }

    /// <summary>Creates a timeout token source owned by the caller.</summary>
    /// <returns>The cancellation source.</returns>
    private static CancellationTokenSource CreateWaitCancellation()
    {
        var cancellation = new CancellationTokenSource();
        cancellation.CancelAfter(WaitTimeout);
        return cancellation;
    }

    /// <summary>Converts a publish receipt operation id to the public activity text format.</summary>
    /// <param name="receipt">The publish receipt.</param>
    /// <returns>The public operation id text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string ToOperationText(PublishReceipt receipt) => receipt.OperationId.Value.ToString("N");

    /// <summary>Creates a predicate for synchronized statuses matching a receipt.</summary>
    /// <param name="receipt">The receipt.</param>
    /// <returns>The predicate.</returns>
    private static Func<SyncOperationStatus, bool> IsSynchronized(PublishReceipt receipt) =>
        value => value.OperationId == receipt.OperationId && value.State == SyncOperationState.Synchronized;

    /// <summary>Creates a predicate for conflict statuses matching a receipt.</summary>
    /// <param name="receipt">The receipt.</param>
    /// <returns>The predicate.</returns>
    private static Func<SyncOperationStatus, bool> IsConflict(PublishReceipt receipt) =>
        value => value.OperationId == receipt.OperationId && value.State == SyncOperationState.Conflict;

    /// <summary>Determines whether an operation status is still pending terminal synchronization.</summary>
    /// <param name="value">The observed status.</param>
    /// <param name="receipt">The expected receipt.</param>
    /// <returns>Whether the status is non-terminal for the expected receipt.</returns>
    private static bool IsNonTerminal(SyncOperationStatus value, PublishReceipt receipt) =>
        value.OperationId == receipt.OperationId
        && value.State is SyncOperationState.SavedLocally or SyncOperationState.QueuedForUpload or SyncOperationState.Uploading
            or SyncOperationState.Ambiguous;

    /// <summary>Determines whether an activity view is the local pending outage update.</summary>
    /// <param name="value">The observed view.</param>
    /// <param name="receipt">The expected receipt.</param>
    /// <returns>Whether the view is the expected local pending view.</returns>
    private static bool IsPendingOutageView(ActivityView value, PublishReceipt receipt) =>
        value.IsPending
        && string.Equals(value.AcceptedOperationId, ToOperationText(receipt), StringComparison.Ordinal)
        && string.Equals(value.Status, OutageStatus, StringComparison.Ordinal)
        && string.Equals(value.Details, OutageDetails, StringComparison.Ordinal);

    /// <summary>Determines whether an activity view is the accepted outage update.</summary>
    /// <param name="value">The observed view.</param>
    /// <param name="receipt">The expected receipt.</param>
    /// <returns>Whether the view is the expected accepted view.</returns>
    private static bool IsAcceptedOutageView(ActivityView value, PublishReceipt receipt) =>
        !value.IsPending
        && string.Equals(value.AcceptedOperationId, ToOperationText(receipt), StringComparison.Ordinal)
        && string.Equals(value.Status, OutageStatus, StringComparison.Ordinal)
        && string.Equals(value.Details, OutageDetails, StringComparison.Ordinal);

    /// <summary>Determines whether a remote message carries the outage update.</summary>
    /// <param name="message">The remote message.</param>
    /// <returns>Whether the message matches the outage update.</returns>
    private static bool IsRemoteOutageUpdate(RemoteMessage<ActivityUpdate> message) =>
        string.Equals(message.Value.Status, OutageStatus, StringComparison.Ordinal)
        && string.Equals(message.Value.Details, OutageDetails, StringComparison.Ordinal);

    /// <summary>Determines whether a remote message carries the initial online update.</summary>
    /// <param name="message">The remote message.</param>
    /// <returns>Whether the message matches the initial online update.</returns>
    private static bool IsRemoteOnlineUpdate(RemoteMessage<ActivityUpdate> message) =>
        string.Equals(message.Value.Status, OnlineStatus, StringComparison.Ordinal)
        && string.Equals(message.Value.Details, OnlineDetails, StringComparison.Ordinal);

    /// <summary>Creates client options for one SQLite client.</summary>
    /// <param name="serverUri">The server URI.</param>
    /// <param name="databasePath">The client SQLite database path.</param>
    /// <param name="token">The development token.</param>
    /// <param name="clientId">The client id.</param>
    /// <returns>The client options.</returns>
    private static CollaborationClientOptions CreateClientOptions(
        Uri serverUri,
        string databasePath,
        string token,
        string clientId) =>
        new() { ServerUri = serverUri, DatabasePath = databasePath, Token = token, ClientId = clientId, WaitTimeout = WaitTimeout };

    /// <summary>Reads the first address reported by the ASP.NET server.</summary>
    /// <param name="services">The host service provider.</param>
    /// <returns>The bound HTTP address.</returns>
    /// <exception cref="InvalidOperationException">The server did not expose a bound address.</exception>
    private static string GetBoundAddress(IServiceProvider services)
    {
        var server = services.GetRequiredService<IServer>();
        var feature = server.Features.Get<IServerAddressesFeature>()
            ?? throw new InvalidOperationException("The server address feature was not available.");
        return feature.Addresses.FirstOrDefault()
            ?? throw new InvalidOperationException("The server did not report a bound address.");
    }

    /// <summary>Owns temporary database paths for the collaboration client fixture.</summary>
    private sealed class CollaborationClientDatabaseLease : IDisposable
    {
        /// <summary>The owned temporary directory.</summary>
        private readonly string _directory;

        /// <summary>Initializes a new instance of the <see cref="CollaborationClientDatabaseLease"/> class.</summary>
        internal CollaborationClientDatabaseLease()
        {
            _directory = Path.Combine(Path.GetTempPath(), $"rxui-oc-client-example-{Guid.NewGuid():N}");
            _ = Directory.CreateDirectory(_directory);
            ServerPath = Path.Combine(_directory, "server.db");
            ClientAPath = Path.Combine(_directory, "client-a.db");
            ClientBPath = Path.Combine(_directory, "client-b.db");
        }

        /// <summary>Gets the server database path.</summary>
        internal string ServerPath { get; }

        /// <summary>Gets the client A database path.</summary>
        internal string ClientAPath { get; }

        /// <summary>Gets the client B database path.</summary>
        internal string ClientBPath { get; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(_directory, recursive: true);
    }
}
