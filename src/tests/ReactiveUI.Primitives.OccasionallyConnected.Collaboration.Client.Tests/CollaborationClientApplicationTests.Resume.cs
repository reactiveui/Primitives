// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>Resume proof helpers for <see cref="CollaborationClientApplicationTests"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>The minimum schema version requested from the public SQLite adapter.</summary>
    private const int LocalStoreMinimumSchemaVersion = 1;

    /// <summary>Reads the actual client stream recovered from its durable SQLite store.</summary>
    /// <param name="databasePath">The client database path.</param>
    /// <param name="subscriptionId">The expected durable subscription id.</param>
    /// <returns>The recovered resume proof.</returns>
    /// <exception cref="InvalidOperationException">
    /// The recovered client stream does not have a durable server cursor.
    /// </exception>
    private static async Task<ResumeProof> ReadActualClientResumeProofAsync(
        string databasePath,
        SubscriptionId subscriptionId)
    {
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        await store.InitializeAsync(CreateStoreInitialization(ClientA), CancellationToken.None).ConfigureAwait(false);
        var recovered = await store.RecoverStreamAsync(
                ActivityContracts.StreamId,
                subscriptionId,
                CancellationToken.None)
            .ConfigureAwait(false);

        var serverCursor = recovered.ServerCursor
            ?? throw new InvalidOperationException("The recovered client stream did not have a durable server cursor.");
        await Assert.That(recovered.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(serverCursor).IsNotEmpty();
        return new(recovered.SubscriptionId, serverCursor);
    }

    /// <summary>Creates the local store initialization used by the collaboration client.</summary>
    /// <param name="clientId">The initialized client id.</param>
    /// <returns>The local store initialization.</returns>
    private static LocalStoreInitialization CreateStoreInitialization(string clientId) =>
        new(CollaborationClientOptions.DefaultStoreIdentity, LocalStoreMinimumSchemaVersion, RequireAuthenticatedEncryptionAtRest: false) { ClientId = clientId };

    /// <summary>Captures the HTTP transport exception thrown by an asynchronous action.</summary>
    /// <param name="action">The action expected to fail.</param>
    /// <returns>The captured HTTP transport exception.</returns>
    /// <exception cref="InvalidOperationException">The action did not throw an HTTP transport exception.</exception>
    private static async Task<HttpRemoteTransportException> CaptureHttpExceptionAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (HttpRemoteTransportException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("The action did not throw an HTTP transport exception.");
    }

    /// <summary>Waits for the reopened operation or throws with durable diagnostics.</summary>
    /// <param name="lease">The database lease.</param>
    /// <param name="clientA">The first client.</param>
    /// <param name="clientB">The second client.</param>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <param name="telemetryB">The second client telemetry.</param>
    /// <param name="receipt">The expected operation receipt.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The operation does not synchronize before the timeout.</exception>
    private static async Task WaitForReopenedOperationSynchronizedAsync(
        CollaborationClientDatabaseLease lease,
        CollaborationClientSession clientA,
        CollaborationClientSession clientB,
        ActivityTelemetry telemetryA,
        ActivityTelemetry telemetryB,
        PublishReceipt receipt)
    {
        try
        {
            _ = await telemetryA.Operations.WaitForAsync(IsSynchronized(receipt), WaitTimeout).ConfigureAwait(false);
        }
        catch (TaskCanceledException exception)
        {
            var snapshot = CaptureReopenedTelemetry(telemetryA, telemetryB);
            await StopClientsAsync(clientA, clientB).ConfigureAwait(false);
            await clientB.DisposeAsync().ConfigureAwait(false);
            await clientA.DisposeAsync().ConfigureAwait(false);
            var durable = await ReadActualClientOperationStatusAsync(lease.ClientAPath, receipt.OperationId)
                .ConfigureAwait(false);
            throw new InvalidOperationException(
                CreateReopenedOperationTimeoutMessage(receipt, durable, snapshot),
                exception);
        }
    }

    /// <summary>Reads one actual client operation status from the public SQLite adapter.</summary>
    /// <param name="databasePath">The client database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The durable operation status.</returns>
    private static async Task<SyncOperationStatus?> ReadActualClientOperationStatusAsync(
        string databasePath,
        OperationId operationId)
    {
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        await store.InitializeAsync(CreateStoreInitialization(ClientA), CancellationToken.None).ConfigureAwait(false);
        return await store.GetOperationStatusAsync(operationId, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Creates a redacted reopened-operation timeout message.</summary>
    /// <param name="receipt">The expected receipt.</param>
    /// <param name="durable">The durable operation status.</param>
    /// <param name="snapshot">The telemetry snapshot.</param>
    /// <returns>The diagnostic message.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateReopenedOperationTimeoutMessage(
        PublishReceipt receipt,
        SyncOperationStatus? durable,
        ReopenedTelemetrySnapshot snapshot) =>
        string.Concat(
            "Timed out waiting for reopened operation synchronization. operation=",
            receipt.OperationId.Value.ToString("N"),
            "; durable=",
            FormatOperationStatus(durable),
            "; telemetryA=",
            snapshot.CreatedA.ToString("O", CultureInfo.InvariantCulture),
            "; telemetryB=",
            snapshot.CreatedB.ToString("O", CultureInfo.InvariantCulture),
            "; operationsA=",
            FormatOperationStatuses(snapshot.OperationsA),
            "; operationsB=",
            FormatOperationStatuses(snapshot.OperationsB),
            "; faultsA=",
            FormatFaults(snapshot.FaultsA),
            "; faultsB=",
            FormatFaults(snapshot.FaultsB));

    /// <summary>Captures retained telemetry before clients are disposed for durable inspection.</summary>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <param name="telemetryB">The second client telemetry.</param>
    /// <returns>The retained telemetry snapshot.</returns>
    private static ReopenedTelemetrySnapshot CaptureReopenedTelemetry(
        ActivityTelemetry telemetryA,
        ActivityTelemetry telemetryB) =>
        new(
            telemetryA.CreatedAtUtc,
            telemetryB.CreatedAtUtc,
            telemetryA.Operations.Snapshot(),
            telemetryB.Operations.Snapshot(),
            telemetryA.Faults.Snapshot(),
            telemetryB.Faults.Snapshot());

    /// <summary>Formats retained operation states without raw payloads.</summary>
    /// <param name="statuses">The operation states.</param>
    /// <returns>The formatted operation states.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FormatOperationStatuses(IEnumerable<SyncOperationStatus> statuses) =>
        string.Join(",", statuses.Select(FormatOperationStatus));

    /// <summary>Formats one operation state without raw payloads.</summary>
    /// <param name="status">The operation state.</param>
    /// <returns>The formatted operation state.</returns>
    private static string FormatOperationStatus(SyncOperationStatus? status) =>
        status is null
            ? "none"
            : string.Concat(
                status.OperationId.Value.ToString("N"),
                ":",
                status.State,
                ":attempt=",
                status.Attempt.ToString(CultureInfo.InvariantCulture),
                ":reason=",
                status.ReasonCode ?? "none");

    /// <summary>Formats retained faults without raw exception messages.</summary>
    /// <param name="faults">The retained faults.</param>
    /// <returns>The formatted faults.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FormatFaults(IEnumerable<OccasionallyConnectedFault> faults) =>
        string.Join(",", faults.Select(FormatFault));

    /// <summary>Formats one fault without raw exception messages.</summary>
    /// <param name="fault">The fault.</param>
    /// <returns>The formatted fault.</returns>
    private static string FormatFault(OccasionallyConnectedFault fault)
    {
        var operationId = fault.OperationId?.Value.ToString("N") ?? "none";
        return string.Concat(fault.Code, ":", operationId, ":", fault.Category, ":", fault.Severity);
    }

    /// <summary>Captures reopened telemetry before diagnostic cleanup.</summary>
    /// <param name="CreatedA">The first telemetry creation time.</param>
    /// <param name="CreatedB">The second telemetry creation time.</param>
    /// <param name="OperationsA">The first client operation states.</param>
    /// <param name="OperationsB">The second client operation states.</param>
    /// <param name="FaultsA">The first client faults.</param>
    /// <param name="FaultsB">The second client faults.</param>
    private sealed record ReopenedTelemetrySnapshot(
        DateTimeOffset CreatedA,
        DateTimeOffset CreatedB,
        IReadOnlyList<SyncOperationStatus> OperationsA,
        IReadOnlyList<SyncOperationStatus> OperationsB,
        IReadOnlyList<OccasionallyConnectedFault> FaultsA,
        IReadOnlyList<OccasionallyConnectedFault> FaultsB);

    /// <summary>Describes the actual client resume state recovered from the durable store.</summary>
    /// <param name="ClientSubscriptionId">The durable client subscription id.</param>
    /// <param name="ServerCursor">The durable server cursor recovered for the client subscription.</param>
    private sealed record ResumeProof(SubscriptionId ClientSubscriptionId, string ServerCursor);
}
