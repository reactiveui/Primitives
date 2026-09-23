// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using static ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.DurableHttpLostAckProofEvaluator;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the durable HTTP lost acknowledgement recovery scenario.</summary>
internal static partial class DurableHttpLostAckScenario
{
    /// <summary>Reads a snapshot counter value.</summary>
    /// <param name="snapshot">The snapshot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    internal static async ValueTask<long?> ReadSnapshotCounterAsync(LocalSnapshot? snapshot, CancellationToken cancellationToken)
    {
        if (snapshot is null)
        {
            return null;
        }

        var serializer = new CrdtPayloadSerializer(CreateBounds());
        var deserialized = await serializer.DeserializeAsync(snapshot.State, typeof(CrdtState), cancellationToken).ConfigureAwait(false);
        return deserialized is CrdtState state ? state.Value.Counter : null;
    }

    /// <summary>Reads durable state through a session's initialized SQLite store.</summary>
    /// <param name="store">The initialized local store.</param>
    /// <param name="databasePath">The database path.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable store proof.</returns>
    internal static async ValueTask<ClientStoreProof> ReadInitializedClientStoreProofAsync(
        SqliteLocalStoreAdapter store,
        string databasePath,
        string storeIdentity,
        SubscriptionId subscriptionId,
        OperationId? operationId,
        CancellationToken cancellationToken)
    {
        var storedSubscription = await store.GetOrCreateSubscriptionIdAsync(Stream, subscriptionId, cancellationToken).ConfigureAwait(false);
        var recovered = await store.RecoverStreamAsync(Stream, storedSubscription, cancellationToken).ConfigureAwait(false);
        var status = await ReadOperationStatusAsync(store, operationId, cancellationToken).ConfigureAwait(false);
        var retryState = await ReadRetryStateAsync(store, operationId, cancellationToken).ConfigureAwait(false);
        var snapshotCounter = await ReadSnapshotCounterAsync(recovered.Snapshot, cancellationToken).ConfigureAwait(false);
        var inboxCount = ReadInboxCount(databasePath, storeIdentity);
        return new(
            storedSubscription,
            recovered.ServerCursor,
            snapshotCounter,
            GetPendingOperationId(recovered),
            GetPendingClientSequence(recovered),
            recovered.PendingOperations.Count,
            status,
            retryState,
            inboxCount);
    }

    /// <summary>Creates proof cases from observed durable facts.</summary>
    /// <param name="proof">The proof.</param>
    /// <returns>The result.</returns>
    private static IReadOnlyList<ResilienceLabCaseResult> BuildCases(LostAckProof proof) =>
    [
        Case("durable-http-lost-ack.first-ack-lost", true, proof.FirstAckLost),
        Case("durable-http-lost-ack.server-effect-before-ack-loss", 1, proof.ServerEffectCountBeforeAckLoss),
        Case("durable-http-lost-ack.operation-id-reused", proof.OriginalOperationId, proof.PersistedOperationId),
        Case("durable-http-lost-ack.client-sequence-persisted", proof.OriginalClientSequence, proof.PersistedClientSequence ?? 0),
        Case("durable-http-lost-ack.persisted-pending-before-reopen", "pending", proof.PendingBeforeReopen),
        Case("durable-http-lost-ack.pending-survives-client-close", true, proof.PendingSurvivesClose),
        Case("durable-http-lost-ack.retry-uses-persisted-operation-id", proof.PersistedOperationId, proof.RetriedOperationId),
        CaseAtLeast("durable-http-lost-ack.push-attempts", MinimumPushRequests, proof.PushAttempts),
        Case("durable-http-lost-ack.server-effect-count", 1, proof.ServerEffectCount),
        CaseAtLeast("durable-http-lost-ack.client-attempt-count", MinimumPushRequests, proof.ClientAttemptCount),
        Case("durable-http-lost-ack.client-final-status", SyncOperationState.Synchronized.ToString(), proof.FinalStatus),
        Case("durable-http-lost-ack.writer-pending-after-retry", 0, proof.WriterPendingCount),
        Case("durable-http-lost-ack.subscription-id-stable", "same", proof.SubscriptionStability),
        Case("durable-http-lost-ack.cursor-restored-and-advanced", "advanced", proof.CursorProgress),
        Case("durable-http-lost-ack.snapshot-restored", "restored", proof.SnapshotRestore),
        CaseAtLeast("durable-http-lost-ack.remote-notification-observed", 1, proof.RemoteNotificationCount),
        Case("durable-http-lost-ack.observer-inbox-effect-count", 1, proof.ObserverInboxCount),
        Case("durable-http-lost-ack.observer-final-counter", 1L, proof.ObserverCounter),
        Case("durable-http-lost-ack.observer-subscription-distinct", "distinct", proof.ObserverSubscriptionDistinct),
        Case("durable-http-lost-ack.no-terminal-faults", 0, proof.TerminalFaultCount),
    ];

    /// <summary>Creates the immutable proof from observed workflow facts.</summary>
    /// <param name="first">The first.</param>
    /// <param name="second">The second.</param>
    /// <param name="observer">The observer.</param>
    /// <param name="writerFinal">The writer final.</param>
    /// <param name="observerFinal">The observer final.</param>
    /// <param name="host">The host.</param>
    /// <param name="serverEffectCount">The server effect count.</param>
    /// <returns>The result.</returns>
    private static LostAckProof CreateProof(
        FirstClientProof first,
        SecondClientProof second,
        ObserverProof observer,
        ClientStoreProof writerFinal,
        ClientStoreProof observerFinal,
        DurableHttpLostAckHost host,
        int serverEffectCount)
    {
        var beforeRestart = first.BeforeRestart;
        var original = Format(first.Receipt.OperationId);
        var persisted = Format(beforeRestart.PendingOperationId);
        var retried = Format(second.RetryPushOperationId);
        return new(
            host.FirstPushResponseAborted,
            first.ServerEffectCountBeforeAckLoss,
            original,
            persisted,
            first.Receipt.ClientSequence,
            beforeRestart.PendingClientSequence,
            IsPendingRetryable(beforeRestart) ? "pending" : FormatPendingState(beforeRestart),
            PendingSurvivesClose(first.BeforeClose, beforeRestart),
            retried,
            host.PushRequestCount,
            serverEffectCount,
            writerFinal.OperationStatus?.Attempt ?? 0,
            second.ObservedSynchronized.State.ToString(),
            writerFinal.PendingCount,
            first.SubscriptionId == second.SubscriptionId ? "same" : $"{first.SubscriptionId.Value:D}!={second.SubscriptionId.Value:D}",
            CursorAdvanced(beforeRestart, writerFinal) ? "advanced" : FormatCursorProgress(beforeRestart, writerFinal),
            SnapshotRestored(writerFinal, observerFinal) ? "restored" : FormatSnapshotProof(writerFinal, observerFinal),
            observer.RemoteNotificationCount,
            observerFinal.InboxCount,
            observer.Counter,
            observer.SubscriptionId != first.SubscriptionId ? "distinct" : "same",
            first.TerminalFaultCount + second.TerminalFaultCount + observer.TerminalFaultCount);
    }

    /// <summary>Reads writer durable state without driving local store transitions.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<ClientStoreProof> ReadWriterStoreProofAsync(
        string databasePath,
        MutableTimeProvider clock,
        OperationId operationId,
        CancellationToken cancellationToken) =>
        await ReadClientStoreProofAsync(
            databasePath,
            clock,
            WriterClientId,
            WriterStoreIdentity,
            WriterSubscription,
            operationId,
            cancellationToken).ConfigureAwait(false);

    /// <summary>Reads durable client state without driving local store transitions.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="clientId">The client id.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<ClientStoreProof> ReadClientStoreProofAsync(
        string databasePath,
        MutableTimeProvider clock,
        string clientId,
        string storeIdentity,
        SubscriptionId subscriptionId,
        OperationId? operationId,
        CancellationToken cancellationToken)
    {
        await using var store = CreateSqliteStore(databasePath, clock);
        await store.InitializeAsync(CreateStoreInitialization(clientId, storeIdentity), cancellationToken).ConfigureAwait(false);
        return await ReadInitializedClientStoreProofAsync(store, databasePath, storeIdentity, subscriptionId, operationId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads an optional operation status.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<SyncOperationStatus?> ReadOperationStatusAsync(
        SqliteLocalStoreAdapter store,
        OperationId? operationId,
        CancellationToken cancellationToken) =>
        operationId.HasValue ? await store.GetOperationStatusAsync(operationId.Value, cancellationToken).ConfigureAwait(false) : null;

    /// <summary>Reads an optional retry state.</summary>
    /// <param name="store">The store.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<RetryState?> ReadRetryStateAsync(
        SqliteLocalStoreAdapter store,
        OperationId? operationId,
        CancellationToken cancellationToken) =>
        operationId.HasValue ? await store.GetRetryStateAsync(operationId.Value, cancellationToken).ConfigureAwait(false) : null;

    /// <summary>Reads the durable server effect count for one operation from the server journal.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The result.</returns>
    private static int ReadServerEffectCount(string databasePath, OperationId operationId)
    {
        using var connection = new SqliteConnection(CreateSqliteConnectionString(databasePath, SqliteOpenMode.ReadOnly));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM oc_server_journal_events
            WHERE tenant_id = $tenantId
              AND stream_id = $streamId
              AND client_id = $clientId
              AND operation_id = $operationId;
            """;
        _ = command.Parameters.AddWithValue("$tenantId", TenantId);
        _ = command.Parameters.AddWithValue("$streamId", Stream.Value);
        _ = command.Parameters.AddWithValue("$clientId", WriterClientId);
        _ = command.Parameters.AddWithValue("$operationId", operationId.Value.ToString("D"));
        return Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    /// <summary>Reads durable receive inbox rows for one client store.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <returns>The result.</returns>
    private static int ReadInboxCount(string databasePath, string storeIdentity)
    {
        using var connection = new SqliteConnection(CreateSqliteConnectionString(databasePath, SqliteOpenMode.ReadOnly));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM oc_inbox
            WHERE store_identity = $storeIdentity
              AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$storeIdentity", storeIdentity);
        _ = command.Parameters.AddWithValue("$streamId", Stream.Value);
        return Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    /// <summary>Gets the single pending operation id when present.</summary>
    /// <param name="recovered">The recovered.</param>
    /// <returns>The result.</returns>
    private static OperationId? GetPendingOperationId(RecoveredStream recovered) =>
        recovered.PendingOperations.Count == 1 ? recovered.PendingOperations[0].OperationId : null;

    /// <summary>Gets the sequence of the single pending operation when present.</summary>
    /// <param name="recovered">The recovered.</param>
    /// <returns>The result.</returns>
    private static long? GetPendingClientSequence(RecoveredStream recovered) =>
        recovered.PendingOperations.Count == 1 ? recovered.PendingOperations[0].ClientSequence : null;

    /// <summary>Creates a standard equality case.</summary>
    /// <param name="name">The name.</param>
    /// <param name="expected">The expected.</param>
    /// <param name="actual">The actual.</param>
    /// <returns>The result.</returns>
    private static ResilienceLabCaseResult Case(string name, object expected, object actual) =>
        new(name, expected, actual, Equals(expected, actual));

    /// <summary>Creates a minimum numeric case.</summary>
    /// <param name="name">The name.</param>
    /// <param name="minimum">The minimum.</param>
    /// <param name="actual">The actual.</param>
    /// <returns>The result.</returns>
    private static ResilienceLabCaseResult CaseAtLeast(string name, int minimum, int actual) =>
        new(name, minimum, actual, actual >= minimum);

    /// <summary>Creates the CRDT bounds used by the lab stream.</summary>
    /// <returns>The result.</returns>
    private static CrdtBounds CreateBounds() =>
        new()
        {
            MaximumCounterComponents = SmallCapacity,
            MaximumDotBindings = JournalCapacity,
            MaximumTombstones = JournalCapacity,
            MaximumElements = SmallCapacity,
            MaximumElementBytes = ElementBytes,
            MaximumRegisterBytes = ElementBytes,
            MaximumEncodedBytes = CounterEncodedBytes,
        };

    /// <summary>The collected lost-ACK proof facts.</summary>
    /// <param name="FirstAckLost">The first ack lost.</param>
    /// <param name="ServerEffectCountBeforeAckLoss">The server effect count before ack loss.</param>
    /// <param name="OriginalOperationId">The original operation id.</param>
    /// <param name="PersistedOperationId">The persisted operation id.</param>
    /// <param name="OriginalClientSequence">The original client sequence.</param>
    /// <param name="PersistedClientSequence">The persisted client sequence.</param>
    /// <param name="PendingBeforeReopen">The pending before reopen.</param>
    /// <param name="PendingSurvivesClose">Whether pending SQLite state survives client closure.</param>
    /// <param name="RetriedOperationId">The retried operation id.</param>
    /// <param name="PushAttempts">The push attempts.</param>
    /// <param name="ServerEffectCount">The server effect count.</param>
    /// <param name="ClientAttemptCount">The client attempt count.</param>
    /// <param name="FinalStatus">The final status.</param>
    /// <param name="WriterPendingCount">The writer's remaining durable pending operation count.</param>
    /// <param name="SubscriptionStability">The subscription stability.</param>
    /// <param name="CursorProgress">The cursor progress.</param>
    /// <param name="SnapshotRestore">The snapshot restore.</param>
    /// <param name="RemoteNotificationCount">The remote notification count.</param>
    /// <param name="ObserverInboxCount">The observer inbox count.</param>
    /// <param name="ObserverCounter">The observer counter.</param>
    /// <param name="ObserverSubscriptionDistinct">The observer subscription distinct.</param>
    /// <param name="TerminalFaultCount">The terminal fault count.</param>
    private sealed record LostAckProof(
        bool FirstAckLost,
        int ServerEffectCountBeforeAckLoss,
        string OriginalOperationId,
        string PersistedOperationId,
        long OriginalClientSequence,
        long? PersistedClientSequence,
        string PendingBeforeReopen,
        bool PendingSurvivesClose,
        string RetriedOperationId,
        int PushAttempts,
        int ServerEffectCount,
        int ClientAttemptCount,
        string FinalStatus,
        int WriterPendingCount,
        string SubscriptionStability,
        string CursorProgress,
        string SnapshotRestore,
        int RemoteNotificationCount,
        int ObserverInboxCount,
        long ObserverCounter,
        string ObserverSubscriptionDistinct,
        int TerminalFaultCount);

    /// <summary>The first writer proof facts.</summary>
    /// <param name="Receipt">The receipt.</param>
    /// <param name="SubscriptionId">The subscription id.</param>
    /// <param name="BeforeClose">The active-store proof before client closure.</param>
    /// <param name="BeforeRestart">The fresh-store proof after client closure.</param>
    /// <param name="ServerEffectCountBeforeAckLoss">The server effect count before ack loss.</param>
    /// <param name="TerminalFaultCount">The terminal fault count.</param>
    private sealed record FirstClientProof(
        PublishReceipt Receipt,
        SubscriptionId SubscriptionId,
        ClientStoreProof BeforeClose,
        ClientStoreProof BeforeRestart,
        int ServerEffectCountBeforeAckLoss,
        int TerminalFaultCount);

    /// <summary>The reopened writer proof facts.</summary>
    /// <param name="SubscriptionId">The subscription id.</param>
    /// <param name="RetryPushOperationId">The retry push operation id.</param>
    /// <param name="ObservedSynchronized">The observed synchronized.</param>
    /// <param name="TerminalFaultCount">The terminal fault count.</param>
    private sealed record SecondClientProof(
        SubscriptionId SubscriptionId,
        OperationId RetryPushOperationId,
        SyncOperationStatus ObservedSynchronized,
        int TerminalFaultCount);

    /// <summary>The observer proof facts.</summary>
    /// <param name="SubscriptionId">The subscription id.</param>
    /// <param name="RemoteNotificationCount">The remote notification count.</param>
    /// <param name="Counter">The counter.</param>
    /// <param name="TerminalFaultCount">The terminal fault count.</param>
    private sealed record ObserverProof(
        SubscriptionId SubscriptionId,
        int RemoteNotificationCount,
        long Counter,
        int TerminalFaultCount);
}
