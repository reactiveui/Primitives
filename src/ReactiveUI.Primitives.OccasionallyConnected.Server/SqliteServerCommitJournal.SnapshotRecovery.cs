// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#nullable enable

using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <content>Provides snapshot recovery offer helpers for the server commit journal.</content>
internal sealed partial class SqliteServerCommitJournal
{
    /// <summary>Creates a snapshot offer result.</summary>
    /// <param name="status">The offer status.</param>
    /// <param name="state">The subscription state, or null when unavailable.</param>
    /// <param name="cursor">The cursor that was offered or replayed.</param>
    /// <returns>The offer result.</returns>
    private static ServerSnapshotOfferResult CreateSnapshotOfferResult(
        ServerSnapshotOfferStatus status,
        ServerSubscriptionState? state,
        string? cursor) =>
        new() { Status = status, SubscriptionState = state, Cursor = cursor };

    /// <summary>Creates operation keys for the captured view proof.</summary>
    /// <param name="identity">The trusted subscription identity.</param>
    /// <param name="view">The captured recovery view.</param>
    /// <returns>The requested operation keys.</returns>
    private static ServerOperationKey[] CreateOperationKeys(ServerSubscriptionIdentity identity, ServerSnapshotRecoveryView view)
    {
        var keys = new ServerOperationKey[view.OperationDispositions.Count];
        for (var index = 0; index < keys.Length; index++)
        {
            keys[index] = new(identity.ClientId, view.OperationDispositions[index].OperationId);
        }

        return keys;
    }

    /// <summary>Checks whether the current stream snapshot still matches the captured recovery view.</summary>
    /// <param name="identity">The trusted subscription identity.</param>
    /// <param name="view">The captured recovery view.</param>
    /// <param name="current">The current stream snapshot.</param>
    /// <returns>Whether the stream has not semantically changed.</returns>
    private static bool SnapshotMatches(ServerSubscriptionIdentity identity, ServerSnapshotRecoveryView view, ServerCommitSnapshot current) =>
        view.Snapshot.Revision == current.Revision
        && view.Snapshot.LastEventSequence == current.LastEventSequence
        && view.Snapshot.LastGroupSequence == current.LastGroupSequence
        && string.Equals(view.Snapshot.LastCursor, current.LastCursor, StringComparison.Ordinal)
        && ServerSnapshotRecoveryJournalOperations.PositiveProofsMatch(identity, view, current);

    /// <summary>Checks whether a recovered checkpoint matches the durable frontier.</summary>
    /// <param name="request">The offer request.</param>
    /// <param name="checkpoint">The already validated recovered snapshot checkpoint.</param>
    /// <param name="stream">The stream record.</param>
    /// <returns>Whether the checkpoint is bound to the view frontier.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool CheckpointMatches(
        ServerSnapshotOfferRequest request,
        RemoteSnapshotCheckpoint checkpoint,
        ServerCommitStreamRecord? stream) =>
        string.Equals(
            checkpoint.FrontierCursor,
            ServerSnapshotRecoveryJournalOperations.CreateFrontierCursor(request.StreamKey, stream, request.View.Snapshot),
            StringComparison.Ordinal);

    /// <summary>Checks whether a retained snapshot offer is an identical replay of this request.</summary>
    /// <param name="offer">The retained offer.</param>
    /// <param name="request">The offer request.</param>
    /// <returns>Whether the proof and payload match.</returns>
    private static bool SnapshotOfferMatches(ServerSubscriptionOffer offer, ServerSnapshotOfferRequest request)
    {
        var viewState = request.View.SubscriptionState;
        var checkpoint = request.RecoveryResult.Checkpoint;
        return viewState is not null
            && checkpoint is not null
            && viewState.Revision < long.MaxValue
            && offer.SnapshotStreamRevision == request.View.Snapshot.Revision
            && offer.SnapshotLastEventSequence == request.View.Snapshot.LastEventSequence
            && offer.SnapshotSubscriptionGeneration == viewState.Generation
            && offer.SnapshotOriginatingSubscriptionRevision == viewState.Revision
            && offer.SnapshotIssuedSubscriptionRevision == viewState.Revision + 1
            && offer.SnapshotFormatVersion == checkpoint.SnapshotFormatVersion
            && ServerSnapshotRecoveryJournalOperations.PayloadMatches(offer.SnapshotClientState, checkpoint.ClientState);
    }

    /// <summary>Inserts a snapshot offer row with immutable proof data.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="context">The validated insert context.</param>
    private static void InsertSnapshotOffer(
        SqliteConnection connection,
        SqliteTransaction transaction,
        in SnapshotOfferInsertContext context)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_server_journal_subscription_offers
                (subscription_id, cursor, group_sequence, offered_at_utc, logical_bytes,
                 snapshot_stream_revision, snapshot_last_event_sequence, snapshot_subscription_generation,
                 snapshot_originating_subscription_revision, snapshot_issued_subscription_revision, snapshot_format_version,
                 snapshot_client_state_payload_contract_id, snapshot_client_state_payload_schema_version,
                 snapshot_client_state_payload_content_type, snapshot_client_state_payload, snapshot_client_state_payload_hash)
            VALUES
                ($subscriptionId, $cursor, $groupSequence, $offeredAtUtc, $logicalBytes,
                 $snapshotStreamRevision, $snapshotLastEventSequence, $snapshotSubscriptionGeneration,
                 $snapshotOriginatingSubscriptionRevision, $snapshotIssuedSubscriptionRevision, $snapshotFormatVersion,
                 $snapshotClientStatePayloadContractId, $snapshotClientStatePayloadSchemaVersion,
                 $snapshotClientStatePayloadContentType, $snapshotClientStatePayload, $snapshotClientStatePayloadHash);
            """;
        AddSubscriptionIdParameter(command, context.SubscriptionId);
        _ = command.Parameters.AddWithValue(CursorParameterName, context.Checkpoint.FrontierCursor);
        _ = command.Parameters.AddWithValue(GroupSequenceParameterName, context.Request.View.Snapshot.LastGroupSequence);
        _ = command.Parameters.AddWithValue("$offeredAtUtc", FormatDateTimeOffset(context.OfferedAtUtc));
        _ = command.Parameters.AddWithValue("$logicalBytes", context.LogicalBytes);
        _ = command.Parameters.AddWithValue("$snapshotStreamRevision", context.Request.View.Snapshot.Revision);
        _ = command.Parameters.AddWithValue("$snapshotLastEventSequence", context.Request.View.Snapshot.LastEventSequence);
        _ = command.Parameters.AddWithValue("$snapshotSubscriptionGeneration", context.ViewState.Generation);
        _ = command.Parameters.AddWithValue("$snapshotOriginatingSubscriptionRevision", context.ViewState.Revision);
        _ = command.Parameters.AddWithValue("$snapshotIssuedSubscriptionRevision", context.IssuedRevision);
        _ = command.Parameters.AddWithValue("$snapshotFormatVersion", context.Checkpoint.SnapshotFormatVersion);
        AddPayloadParameters(command, "snapshotClientState", context.Checkpoint.ClientState);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Creates a capacity-exceeded snapshot offer result from current durable state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The retained subscription record.</param>
    /// <returns>The capacity-exceeded result.</returns>
    private static ServerSnapshotOfferResult CreateCapacityExceededSnapshotOfferResult(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record)
    {
        var capacityState = ReadSubscriptionRecord(connection, transaction, record.Identity.SubscriptionId);
        ArgumentExceptionHelper.ThrowIfNull(capacityState);
        return CreateSnapshotOfferResult(
            ServerSnapshotOfferStatus.CapacityExceeded,
            ServerSubscriptionJournalOperations.CreateState(capacityState),
            null);
    }

    /// <summary>Creates an offered snapshot result from current durable state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="record">The retained subscription record.</param>
    /// <param name="cursor">The offered cursor.</param>
    /// <param name="offeredUtc">The offer timestamp.</param>
    /// <returns>The offered snapshot result.</returns>
    private static ServerSnapshotOfferResult CreateOfferedSnapshotResult(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSubscriptionRecord record,
        string cursor,
        DateTimeOffset offeredUtc)
    {
        WriteLatestUtc(connection, transaction, offeredUtc);
        var updated = ReadSubscriptionRecord(connection, transaction, record.Identity.SubscriptionId);
        ArgumentExceptionHelper.ThrowIfNull(updated);
        return CreateSnapshotOfferResult(
            ServerSnapshotOfferStatus.Offered,
            ServerSubscriptionJournalOperations.CreateState(updated),
            cursor);
    }

    /// <summary>Offers a recovered snapshot cursor inside an open transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="request">The offer request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the transaction.</param>
    /// <returns>The durable offer result.</returns>
    private ServerSnapshotOfferResult TryOfferSnapshot(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSnapshotOfferRequest request,
        DateTimeOffset observedUtc)
    {
        var record = ReadSubscriptionRecord(connection, transaction, request.Subscription.SubscriptionId);
        if (record is null)
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.MissingSubscription, null, null);
        }

        return ServerSubscriptionJournalOperations.IdentityMatches(request.Subscription, record)
            ? TryOfferSnapshotForRecord(connection, transaction, request, observedUtc, record)
            : CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ValidationRejected, null, null);
    }

    /// <summary>Offers a recovered snapshot cursor for a matching subscription row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="request">The offer request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the transaction.</param>
    /// <param name="record">The retained subscription record.</param>
    /// <returns>The durable offer result.</returns>
    private ServerSnapshotOfferResult TryOfferSnapshotForRecord(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSnapshotOfferRequest request,
        DateTimeOffset observedUtc,
        ServerSubscriptionRecord record)
    {
        var currentState = ServerSubscriptionJournalOperations.CreateState(record);
        var viewState = request.View.SubscriptionState;
        var checkpoint = request.RecoveryResult.Checkpoint;
        if (viewState is null || checkpoint is null)
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ConcurrentChange, currentState, null);
        }

        if (viewState.Generation != record.Generation || viewState.Identity != record.Identity)
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ConcurrentChange, currentState, null);
        }

        var stream = ReadStreamRecord(connection, transaction, request.StreamKey);
        var current = ServerCommitJournalOperations.CreateSnapshot(request.StreamKey, stream, CreateOperationKeys(record.Identity, request.View));
        if (!SnapshotMatches(record.Identity, request.View, current))
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ConcurrentChange, currentState, null);
        }

        return CheckpointMatches(request, checkpoint, stream)
            && request.View.Snapshot.LastGroupSequence >= record.AcknowledgedGroupSequence
            ? TryPersistSnapshotOffer(connection, transaction, request, observedUtc, record, viewState, checkpoint)
            : CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ValidationRejected, currentState, null);
    }

    /// <summary>Persists a recovered snapshot offer after all durable fences match.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="request">The offer request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the transaction.</param>
    /// <param name="record">The retained subscription record.</param>
    /// <param name="viewState">The subscription state captured in the recovery view.</param>
    /// <param name="checkpoint">The recovered snapshot checkpoint.</param>
    /// <returns>The durable offer result.</returns>
    private ServerSnapshotOfferResult TryPersistSnapshotOffer(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ServerSnapshotOfferRequest request,
        DateTimeOffset observedUtc,
        ServerSubscriptionRecord record,
        ServerSubscriptionState viewState,
        RemoteSnapshotCheckpoint checkpoint)
    {
        var currentState = ServerSubscriptionJournalOperations.CreateState(record);
        var cursor = checkpoint.FrontierCursor;
        if (record.Offers.TryGetValue(cursor, out var existing)
            && SnapshotOfferMatches(existing, request)
            && existing.SnapshotIssuedSubscriptionRevision == record.Revision)
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.AlreadyOffered, currentState, cursor);
        }

        if (record.Offers.ContainsKey(cursor) || viewState.Revision != record.Revision)
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ConcurrentChange, currentState, null);
        }

        var issuedRevision = ServerSubscriptionJournalOperations.GetNextSubscriptionRevision(record);
        var offeredUtc = ServerCommitJournalOperations.Max(ReadLatestUtc(connection, transaction), observedUtc);
        var latestDelta = request.View.Snapshot.LastGroupSequence > record.LatestOfferedGroupSequence
            ? ServerSubscriptionJournalOperations.GetSubscriptionCursorDelta(record.LatestOfferedCursor, cursor)
            : 0;
        var logicalBytes = ServerSubscriptionJournalOperations.GetSnapshotOfferBytes(cursor, checkpoint.ClientState);
        var addedLogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, latestDelta);
        if (!HasSnapshotOfferCapacity(connection, transaction, offeredUtc, addedLogicalBytes))
        {
            return CreateCapacityExceededSnapshotOfferResult(connection, transaction, record);
        }

        var insertContext = new SnapshotOfferInsertContext(
            record.Identity.SubscriptionId,
            request,
            viewState,
            checkpoint,
            offeredUtc,
            logicalBytes,
            issuedRevision);
        InsertSnapshotOffer(connection, transaction, in insertContext);
        UpdatePersistedOfferState(
            connection,
            transaction,
            record,
            cursor,
            request.View.Snapshot.LastGroupSequence,
            offeredUtc,
            issuedRevision);
        return CreateOfferedSnapshotResult(connection, transaction, record, cursor, offeredUtc);
    }

    /// <summary>Checks capacity for a snapshot offer, compacting expired offers once when necessary.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="offeredUtc">The offer timestamp.</param>
    /// <param name="addedLogicalBytes">The logical bytes required by the offer.</param>
    /// <returns>Whether the snapshot offer can fit.</returns>
    private bool HasSnapshotOfferCapacity(
        SqliteConnection connection,
        SqliteTransaction transaction,
        DateTimeOffset offeredUtc,
        long addedLogicalBytes)
    {
        if (HasSubscriptionCapacity(connection, transaction, 0, 1, addedLogicalBytes, _options))
        {
            return true;
        }

        DeleteExpiredOffers(connection, transaction, offeredUtc, _options);
        return HasSubscriptionCapacity(connection, transaction, 0, 1, addedLogicalBytes, _options);
    }

    /// <summary>Groups the already validated fields needed to insert a snapshot offer row.</summary>
    /// <param name="SubscriptionId">The subscription id.</param>
    /// <param name="Request">The offer request.</param>
    /// <param name="ViewState">The subscription state captured in the recovery view.</param>
    /// <param name="Checkpoint">The recovered snapshot checkpoint.</param>
    /// <param name="OfferedAtUtc">The offer timestamp.</param>
    /// <param name="LogicalBytes">The retained offer bytes.</param>
    /// <param name="IssuedRevision">The assigned subscription revision.</param>
    private readonly record struct SnapshotOfferInsertContext(
        SubscriptionId SubscriptionId,
        ServerSnapshotOfferRequest Request,
        ServerSubscriptionState ViewState,
        RemoteSnapshotCheckpoint Checkpoint,
        DateTimeOffset OfferedAtUtc,
        long LogicalBytes,
        long IssuedRevision);
}
