// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Data;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores durable atomic server state, events and terminal operation replays in SQLite.</summary>
/// <remarks>
/// Authenticated tenant and client identifiers are trusted inputs from the host. This journal does not perform
/// authorization, network coordination or capability advertisement.
/// </remarks>
internal sealed partial class SqliteServerCommitJournal : IServerCommitJournal, IServerReceiveJournal, IServerSubscriptionAcknowledgementJournal, IServerSnapshotRecoveryJournal, IDisposable
{
    /// <summary>The current durable schema version.</summary>
    private const int CurrentSchemaVersion = 5;

    /// <summary>The previous durable schema version.</summary>
    private const int SchemaVersionFour = 4;

    /// <summary>The previous durable schema version.</summary>
    private const int SchemaVersionThree = 3;

    /// <summary>The previous durable schema version.</summary>
    private const int SchemaVersionTwo = 2;

    /// <summary>The original durable schema version.</summary>
    private const int SchemaVersionOne = 1;

    /// <summary>The SQL statement that stamps schema version three during migration.</summary>
    private const string SetSchemaVersionThreeSql = "PRAGMA user_version = 3;";

    /// <summary>The SQL statement that stamps schema version two during migration.</summary>
    private const string SetSchemaVersionTwoSql = "PRAGMA user_version = 2;";

    /// <summary>The metadata key for the schema version.</summary>
    private const string SchemaVersionKey = "schema_version";

    /// <summary>The metadata key for the latest retained UTC high-water timestamp.</summary>
    private const string LatestUtcKey = "latest_utc";

    /// <summary>The metadata key for the durable subscription generation high-water value.</summary>
    private const string SubscriptionGenerationHighWaterKey = "subscription_generation_high_water";

    /// <summary>The SQLite integer value for FULL synchronous writes.</summary>
    private const long SqliteFullSynchronous = 2;

    /// <summary>The SQLite metadata table.</summary>
    private const string MetadataTableName = "oc_server_journal_metadata";

    /// <summary>The SQLite stream table.</summary>
    private const string StreamsTableName = "oc_server_journal_streams";

    /// <summary>The SQLite ledger table.</summary>
    private const string LedgerTableName = "oc_server_journal_ledger";

    /// <summary>The SQLite conflict table.</summary>
    private const string ConflictsTableName = "oc_server_journal_conflicts";

    /// <summary>The SQLite events table.</summary>
    private const string EventsTableName = "oc_server_journal_events";

    /// <summary>The SQLite event metadata table.</summary>
    private const string EventMetadataTableName = "oc_server_journal_event_metadata";

    /// <summary>The SQLite subscription acknowledgement table.</summary>
    private const string SubscriptionsTableName = "oc_server_journal_subscriptions";

    /// <summary>The SQLite subscription offer table.</summary>
    private const string SubscriptionOffersTableName = "oc_server_journal_subscription_offers";

    /// <summary>The invalid schema exception message.</summary>
    private const string InvalidSchemaMessage = "The SQLite server journal schema is invalid.";

    /// <summary>The invalid event sequence exception message.</summary>
    private const string InvalidEventSequenceMessage = "The SQLite server journal event sequence is invalid.";

    /// <summary>The invalid logical byte count exception message.</summary>
    private const string InvalidLogicalBytesMessage = "The SQLite server journal logical bytes are invalid.";

    /// <summary>The unsupported metadata schema version message.</summary>
    private const string UnsupportedMetadataSchemaVersionMessage = "The SQLite server journal metadata schema version is not supported.";

    /// <summary>The event sequence SQL parameter name.</summary>
    private const string EventSequenceParameterName = "$eventSequence";

    /// <summary>The metadata value SQL parameter name.</summary>
    private const string ValueParameterName = "$value";

    /// <summary>The UTC timestamp SQL parameter name.</summary>
    private const string UtcNowParameterName = "$utcNow";

    /// <summary>The payload contract id parameter suffix.</summary>
    private const string PayloadContractIdSuffix = "PayloadContractId";

    /// <summary>The payload schema version parameter suffix.</summary>
    private const string PayloadSchemaVersionSuffix = "PayloadSchemaVersion";

    /// <summary>The payload content type parameter suffix.</summary>
    private const string PayloadContentTypeSuffix = "PayloadContentType";

    /// <summary>The payload parameter suffix.</summary>
    private const string PayloadSuffix = "Payload";

    /// <summary>The payload hash parameter suffix.</summary>
    private const string PayloadHashSuffix = "PayloadHash";

    /// <summary>The SQL definition for the metadata table.</summary>
    private const string MetadataTableSql = "CREATE TABLE oc_server_journal_metadata (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL);";

    /// <summary>The SQL definition for the streams table.</summary>
    private const string StreamsTableSql = """
        CREATE TABLE oc_server_journal_streams (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            revision INTEGER NOT NULL,
            state_version TEXT NULL,
            state_payload_contract_id TEXT NULL,
            state_payload_schema_version INTEGER NULL,
            state_payload_content_type TEXT NULL,
            state_payload BLOB NULL,
            state_payload_hash TEXT NULL,
            write_stamp_committed_at_utc TEXT NULL,
            write_stamp_client_id TEXT NULL,
            write_stamp_operation_id TEXT NULL,
            last_cursor TEXT NULL,
            last_event_sequence INTEGER NOT NULL,
            state_bytes INTEGER NOT NULL,
            last_cursor_bytes INTEGER NOT NULL,
            last_group_sequence INTEGER NOT NULL,
            receive_history_incomplete INTEGER NOT NULL,
            PRIMARY KEY (tenant_id, stream_id));
        """;

    /// <summary>The SQL definition for the ledger table.</summary>
    private const string LedgerTableSql = """
        CREATE TABLE oc_server_journal_ledger (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            fingerprint BLOB NOT NULL,
            result_kind INTEGER NOT NULL,
            result_reason_code TEXT NULL,
            result_server_version TEXT NULL,
            committed_at_utc TEXT NOT NULL,
            expires_at_utc TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL,
            group_sequence INTEGER NULL,
            PRIMARY KEY (tenant_id, stream_id, client_id, operation_id),
            FOREIGN KEY (tenant_id, stream_id)
                REFERENCES oc_server_journal_streams (tenant_id, stream_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the conflict table.</summary>
    private const string ConflictsTableSql = """
        CREATE TABLE oc_server_journal_conflicts (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            conflict_index INTEGER NOT NULL,
            resolution_code TEXT NOT NULL,
            resolved_payload_contract_id TEXT NULL,
            resolved_payload_schema_version INTEGER NULL,
            resolved_payload_content_type TEXT NULL,
            resolved_payload BLOB NULL,
            resolved_payload_hash TEXT NULL,
            PRIMARY KEY (tenant_id, stream_id, client_id, operation_id, conflict_index),
            FOREIGN KEY (tenant_id, stream_id, client_id, operation_id)
                REFERENCES oc_server_journal_ledger (tenant_id, stream_id, client_id, operation_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the events table.</summary>
    private const string EventsTableSql = """
        CREATE TABLE oc_server_journal_events (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            event_sequence INTEGER NOT NULL,
            client_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            event_index INTEGER NOT NULL,
            event_id TEXT NOT NULL,
            server_cursor TEXT NOT NULL,
            committed_at_utc TEXT NOT NULL,
            caused_by_operation_id TEXT NULL,
            origin_client_id TEXT NULL,
            origin_operation_id TEXT NULL,
            payload_contract_id TEXT NOT NULL,
            payload_schema_version INTEGER NOT NULL,
            payload_content_type TEXT NOT NULL,
            payload BLOB NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY (tenant_id, stream_id, event_sequence),
            UNIQUE (tenant_id, stream_id, event_id),
            UNIQUE (tenant_id, stream_id, server_cursor),
            FOREIGN KEY (tenant_id, stream_id, client_id, operation_id)
                REFERENCES oc_server_journal_ledger (tenant_id, stream_id, client_id, operation_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the event metadata table.</summary>
    private const string EventMetadataTableSql = """
        CREATE TABLE oc_server_journal_event_metadata (
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            event_sequence INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (tenant_id, stream_id, event_sequence, key),
            FOREIGN KEY (tenant_id, stream_id, event_sequence)
                REFERENCES oc_server_journal_events (tenant_id, stream_id, event_sequence)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the subscription acknowledgement table.</summary>
    private const string SubscriptionsTableSql = """
        CREATE TABLE oc_server_journal_subscriptions (
            subscription_id TEXT NOT NULL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            initial_position_kind INTEGER NOT NULL,
            initial_sequence INTEGER NULL,
            initial_timestamp_utc TEXT NULL,
            initial_cursor TEXT NULL,
            initial_anchor_cursor TEXT NULL,
            initial_anchor_group_sequence INTEGER NOT NULL,
            initial_anchor_resolved INTEGER NOT NULL,
            acknowledged_cursor TEXT NULL,
            acknowledged_group_sequence INTEGER NOT NULL,
            latest_offered_cursor TEXT NULL,
            latest_offered_group_sequence INTEGER NOT NULL,
            acknowledged_at_utc TEXT NULL,
            updated_at_utc TEXT NOT NULL,
            last_touched_utc TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL,
            generation INTEGER NOT NULL DEFAULT 0,
            revision INTEGER NOT NULL DEFAULT 0);
        """;

    /// <summary>The SQL definition for the schema-four subscription acknowledgement table.</summary>
    private const string SchemaFourSubscriptionsTableSql = """
        CREATE TABLE oc_server_journal_subscriptions (
            subscription_id TEXT NOT NULL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            initial_position_kind INTEGER NOT NULL,
            initial_sequence INTEGER NULL,
            initial_timestamp_utc TEXT NULL,
            initial_cursor TEXT NULL,
            initial_anchor_cursor TEXT NULL,
            initial_anchor_group_sequence INTEGER NOT NULL,
            initial_anchor_resolved INTEGER NOT NULL,
            acknowledged_cursor TEXT NULL,
            acknowledged_group_sequence INTEGER NOT NULL,
            latest_offered_cursor TEXT NULL,
            latest_offered_group_sequence INTEGER NOT NULL,
            acknowledged_at_utc TEXT NULL,
            updated_at_utc TEXT NOT NULL,
            last_touched_utc TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL);
        """;

    /// <summary>The SQL definition for the schema-three subscription acknowledgement table.</summary>
    private const string SchemaThreeSubscriptionsTableSql = """
        CREATE TABLE oc_server_journal_subscriptions (
            subscription_id TEXT NOT NULL PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            stream_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            acknowledged_cursor TEXT NULL,
            acknowledged_group_sequence INTEGER NOT NULL,
            latest_offered_cursor TEXT NULL,
            latest_offered_group_sequence INTEGER NOT NULL,
            acknowledged_at_utc TEXT NULL,
            updated_at_utc TEXT NOT NULL,
            last_touched_utc TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL);
        """;

    /// <summary>The SQL definition for the subscription offer table.</summary>
    private const string SubscriptionOffersTableSql = """
        CREATE TABLE oc_server_journal_subscription_offers (
            subscription_id TEXT NOT NULL,
            cursor TEXT NOT NULL,
            group_sequence INTEGER NOT NULL,
            offered_at_utc TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL,
            snapshot_stream_revision INTEGER NULL,
            snapshot_last_event_sequence INTEGER NULL,
            snapshot_subscription_generation INTEGER NULL,
            snapshot_originating_subscription_revision INTEGER NULL,
            snapshot_issued_subscription_revision INTEGER NULL,
            snapshot_format_version INTEGER NULL,
            snapshot_client_state_payload_contract_id TEXT NULL,
            snapshot_client_state_payload_schema_version INTEGER NULL,
            snapshot_client_state_payload_content_type TEXT NULL,
            snapshot_client_state_payload BLOB NULL,
            snapshot_client_state_payload_hash TEXT NULL,
            PRIMARY KEY (subscription_id, cursor),
            FOREIGN KEY (subscription_id)
                REFERENCES oc_server_journal_subscriptions (subscription_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The SQL definition for the schema-four subscription offer table.</summary>
    private const string SchemaFourSubscriptionOffersTableSql = """
        CREATE TABLE oc_server_journal_subscription_offers (
            subscription_id TEXT NOT NULL,
            cursor TEXT NOT NULL,
            group_sequence INTEGER NOT NULL,
            offered_at_utc TEXT NOT NULL,
            logical_bytes INTEGER NOT NULL,
            PRIMARY KEY (subscription_id, cursor),
            FOREIGN KEY (subscription_id)
                REFERENCES oc_server_journal_subscriptions (subscription_id)
                ON DELETE CASCADE);
        """;

    /// <summary>The canonical strict string encoding used for schema normalization.</summary>
    private static readonly Encoding TextEncoding = new UTF8Encoding(false, true);

    /// <summary>The SQLite database path.</summary>
    private readonly string _databasePath;

    /// <summary>The journal options.</summary>
    private readonly ServerCommitJournalOptions _options;

    /// <summary>Whether this instance has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SqliteServerCommitJournal"/> class.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="options">The finite journal bounds.</param>
    internal SqliteServerCommitJournal(string databasePath, ServerCommitJournalOptions? options = null)
    {
        ArgumentExceptionHelper.ThrowIfNull(databasePath);
        ThrowIfBlank(databasePath, nameof(databasePath));
        ThrowIfUnsupportedPath(databasePath);

        _databasePath = Path.GetFullPath(databasePath);
        _options = options ?? new();
        _options.Validate();
        InitializeSchema();
    }

    /// <summary>Gets the current retained stream count.</summary>
    internal int StreamCount => ReadMetrics().StreamCount;

    /// <summary>Gets the current retained terminal entry count.</summary>
    internal int LedgerEntryCount => ReadMetrics().LedgerEntryCount;

    /// <summary>Gets the current retained event count.</summary>
    internal int EventCount => ReadMetrics().EventCount;

    /// <summary>Gets the current retained subscription count.</summary>
    internal int SubscriptionCount => ReadMetrics().SubscriptionCount;

    /// <summary>Gets the current retained subscription offer count.</summary>
    internal int SubscriptionOfferCount => ReadMetrics().SubscriptionOfferCount;

    /// <summary>Gets the retained logical encoded byte count.</summary>
    internal long LogicalBytes => ReadMetrics().LogicalBytes;

    /// <inheritdoc/>
    public void Dispose() => _disposed = true;

    /// <summary>Reads a stream revision and requested terminal operation entries atomically.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="operationKeys">The bounded operation keys requested for replay.</param>
    /// <returns>The atomic stream snapshot.</returns>
    internal ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys)
    {
        ThrowIfDisposed();
        ServerCommitJournalGuard.ValidateStreamKey(streamKey);
        var requested = ServerCommitJournalGuard.CaptureOperationKeys(operationKeys, _options.MaximumOperationCaptureCount);
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var stream = ReadStreamRecord(connection, transaction, streamKey);
        var snapshot = ServerCommitJournalOperations.CreateSnapshot(streamKey, stream, requested);
        transaction.Commit();
        return snapshot;
    }

    /// <summary>Attempts to atomically admit a fully prepared terminal server commit.</summary>
    /// <param name="plan">The prepared commit plan.</param>
    /// <returns>The result and atomic stream snapshot observed by the attempt.</returns>
    internal ServerCommitResult TryCommit(ServerCommitPlan plan)
    {
        ThrowIfDisposed();
        ArgumentExceptionHelper.ThrowIfNull(plan);
        var commit = ServerCommitJournalGuard.ValidatePlan(plan, _options);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var streamExists = TryReadStreamRecord(connection, transaction, commit.StreamKey, out var stream);
        stream ??= new();
        var status = ServerCommitJournalOperations.GetPreCommitStatus(stream, commit);
        if (status != ServerCommitStatus.Committed)
        {
            var rejectedSnapshot = ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys);
            transaction.Commit();
            return new(status, rejectedSnapshot);
        }

        var latestUtc = ReadLatestUtc(connection, transaction);
        var committedUtc = ServerCommitJournalOperations.Max(latestUtc, observedUtc);
        var stateDelta = ServerCommitJournalSizer.GetStateDelta(stream, commit);
        var streamDelta = streamExists ? 0 : ServerCommitJournalSizer.GetStreamKeyBytes(commit.StreamKey);
        var lastCursorDelta = GetLastCursorDelta(stream, commit);
        var metrics = ReadMetrics(connection, transaction);
        if (!HasCapacity(metrics, commit, stateDelta, streamDelta, lastCursorDelta, null))
        {
            var expired = ReadExpiredMetrics(connection, transaction, committedUtc);
            if (!HasCapacity(metrics, commit, stateDelta, streamDelta, lastCursorDelta, expired))
            {
                var snapshot = ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys);
                transaction.Commit();
                return new(ServerCommitStatus.CapacityExceeded, snapshot);
            }

            _ = DeleteExpired(connection, transaction, committedUtc);
        }

        var expiresUtc = GetExpiry(committedUtc);
        var committedEntries = ServerCommitJournalOperations.CommitEntries(commit.Entries, committedUtc, expiresUtc);
        if (!streamExists)
        {
            InsertStream(connection, transaction, commit.StreamKey);
        }

        InsertLedger(connection, transaction, commit.StreamKey, committedEntries, commit.EntryBytes);
        UpsertStream(connection, transaction, commit.StreamKey, stream, commit);
        WriteLatestUtc(connection, transaction, committedUtc);
        var committedStream = ReadStreamRecord(connection, transaction, commit.StreamKey);
        var committedSnapshot = ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, committedStream, commit.OperationKeys);
        transaction.Commit();
        return new(ServerCommitStatus.Committed, committedSnapshot);
    }

    /// <summary>Reads a bounded page of complete operation groups for receive subscribers.</summary>
    /// <param name="request">The receive page request.</param>
    /// <returns>The receive page result.</returns>
    internal ServerReceivePageResult ReadReceivePage(ServerReceivePageRequest request)
    {
        ThrowIfDisposed();
        ArgumentExceptionHelper.ThrowIfNull(request);
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var stream = ReadStreamRecord(connection, transaction, request.StreamKey);
        var result = ServerReceivePageOperations.Create(request, stream);
        transaction.Commit();
        return result;
    }

    /// <summary>Registers or reads a trusted subscription binding.</summary>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The persisted subscription state.</returns>
    internal ServerSubscriptionState RegisterSubscription(ServerSubscriptionIdentity identity)
    {
        ThrowIfDisposed();
        ServerSubscriptionJournalOperations.ValidateIdentity(identity);
        return RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(0)));
    }

    /// <summary>Registers or reads a trusted subscription binding with an initial stream position.</summary>
    /// <param name="request">The registration request.</param>
    /// <returns>The persisted subscription state.</returns>
    internal ServerSubscriptionState RegisterSubscription(ServerSubscriptionRegistrationRequest request)
    {
        ThrowIfDisposed();
        ServerSubscriptionJournalOperations.ValidateRegistrationRequest(request);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var updatedUtc = ServerCommitJournalOperations.Max(ReadLatestUtc(connection, transaction), observedUtc);
        var state = RegisterSubscription(connection, transaction, request, updatedUtc, _options);
        WriteLatestUtc(connection, transaction, updatedUtc);
        transaction.Commit();
        return state;
    }

    /// <summary>Reads and durably offers a bounded page for a registered subscription.</summary>
    /// <param name="request">The subscription page request.</param>
    /// <returns>The receive page result.</returns>
    internal ServerReceivePageResult OfferReceivePage(ServerSubscriptionPageRequest request)
    {
        ThrowIfDisposed();
        ServerSubscriptionJournalOperations.ValidatePageRequest(request);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var record = ReadRegisteredSubscription(connection, transaction, request.Identity);
        var stream = ReadStreamRecord(connection, transaction, request.Identity.StreamKey);
        var initialRead = ResolveInitialReadCursor(connection, transaction, record, request.Cursor, stream, observedUtc);
        if (!initialRead.HasReadCursor)
        {
            transaction.Commit();
            return initialRead.PendingResult;
        }

        var result = ServerReceivePageOperations.Create(ServerSubscriptionJournalOperations.CreateReceiveRequest(request with { Cursor = initialRead.ReadCursor }), stream);
        result = ServerSubscriptionStartPositionOperations.WithClientPreviousCursor(result, request.Cursor);
        if (result.Batch is not null)
        {
            ThrowIfPageRewindsAcknowledgement(record, result.NextGroupSequence);
            var offeredUtc = ServerCommitJournalOperations.Max(ReadLatestUtc(connection, transaction), observedUtc);
            AddOffer(connection, transaction, record, result.Batch.NextCursor, result.NextGroupSequence, offeredUtc, _options);
            WriteLatestUtc(connection, transaction, offeredUtc);
        }

        transaction.Commit();
        return result;
    }

    /// <summary>Durably acknowledges a previously offered complete receive position.</summary>
    /// <param name="request">The acknowledgement request.</param>
    /// <returns>The persisted subscription state after acknowledgement.</returns>
    internal ServerSubscriptionState Acknowledge(ServerSubscriptionAcknowledgementRequest request)
    {
        ThrowIfDisposed();
        ServerSubscriptionJournalOperations.ValidateAcknowledgementRequest(request);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var identity = new ServerSubscriptionIdentity(request.StreamKey, request.ClientId, request.Acknowledgement.SubscriptionId);
        var record = ReadRegisteredSubscription(connection, transaction, identity);
        var state = Acknowledge(connection, transaction, record, request.Acknowledgement.Cursor, observedUtc);
        transaction.Commit();
        return state;
    }

    /// <summary>Reads a trusted bounded view used to evaluate a snapshot recovery request.</summary>
    /// <param name="request">The read request.</param>
    /// <returns>The retained snapshot-recovery view.</returns>
    internal ServerSnapshotRecoveryView ReadSnapshotRecoveryView(ServerSnapshotRecoveryReadRequest request)
    {
        ThrowIfDisposed();
        ServerSnapshotRecoveryJournalOperations.ValidateReadRequest(request);
        var operationKeys = ServerSnapshotRecoveryJournalOperations.CaptureOperationProofs(request, out var fingerprints);
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var stream = ReadStreamRecord(connection, transaction, request.StreamKey);
        var snapshot = ServerCommitJournalOperations.CreateSnapshot(request.StreamKey, stream, operationKeys);
        var record = ReadSubscriptionRecord(connection, transaction, request.Subscription.SubscriptionId);
        ServerSubscriptionState? state = null;
        ServerSubscriptionOffer? expiredCursorOffer = null;
        if (record is not null)
        {
            ThrowIfIdentityMismatch(request.Subscription, record);
            state = ServerSubscriptionJournalOperations.CreateState(record);
            if (request.RecoveryRequest.ExpiredCursor is not null
                && record.Offers.TryGetValue(request.RecoveryRequest.ExpiredCursor, out var offer))
            {
                expiredCursorOffer = offer;
            }
        }

        var view = new ServerSnapshotRecoveryView
        {
            Snapshot = snapshot,
            SubscriptionState = state,
            ExpiredCursorOffer = expiredCursorOffer,
            RequestedExpiredCursor = request.RecoveryRequest.ExpiredCursor,
            CapturedPendingOperationCount = request.RecoveryRequest.PendingOperations.Count,
            OperationDispositions = ServerSnapshotRecoveryJournalOperations.CreateOperationDispositions(snapshot, operationKeys, fingerprints),
            OperationFingerprints = fingerprints,
        };
        transaction.Commit();
        return view;
    }

    /// <summary>Durably offers a recovered snapshot cursor for later authenticated acknowledgement.</summary>
    /// <param name="request">The offer request.</param>
    /// <returns>The offer result.</returns>
    internal ServerSnapshotOfferResult TryOfferSnapshot(ServerSnapshotOfferRequest request)
    {
        ThrowIfDisposed();
        if (!ServerSnapshotRecoveryJournalOperations.ValidateOfferRequest(request)
            || !ServerSnapshotRecoveryJournalOperations.OfferRequestMatchesView(request)
            || !ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(request.View, request.RecoveryResult))
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ValidationRejected, null, null);
        }

        var observedUtc = _options.TimeProvider.GetUtcNow();
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        ValidateExistingSchema(connection, transaction);
        ValidateReadCapacity(connection, transaction);
        var result = TryOfferSnapshot(connection, transaction, request, observedUtc);
        transaction.Commit();
        return result;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerCommitSnapshot IServerCommitJournal.Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys) =>
        Read(streamKey, operationKeys);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerCommitResult IServerCommitJournal.TryCommit(ServerCommitPlan plan) => TryCommit(plan);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerReceivePageResult IServerReceiveJournal.ReadReceivePage(ServerReceivePageRequest request) => ReadReceivePage(request);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerSubscriptionState IServerSubscriptionAcknowledgementJournal.RegisterSubscription(ServerSubscriptionIdentity identity) =>
        RegisterSubscription(identity);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerSubscriptionState IServerSubscriptionAcknowledgementJournal.RegisterSubscription(ServerSubscriptionRegistrationRequest request) =>
        RegisterSubscription(request);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerReceivePageResult IServerSubscriptionAcknowledgementJournal.OfferReceivePage(ServerSubscriptionPageRequest request) =>
        OfferReceivePage(request);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerSubscriptionState IServerSubscriptionAcknowledgementJournal.Acknowledge(ServerSubscriptionAcknowledgementRequest request) =>
        Acknowledge(request);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerSnapshotRecoveryView IServerSnapshotRecoveryJournal.ReadSnapshotRecoveryView(ServerSnapshotRecoveryReadRequest request) =>
        ReadSnapshotRecoveryView(request);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ServerSnapshotOfferResult IServerSnapshotRecoveryJournal.TryOfferSnapshot(ServerSnapshotOfferRequest request) =>
        TryOfferSnapshot(request);

    /// <summary>Compacts expired terminal ledger entries and event rows using the journal clock.</summary>
    /// <returns>The number of terminal entries removed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal int Compact() => Compact(null);

    /// <summary>Compacts expired terminal ledger entries and event rows.</summary>
    /// <param name="utcNow">The optional caller-sampled timestamp.</param>
    /// <returns>The number of terminal entries removed.</returns>
    internal int Compact(DateTimeOffset? utcNow)
    {
        ThrowIfDisposed();
        var sampledUtc = utcNow ?? _options.TimeProvider.GetUtcNow();
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        ValidateExistingSchema(connection, transaction);
        var compactUtc = ServerCommitJournalOperations.Max(ReadLatestUtc(connection, transaction), sampledUtc);
        var removed = DeleteExpired(connection, transaction, compactUtc);
        DeleteExpiredSubscriptions(connection, transaction, compactUtc, _options);
        WriteLatestUtc(connection, transaction, compactUtc);
        transaction.Commit();
        return removed;
    }

    /// <summary>Checks whether the commit can fit after an optional expired-row reclamation.</summary>
    /// <param name="metrics">The retained metrics.</param>
    /// <param name="commit">The validated commit.</param>
    /// <param name="stateDelta">The retained state byte delta.</param>
    /// <param name="streamDelta">The new stream logical byte delta.</param>
    /// <param name="lastCursorDelta">The retained cursor byte delta.</param>
    /// <param name="expired">The optional projected expired rows.</param>
    /// <returns>Whether capacity remains.</returns>
    private bool HasCapacity(
        RetainedMetrics metrics,
        ServerCommitValidationResult commit,
        long stateDelta,
        long streamDelta,
        long lastCursorDelta,
        RetainedMetrics? expired)
    {
        var streamCount = checked((long)metrics.StreamCount + (streamDelta == 0 ? 0 : 1));
        var ledgerCount = checked((long)metrics.LedgerEntryCount + commit.Entries.Length - (expired?.LedgerEntryCount ?? 0));
        var eventCount = checked((long)metrics.EventCount + commit.EventCount - (expired?.EventCount ?? 0));
        var logicalBytes = checked(metrics.LogicalBytes + commit.LedgerBytes + stateDelta + streamDelta + lastCursorDelta - (expired?.LogicalBytes ?? 0));
        return HasCountCapacity(streamCount, ledgerCount, eventCount)
            && HasSubscriptionCountCapacity(metrics)
            && logicalBytes <= _options.MaximumLogicalBytes;
    }

    /// <summary>Rejects retained data exceeding this instance's bounds before reconstructing replay payloads.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction protecting the preflight and subsequent read.</param>
    /// <exception cref="InvalidOperationException">Retained data exceeds the configured read bounds.</exception>
    private void ValidateReadCapacity(SqliteConnection connection, SqliteTransaction transaction)
    {
        var metrics = ReadMetrics(connection, transaction);
        if (HasCountCapacity(metrics.StreamCount, metrics.LedgerEntryCount, metrics.EventCount)
            && HasSubscriptionCountCapacity(metrics)
            && metrics.LogicalBytes <= _options.MaximumLogicalBytes)
        {
            return;
        }

        throw new InvalidOperationException("Retained server journal data exceeds the configured read bounds.");
    }

    /// <summary>Checks retained count capacity.</summary>
    /// <param name="streamCount">The projected stream count.</param>
    /// <param name="ledgerCount">The projected ledger count.</param>
    /// <param name="eventCount">The projected event count.</param>
    /// <returns>Whether count capacity remains.</returns>
    private bool HasCountCapacity(long streamCount, long ledgerCount, long eventCount) =>
        streamCount <= _options.MaximumStreams
        && ledgerCount <= _options.MaximumLedgerEntries
        && eventCount <= _options.MaximumEvents;

    /// <summary>Checks subscription acknowledgement count capacity.</summary>
    /// <param name="metrics">The retained metrics.</param>
    /// <returns>Whether subscription count capacity remains.</returns>
    private bool HasSubscriptionCountCapacity(RetainedMetrics metrics) =>
        metrics.SubscriptionCount <= _options.MaximumSubscriptions
        && metrics.SubscriptionOfferCount <= _options.MaximumSubscriptionOffers;

    /// <summary>Initializes or validates the durable schema.</summary>
    private void InitializeSchema()
    {
        _ = Directory.CreateDirectory(GetDirectoryForCreate(_databasePath));
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: false);
        var userVersion = GetUserVersion(connection, transaction);
        if (userVersion == 0 && !HasUserTables(connection, transaction))
        {
            CreateSchema(connection, transaction);
        }
        else if (userVersion == SchemaVersionOne)
        {
            MigrateSchemaOneToTwo(connection, transaction);
            MigrateSchemaTwoToThree(connection, transaction);
            MigrateSchemaThreeToFour(connection, transaction);
            MigrateSchemaFourToFive(connection, transaction);
        }
        else if (userVersion == SchemaVersionTwo)
        {
            MigrateSchemaTwoToThree(connection, transaction);
            MigrateSchemaThreeToFour(connection, transaction);
            MigrateSchemaFourToFive(connection, transaction);
        }
        else if (userVersion == SchemaVersionThree)
        {
            MigrateSchemaThreeToFour(connection, transaction);
            MigrateSchemaFourToFive(connection, transaction);
        }
        else if (userVersion == SchemaVersionFour)
        {
            MigrateSchemaFourToFive(connection, transaction);
        }
        else
        {
            ValidateExistingSchema(connection, transaction, userVersion);
        }

        transaction.Commit();
        ConfigureDurability(connection);
    }

    /// <summary>Reads retained metrics from the database.</summary>
    /// <returns>The retained metrics.</returns>
    private RetainedMetrics ReadMetrics()
    {
        ThrowIfDisposed();
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, deferred: true);
        ValidateExistingSchema(connection, transaction);
        var metrics = ReadMetrics(connection, transaction);
        transaction.Commit();
        return metrics;
    }

    /// <summary>Opens a SQLite connection with pooling disabled.</summary>
    /// <returns>The open SQLite connection.</returns>
    private SqliteConnection OpenConnection()
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = _databasePath, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = false }.ToString();
        var connection = new SqliteConnection(connectionString);
        try
        {
            connection.Open();
            ConfigureBusyTimeout(connection);
            ConfigureOperationalConnection(connection);
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>Computes an inclusive replay expiry for a successful commit.</summary>
    /// <param name="committedUtc">The successful commit timestamp.</param>
    /// <returns>The inclusive expiry timestamp.</returns>
    private DateTimeOffset GetExpiry(DateTimeOffset committedUtc)
    {
        try
        {
            return committedUtc.Add(_options.OperationRetention);
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MaxValue;
        }
    }

    /// <summary>Throws if this instance has been disposed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

    /// <summary>Stores retained count and byte metrics.</summary>
    private sealed class RetainedMetrics
    {
        /// <summary>Gets or sets the stream count.</summary>
        internal int StreamCount { get; set; }

        /// <summary>Gets or sets the ledger entry count.</summary>
        internal int LedgerEntryCount { get; set; }

        /// <summary>Gets or sets the event count.</summary>
        internal int EventCount { get; set; }

        /// <summary>Gets or sets the subscription count.</summary>
        internal int SubscriptionCount { get; set; }

        /// <summary>Gets or sets the subscription offer count.</summary>
        internal int SubscriptionOfferCount { get; set; }

        /// <summary>Gets or sets the retained logical bytes.</summary>
        internal long LogicalBytes { get; set; }
    }
}
