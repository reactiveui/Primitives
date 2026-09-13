// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Processes authorized client operations through an internal server commit journal.</summary>
internal sealed class ServerOperationProcessor
{
    /// <summary>The reason returned for canonical intent mismatches.</summary>
    private const string IntentMismatchReason = "intent-mismatch";

    /// <summary>The reason returned when the journal is at capacity.</summary>
    private const string CapacityExceededReason = "capacity-exceeded";

    /// <summary>The reason returned when bounded compare-and-swap admission cannot complete.</summary>
    private const string StaleRevisionReason = "stale-revision";

    /// <summary>The reason returned when the stream revision cannot advance.</summary>
    private const string RevisionOverflowReason = "revision-overflow";

    /// <summary>The reason returned when the stream event sequence cannot advance.</summary>
    private const string EventSequenceOverflowReason = "event-sequence-overflow";

    /// <summary>The logical accounting bytes for a prepared result shell.</summary>
    private const long PreparedResultBytes = 16L;

    /// <summary>The logical accounting bytes for one operation shell.</summary>
    private const long OperationShellBytes = 32L;

    /// <summary>The commit journal.</summary>
    private readonly IServerCommitJournal _journal;

    /// <summary>The trusted authorization component.</summary>
    private readonly IServerOperationAuthorizer _authorizer;

    /// <summary>The side-effect-free domain preparation component.</summary>
    private readonly IServerOperationHandler _handler;

    /// <summary>The canonical cursor factory.</summary>
    private readonly IServerOperationCursorFactory _cursorFactory;

    /// <summary>The finite processor options.</summary>
    private readonly ServerOperationProcessorOptions _options;

    /// <summary>The current number of active requests admitted by this processor.</summary>
    private int _activeRequests;

    /// <summary>Initializes a new instance of the <see cref="ServerOperationProcessor"/> class.</summary>
    /// <param name="journal">The commit journal.</param>
    /// <param name="authorizer">The authorizer.</param>
    /// <param name="handler">The side-effect-free domain handler.</param>
    /// <param name="cursorFactory">The optional cursor factory.</param>
    /// <param name="options">The optional processor bounds.</param>
    internal ServerOperationProcessor(
        IServerCommitJournal journal,
        IServerOperationAuthorizer authorizer,
        IServerOperationHandler handler,
        IServerOperationCursorFactory? cursorFactory = null,
        ServerOperationProcessorOptions? options = null)
    {
        ArgumentExceptionHelper.ThrowIfNull(journal);
        ArgumentExceptionHelper.ThrowIfNull(authorizer);
        ArgumentExceptionHelper.ThrowIfNull(handler);
        _options = options ?? new();
        _options.Validate();
        _journal = journal;
        _authorizer = authorizer;
        _handler = handler;
        _cursorFactory = cursorFactory ?? new ServerOperationCursorFactory();
    }

    /// <summary>Processes a batch of authorized operations.</summary>
    /// <param name="batch">The batch to process.</param>
    /// <param name="client">The authenticated client identity.</param>
    /// <param name="cancellationToken">The token used to cancel processing.</param>
    /// <returns>The synchronization result and produced events.</returns>
    internal async ValueTask<ServerSyncResult> ProcessAsync(
        SyncBatch batch,
        ClientIdentity client,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ArgumentExceptionHelper.ThrowIfNull(client);
        cancellationToken.ThrowIfCancellationRequested();
        ServerCommitJournalGuard.ValidateText(client.ClientId, nameof(client.ClientId));
        ValidateBatchId(batch.BatchId);
        EnterActiveRequest();
        try
        {
            var operations = CaptureOperations(batch.Operations);
            var results = new OperationSyncResult[operations.Length];
            var producedEvents = new List<RemoteEvent>();
            string? serverCursor = null;
            for (var index = 0; index < operations.Length; index++)
            {
                var operationResult = await ProcessOperationAsync(client, operations[index], cancellationToken).ConfigureAwait(false);
                results[index] = operationResult.Result;
                for (var eventIndex = 0; eventIndex < operationResult.Events.Count; eventIndex++)
                {
                    var remoteEvent = operationResult.Events[eventIndex];
                    producedEvents.Add(remoteEvent);
                    serverCursor = remoteEvent.ServerCursor;
                }
            }

            return new(new(batch.BatchId, results, serverCursor, GetRetryAfter(results)), producedEvents);
        }
        finally
        {
            _ = Interlocked.Decrement(ref _activeRequests);
        }
    }

    /// <summary>Returns a retained replay when one exists for the current operation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="snapshot">The stream snapshot.</param>
    /// <param name="fingerprint">The canonical fingerprint.</param>
    /// <returns>The replayed receipt, or null when no retained entry exists.</returns>
    private static ServerOperationReceipt? TryReplay(
        OperationId operationId,
        ServerCommitSnapshot snapshot,
        ServerCommitFingerprint fingerprint)
    {
        if (snapshot.Entries.Count == 0)
        {
            return null;
        }

        var entry = snapshot.Entries[0];
        return entry.Fingerprint.Matches(fingerprint)
            ? new(entry.Result, entry.Events)
            : Rejected(operationId, IntentMismatchReason);
    }

    /// <summary>Replays the committed entry returned by the journal.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="snapshot">The committed snapshot.</param>
    /// <param name="fingerprint">The canonical fingerprint.</param>
    /// <returns>The committed receipt.</returns>
    /// <exception cref="InvalidOperationException">The journal did not return the committed entry.</exception>
    private static ServerOperationReceipt ReplayCommitted(
        OperationId operationId,
        ServerCommitSnapshot snapshot,
        ServerCommitFingerprint fingerprint) =>
        TryReplay(operationId, snapshot, fingerprint) ?? throw new InvalidOperationException("The committed operation replay is missing.");

    /// <summary>Creates a retryable receipt.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <returns>The receipt.</returns>
    private static ServerOperationReceipt Retryable(OperationId operationId, string reasonCode) =>
        new(new(operationId, OperationResultKind.Retryable, reasonCode, null), []);

    /// <summary>Creates a rejected receipt.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <returns>The receipt.</returns>
    private static ServerOperationReceipt Rejected(OperationId operationId, string reasonCode) =>
        new(new(operationId, OperationResultKind.Rejected, reasonCode, null), []);

    /// <summary>Validates a non-empty batch identifier.</summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <exception cref="SyncBatchValidationException">The identifier is empty.</exception>
    private static void ValidateBatchId(Guid batchId)
    {
        if (batchId != Guid.Empty)
        {
            return;
        }

        throw CreateBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch identifier must be non-empty.");
    }

    /// <summary>Validates one operation in batch context.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="operationIds">The operation identifiers seen so far.</param>
    /// <param name="clientSequences">The client sequences seen so far.</param>
    /// <param name="streamId">The first stream identifier seen in the batch.</param>
    /// <param name="previousSequence">The previous client sequence.</param>
    /// <exception cref="ArgumentException">The operation contains invalid text or payload data.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The operation contains an invalid bounded value.</exception>
    /// <exception cref="SyncBatchValidationException">The operation is malformed.</exception>
    private static void ValidateOperation(
        SyncOperation? operation,
        HashSet<OperationId> operationIds,
        HashSet<long> clientSequences,
        ref StreamId? streamId,
        ref long previousSequence)
    {
        if (operation is null || operation.Payload is null || operation.Policy is null)
        {
            throw CreateBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch contains a malformed operation.");
        }

        ValidateOperationCore(operation);
        operation.Policy.Validate();
        ValidateBatchMembership(operation, operationIds, clientSequences, ref streamId, ref previousSequence);
    }

    /// <summary>Validates the shape of one operation independent of its batch membership.</summary>
    /// <param name="operation">The operation.</param>
    /// <exception cref="ArgumentException">The operation contains invalid text or payload data.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The operation contains an invalid bounded value.</exception>
    /// <exception cref="SyncBatchValidationException">The operation is malformed.</exception>
    private static void ValidateOperationCore(SyncOperation operation)
    {
        if (operation.OperationId.Value == Guid.Empty || operation.StreamId.Value is null || operation.ClientSequence <= 0 || !IsDefined(operation.Type))
        {
            throw CreateBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch contains a malformed operation.");
        }

        ServerCommitJournalGuard.ValidateStreamKey(new("t", operation.StreamId));
        ValidatePayload(operation.Payload);
        ValidateMetadata(operation.Metadata);
        if (operation.BaseVersion is null)
        {
            return;
        }

        ServerCommitJournalGuard.ValidateText(operation.BaseVersion, nameof(operation.BaseVersion));
    }

    /// <summary>Validates operation membership against the whole batch.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="operationIds">The operation identifiers seen so far.</param>
    /// <param name="clientSequences">The client sequences seen so far.</param>
    /// <param name="streamId">The first stream identifier seen in the batch.</param>
    /// <param name="previousSequence">The previous client sequence.</param>
    /// <exception cref="SyncBatchValidationException">The operation is duplicated or out of order.</exception>
    private static void ValidateBatchMembership(
        SyncOperation operation,
        HashSet<OperationId> operationIds,
        HashSet<long> clientSequences,
        ref StreamId? streamId,
        ref long previousSequence)
    {
        if (streamId is not null && streamId.Value != operation.StreamId)
        {
            throw CreateBatchValidationException(SyncBatchValidationError.MixedStreams, "The synchronization batch contains operations for multiple streams.");
        }

        streamId ??= operation.StreamId;
        if (!operationIds.Add(operation.OperationId))
        {
            throw CreateBatchValidationException(SyncBatchValidationError.DuplicateOperation, "The synchronization batch contains a duplicate operation identifier.");
        }

        if (!clientSequences.Add(operation.ClientSequence))
        {
            throw CreateBatchValidationException(SyncBatchValidationError.DuplicateClientSequence, "The synchronization batch contains a duplicate client sequence.");
        }

        if (operation.ClientSequence < previousSequence)
        {
            throw CreateBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch is not in client sequence order.");
        }

        previousSequence = operation.ClientSequence;
    }

    /// <summary>Validates a prepared terminal operation result.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="result">The prepared result.</param>
    /// <exception cref="InvalidOperationException">The result is not valid for the operation.</exception>
    private static void ValidatePreparation(OperationId operationId, OperationSyncResult result)
    {
        if (result.OperationId != operationId)
        {
            throw new InvalidOperationException("A prepared result must match the operation being processed.");
        }

        if (result.Kind is OperationResultKind.Accepted or OperationResultKind.Conflict or OperationResultKind.Rejected)
        {
            return;
        }

        throw new InvalidOperationException("A prepared result must be terminal.");
    }

    /// <summary>Validates the prepared state before admission.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="state">The state.</param>
    /// <exception cref="ArgumentException">The state contains invalid text or payload data.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The state contains an invalid bounded value.</exception>
    /// <exception cref="InvalidOperationException">The state is not valid for the stream.</exception>
    private static void ValidatePreparedState(ServerStreamKey streamKey, ServerState? state)
    {
        if (state is null)
        {
            return;
        }

        if (state.StreamId != streamKey.StreamId)
        {
            throw new InvalidOperationException("A prepared state must belong to the operation stream.");
        }

        ServerCommitJournalGuard.ValidateText(state.Version, nameof(state.Version));
        ValidatePayload(state.State);
    }

    /// <summary>Adds optional prepared state bytes.</summary>
    /// <param name="logicalBytes">The current byte count.</param>
    /// <param name="state">The prepared state.</param>
    /// <returns>The updated byte count.</returns>
    private static long AddPreparedStateBytes(long logicalBytes, ServerState? state) =>
        state is null ? logicalBytes : ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, ServerCommitJournalSizer.GetStateBytes(state));

    /// <summary>Adds prepared conflict bytes.</summary>
    /// <param name="logicalBytes">The current byte count.</param>
    /// <param name="preparation">The preparation.</param>
    /// <returns>The updated byte count.</returns>
    /// <exception cref="ArgumentException">A conflict contains invalid text or payload data.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A conflict contains an invalid bounded value.</exception>
    /// <exception cref="InvalidOperationException">A conflict is invalid.</exception>
    private static long AddPreparedConflictBytes(long logicalBytes, ServerOperationPreparation preparation)
    {
        for (var index = 0; index < preparation.Conflicts.Count; index++)
        {
            var conflict = preparation.Conflicts[index];
            ArgumentExceptionHelper.ThrowIfNull(conflict, nameof(preparation));
            if (conflict.OperationId != preparation.Result.OperationId)
            {
                throw new InvalidOperationException("A prepared conflict must match its operation result.");
            }

            ServerCommitJournalGuard.ValidateText(conflict.ResolutionCode, nameof(conflict.ResolutionCode));
            logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, GetConflictBytes(conflict));
        }

        return logicalBytes;
    }

    /// <summary>Adds prepared event bytes.</summary>
    /// <param name="logicalBytes">The current byte count.</param>
    /// <param name="preparation">The preparation.</param>
    /// <returns>The updated byte count.</returns>
    /// <exception cref="ArgumentException">An event contains invalid text or payload data.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An event contains an invalid bounded value.</exception>
    /// <exception cref="InvalidOperationException">An event is invalid.</exception>
    private static long AddPreparedEventBytes(long logicalBytes, ServerOperationPreparation preparation)
    {
        var eventIds = new HashSet<Guid>();
        for (var index = 0; index < preparation.Events.Count; index++)
        {
            var prepared = preparation.Events[index];
            ArgumentExceptionHelper.ThrowIfNull(prepared, nameof(preparation));
            if (prepared.EventId == Guid.Empty || !eventIds.Add(prepared.EventId))
            {
                throw new InvalidOperationException("A prepared event identifier must be non-empty and unique.");
            }

            ValidatePayload(prepared.Payload);
            ValidateMetadata(prepared.Metadata);
            logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, GetPreparedEventBytes(prepared));
        }

        return logicalBytes;
    }

    /// <summary>Computes logical operation bytes.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetOperationBytes(SyncOperation operation)
    {
        var bytes = OperationShellBytes + ServerCommitJournalGuard.GetTextBytes(operation.StreamId.Value) + ServerCommitJournalSizer.GetPayloadBytes(operation.Payload);
        bytes = AddOptionalTextBytes(bytes, operation.BaseVersion);
        return AddMetadataBytes(bytes, operation.Metadata);
    }

    /// <summary>Computes logical prepared result bytes.</summary>
    /// <param name="result">The result.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetResultBytes(OperationSyncResult result)
    {
        var bytes = PreparedResultBytes;
        bytes = AddOptionalTextBytes(bytes, result.ReasonCode);
        return AddOptionalTextBytes(bytes, result.ServerVersion);
    }

    /// <summary>Computes logical prepared conflict bytes.</summary>
    /// <param name="conflict">The conflict.</param>
    /// <returns>The logical byte count.</returns>
    private static long GetConflictBytes(ResolvedConflict conflict)
    {
        var bytes = PreparedResultBytes + ServerCommitJournalGuard.GetTextBytes(conflict.ResolutionCode);
        return conflict.ResolvedPayload is null ? bytes : ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalSizer.GetPayloadBytes(conflict.ResolvedPayload));
    }

    /// <summary>Computes logical prepared event bytes before server stamping.</summary>
    /// <param name="prepared">The prepared event.</param>
    /// <returns>The logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetPreparedEventBytes(ServerPreparedEvent prepared) =>
        AddMetadataBytes(ServerCommitJournalSizer.GetPayloadBytes(prepared.Payload), prepared.Metadata);

    /// <summary>Adds optional text bytes.</summary>
    /// <param name="bytes">The current byte count.</param>
    /// <param name="value">The optional text.</param>
    /// <returns>The updated byte count.</returns>
    private static long AddOptionalTextBytes(long bytes, string? value) =>
        value is null ? bytes : ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(value));

    /// <summary>Adds metadata bytes.</summary>
    /// <param name="bytes">The current byte count.</param>
    /// <param name="metadata">The metadata.</param>
    /// <returns>The updated byte count.</returns>
    private static long AddMetadataBytes(long bytes, IReadOnlyDictionary<string, string> metadata)
    {
        foreach (var item in metadata)
        {
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(item.Key));
            bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(item.Value));
        }

        return bytes;
    }

    /// <summary>Validates payload envelope shape for logical accounting.</summary>
    /// <param name="payload">The payload.</param>
    /// <exception cref="ArgumentException">The payload is malformed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The payload schema version is invalid.</exception>
    private static void ValidatePayload(PayloadEnvelope payload)
    {
        ArgumentExceptionHelper.ThrowIfNull(payload);
        ServerCommitJournalGuard.ValidateText(payload.ContractId, nameof(payload.ContractId));
        ServerCommitJournalGuard.ValidateText(payload.ContentType, nameof(payload.ContentType));
        ServerCommitJournalGuard.ValidateText(payload.PayloadHash, nameof(payload.PayloadHash));
        if (payload.SchemaVersion > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(payload), payload.SchemaVersion, "Payload schema versions must be positive.");
    }

    /// <summary>Validates metadata keys and values.</summary>
    /// <param name="metadata">The metadata.</param>
    /// <exception cref="ArgumentException">The metadata is malformed.</exception>
    private static void ValidateMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        ArgumentExceptionHelper.ThrowIfNull(metadata);
        foreach (var item in metadata)
        {
            ServerCommitJournalGuard.ValidateText(item.Key, nameof(metadata));
            ServerCommitJournalGuard.ValidateText(item.Value, nameof(metadata));
        }
    }

    /// <summary>Determines whether a rejected preparation contains durable effects.</summary>
    /// <param name="preparation">The preparation.</param>
    /// <returns>Whether durable effects are present.</returns>
    private static bool HasPreparedEffects(ServerOperationPreparation preparation) =>
        preparation.NewState is not null || preparation.Conflicts.Count != 0 || preparation.Events.Count != 0;

    /// <summary>Determines whether an operation type is defined.</summary>
    /// <param name="type">The operation type.</param>
    /// <returns>Whether the operation type is defined.</returns>
    private static bool IsDefined(SyncOperationType type) =>
        type is SyncOperationType.Append or SyncOperationType.Update or SyncOperationType.Delete;

    /// <summary>Creates a synchronization batch validation exception.</summary>
    /// <param name="error">The validation error.</param>
    /// <param name="message">The validation message.</param>
    /// <returns>The validation exception.</returns>
    private static SyncBatchValidationException CreateBatchValidationException(SyncBatchValidationError error, string message) =>
        new(error, message);

    /// <summary>Admits the request into the processor active set.</summary>
    /// <exception cref="QueueCapacityExceededException">The active request capacity has been reached.</exception>
    private void EnterActiveRequest()
    {
        while (true)
        {
            var current = Volatile.Read(ref _activeRequests);
            if (current >= _options.MaximumActiveRequests)
            {
                throw new QueueCapacityExceededException("The server operation processor is at active request capacity.", canFitWhenEmpty: true);
            }

            if (Interlocked.CompareExchange(ref _activeRequests, current + 1, current) == current)
            {
                return;
            }
        }
    }

    /// <summary>Processes one operation with bounded compare-and-swap retries.</summary>
    /// <param name="client">The authenticated client identity.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="cancellationToken">The token used to cancel processing.</param>
    /// <returns>The operation receipt.</returns>
    private async ValueTask<ServerOperationReceipt> ProcessOperationAsync(
        ClientIdentity client,
        SyncOperation operation,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var scope = Authorize(client, operation);
        var streamKey = new ServerStreamKey(scope.TenantId, operation.StreamId);
        var operationKey = new ServerOperationKey(scope.ClientId, operation.OperationId);
        var fingerprint = new ServerCommitFingerprint(CanonicalOperationFingerprint.Compute(
            scope.TenantId,
            scope.ClientId,
            operation,
            _options.MaximumCanonicalOperationBytes));

        for (var attempt = 0; attempt < _options.MaximumCommitAttempts; attempt++)
        {
            var snapshot = _journal.Read(streamKey, [operationKey]);
            var replay = TryReplay(operation.OperationId, snapshot, fingerprint);
            if (replay is not null)
            {
                return replay;
            }

            var context = new ServerOperationContext(client, operation, scope, streamKey, operationKey, snapshot, CreateCandidateWrite(scope, operation, snapshot));
            var preparation = await _handler.PrepareAsync(context, cancellationToken).ConfigureAwait(false);
            ArgumentExceptionHelper.ThrowIfNull(preparation);
            cancellationToken.ThrowIfCancellationRequested();
            var receipt = TryCommitPrepared(context, fingerprint, preparation, cancellationToken);
            if (receipt is not null)
            {
                return receipt;
            }
        }

        return Retryable(operation.OperationId, StaleRevisionReason);
    }

    /// <summary>Authorizes and validates trusted scope before journal lookup.</summary>
    /// <param name="client">The authenticated client.</param>
    /// <param name="operation">The candidate operation.</param>
    /// <returns>The trusted scope.</returns>
    /// <exception cref="UnauthorizedAccessException">The authorized scope does not match the authenticated client.</exception>
    private ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        var scope = _authorizer.Authorize(client, operation);
        ServerCommitJournalGuard.ValidateStreamKey(new(scope.TenantId, operation.StreamId));
        ServerCommitJournalGuard.ValidateText(scope.ClientId, nameof(scope.ClientId));
        if (!StringComparer.Ordinal.Equals(scope.ClientId, client.ClientId))
        {
            throw new UnauthorizedAccessException("The authorized scope client must match the authenticated client.");
        }

        return scope;
    }

    /// <summary>Attempts to commit one prepared operation.</summary>
    /// <param name="context">The operation context.</param>
    /// <param name="fingerprint">The canonical fingerprint.</param>
    /// <param name="preparation">The prepared result.</param>
    /// <param name="cancellationToken">The token used to cancel before durable admission.</param>
    /// <returns>The receipt, or null when the caller must retry from a fresh snapshot.</returns>
    /// <exception cref="InvalidOperationException">The prepared result or journal status is invalid.</exception>
    private ServerOperationReceipt? TryCommitPrepared(
        ServerOperationContext context,
        ServerCommitFingerprint fingerprint,
        ServerOperationPreparation preparation,
        CancellationToken cancellationToken)
    {
        ValidatePreparation(context, preparation);
        var events = StampEvents(context, preparation.Events);
        var entry = new ServerLedgerEntry(context.OperationKey, fingerprint, preparation.Result, preparation.Conflicts, events);
        var stamp = preparation.NewState is null
            ? (ServerWriteStamp?)null
            : context.CandidateWrite;
        cancellationToken.ThrowIfCancellationRequested();
        var commit = _journal.TryCommit(new(context.StreamKey, context.Snapshot.Revision, preparation.NewState, stamp, [entry]));
        return commit.Status switch
        {
            ServerCommitStatus.Committed => ReplayCommitted(context.Operation.OperationId, commit.Snapshot, fingerprint),
            ServerCommitStatus.StaleRevision => null,
            ServerCommitStatus.IntentMismatch => Rejected(context.Operation.OperationId, IntentMismatchReason),
            ServerCommitStatus.CapacityExceeded => Retryable(context.Operation.OperationId, CapacityExceededReason),
            ServerCommitStatus.RevisionOverflow => Rejected(context.Operation.OperationId, RevisionOverflowReason),
            ServerCommitStatus.EventSequenceOverflow => Rejected(context.Operation.OperationId, EventSequenceOverflowReason),
            _ => throw new InvalidOperationException("The server commit journal returned an unknown status."),
        };
    }

    /// <summary>Creates canonical remote events for a prepared result.</summary>
    /// <param name="context">The operation context.</param>
    /// <param name="preparedEvents">The prepared events.</param>
    /// <returns>The stamped remote events.</returns>
    private ReadOnlyCollection<RemoteEvent> StampEvents(
        ServerOperationContext context,
        IReadOnlyList<ServerPreparedEvent> preparedEvents)
    {
        var events = new RemoteEvent[preparedEvents.Count];
        for (var index = 0; index < events.Length; index++)
        {
            var prepared = preparedEvents[index];
            events[index] = new(
                prepared.EventId,
                context.StreamKey.StreamId,
                _cursorFactory.CreateCursor(context, index),
                context.CandidateWrite.CommittedAtUtc,
                context.Operation.OperationId,
                prepared.Payload,
                prepared.Metadata)
            { Origin = new(context.Scope.ClientId, context.Operation.OperationId) };
        }

        return Array.AsReadOnly(events);
    }

    /// <summary>Captures one server timestamp and prevents a backward clock from reversing write time.</summary>
    /// <param name="scope">The trusted client scope.</param>
    /// <param name="operation">The authorized operation.</param>
    /// <param name="snapshot">The state read for this compare-and-swap attempt.</param>
    /// <returns>The immutable candidate write stamp.</returns>
    private ServerWriteStamp CreateCandidateWrite(
        ServerOperationScope scope,
        SyncOperation operation,
        ServerCommitSnapshot snapshot)
    {
        var timestamp = _options.TimeProvider.GetUtcNow();
        if (snapshot.LastWriteStamp is { } previous && timestamp < previous.CommittedAtUtc)
        {
            timestamp = previous.CommittedAtUtc;
        }

        return new(timestamp, scope.ClientId, operation.OperationId);
    }

    /// <summary>Captures and validates the batch operation list under finite bounds.</summary>
    /// <param name="operations">The batch operations.</param>
    /// <returns>The captured operations.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The operation count is outside the configured bounds.</exception>
    /// <exception cref="SyncBatchValidationException">The operation list is malformed.</exception>
    private SyncOperation[] CaptureOperations(IReadOnlyList<SyncOperation> operations)
    {
        ArgumentExceptionHelper.ThrowIfNull(operations);
        var count = operations.Count;
        if (count <= 0)
        {
            throw CreateBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch must contain at least one operation.");
        }

        if (count > _options.MaximumBatchOperations)
        {
            throw new ArgumentOutOfRangeException(nameof(operations), count, "The operation count is outside the configured bounds.");
        }

        ValidateOperationList(operations);
        var captured = new SyncOperation[count];
        for (var index = 0; index < captured.Length; index++)
        {
            var operation = operations[index];
            ArgumentExceptionHelper.ThrowIfNull(operation, nameof(operations));
            captured[index] = operation;
        }

        return captured;
    }

    /// <summary>Validates operation membership and size before retaining operation references.</summary>
    /// <param name="operations">The operation list.</param>
    /// <exception cref="ArgumentOutOfRangeException">The batch logical bytes exceed the configured bound.</exception>
    /// <exception cref="SyncBatchValidationException">The operation list is malformed.</exception>
    private void ValidateOperationList(IReadOnlyList<SyncOperation> operations)
    {
        var operationIds = new HashSet<OperationId>();
        var clientSequences = new HashSet<long>();
        StreamId? streamId = null;
        var previousSequence = 0L;
        var logicalBytes = 0L;
        for (var index = 0; index < operations.Count; index++)
        {
            var operation = operations[index];
            ValidateOperation(operation, operationIds, clientSequences, ref streamId, ref previousSequence);
            logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, GetOperationBytes(operation));
            if (logicalBytes > _options.MaximumBatchLogicalBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(operations), logicalBytes, "The synchronization batch logical byte count is outside the configured bounds.");
            }
        }
    }

    /// <summary>Validates a handler preparation before journal admission.</summary>
    /// <param name="context">The operation context.</param>
    /// <param name="preparation">The preparation.</param>
    /// <exception cref="ArgumentException">The preparation contains invalid text or payload data.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The preparation exceeds configured bounds.</exception>
    /// <exception cref="InvalidOperationException">The preparation is not valid for the operation.</exception>
    private void ValidatePreparation(ServerOperationContext context, ServerOperationPreparation preparation)
    {
        ValidatePreparation(context.Operation.OperationId, preparation.Result);
        ValidatePreparedState(context.StreamKey, preparation.NewState);
        ValidatePreparedEffects(preparation);
    }

    /// <summary>Validates prepared effects before stamped event and ledger copies are allocated.</summary>
    /// <param name="preparation">The preparation.</param>
    /// <exception cref="ArgumentException">The preparation contains invalid text or payload data.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The preparation exceeds configured bounds.</exception>
    /// <exception cref="InvalidOperationException">The preparation contains invalid effects.</exception>
    private void ValidatePreparedEffects(ServerOperationPreparation preparation)
    {
        ValidatePreparedCounts(preparation);
        if (preparation.Result.Kind == OperationResultKind.Rejected && HasPreparedEffects(preparation))
        {
            throw new InvalidOperationException("A rejected preparation cannot carry durable state or events.");
        }

        var logicalBytes = GetResultBytes(preparation.Result);
        logicalBytes = AddPreparedStateBytes(logicalBytes, preparation.NewState);
        logicalBytes = AddPreparedConflictBytes(logicalBytes, preparation);
        logicalBytes = AddPreparedEventBytes(logicalBytes, preparation);
        if (logicalBytes <= _options.MaximumPreparedLogicalBytes)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(preparation), logicalBytes, "The prepared result logical byte count is outside the configured bounds.");
    }

    /// <summary>Validates prepared conflict and event counts.</summary>
    /// <param name="preparation">The preparation.</param>
    /// <exception cref="ArgumentOutOfRangeException">A prepared collection exceeds configured bounds.</exception>
    private void ValidatePreparedCounts(ServerOperationPreparation preparation)
    {
        if (preparation.Conflicts.Count > _options.MaximumPreparedConflicts)
        {
            throw new ArgumentOutOfRangeException(nameof(preparation), preparation.Conflicts.Count, "The prepared conflict count is outside the configured bounds.");
        }

        if (preparation.Events.Count <= _options.MaximumPreparedEvents)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(preparation), preparation.Events.Count, "The prepared event count is outside the configured bounds.");
    }

    /// <summary>Returns retry delay when at least one operation needs another push.</summary>
    /// <param name="results">The operation results.</param>
    /// <returns>The retry delay.</returns>
    private TimeSpan? GetRetryAfter(OperationSyncResult[] results)
    {
        for (var index = 0; index < results.Length; index++)
        {
            if (results[index].Kind == OperationResultKind.Retryable)
            {
                return _options.RetryAfter;
            }
        }

        return null;
    }
}
