// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <typeparam name="TInput">The local input type.</typeparam>
internal sealed partial class LocalStreamCommitter<TState, TInput>
{
    /// <summary>The message used when an async operation overlaps another one.</summary>
    private const string BusyMessage = "A local stream transaction is already in progress.";

    /// <summary>The maximum encoded server cursor size accepted by the local remote receive path.</summary>
    private const int MaximumCursorUtf8Bytes = 4096;

    /// <summary>The immutable committer options.</summary>
    private readonly LocalStreamCommitterOptions<TState, TInput> _options;

    /// <summary>The lock protecting current-state reads and swaps.</summary>
    private readonly Lock _gate = new();

    /// <summary>The current state snapshot.</summary>
    private LocalStreamCommitterState<TState> _current;

    /// <summary>Tracks an active asynchronous call without building waiter lists.</summary>
    private int _busy;

    /// <summary>Tracks unrecoverable uncertainty after a violated store contract.</summary>
    private bool _poisoned;

    /// <summary>Tracks whether store recovery has completed successfully.</summary>
    private bool _recovered;

    /// <summary>Stores the monotonic per-committer queue diagnostic revision.</summary>
    private long _queueDiagnosticRevision;

    /// <summary>Initializes a new instance of the <see cref="LocalStreamCommitter{TState,TInput}"/> class.</summary>
    /// <param name="options">The committer options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is malformed.</exception>
    public LocalStreamCommitter(LocalStreamCommitterOptions<TState, TInput> options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _current = CreateInitialState(options);
    }

    /// <summary>Gets an immutable snapshot of the current state.</summary>
    public LocalStreamCommitterState<TState> Current
    {
        get
        {
#if NET9_0_OR_GREATER
            using var scope = _gate.EnterScope();
            return _current;
#else
            lock (_gate)
            {
                return _current;
            }
#endif
        }
    }

    /// <summary>Gets the bounded queue aggregate observed during the last successful queue transition.</summary>
    internal QueueDiagnosticSnapshot RecoveredQueueSnapshot { get; private set; }

    /// <summary>Gets recovered upload scheduling metadata when durable work is pending.</summary>
    internal RecoveredUploadHead? RecoveredUploadHead { get; private set; }

    /// <summary>Recovers durable stream state from the store.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovered state.</returns>
    internal async ValueTask<LocalStreamCommitterState<TState>> RecoverAsync(CancellationToken cancellationToken)
    {
        EnterExclusive();
        try
        {
            _recovered = false;
            cancellationToken.ThrowIfCancellationRequested();
            var recovered = await _options.Dependencies.Store
                .RecoverStreamAsync(_options.StreamId, _options.SubscriptionId, cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var state = await DecodeRecoveredStateAsync(recovered, cancellationToken).ConfigureAwait(false);
            var queueSnapshot = CreateRecoveredQueueSnapshot(recovered);
            var recoveredUploadHead = CreateRecoveredUploadHead(recovered);
            cancellationToken.ThrowIfCancellationRequested();
            SetRecoveredQueueSnapshot(queueSnapshot);
            RecoveredUploadHead = recoveredUploadHead;
            SwapCurrent(state);
            return state;
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Commits a local input atomically with its optimistic snapshot.</summary>
    /// <param name="input">The caller input.</param>
    /// <param name="policy">The operation policy.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask<LocalStreamCommitResult<TState, TInput>> CommitAsync(
        TInput input,
        OperationPolicy policy,
        CancellationToken cancellationToken) => CommitAsync(input, policy, null, cancellationToken);

    /// <summary>Commits a local input with its concurrency version and optimistic snapshot.</summary>
    /// <param name="input">The caller input.</param>
    /// <param name="policy">The operation policy.</param>
    /// <param name="baseVersion">The optional authoritative version observed by the caller.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    internal async ValueTask<LocalStreamCommitResult<TState, TInput>> CommitAsync(
        TInput input,
        OperationPolicy policy,
        string? baseVersion,
        CancellationToken cancellationToken)
    {
        EnterExclusive();
        try
        {
            ValidatePolicy(policy);
            SerializedOperationValidation.ValidateOptionalText(baseVersion, "Operation base version is malformed.");
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            var observed = Current;
            ThrowIfSequenceOverflow(observed.NextClientSequence);
            ThrowIfRevisionOverflow(observed.Revision);

            var operationId = _options.Dependencies.OperationIdSource.New();
            ThrowIfDefaultOperationId(operationId);
            var timestamp = _options.Dependencies.TimeProvider.GetUtcNow();
            var payload = await _options.Dependencies.Serializer
                .SerializeAsync(_options.Contracts.InputContractId, _options.Contracts.InputSchemaVersion, input, cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var decodedInput = await DecodeLocalInputAsync(payload, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            var operation = CreateOperation(policy, observed.NextClientSequence, operationId, timestamp, payload, baseVersion);
            var prepared = await PrepareProjectionStateAsync(observed, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var nextStateValue = _options.Dependencies.Projection.ApplyLocal(prepared.State, decodedInput, operation);
            cancellationToken.ThrowIfCancellationRequested();
            return await CommitPreparedLocalAsync(operation, decodedInput, nextStateValue, prepared.Payload, observed, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Applies a remote batch atomically with inbox deduplication and cursor advancement.</summary>
    /// <param name="batch">The received remote batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The remote commit result.</returns>
    internal async ValueTask<RemoteStreamCommitResult<TState, TInput>> ApplyRemoteBatchAsync(
        RemoteEventBatch batch,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        EnterExclusive();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfNotRecovered();
            var observed = Current;
            ValidateCompleteRemoteBatch(batch);
            ValidateRemoteBatchHeader(batch);
            var allEventIds = GetRemoteEventIds(batch);
            var unappliedLookupResult = await _options.Dependencies.Store
                .GetUnappliedEventIdsAsync(_options.StreamId, allEventIds, cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var unappliedEventIds = CreateUnappliedEventIdSnapshot(allEventIds, unappliedLookupResult);

            var filteredEvents = FilterUnappliedEvents(batch.Events, unappliedEventIds);
            var duplicateCount = checked(batch.Events.Count - filteredEvents.Count);
            if (TryGetDuplicateReplayCursor(batch, observed.ServerCursor, filteredEvents, out var replayCursor))
            {
                if (batch.CompletedOperations.Count == 0)
                {
                    return CreateDuplicateRemoteResult(batch, observed, replayCursor, duplicateCount, RecoveredQueueSnapshot);
                }

                batch = new(batch.BatchId, batch.StreamId, replayCursor, replayCursor, batch.Events) { CompletedOperations = batch.CompletedOperations };
            }

            var recovered = await _options.Dependencies.Store.RecoverStreamAsync(_options.StreamId, _options.SubscriptionId, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            ValidateReplayRecovery(recovered, observed);
            return await CommitFilteredRemoteBatchAsync(batch, observed, filteredEvents, duplicateCount, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ExitExclusive();
        }
    }

    /// <summary>Rejects client sequence overflow before store mutation.</summary>
    /// <param name="nextClientSequence">The next sequence.</param>
    /// <exception cref="InvalidOperationException">The next sequence cannot be incremented after commit.</exception>
    private static void ThrowIfSequenceOverflow(long nextClientSequence)
    {
        if (nextClientSequence is > 0 and < long.MaxValue)
        {
            return;
        }

        throw new InvalidOperationException("Client sequence overflow would make the commit unrecoverable.");
    }

    /// <summary>Rejects snapshot revision overflow before store mutation.</summary>
    /// <param name="revision">The current revision.</param>
    /// <exception cref="InvalidOperationException">The next revision cannot be represented.</exception>
    private static void ThrowIfRevisionOverflow(long revision)
    {
        if (revision is >= 0 and < long.MaxValue)
        {
            return;
        }

        throw new InvalidOperationException("Snapshot revision overflow would make the commit unrecoverable.");
    }

    /// <summary>Creates the received remote event identifier list.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <returns>The event identifiers.</returns>
    private static List<Guid> GetRemoteEventIds(RemoteEventBatch batch)
    {
        List<Guid> eventIds = [with(capacity: batch.Events.Count)];
        for (var index = 0; index < batch.Events.Count; index++)
        {
            eventIds.Add(batch.Events[index].EventId);
        }

        return eventIds;
    }

    /// <summary>Filters events to the store-selected unapplied identifiers while preserving received order.</summary>
    /// <param name="events">The received events.</param>
    /// <param name="unappliedEventIds">The selected identifiers.</param>
    /// <returns>The filtered events.</returns>
    private static List<RemoteEvent> FilterUnappliedEvents(
        IReadOnlyList<RemoteEvent> events,
        List<Guid> unappliedEventIds)
    {
        HashSet<Guid> unapplied = new(unappliedEventIds);
        List<RemoteEvent> filtered = [with(capacity: unappliedEventIds.Count)];
        for (var index = 0; index < events.Count; index++)
        {
            var remoteEvent = events[index];
            if (unapplied.Contains(remoteEvent.EventId))
            {
                filtered.Add(remoteEvent);
            }
        }

        return filtered;
    }

    /// <summary>Validates the previous cursor for batches that contain new events.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <param name="currentCursor">The current durable cursor.</param>
    /// <exception cref="InvalidOperationException">The batch does not follow the current cursor.</exception>
    private static void ValidateRemoteCursor(RemoteEventBatch batch, string? currentCursor)
    {
        if (string.Equals(batch.PreviousCursor, currentCursor, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("Remote batch previous cursor does not match the current stream cursor.");
    }

    /// <summary>Determines whether a duplicate-only batch is an old replay.</summary>
    /// <param name="batch">The original batch.</param>
    /// <param name="currentCursor">The current cursor.</param>
    /// <param name="filteredEvents">The filtered new events.</param>
    /// <param name="replayCursor">The durable cursor to report for an old replay.</param>
    /// <returns><see langword="true"/> when the batch is a replay that must not advance state.</returns>
    /// <exception cref="InvalidOperationException">A duplicate replay cannot be verified without a current cursor.</exception>
    private static bool TryGetDuplicateReplayCursor(
        RemoteEventBatch batch,
        string? currentCursor,
        List<RemoteEvent> filteredEvents,
        out string replayCursor)
    {
        replayCursor = string.Empty;
        if (batch.Events.Count == 0 || filteredEvents.Count > 0 || string.Equals(batch.PreviousCursor, currentCursor, StringComparison.Ordinal))
        {
            return false;
        }

        if (currentCursor is { Length: > 0 })
        {
            replayCursor = currentCursor;
            return true;
        }

        throw new InvalidOperationException("Remote duplicate replay cannot be verified without a current stream cursor.");
    }

    /// <summary>Creates a no-op result for a fully duplicate replay.</summary>
    /// <param name="batch">The original batch.</param>
    /// <param name="observed">The observed state.</param>
    /// <param name="currentCursor">The current durable cursor.</param>
    /// <param name="duplicateCount">The duplicate count.</param>
    /// <param name="queueSnapshot">The current queue snapshot.</param>
    /// <returns>The no-op remote commit result.</returns>
    private static RemoteStreamCommitResult<TState, TInput> CreateDuplicateRemoteResult(
        RemoteEventBatch batch,
        LocalStreamCommitterState<TState> observed,
        string currentCursor,
        int duplicateCount,
        QueueDiagnosticSnapshot queueSnapshot)
    {
        var receipt = new RemoteApplyResult(currentCursor, 0, duplicateCount, observed.Revision);
        var filteredBatch = new RemoteEventBatch(batch.BatchId, batch.StreamId, batch.PreviousCursor, batch.NextCursor, []);
        return new(receipt, filteredBatch, new System.Collections.ObjectModel.ReadOnlyCollection<TInput>([]), observed, queueSnapshot, CursorAdvanced: false);
    }

    /// <summary>Rejects default operation identifiers before persistence.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <exception cref="InvalidOperationException">The operation identifier is default.</exception>
    private static void ThrowIfDefaultOperationId(OperationId operationId)
    {
        if (operationId.Value != Guid.Empty)
        {
            return;
        }

        throw new InvalidOperationException("OperationIdSource returned the default operation identifier.");
    }

    /// <summary>Validates recovered counters and cursor consistency.</summary>
    /// <param name="recovered">The recovered stream.</param>
    /// <param name="snapshot">The recovered snapshot.</param>
    /// <exception cref="InvalidOperationException">Recovered metadata is malformed.</exception>
    private static void ValidateRecoveredCounters(RecoveredStream recovered, LocalSnapshot snapshot)
    {
        if (recovered.NextClientSequence <= 0)
        {
            throw new InvalidOperationException("Recovered next client sequence must be positive.");
        }

        if (snapshot.Revision < 0)
        {
            throw new InvalidOperationException("Recovered snapshot revision must not be negative.");
        }

        if (string.Equals(recovered.ServerCursor, snapshot.ServerCursor, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("Recovered cursor does not match the snapshot cursor.");
    }

    /// <summary>Creates the initial state from the pure projection.</summary>
    /// <param name="options">The committer options.</param>
    /// <returns>The initial state snapshot.</returns>
    private static LocalStreamCommitterState<TState> CreateInitialState(LocalStreamCommitterOptions<TState, TInput> options) =>
        new(
            options.StreamId,
            options.SubscriptionId,
            options.Dependencies.Projection.InitialState,
            Revision: 0,
            NextClientSequence: 1,
            ServerCursor: null);

    /// <summary>Validates the final event cursor matches the batch next cursor.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <exception cref="InvalidOperationException">The final event cursor does not match the batch cursor.</exception>
    private static void ValidateFinalEventCursor(RemoteEventBatch batch)
    {
        if (batch.Events.Count == 0)
        {
            return;
        }

        var finalEvent = batch.Events[batch.Events.Count - 1];
        if (string.Equals(finalEvent.ServerCursor, batch.NextCursor, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("Remote batch next cursor must match the last event cursor.");
    }

    /// <summary>Validates an optional server cursor.</summary>
    /// <param name="cursor">The cursor.</param>
    /// <param name="displayName">The display name used in the exception.</param>
    /// <exception cref="InvalidOperationException">The cursor is malformed.</exception>
    private static void ValidateOptionalCursor(string? cursor, string displayName)
    {
        if (cursor is null)
        {
            return;
        }

        ValidateCursor(cursor, displayName);
    }

    /// <summary>Validates a required server cursor.</summary>
    /// <param name="cursor">The cursor.</param>
    /// <param name="displayName">The display name used in the exception.</param>
    /// <exception cref="InvalidOperationException">The cursor is malformed.</exception>
    private static void ValidateCursor(string? cursor, string displayName)
    {
        if (cursor is { Length: > 0 }
            && IsWellFormedUnicode(cursor)
            && Encoding.UTF8.GetByteCount(cursor) <= MaximumCursorUtf8Bytes)
        {
            return;
        }

        throw new InvalidOperationException($"{displayName} must be non-empty, well-formed Unicode, and no more than 4096 UTF-8 bytes.");
    }

    /// <summary>Determines whether a string contains only well-formed UTF-16 surrogate pairs.</summary>
    /// <param name="value">The value to validate.</param>
    /// <returns><see langword="true"/> when the value is well-formed; otherwise, <see langword="false"/>.</returns>
    private static bool IsWellFormedUnicode(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (char.IsHighSurrogate(character))
            {
                if (index == value.Length - 1 || !char.IsLowSurrogate(value[index + 1]))
                {
                    return false;
                }

                index++;
            }
            else if (char.IsLowSurrogate(character))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Enters the exclusive asynchronous call lane.</summary>
    /// <exception cref="InvalidOperationException">The committer is busy or poisoned.</exception>
    private void EnterExclusive()
    {
        if (Interlocked.CompareExchange(ref _busy, 1, 0) != 0)
        {
            throw new InvalidOperationException(BusyMessage);
        }

        if (!_poisoned)
        {
            return;
        }

        ExitExclusive();
        throw new InvalidOperationException("The local stream committer is poisoned by an uncertain store result.");
    }

    /// <summary>Leaves the exclusive asynchronous call lane.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ExitExclusive() => Volatile.Write(ref _busy, 0);

    /// <summary>Swaps the current state under the state lock.</summary>
    /// <param name="state">The new state.</param>
    private void SwapCurrent(LocalStreamCommitterState<TState> state)
    {
#if NET9_0_OR_GREATER
        using var scope = _gate.EnterScope();
        _current = state;
#else
        lock (_gate)
        {
            _current = state;
        }
#endif
        _recovered = true;
    }

    /// <summary>Decodes recovery when no snapshot exists.</summary>
    /// <param name="recovered">The recovered stream.</param>
    /// <returns>The initial pristine state.</returns>
    /// <exception cref="InvalidOperationException">The stream is not pristine.</exception>
    private LocalStreamCommitterState<TState> DecodePristineRecovery(RecoveredStream recovered)
    {
        if (recovered.NextClientSequence == 1
            && recovered.PendingOperations.Count == 0
            && recovered.DeadLetters.Count == 0
            && string.IsNullOrEmpty(recovered.ServerCursor))
        {
            return new(
                _options.StreamId,
                recovered.SubscriptionId,
                _options.Dependencies.Projection.InitialState,
                Revision: 0,
                NextClientSequence: 1,
                ServerCursor: null);
        }

        throw new InvalidOperationException("Recovered stream has no valid snapshot and is not pristine.");
    }

    /// <summary>Validates recovered snapshot metadata before decoding.</summary>
    /// <param name="snapshot">The snapshot.</param>
    /// <exception cref="InvalidOperationException">The snapshot metadata is invalid.</exception>
    private void ValidateSnapshotHeader(LocalSnapshot snapshot)
    {
        if (snapshot.StreamId != _options.StreamId)
        {
            throw new InvalidOperationException("Recovered snapshot belongs to a different stream.");
        }

        if (snapshot.FormatVersion != _options.Contracts.SnapshotFormatVersion)
        {
            throw new InvalidOperationException("Recovered snapshot format is not supported.");
        }

        ValidateStatePayload(snapshot.State);
        if (snapshot.AuthoritativeState is not { } authoritativeState)
        {
            return;
        }

        ValidateStatePayload(authoritativeState);
    }

    /// <summary>Validates the configured state payload contract before deserialization.</summary>
    /// <param name="payload">The state payload.</param>
    /// <exception cref="InvalidOperationException">The payload is missing or uses another state contract.</exception>
    private void ValidateStatePayload(PayloadEnvelope? payload)
    {
        if (payload is null)
        {
            throw new InvalidOperationException("Recovered snapshot state payload is missing.");
        }

        var contractMatches = string.Equals(payload.ContractId, _options.Contracts.StateContractId, StringComparison.Ordinal)
            && payload.SchemaVersion > 0
            && payload.SchemaVersion <= _options.Contracts.StateSchemaVersion;
        if (contractMatches)
        {
            return;
        }

        throw new InvalidOperationException("Recovered snapshot contract does not match the configured state contract.");
    }

    /// <summary>Decodes a committed input payload.</summary>
    /// <param name="payload">The input payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">The serializer returned a value with the wrong type.</exception>
    private async ValueTask<TInput> DecodeInputAsync(PayloadEnvelope payload, CancellationToken cancellationToken)
    {
        var decoded = await _options.Dependencies.Serializer
            .DeserializeAsync(payload, typeof(TInput), cancellationToken)
            .ConfigureAwait(false);
        if (decoded is not TInput typed)
        {
            throw new InvalidOperationException("Serialized input decoded to the wrong input type.");
        }

        return typed;
    }

    /// <summary>Decodes unpublished or caller-supplied local input without quarantining the stream.</summary>
    /// <param name="payload">The input payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">The input payload is rejected.</exception>
    private async ValueTask<TInput> DecodeLocalInputAsync(PayloadEnvelope payload, CancellationToken cancellationToken)
    {
        try
        {
            return await DecodeInputAsync(payload, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PayloadSchemaException exception)
        {
            throw new InvalidOperationException("Local input payload could not be decoded.", exception);
        }
    }

    /// <summary>Decodes filtered remote event inputs.</summary>
    /// <param name="events">The remote events to decode.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The decoded inputs.</returns>
    private async ValueTask<IReadOnlyList<TInput>> DecodeRemoteInputsAsync(
        List<RemoteEvent> events,
        CancellationToken cancellationToken)
    {
        List<TInput> decodedInputs = [with(capacity: events.Count)];
        foreach (var remoteEvent in events)
        {
            var decoded = await DecodeRemoteInputAsync(remoteEvent, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            decodedInputs.Add(decoded);
        }

        return new System.Collections.ObjectModel.ReadOnlyCollection<TInput>(decodedInputs);
    }

    /// <summary>Decodes one received remote event input.</summary>
    /// <param name="remoteEvent">The remote event.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The decoded input.</returns>
    /// <exception cref="InvalidOperationException">The payload is quarantined or decoded to the wrong type.</exception>
    private async ValueTask<TInput> DecodeRemoteInputAsync(RemoteEvent remoteEvent, CancellationToken cancellationToken)
    {
        try
        {
            return await DecodeInputAsync(remoteEvent.Payload, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (PayloadSchemaException exception)
        {
            await QuarantineRemoteEventAsync(remoteEvent, exception, cancellationToken).ConfigureAwait(false);
            throw CreateQuarantinedStreamException("Remote event payload was quarantined.", exception);
        }
    }

    /// <summary>Applies filtered remote events to a projected state.</summary>
    /// <param name="state">The starting state.</param>
    /// <param name="events">The remote events.</param>
    /// <param name="inputs">The decoded inputs.</param>
    /// <returns>The projected state.</returns>
    private TState ApplyRemoteProjection(
        TState state,
        IReadOnlyList<RemoteEvent> events,
        IReadOnlyList<TInput> inputs)
    {
        var current = state;
        for (var index = 0; index < events.Count; index++)
        {
            current = _options.Dependencies.Projection.ApplyRemote(current, inputs[index], events[index]);
        }

        return current;
    }

    /// <summary>Commits the filtered remote batch once inbox and cursor checks pass.</summary>
    /// <param name="batch">The original remote batch.</param>
    /// <param name="observed">The observed committer state.</param>
    /// <param name="filteredEvents">The events selected by durable inbox lookup.</param>
    /// <param name="duplicateCount">The duplicate count from the original batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The remote commit result.</returns>
    /// <exception cref="InvalidOperationException">The remote cursor or store receipt violates the transaction contract.</exception>
    private async ValueTask<RemoteStreamCommitResult<TState, TInput>> CommitFilteredRemoteBatchAsync(
        RemoteEventBatch batch,
        LocalStreamCommitterState<TState> observed,
        List<RemoteEvent> filteredEvents,
        int duplicateCount,
        CancellationToken cancellationToken)
    {
        ValidateRemoteCursor(batch, observed.ServerCursor);
        ThrowIfRevisionOverflow(observed.Revision);
        var decodedInputs = await DecodeRemoteInputsAsync(filteredEvents, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        var rebuilt = await RebuildRemoteStateAsync(batch, observed, filteredEvents, decodedInputs, cancellationToken).ConfigureAwait(false);
        var nextStateValue = rebuilt.State;
        cancellationToken.ThrowIfCancellationRequested();
        var nextStatePayload = await _options.Dependencies.Serializer
            .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, nextStateValue, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateStatePayload(nextStatePayload);

        var filteredBatch = new RemoteEventBatch(batch.BatchId, batch.StreamId, batch.PreviousCursor, batch.NextCursor, filteredEvents);
        var mutation = new SnapshotMutation(_options.StreamId, nextStatePayload, _options.Contracts.SnapshotFormatVersion, observed.Revision) { AuthoritativeState = rebuilt.Authoritative };
        var queueSnapshot = RecoveredQueueSnapshot;
        var storeResult = await ApplyRemoteStoreTransactionAsync(batch, mutation, filteredEvents.Count, cancellationToken)
            .ConfigureAwait(false);
        var nextState = new LocalStreamCommitterState<TState>(
            _options.StreamId,
            observed.SubscriptionId,
            nextStateValue,
            storeResult.SnapshotRevision,
            observed.NextClientSequence,
            storeResult.NextCursor) { MaterializedPayload = nextStatePayload, AuthoritativePayload = rebuilt.Authoritative };
        SwapCurrent(nextState);
        CommitQueueDiagnosticSnapshotIfChanged(queueSnapshot);
        var receipt = storeResult with { DuplicateCount = duplicateCount };
        var cursorAdvanced = !string.Equals(observed.ServerCursor, storeResult.NextCursor, StringComparison.Ordinal);
        return new(receipt, filteredBatch, decodedInputs, nextState, queueSnapshot, cursorAdvanced);
    }

    /// <summary>Applies the complete remote batch to the local store under its revision fence.</summary>
    /// <param name="batch">The complete remote batch.</param>
    /// <param name="mutation">The prepared snapshot mutation.</param>
    /// <param name="appliedCount">The number of projected new events.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The remote apply receipt.</returns>
    /// <exception cref="InvalidOperationException">The store receipt violates the transaction contract.</exception>
    private async ValueTask<RemoteApplyResult> ApplyRemoteStoreTransactionAsync(
        RemoteEventBatch batch,
        SnapshotMutation mutation,
        int appliedCount,
        CancellationToken cancellationToken)
    {
        var storeResult = await _options.Dependencies.Store
            .ApplyRemoteBatchAsync(batch, mutation, cancellationToken)
            .ConfigureAwait(false);
        ValidateRemoteStoreResult(storeResult, batch.NextCursor, appliedCount, batch.Events.Count - appliedCount, mutation.ExpectedRevision);
        return storeResult;
    }

    /// <summary>Creates an immutable local operation.</summary>
    /// <param name="policy">The operation policy.</param>
    /// <param name="clientSequence">The assigned client sequence.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="timestamp">The operation timestamp.</param>
    /// <param name="payload">The input payload.</param>
    /// <param name="baseVersion">The optional authoritative version observed by the caller.</param>
    /// <returns>The synchronization operation.</returns>
    private SyncOperation CreateOperation(
        OperationPolicy policy,
        long clientSequence,
        OperationId operationId,
        DateTimeOffset timestamp,
        PayloadEnvelope payload,
        string? baseVersion) =>
        new()
        {
            OperationId = operationId,
            StreamId = _options.StreamId,
            ClientSequence = clientSequence,
            TimestampUtc = timestamp,
            Type = SyncOperationType.Update,
            Payload = payload,
            Policy = policy,
            BaseVersion = baseVersion,
            Metadata = new Dictionary<string, string>(),
        };

    /// <summary>Validates the atomicity and durability required by the operation policy.</summary>
    /// <param name="policy">The policy.</param>
    /// <exception cref="InvalidOperationException">The policy is not supported.</exception>
    private void ValidatePolicy(OperationPolicy policy)
    {
        ArgumentExceptionHelper.ThrowIfNull(policy);
        policy.Validate(_options.MinimumPriority, _options.MaximumPriority);
        var requiredCapabilities = LocalStoreCapabilities.AtomicLocalCommit;
        if (policy.Durability == OperationDurability.Durable)
        {
            requiredCapabilities |= LocalStoreCapabilities.DurableLocalCommit;
        }

        if ((_options.Dependencies.Store.Capabilities & requiredCapabilities) == requiredCapabilities)
        {
            return;
        }

        throw new InvalidOperationException("Local stream commits require atomic storage and the durability requested by the operation policy.");
    }

    /// <summary>Validates the store result before making state visible.</summary>
    /// <param name="result">The store result.</param>
    /// <param name="operation">The committed operation.</param>
    /// <param name="expectedRevision">The expected prior revision.</param>
    /// <exception cref="InvalidOperationException">The store result violates the transaction contract.</exception>
    private void ValidateStoreResult(LocalCommitResult? result, SyncOperation operation, long expectedRevision)
    {
        if (result is not null
            && result.OperationId == operation.OperationId
            && result.ClientSequence == operation.ClientSequence
            && result.SnapshotRevision == expectedRevision + 1)
        {
            return;
        }

        _poisoned = true;
        throw new InvalidOperationException("The local store returned a malformed commit receipt.");
    }

    /// <summary>Validates remote batch metadata that does not require durable inbox state.</summary>
    /// <param name="batch">The remote batch.</param>
    /// <exception cref="InvalidOperationException">The batch metadata is invalid.</exception>
    private void ValidateRemoteBatchHeader(RemoteEventBatch batch)
    {
        if (batch.StreamId != _options.StreamId)
        {
            throw new InvalidOperationException("Remote batch belongs to a different stream.");
        }

        ValidateCursor(batch.NextCursor, "Remote batch next cursor");
        ValidateOptionalCursor(batch.PreviousCursor, "Remote batch previous cursor");

        for (var index = 0; index < batch.Events.Count; index++)
        {
            ValidateRemoteEvent(batch.Events[index]);
        }

        ValidateFinalEventCursor(batch);
    }

    /// <summary>Validates a remote event before inbox lookup.</summary>
    /// <param name="remoteEvent">The remote event.</param>
    /// <exception cref="InvalidOperationException">The remote event metadata is invalid.</exception>
    private void ValidateRemoteEvent(RemoteEvent remoteEvent)
    {
        ValidateCursor(remoteEvent.ServerCursor, "Remote event cursor");
        if (remoteEvent.CausedByOperationId.HasValue && remoteEvent.CausedByOperationId.Value.Value == Guid.Empty)
        {
            throw new InvalidOperationException("Remote event causal operation identifier must be non-empty.");
        }

        ValidateRemotePayload(remoteEvent.Payload);
    }

    /// <summary>Validates a remote event payload before it is decoded.</summary>
    /// <param name="payload">The payload.</param>
    /// <exception cref="InvalidOperationException">The payload is missing or does not match the input contract.</exception>
    private void ValidateRemotePayload(PayloadEnvelope? payload)
    {
        if (payload is null)
        {
            throw new InvalidOperationException("Remote event payload is missing.");
        }

        var contractMatches = string.Equals(payload.ContractId, _options.Contracts.InputContractId, StringComparison.Ordinal)
            && payload.SchemaVersion > 0
            && payload.SchemaVersion <= _options.Contracts.InputSchemaVersion;
        if (contractMatches)
        {
            return;
        }

        throw new InvalidOperationException("Remote event contract does not match the configured input contract.");
    }

    /// <summary>Validates that the store returned an exact subset of the received identifiers.</summary>
    /// <param name="candidateEventIds">The received event identifiers.</param>
    /// <param name="unappliedEventIds">The store-selected identifiers.</param>
    /// <returns>An owned snapshot of the store-selected event identifiers.</returns>
    /// <exception cref="InvalidOperationException">The store lookup result violates the inbox contract.</exception>
    private List<Guid> CreateUnappliedEventIdSnapshot(
        IReadOnlyList<Guid> candidateEventIds,
        IReadOnlyList<Guid>? unappliedEventIds)
    {
        if (unappliedEventIds is null)
        {
            _poisoned = true;
            throw new InvalidOperationException("The local store returned no remote inbox lookup result.");
        }

        HashSet<Guid> candidates = new(candidateEventIds);
        HashSet<Guid> seen = [];
        List<Guid> snapshot = [with(capacity: unappliedEventIds.Count)];
        for (var index = 0; index < unappliedEventIds.Count; index++)
        {
            var eventId = unappliedEventIds[index];
            if (candidates.Contains(eventId) && seen.Add(eventId))
            {
                snapshot.Add(eventId);
                continue;
            }

            _poisoned = true;
            throw new InvalidOperationException("The local store returned a malformed remote inbox lookup result.");
        }

        return snapshot;
    }

    /// <summary>Validates the store result before making remote state visible.</summary>
    /// <param name="result">The remote apply result.</param>
    /// <param name="nextCursor">The expected cursor.</param>
    /// <param name="appliedCount">The expected applied count.</param>
    /// <param name="duplicateCount">The expected duplicate count.</param>
    /// <param name="expectedRevision">The expected prior revision.</param>
    /// <exception cref="InvalidOperationException">The store result violates the transaction contract.</exception>
    private void ValidateRemoteStoreResult(
        RemoteApplyResult? result,
        string nextCursor,
        int appliedCount,
        int duplicateCount,
        long expectedRevision)
    {
        if (result is not null
            && string.Equals(result.NextCursor, nextCursor, StringComparison.Ordinal)
            && result.AppliedCount == appliedCount
            && result.DuplicateCount == duplicateCount
            && result.SnapshotRevision == expectedRevision + 1)
        {
            return;
        }

        _poisoned = true;
        throw new InvalidOperationException("The local store returned a malformed remote apply receipt.");
    }

    /// <summary>Rejects commits before recovery completes.</summary>
    /// <exception cref="InvalidOperationException">Recovery has not completed.</exception>
    private void ThrowIfNotRecovered()
    {
        if (_recovered)
        {
            return;
        }

        throw new InvalidOperationException("RecoverAsync must complete before committing local operations.");
    }
}
