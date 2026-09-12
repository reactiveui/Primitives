// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates atomic local stream recovery and optimistic operation commits.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <typeparam name="TInput">The local input type.</typeparam>
internal sealed class LocalStreamCommitter<TState, TInput>
{
    /// <summary>The message used when an async operation overlaps another one.</summary>
    private const string BusyMessage = "A local stream transaction is already in progress.";

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
            cancellationToken.ThrowIfCancellationRequested();
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
    internal async ValueTask<LocalStreamCommitResult<TState, TInput>> CommitAsync(
        TInput input,
        OperationPolicy policy,
        CancellationToken cancellationToken)
    {
        EnterExclusive();
        try
        {
            ValidatePolicy(policy);
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
            var decodedInput = await DecodeInputAsync(payload, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            var operation = CreateOperation(policy, observed.NextClientSequence, operationId, timestamp, payload);
            var nextStateValue = _options.Dependencies.Projection.ApplyLocal(observed.State, decodedInput, operation);
            cancellationToken.ThrowIfCancellationRequested();
            var nextStatePayload = await _options.Dependencies.Serializer
                .SerializeAsync(_options.Contracts.StateContractId, _options.Contracts.StateSchemaVersion, nextStateValue, cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            var mutation = new SnapshotMutation(
                _options.StreamId,
                nextStatePayload,
                _options.Contracts.SnapshotFormatVersion,
                observed.Revision);
            var storeResult = await _options.Dependencies.Store
                .CommitLocalOperationAsync(operation, mutation, cancellationToken)
                .ConfigureAwait(false);
            ValidateStoreResult(storeResult, operation, observed.Revision);

            var nextState = new LocalStreamCommitterState<TState>(
                _options.StreamId,
                observed.SubscriptionId,
                nextStateValue,
                storeResult.SnapshotRevision,
                checked(operation.ClientSequence + 1),
                observed.ServerCursor);
            SwapCurrent(nextState);
            var receipt = new PublishReceipt(
                storeResult.OperationId,
                storeResult.ClientSequence,
                SyncOperationState.SavedLocally,
                storeResult.CommittedAtUtc);
            return new(receipt, operation, decodedInput, nextState);
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

    /// <summary>Decodes recovered store state.</summary>
    /// <param name="recovered">The recovered stream.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovered committer state.</returns>
    /// <exception cref="InvalidOperationException">The recovered stream or snapshot is not safe to use.</exception>
    private async ValueTask<LocalStreamCommitterState<TState>> DecodeRecoveredStateAsync(
        RecoveredStream recovered,
        CancellationToken cancellationToken)
    {
        if (recovered is null)
        {
            throw new InvalidOperationException("Local store recovery returned no stream state.");
        }

        if (recovered.SubscriptionId != _options.SubscriptionId)
        {
            throw new InvalidOperationException("Recovered subscription identity does not match the configured subscription.");
        }

        if (recovered.Snapshot is null)
        {
            return DecodePristineRecovery(recovered);
        }

        ValidateSnapshotHeader(recovered.Snapshot);
        ValidateRecoveredCounters(recovered, recovered.Snapshot);
        try
        {
            var value = await _options.Dependencies.Serializer
                .DeserializeAsync(recovered.Snapshot.State, typeof(TState), cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (value is TState typed)
            {
                return new(
                    _options.StreamId,
                    recovered.SubscriptionId,
                    typed,
                    recovered.Snapshot.Revision,
                    recovered.NextClientSequence,
                    recovered.ServerCursor);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException("Recovered snapshot could not be decoded.", exception);
        }

        throw new InvalidOperationException("Recovered snapshot decoded to the wrong state type.");
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

        if (snapshot.State is null)
        {
            throw new InvalidOperationException("Recovered snapshot state payload is missing.");
        }

        var contractMatches = string.Equals(snapshot.State.ContractId, _options.Contracts.StateContractId, StringComparison.Ordinal)
            && snapshot.State.SchemaVersion > 0
            && snapshot.State.SchemaVersion <= _options.Contracts.StateSchemaVersion;
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

    /// <summary>Creates an immutable local operation.</summary>
    /// <param name="policy">The operation policy.</param>
    /// <param name="clientSequence">The assigned client sequence.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="timestamp">The operation timestamp.</param>
    /// <param name="payload">The input payload.</param>
    /// <returns>The synchronization operation.</returns>
    private SyncOperation CreateOperation(
        OperationPolicy policy,
        long clientSequence,
        OperationId operationId,
        DateTimeOffset timestamp,
        PayloadEnvelope payload) =>
        new()
        {
            OperationId = operationId,
            StreamId = _options.StreamId,
            ClientSequence = clientSequence,
            TimestampUtc = timestamp,
            Type = SyncOperationType.Update,
            Payload = payload,
            Policy = policy,
            Metadata = new Dictionary<string, string>(),
        };

    /// <summary>Validates a policy for this durable atomic kernel.</summary>
    /// <param name="policy">The policy.</param>
    /// <exception cref="InvalidOperationException">The policy is not supported.</exception>
    private void ValidatePolicy(OperationPolicy policy)
    {
        ArgumentExceptionHelper.ThrowIfNull(policy);
        policy.Validate(_options.MinimumPriority, _options.MaximumPriority);
        if (policy.Durability == OperationDurability.Durable)
        {
            return;
        }

        throw new InvalidOperationException("Local stream commits require durable operation policy.");
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
