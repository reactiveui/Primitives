// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Stores atomic process-local server state, events and terminal operation replays.</summary>
/// <remarks>
/// Authenticated tenant and client identifiers are trusted inputs from the host. This journal does not perform
/// authorization, durability, cross-process coordination or capability advertisement.
/// </remarks>
internal sealed class InMemoryServerCommitJournal : IServerCommitJournal, IServerReceiveJournal, IServerSubscriptionAcknowledgementJournal, IServerSnapshotRecoveryJournal
{
    /// <summary>Protects stream state and retained journal accounting.</summary>
    private readonly Lock _gate = new();

    /// <summary>The retained process-local stream records.</summary>
    private readonly Dictionary<ServerStreamKey, ServerCommitStreamRecord> _streams = [];

    /// <summary>The retained process-local subscription records.</summary>
    private readonly Dictionary<SubscriptionId, ServerSubscriptionRecord> _subscriptions = [];

    /// <summary>The journal options.</summary>
    private readonly ServerCommitJournalOptions _options;

    /// <summary>The retained terminal entry count.</summary>
    private int _ledgerEntryCount;

    /// <summary>The retained event count.</summary>
    private int _eventCount;

    /// <summary>The retained offered cursor count.</summary>
    private int _subscriptionOfferCount;

    /// <summary>The retained logical encoded bytes.</summary>
    private long _logicalBytes;

    /// <summary>The latest clock value accepted by commit or compaction.</summary>
    private DateTimeOffset _latestUtc = DateTimeOffset.MinValue;

    /// <summary>The highest durable subscription generation allocated.</summary>
    private long _lastSubscriptionGeneration;

    /// <summary>Initializes a new instance of the <see cref="InMemoryServerCommitJournal"/> class.</summary>
    /// <param name="options">The finite journal bounds.</param>
    internal InMemoryServerCommitJournal(ServerCommitJournalOptions? options = null)
    {
        _options = options ?? new();
        _options.Validate();
    }

    /// <summary>Gets the current retained stream count.</summary>
    internal int StreamCount
    {
        get
        {
            lock (_gate)
            {
                return _streams.Count;
            }
        }
    }

    /// <summary>Gets the current retained terminal entry count.</summary>
    internal int LedgerEntryCount
    {
        get
        {
            lock (_gate)
            {
                return _ledgerEntryCount;
            }
        }
    }

    /// <summary>Gets the current retained event count.</summary>
    internal int EventCount
    {
        get
        {
            lock (_gate)
            {
                return _eventCount;
            }
        }
    }

    /// <summary>Gets the current retained subscription count.</summary>
    internal int SubscriptionCount
    {
        get
        {
            lock (_gate)
            {
                return _subscriptions.Count;
            }
        }
    }

    /// <summary>Gets the current retained subscription offer count.</summary>
    internal int SubscriptionOfferCount
    {
        get
        {
            lock (_gate)
            {
                return _subscriptionOfferCount;
            }
        }
    }

    /// <summary>Gets the retained logical encoded byte count.</summary>
    internal long LogicalBytes
    {
        get
        {
            lock (_gate)
            {
                return _logicalBytes;
            }
        }
    }

    /// <summary>Reads a trusted bounded view used to evaluate a snapshot recovery request.</summary>
    /// <param name="request">The read request.</param>
    /// <returns>The retained snapshot-recovery view.</returns>
    internal ServerSnapshotRecoveryView ReadSnapshotRecoveryView(ServerSnapshotRecoveryReadRequest request)
    {
        ServerSnapshotRecoveryJournalOperations.ValidateReadRequest(request);
        var operationKeys = ServerSnapshotRecoveryJournalOperations.CaptureOperationProofs(request, out var fingerprints);
        lock (_gate)
        {
            _ = _streams.TryGetValue(request.StreamKey, out var stream);
            var snapshot = ServerCommitJournalOperations.CreateSnapshot(request.StreamKey, stream, operationKeys);
            ServerSubscriptionState? state = null;
            ServerSubscriptionOffer? expiredCursorOffer = null;
            if (_subscriptions.TryGetValue(request.Subscription.SubscriptionId, out var record))
            {
                ThrowIfIdentityMismatch(request.Subscription, record);
                state = ServerSubscriptionJournalOperations.CreateState(record);
                if (request.RecoveryRequest.ExpiredCursor is not null
                    && record.Offers.TryGetValue(request.RecoveryRequest.ExpiredCursor, out var offer))
                {
                    expiredCursorOffer = offer;
                }
            }

            return new()
            {
                Snapshot = snapshot,
                SubscriptionState = state,
                ExpiredCursorOffer = expiredCursorOffer,
                RequestedExpiredCursor = request.RecoveryRequest.ExpiredCursor,
                CapturedPendingOperationCount = request.RecoveryRequest.PendingOperations.Count,
                OperationDispositions = ServerSnapshotRecoveryJournalOperations.CreateOperationDispositions(snapshot, operationKeys, fingerprints),
                OperationFingerprints = fingerprints,
            };
        }
    }

    /// <summary>Durably offers a recovered snapshot cursor for later authenticated acknowledgement.</summary>
    /// <param name="request">The offer request.</param>
    /// <returns>The offer result.</returns>
    internal ServerSnapshotOfferResult TryOfferSnapshot(ServerSnapshotOfferRequest request)
    {
        if (!ServerSnapshotRecoveryJournalOperations.ValidateOfferRequest(request)
            || !ServerSnapshotRecoveryJournalOperations.OfferRequestMatchesView(request)
            || !ServerSnapshotRecoveryJournalOperations.RecoveryResultMatchesView(request.View, request.RecoveryResult))
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ValidationRejected, null, null);
        }

        var observedUtc = _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            return TryOfferSnapshotUnderGate(request, observedUtc);
        }
    }

    /// <summary>Reads a stream revision and requested terminal operation entries atomically.</summary>
    /// <param name="streamKey">The authenticated stream key.</param>
    /// <param name="operationKeys">The bounded operation keys requested for replay.</param>
    /// <returns>The atomic stream snapshot.</returns>
    internal ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys)
    {
        ServerCommitJournalGuard.ValidateStreamKey(streamKey);
        var requested = ServerCommitJournalGuard.CaptureOperationKeys(operationKeys, _options.MaximumOperationCaptureCount);
        lock (_gate)
        {
            _ = _streams.TryGetValue(streamKey, out var stream);
            return ServerCommitJournalOperations.CreateSnapshot(streamKey, stream, requested);
        }
    }

    /// <summary>Attempts to atomically admit a fully prepared terminal server commit.</summary>
    /// <param name="plan">The prepared commit plan.</param>
    /// <returns>The result and atomic stream snapshot observed by the attempt.</returns>
    internal ServerCommitResult TryCommit(ServerCommitPlan plan)
    {
        ArgumentExceptionHelper.ThrowIfNull(plan);
        var commit = ServerCommitJournalGuard.ValidatePlan(plan, _options);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            return TryCommitUnderGate(commit, observedUtc);
        }
    }

    /// <summary>Reads a bounded page of complete operation groups for receive subscribers.</summary>
    /// <param name="request">The receive page request.</param>
    /// <returns>The receive page result.</returns>
    internal ServerReceivePageResult ReadReceivePage(ServerReceivePageRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        lock (_gate)
        {
            _ = _streams.TryGetValue(request.StreamKey, out var stream);
            return ServerReceivePageOperations.Create(request, stream);
        }
    }

    /// <summary>Registers or reads a trusted subscription binding.</summary>
    /// <param name="identity">The subscription identity.</param>
    /// <returns>The persisted subscription state.</returns>
    internal ServerSubscriptionState RegisterSubscription(ServerSubscriptionIdentity identity)
    {
        ServerSubscriptionJournalOperations.ValidateIdentity(identity);
        return RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, StartPosition.FromSequence(0)));
    }

    /// <summary>Registers or reads a trusted subscription binding with an initial stream position.</summary>
    /// <param name="request">The registration request.</param>
    /// <returns>The persisted subscription state.</returns>
    internal ServerSubscriptionState RegisterSubscription(ServerSubscriptionRegistrationRequest request)
    {
        ServerSubscriptionJournalOperations.ValidateRegistrationRequest(request);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            return RegisterSubscriptionUnderGate(request, observedUtc);
        }
    }

    /// <summary>Reads and durably offers a bounded page for a registered subscription.</summary>
    /// <param name="request">The subscription page request.</param>
    /// <returns>The receive page result.</returns>
    internal ServerReceivePageResult OfferReceivePage(ServerSubscriptionPageRequest request)
    {
        ServerSubscriptionJournalOperations.ValidatePageRequest(request);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            return OfferReceivePageUnderGate(request, observedUtc);
        }
    }

    /// <summary>Durably acknowledges a previously offered complete receive position.</summary>
    /// <param name="request">The acknowledgement request.</param>
    /// <returns>The persisted subscription state after acknowledgement.</returns>
    internal ServerSubscriptionState Acknowledge(ServerSubscriptionAcknowledgementRequest request)
    {
        ServerSubscriptionJournalOperations.ValidateAcknowledgementRequest(request);
        var observedUtc = _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            return AcknowledgeUnderGate(request, observedUtc);
        }
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
        var sampledUtc = utcNow ?? _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            var compactUtc = ServerCommitJournalOperations.Max(_latestUtc, sampledUtc);
            var expired = GetExpiredRows(compactUtc);
            ApplyExpired(expired);
            CompactSubscriptions(compactUtc);
            _latestUtc = compactUtc;
            return expired.LedgerRows.Count;
        }
    }

    /// <summary>Computes the retained cursor byte delta for a commit.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <returns>The retained cursor byte delta.</returns>
    private static long GetLastCursorDelta(ServerCommitStreamRecord stream, ServerCommitValidationResult commit) =>
        commit.LastCursor is null ? 0 : commit.LastCursorBytes - stream.LastCursorBytes;

    /// <summary>Applies retained cursor byte accounting to the stream.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    private static void ApplyLastCursorBytes(ServerCommitStreamRecord stream, ServerCommitValidationResult commit)
    {
        if (commit.LastCursor is null)
        {
            return;
        }

        stream.LastCursorBytes = commit.LastCursorBytes;
    }

    /// <summary>Rejects an identity that attempts to reuse another binding's subscription id.</summary>
    /// <param name="identity">The supplied identity.</param>
    /// <param name="record">The retained record.</param>
    /// <exception cref="InvalidOperationException">The subscription belongs to another identity.</exception>
    private static void ThrowIfIdentityMismatch(ServerSubscriptionIdentity identity, ServerSubscriptionRecord record)
    {
        if (ServerSubscriptionJournalOperations.IdentityMatches(identity, record))
        {
            return;
        }

        throw new InvalidOperationException("The subscription identifier is already bound to another trusted identity.");
    }

    /// <summary>Rejects an identity or start position that conflicts with retained state.</summary>
    /// <param name="request">The supplied request.</param>
    /// <param name="record">The retained record.</param>
    /// <exception cref="InvalidOperationException">The registration is incompatible.</exception>
    private static void ThrowIfRegistrationMismatch(ServerSubscriptionRegistrationRequest request, ServerSubscriptionRecord record)
    {
        if (ServerSubscriptionJournalOperations.RegistrationMatches(request, record))
        {
            return;
        }

        throw new InvalidOperationException("The subscription registration is incompatible with retained state.");
    }

    /// <summary>Rejects a page that would move a subscription behind its durable acknowledgement.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="nextGroupSequence">The offered page sequence.</param>
    /// <exception cref="InvalidOperationException">The offered page would rewind the subscription.</exception>
    private static void ThrowIfPageRewindsAcknowledgement(ServerSubscriptionRecord record, long nextGroupSequence)
    {
        if (nextGroupSequence > record.AcknowledgedGroupSequence)
        {
            return;
        }

        throw new InvalidOperationException("The offered receive page would rewind the subscription acknowledgement.");
    }

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

    /// <summary>Checks whether a recovered checkpoint matches the current durable frontier.</summary>
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
            && SnapshotOfferRevisionMatches(offer, viewState)
            && offer.SnapshotStreamRevision == request.View.Snapshot.Revision
            && offer.SnapshotLastEventSequence == request.View.Snapshot.LastEventSequence
            && offer.SnapshotSubscriptionGeneration == viewState.Generation
            && offer.SnapshotFormatVersion == checkpoint.SnapshotFormatVersion
            && ServerSnapshotRecoveryJournalOperations.PayloadMatches(offer.SnapshotClientState, checkpoint.ClientState);
    }

    /// <summary>Checks whether a retained snapshot offer is bound to this capture or its issued lost-response retry.</summary>
    /// <param name="offer">The retained offer.</param>
    /// <param name="viewState">The captured subscription state.</param>
    /// <returns>Whether the revision fence matches.</returns>
    private static bool SnapshotOfferRevisionMatches(ServerSubscriptionOffer offer, ServerSubscriptionState viewState)
    {
        if (offer.SnapshotOriginatingSubscriptionRevision is not { } origin
            || offer.SnapshotIssuedSubscriptionRevision is not { } issued
            || origin == long.MaxValue)
        {
            return false;
        }

        return (viewState.Revision == origin || viewState.Revision == issued)
            && issued == origin + 1;
    }

    /// <summary>Allocates the next durable subscription generation.</summary>
    /// <returns>The generation.</returns>
    /// <exception cref="InvalidOperationException">The generation allocator overflowed.</exception>
    private long AllocateSubscriptionGeneration()
    {
        var generation = ServerSubscriptionJournalOperations.GetNextSubscriptionGeneration(_lastSubscriptionGeneration);
        _lastSubscriptionGeneration = generation;
        return generation;
    }

    /// <summary>Offers a recovered snapshot cursor while the journal gate is held.</summary>
    /// <param name="request">The offer request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <returns>The durable offer result.</returns>
    private ServerSnapshotOfferResult TryOfferSnapshotUnderGate(ServerSnapshotOfferRequest request, DateTimeOffset observedUtc)
    {
        if (!_subscriptions.TryGetValue(request.Subscription.SubscriptionId, out var record))
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.MissingSubscription, null, null);
        }

        return ServerSubscriptionJournalOperations.IdentityMatches(request.Subscription, record)
            ? TryOfferSnapshotForRecord(request, observedUtc, record)
            : CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ValidationRejected, null, null);
    }

    /// <summary>Offers a recovered snapshot cursor for a matching subscription record.</summary>
    /// <param name="request">The offer request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <param name="record">The retained subscription record.</param>
    /// <returns>The durable offer result.</returns>
    private ServerSnapshotOfferResult TryOfferSnapshotForRecord(
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

        _ = _streams.TryGetValue(request.StreamKey, out var stream);
        var current = ServerCommitJournalOperations.CreateSnapshot(request.StreamKey, stream, CreateOperationKeys(record.Identity, request.View));
        if (!SnapshotMatches(record.Identity, request.View, current))
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ConcurrentChange, currentState, null);
        }

        return CheckpointMatches(request, checkpoint, stream)
            && request.View.Snapshot.LastGroupSequence >= record.AcknowledgedGroupSequence
            ? TryPersistSnapshotOfferUnderGate(request, observedUtc, record, currentState, viewState, checkpoint)
            : CreateSnapshotOfferResult(ServerSnapshotOfferStatus.ValidationRejected, currentState, null);
    }

    /// <summary>Persists a recovered snapshot offer after all durable fences match.</summary>
    /// <param name="request">The offer request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <param name="record">The retained subscription record.</param>
    /// <param name="currentState">The state captured before mutation.</param>
    /// <param name="viewState">The subscription state captured in the recovery view.</param>
    /// <param name="checkpoint">The recovered snapshot checkpoint.</param>
    /// <returns>The durable offer result.</returns>
    private ServerSnapshotOfferResult TryPersistSnapshotOfferUnderGate(
        ServerSnapshotOfferRequest request,
        DateTimeOffset observedUtc,
        ServerSubscriptionRecord record,
        ServerSubscriptionState currentState,
        ServerSubscriptionState viewState,
        RemoteSnapshotCheckpoint checkpoint)
    {
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
        var offeredUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
        var latestDelta = request.View.Snapshot.LastGroupSequence > record.LatestOfferedGroupSequence
            ? ServerSubscriptionJournalOperations.GetSubscriptionCursorDelta(record.LatestOfferedCursor, cursor)
            : 0;
        var logicalBytes = ServerSubscriptionJournalOperations.GetSnapshotOfferBytes(cursor, checkpoint.ClientState);
        var addedLogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, latestDelta);
        if (!HasSubscriptionCapacity(0, 1, addedLogicalBytes) && !CompactAndCheckOfferCapacity(record, offeredUtc, addedLogicalBytes))
        {
            return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.CapacityExceeded, ServerSubscriptionJournalOperations.CreateState(record), null);
        }

        var addContext = new SnapshotOfferAddContext(
            request,
            viewState,
            checkpoint,
            cursor,
            offeredUtc,
            logicalBytes,
            issuedRevision);
        AddSnapshotOffer(record, in addContext);
        return CreateSnapshotOfferResult(ServerSnapshotOfferStatus.Offered, ServerSubscriptionJournalOperations.CreateState(record), cursor);
    }

    /// <summary>Compacts expired offers before checking whether the new offer can fit.</summary>
    /// <param name="record">The retained subscription record.</param>
    /// <param name="offeredUtc">The offer timestamp.</param>
    /// <param name="addedLogicalBytes">The logical bytes required by the new offer.</param>
    /// <returns>Whether the offer can fit after compaction.</returns>
    private bool CompactAndCheckOfferCapacity(
        ServerSubscriptionRecord record,
        DateTimeOffset offeredUtc,
        long addedLogicalBytes)
    {
        CompactSubscriptionOffers(record, offeredUtc);
        return HasSubscriptionCapacity(0, 1, addedLogicalBytes);
    }

    /// <summary>Adds a snapshot offer after every durable fence has matched.</summary>
    /// <param name="record">The retained subscription record.</param>
    /// <param name="context">The already validated offer insert context.</param>
    private void AddSnapshotOffer(ServerSubscriptionRecord record, in SnapshotOfferAddContext context)
    {
        record.Offers.Add(
            context.Cursor,
            new()
            {
                Cursor = context.Cursor,
                GroupSequence = context.Request.View.Snapshot.LastGroupSequence,
                OfferedAtUtc = context.OfferedAtUtc,
                LogicalBytes = context.LogicalBytes,
                SnapshotStreamRevision = context.Request.View.Snapshot.Revision,
                SnapshotLastEventSequence = context.Request.View.Snapshot.LastEventSequence,
                SnapshotSubscriptionGeneration = context.ViewState.Generation,
                SnapshotOriginatingSubscriptionRevision = context.ViewState.Revision,
                SnapshotIssuedSubscriptionRevision = context.IssuedRevision,
                SnapshotFormatVersion = context.Checkpoint.SnapshotFormatVersion,
                SnapshotClientState = context.Checkpoint.ClientState,
            });
        ApplyLatestOffer(record, context.Cursor, context.Request.View.Snapshot.LastGroupSequence);
        record.Revision = context.IssuedRevision;
        record.UpdatedAtUtc = context.OfferedAtUtc;
        record.LastTouchedUtc = context.OfferedAtUtc;
        _subscriptionOfferCount++;
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, context.LogicalBytes);
        _latestUtc = context.OfferedAtUtc;
    }

    /// <summary>Registers a subscription while the journal gate is held.</summary>
    /// <param name="request">The registration request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <returns>The subscription state.</returns>
    /// <exception cref="InvalidOperationException">The subscription identity conflicts with retained state.</exception>
    /// <exception cref="QueueCapacityExceededException">The subscription storage is full.</exception>
    private ServerSubscriptionState RegisterSubscriptionUnderGate(ServerSubscriptionRegistrationRequest request, DateTimeOffset observedUtc)
    {
        var updatedUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
        if (_subscriptions.TryGetValue(request.Identity.SubscriptionId, out var existing))
        {
            ThrowIfRegistrationMismatch(request, existing);
            existing.UpdatedAtUtc = updatedUtc;
            existing.LastTouchedUtc = updatedUtc;
            _latestUtc = updatedUtc;
            return ServerSubscriptionJournalOperations.CreateState(existing);
        }

        _ = _streams.TryGetValue(request.Identity.StreamKey, out var stream);
        var anchor = ServerSubscriptionStartPositionOperations.CaptureInitialAnchor(request.Identity.StreamKey, request.StartPosition, stream);
        var logicalBytes = ServerSubscriptionJournalOperations.GetSubscriptionBytes(request.Identity, request.StartPosition, anchor.Cursor);
        if (!HasSubscriptionCapacity(1, 0, logicalBytes))
        {
            CompactSubscriptions(updatedUtc);
            if (!HasSubscriptionCapacity(1, 0, logicalBytes))
            {
                throw new QueueCapacityExceededException("The server subscription acknowledgement journal is full.", canFitWhenEmpty: false);
            }
        }

        var record = new ServerSubscriptionRecord(request.Identity, updatedUtc, logicalBytes)
        {
            Generation = AllocateSubscriptionGeneration(),
            InitialStartPosition = request.StartPosition,
            InitialAnchorCursor = anchor.Cursor,
            InitialAnchorGroupSequence = anchor.GroupSequence,
            InitialAnchorResolved = anchor.IsResolved,
        };
        _subscriptions.Add(request.Identity.SubscriptionId, record);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, logicalBytes);
        _latestUtc = updatedUtc;
        return ServerSubscriptionJournalOperations.CreateState(record);
    }

    /// <summary>Offers a receive page while the journal gate is held.</summary>
    /// <param name="request">The request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <returns>The receive page.</returns>
    private ServerReceivePageResult OfferReceivePageUnderGate(ServerSubscriptionPageRequest request, DateTimeOffset observedUtc)
    {
        var record = ReadRegisteredSubscription(request.Identity);
        _ = _streams.TryGetValue(request.Identity.StreamKey, out var stream);
        if (!TryResolveInitialReadCursor(record, request.Cursor, stream, observedUtc, out var readCursor, out var pendingResult))
        {
            return pendingResult;
        }

        var receiveRequest = ServerSubscriptionJournalOperations.CreateReceiveRequest(request with { Cursor = readCursor });
        var result = ServerReceivePageOperations.Create(receiveRequest, stream);
        result = ServerSubscriptionStartPositionOperations.WithClientPreviousCursor(result, request.Cursor);
        if (result.Batch is null)
        {
            return result;
        }

        ThrowIfPageRewindsAcknowledgement(record, result.NextGroupSequence);
        AddOffer(record, result.Batch.NextCursor, result.NextGroupSequence, observedUtc);
        return result;
    }

    /// <summary>Acknowledges a cursor while the journal gate is held.</summary>
    /// <param name="request">The request.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <returns>The subscription state.</returns>
    /// <exception cref="InvalidOperationException">The acknowledgement is not valid for the subscription.</exception>
    private ServerSubscriptionState AcknowledgeUnderGate(ServerSubscriptionAcknowledgementRequest request, DateTimeOffset observedUtc)
    {
        var record = ReadRegisteredSubscription(new(request.StreamKey, request.ClientId, request.Acknowledgement.SubscriptionId));
        var cursor = request.Acknowledgement.Cursor;
        if (string.Equals(record.AcknowledgedCursor, cursor, StringComparison.Ordinal))
        {
            var duplicateUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
            record.UpdatedAtUtc = duplicateUtc;
            record.LastTouchedUtc = duplicateUtc;
            _latestUtc = duplicateUtc;
            return ServerSubscriptionJournalOperations.CreateState(record);
        }

        if (!record.Offers.TryGetValue(cursor, out var offer))
        {
            throw new InvalidOperationException("The acknowledgement cursor was not offered to this subscription.");
        }

        ServerSnapshotRecoveryJournalOperations.ThrowIfSnapshotOfferGenerationMismatch(offer, record.Generation);

        var nextRevision = ServerSubscriptionJournalOperations.GetNextSubscriptionRevision(record);
        var acknowledgedUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
        ApplySubscriptionBytesDelta(record, ServerSubscriptionJournalOperations.GetSubscriptionCursorDelta(record.AcknowledgedCursor, offer.Cursor));
        record.AcknowledgedCursor = offer.Cursor;
        record.AcknowledgedGroupSequence = offer.GroupSequence;
        record.AcknowledgedAtUtc = acknowledgedUtc;
        record.Revision = nextRevision;
        record.UpdatedAtUtc = acknowledgedUtc;
        record.LastTouchedUtc = acknowledgedUtc;
        PruneAcknowledgedOffers(record);
        _latestUtc = acknowledgedUtc;
        return ServerSubscriptionJournalOperations.CreateState(record);
    }

    /// <summary>Reads a registered subscription and validates its binding.</summary>
    /// <param name="identity">The trusted identity.</param>
    /// <returns>The retained record.</returns>
    /// <exception cref="InvalidOperationException">The subscription is missing or bound to another identity.</exception>
    private ServerSubscriptionRecord ReadRegisteredSubscription(ServerSubscriptionIdentity identity)
    {
        if (_subscriptions.TryGetValue(identity.SubscriptionId, out var record))
        {
            ThrowIfIdentityMismatch(identity, record);
            return record;
        }

        throw new InvalidOperationException("The subscription is not registered.");
    }

    /// <summary>Resolves the effective first-read cursor for a subscription.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="clientCursor">The caller-supplied cursor.</param>
    /// <param name="stream">The retained stream.</param>
    /// <param name="observedUtc">The sampled timestamp.</param>
    /// <param name="readCursor">The effective cursor to read from.</param>
    /// <param name="pendingResult">The result to return when no cursor can be resolved.</param>
    /// <returns>Whether a read cursor is available.</returns>
    private bool TryResolveInitialReadCursor(
        ServerSubscriptionRecord record,
        string? clientCursor,
        ServerCommitStreamRecord? stream,
        DateTimeOffset observedUtc,
        out string? readCursor,
        out ServerReceivePageResult pendingResult)
    {
        pendingResult = new(ServerReceivePageStatus.EndOfStream, null, 0, 0);
        if (clientCursor is not null)
        {
            readCursor = clientCursor;
            return true;
        }

        if (record.InitialAnchorResolved)
        {
            readCursor = ServerSubscriptionStartPositionOperations.GetInitialReadCursor(record);
            return true;
        }

        var resolution = ServerSubscriptionStartPositionOperations.TryResolveAnchor(
            record.Identity.StreamKey,
            record.InitialStartPosition,
            stream,
            out var anchor);
        if (resolution == ServerSubscriptionAnchorResolution.Resolved)
        {
            ApplyInitialAnchor(record, anchor, observedUtc);
            readCursor = ServerSubscriptionStartPositionOperations.GetInitialReadCursor(record);
            return true;
        }

        readCursor = null;
        var lastGroupSequence = stream?.LastGroupSequence ?? 0;
        var status = resolution == ServerSubscriptionAnchorResolution.RetentionGap
            ? ServerReceivePageStatus.RetentionGap
            : ServerReceivePageStatus.EndOfStream;
        pendingResult = new(status, null, lastGroupSequence, lastGroupSequence);
        return false;
    }

    /// <summary>Persists a resolved initial anchor.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="anchor">The resolved anchor.</param>
    /// <param name="observedUtc">The sampled timestamp.</param>
    /// <exception cref="QueueCapacityExceededException">The anchor exceeds the retained byte limit.</exception>
    private void ApplyInitialAnchor(ServerSubscriptionRecord record, ServerSubscriptionInitialAnchor anchor, DateTimeOffset observedUtc)
    {
        var updatedUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
        var delta = ServerSubscriptionJournalOperations.GetInitialAnchorCursorDelta(record.InitialAnchorCursor, anchor.Cursor);
        var nextRevision = ServerSubscriptionJournalOperations.GetNextSubscriptionRevision(record);
        if (!HasSubscriptionCapacity(0, 0, delta))
        {
            throw new QueueCapacityExceededException("The server subscription anchor exceeds the journal byte limit.", canFitWhenEmpty: false);
        }

        ApplySubscriptionBytesDelta(record, delta);
        record.InitialAnchorCursor = anchor.Cursor;
        record.InitialAnchorGroupSequence = anchor.GroupSequence;
        record.InitialAnchorResolved = true;
        record.Revision = nextRevision;
        record.UpdatedAtUtc = updatedUtc;
        record.LastTouchedUtc = updatedUtc;
        _latestUtc = updatedUtc;
    }

    /// <summary>Adds or refreshes an offered cursor.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="cursor">The offered cursor.</param>
    /// <param name="groupSequence">The offered group sequence.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <exception cref="QueueCapacityExceededException">The offer storage is full.</exception>
    private void AddOffer(ServerSubscriptionRecord record, string cursor, long groupSequence, DateTimeOffset observedUtc)
    {
        var offeredUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
        var nextRevision = ServerSubscriptionJournalOperations.GetNextSubscriptionRevision(record);
        var latestDelta = groupSequence > record.LatestOfferedGroupSequence
            ? ServerSubscriptionJournalOperations.GetSubscriptionCursorDelta(record.LatestOfferedCursor, cursor)
            : 0;
        if (record.Offers.TryGetValue(cursor, out var existing))
        {
            record.Offers[cursor] = existing with { OfferedAtUtc = offeredUtc };
            ApplyLatestOffer(record, cursor, groupSequence);
            record.Revision = nextRevision;
            record.UpdatedAtUtc = offeredUtc;
            record.LastTouchedUtc = offeredUtc;
            _latestUtc = offeredUtc;
            return;
        }

        var logicalBytes = ServerSubscriptionJournalOperations.GetOfferBytes(cursor);
        var addedLogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(logicalBytes, latestDelta);
        if (!HasSubscriptionCapacity(0, 1, addedLogicalBytes))
        {
            CompactSubscriptionOffers(record, offeredUtc);
            if (!HasSubscriptionCapacity(0, 1, addedLogicalBytes))
            {
                throw new QueueCapacityExceededException("The server subscription acknowledgement offer journal is full.", canFitWhenEmpty: false);
            }
        }

        record.Offers.Add(
            cursor,
            new() { Cursor = cursor, GroupSequence = groupSequence, OfferedAtUtc = offeredUtc, LogicalBytes = logicalBytes });
        ApplyLatestOffer(record, cursor, groupSequence);
        record.Revision = nextRevision;
        record.UpdatedAtUtc = offeredUtc;
        record.LastTouchedUtc = offeredUtc;
        _subscriptionOfferCount++;
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, logicalBytes);
        _latestUtc = offeredUtc;
    }

    /// <summary>Removes acknowledged and expired offered cursors.</summary>
    /// <param name="utcNow">The compaction timestamp.</param>
    private void CompactSubscriptions(DateTimeOffset utcNow)
    {
        List<SubscriptionId> staleSubscriptions = [];
        foreach (var pair in _subscriptions)
        {
            CompactSubscriptionOffers(pair.Value, utcNow);
            if (ShouldRemoveSubscription(pair.Value, utcNow))
            {
                staleSubscriptions.Add(pair.Key);
            }
        }

        RemoveSubscriptions(staleSubscriptions);
    }

    /// <summary>Applies monotonic latest-offer state to a subscription row.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="cursor">The offered cursor.</param>
    /// <param name="groupSequence">The offered group sequence.</param>
    private void ApplyLatestOffer(ServerSubscriptionRecord record, string cursor, long groupSequence)
    {
        if (groupSequence <= record.LatestOfferedGroupSequence)
        {
            return;
        }

        ApplySubscriptionBytesDelta(record, ServerSubscriptionJournalOperations.GetSubscriptionCursorDelta(record.LatestOfferedCursor, cursor));
        record.LatestOfferedCursor = cursor;
        record.LatestOfferedGroupSequence = groupSequence;
    }

    /// <summary>Removes acknowledged and expired offered cursors for one subscription.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    private void CompactSubscriptionOffers(ServerSubscriptionRecord record, DateTimeOffset utcNow)
    {
        List<string> remove = [];
        foreach (var pair in record.Offers)
        {
            if (ShouldRemoveOffer(pair.Value, utcNow))
            {
                remove.Add(pair.Key);
            }
        }

        RemoveOffers(record, remove);
    }

    /// <summary>Removes offers already covered by the acknowledged cursor.</summary>
    /// <param name="record">The subscription record.</param>
    private void PruneAcknowledgedOffers(ServerSubscriptionRecord record)
    {
        List<string> remove = [];
        foreach (var pair in record.Offers)
        {
            if (pair.Value.GroupSequence <= record.AcknowledgedGroupSequence)
            {
                remove.Add(pair.Key);
            }
        }

        RemoveOffers(record, remove);
    }

    /// <summary>Removes retained offer rows and logical bytes.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="remove">The cursors to remove.</param>
    private void RemoveOffers(ServerSubscriptionRecord record, List<string> remove)
    {
        for (var index = 0; index < remove.Count; index++)
        {
            var offer = record.Offers[remove[index]];
            _ = record.Offers.Remove(remove[index]);
            _subscriptionOfferCount--;
            _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, -offer.LogicalBytes);
        }
    }

    /// <summary>Removes stale subscription rows and their retained bytes.</summary>
    /// <param name="remove">The subscription identifiers to remove.</param>
    private void RemoveSubscriptions(List<SubscriptionId> remove)
    {
        for (var index = 0; index < remove.Count; index++)
        {
            var record = _subscriptions[remove[index]];
            _ = _subscriptions.Remove(remove[index]);
            _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, -record.LogicalBytes);
        }
    }

    /// <summary>Applies subscription-row byte changes to global accounting.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="delta">The logical byte delta.</param>
    private void ApplySubscriptionBytesDelta(ServerSubscriptionRecord record, long delta)
    {
        record.LogicalBytes = ServerCommitJournalSizer.AddLogicalBytes(record.LogicalBytes, delta);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, delta);
    }

    /// <summary>Checks whether an offer is eligible for removal.</summary>
    /// <param name="offer">The offer.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>Whether the offer can be removed.</returns>
    private bool ShouldRemoveOffer(ServerSubscriptionOffer offer, DateTimeOffset utcNow) =>
        offer.OfferedAtUtc < GetExpiryBoundary(utcNow);

    /// <summary>Checks whether a subscription row exceeded its binding retention horizon.</summary>
    /// <param name="record">The subscription record.</param>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>Whether the subscription row can be removed.</returns>
    private bool ShouldRemoveSubscription(ServerSubscriptionRecord record, DateTimeOffset utcNow) =>
        record.LastTouchedUtc < GetSubscriptionExpiryBoundary(utcNow);

    /// <summary>Gets the oldest retained timestamp allowed at a compaction instant.</summary>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>The timestamp before which rows expire.</returns>
    private DateTimeOffset GetExpiryBoundary(DateTimeOffset utcNow)
    {
        try
        {
            return utcNow.Subtract(_options.OperationRetention);
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MinValue;
        }
    }

    /// <summary>Gets the oldest subscription binding timestamp allowed at a compaction instant.</summary>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>The timestamp before which subscription bindings expire.</returns>
    private DateTimeOffset GetSubscriptionExpiryBoundary(DateTimeOffset utcNow)
    {
        try
        {
            return utcNow.Subtract(_options.SubscriptionRetention);
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MinValue;
        }
    }

    /// <summary>Checks whether subscription acknowledgement storage has capacity.</summary>
    /// <param name="addedSubscriptions">The subscriptions to add.</param>
    /// <param name="addedOffers">The offers to add.</param>
    /// <param name="addedLogicalBytes">The logical bytes to add.</param>
    /// <returns>Whether capacity remains.</returns>
    private bool HasSubscriptionCapacity(int addedSubscriptions, int addedOffers, long addedLogicalBytes)
    {
        var subscriptionCount = checked((long)_subscriptions.Count + addedSubscriptions);
        var offerCount = checked((long)_subscriptionOfferCount + addedOffers);
        var logicalBytes = checked(_logicalBytes + addedLogicalBytes);
        return subscriptionCount <= _options.MaximumSubscriptions
            && offerCount <= _options.MaximumSubscriptionOffers
            && logicalBytes <= _options.MaximumLogicalBytes;
    }

    /// <summary>Performs the gated compare-and-swap commit.</summary>
    /// <param name="commit">The validated commit.</param>
    /// <param name="observedUtc">The caller-independent timestamp sampled before the gate.</param>
    /// <returns>The commit result.</returns>
    private ServerCommitResult TryCommitUnderGate(ServerCommitValidationResult commit, DateTimeOffset observedUtc)
    {
        var streamExists = _streams.TryGetValue(commit.StreamKey, out var stream);
        stream ??= new();
        var status = ServerCommitJournalOperations.GetPreCommitStatus(stream, commit);
        if (status != ServerCommitStatus.Committed)
        {
            return new(status, ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys));
        }

        var committedUtc = ServerCommitJournalOperations.Max(_latestUtc, observedUtc);
        var stateDelta = ServerCommitJournalSizer.GetStateDelta(stream, commit);
        var streamDelta = streamExists ? 0 : ServerCommitJournalSizer.GetStreamKeyBytes(commit.StreamKey);
        var lastCursorDelta = GetLastCursorDelta(stream, commit);
        var expired = GetExpiredRows(committedUtc);
        if (!HasCapacity(commit, stateDelta, streamDelta, lastCursorDelta, null)
            && !HasCapacity(commit, stateDelta, streamDelta, lastCursorDelta, expired))
        {
            return new(ServerCommitStatus.CapacityExceeded, ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys));
        }

        var expiresUtc = GetExpiry(committedUtc);
        var committedEntries = ServerCommitJournalOperations.CommitEntries(commit.Entries, committedUtc, expiresUtc);
        ApplyExpired(expired);
        AddStreamIfNeeded(commit.StreamKey, stream, streamExists, streamDelta);
        ApplyCommit(stream, commit, committedEntries, stateDelta, lastCursorDelta);
        _latestUtc = committedUtc;
        return new(ServerCommitStatus.Committed, ServerCommitJournalOperations.CreateSnapshot(commit.StreamKey, stream, commit.OperationKeys));
    }

    /// <summary>Applies a validated commit to the stream.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="commit">The validated commit.</param>
    /// <param name="committedEntries">The committed entries.</param>
    /// <param name="stateDelta">The retained state byte delta.</param>
    /// <param name="lastCursorDelta">The retained cursor byte delta.</param>
    private void ApplyCommit(
        ServerCommitStreamRecord stream,
        ServerCommitValidationResult commit,
        ServerLedgerEntry[] committedEntries,
        long stateDelta,
        long lastCursorDelta)
    {
        ServerCommitJournalOperations.ApplyState(stream, commit);
        for (var index = 0; index < committedEntries.Length; index++)
        {
            ServerCommitJournalOperations.AddLedgerRow(stream, commit.StreamKey, committedEntries[index], commit.EntryBytes[index]);
        }

        stream.Revision++;
        _ledgerEntryCount += committedEntries.Length;
        _eventCount += commit.EventCount;
        ApplyLastCursorBytes(stream, commit);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, commit.LedgerBytes);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, stateDelta);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, lastCursorDelta);
    }

    /// <summary>Adds a stream after successful capacity admission.</summary>
    /// <param name="streamKey">The stream key.</param>
    /// <param name="stream">The stream record.</param>
    /// <param name="streamExists">Whether the stream already exists.</param>
    /// <param name="streamDelta">The stream logical bytes.</param>
    private void AddStreamIfNeeded(
        ServerStreamKey streamKey,
        ServerCommitStreamRecord stream,
        bool streamExists,
        long streamDelta)
    {
        if (streamExists)
        {
            return;
        }

        _streams.Add(streamKey, stream);
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, streamDelta);
    }

    /// <summary>Checks whether the commit can fit after an optional expired-row reclamation.</summary>
    /// <param name="commit">The validated commit.</param>
    /// <param name="stateDelta">The retained state byte delta.</param>
    /// <param name="streamDelta">The new stream logical byte delta.</param>
    /// <param name="lastCursorDelta">The retained cursor byte delta.</param>
    /// <param name="expired">The optional projected expired rows.</param>
    /// <returns>Whether capacity remains.</returns>
    private bool HasCapacity(
        ServerCommitValidationResult commit,
        long stateDelta,
        long streamDelta,
        long lastCursorDelta,
        ServerCommitExpiredRows? expired)
    {
        var streamCount = checked((long)_streams.Count + (streamDelta == 0 ? 0 : 1));
        var ledgerCount = checked((long)_ledgerEntryCount + commit.Entries.Length - (expired?.LedgerRows.Count ?? 0));
        var eventCount = checked((long)_eventCount + commit.EventCount - (expired?.EventRows.Count ?? 0));
        var logicalBytes = checked(_logicalBytes + commit.LedgerBytes + stateDelta + streamDelta + lastCursorDelta - (expired?.LogicalBytes ?? 0));
        return HasCountCapacity(streamCount, ledgerCount, eventCount)
            && HasSubscriptionCountCapacity()
            && logicalBytes <= _options.MaximumLogicalBytes;
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
    /// <returns>Whether subscription count capacity remains.</returns>
    private bool HasSubscriptionCountCapacity() =>
        _subscriptions.Count <= _options.MaximumSubscriptions;

    /// <summary>Collects expired rows without mutating journal state.</summary>
    /// <param name="utcNow">The compaction timestamp.</param>
    /// <returns>The projected expired rows.</returns>
    private ServerCommitExpiredRows GetExpiredRows(DateTimeOffset utcNow)
    {
        var expired = new ServerCommitExpiredRows();
        foreach (var streamPair in _streams)
        {
            ServerCommitJournalOperations.CollectExpiredLedgerRows(streamPair.Value, utcNow, expired);
            ServerCommitJournalOperations.CollectExpiredEventRows(streamPair.Value, utcNow, expired);
        }

        return expired;
    }

    /// <summary>Applies projected expired-row cleanup.</summary>
    /// <param name="expired">The expired rows.</param>
    private void ApplyExpired(ServerCommitExpiredRows expired)
    {
        for (var index = 0; index < expired.LedgerRows.Count; index++)
        {
            ApplyExpiredLedger(expired.LedgerRows[index]);
        }

        for (var index = 0; index < expired.EventRows.Count; index++)
        {
            ApplyExpiredEvent(expired.EventRows[index]);
        }
    }

    /// <summary>Applies one expired ledger row.</summary>
    /// <param name="ledgerRow">The ledger row.</param>
    private void ApplyExpiredLedger(ServerCommitLedgerRow ledgerRow)
    {
        var stream = _streams[ledgerRow.StreamKey];
        _ = stream.Ledger.Remove(ledgerRow.Entry.OperationKey);
        if (ledgerRow.GroupSequence.HasValue)
        {
            _ = stream.Groups.Remove(ledgerRow);
        }

        _ledgerEntryCount--;
        _logicalBytes = ServerCommitJournalSizer.AddLogicalBytes(_logicalBytes, -ledgerRow.LogicalBytes);
    }

    /// <summary>Applies one expired event row.</summary>
    /// <param name="eventRow">The event row.</param>
    private void ApplyExpiredEvent(ServerCommitEventRow eventRow)
    {
        var stream = _streams[eventRow.Ledger.StreamKey];
        _ = stream.Events.Remove(eventRow);
        _eventCount--;
        _ = stream.EventIds.Remove(eventRow.RemoteEvent.EventId);
        _ = stream.Cursors.Remove(eventRow.RemoteEvent.ServerCursor);
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

    /// <summary>Groups the already validated fields needed to add a snapshot offer.</summary>
    /// <param name="Request">The offer request.</param>
    /// <param name="ViewState">The subscription state captured in the recovery view.</param>
    /// <param name="Checkpoint">The recovered snapshot checkpoint.</param>
    /// <param name="Cursor">The recovered cursor.</param>
    /// <param name="OfferedAtUtc">The offer timestamp.</param>
    /// <param name="LogicalBytes">The retained offer bytes.</param>
    /// <param name="IssuedRevision">The assigned subscription revision.</param>
    private readonly record struct SnapshotOfferAddContext(
        ServerSnapshotOfferRequest Request,
        ServerSubscriptionState ViewState,
        RemoteSnapshotCheckpoint Checkpoint,
        string Cursor,
        DateTimeOffset OfferedAtUtc,
        long LogicalBytes,
        long IssuedRevision);
}
