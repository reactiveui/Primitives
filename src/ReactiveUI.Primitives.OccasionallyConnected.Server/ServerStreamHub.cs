// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Concrete authorized server stream hub facade over the internal server journal and processor.</summary>
[System.Diagnostics.DebuggerDisplay("Disposed={_disposed}")]
public sealed partial class ServerStreamHub : IServerStreamHub, IServerSnapshotRecoveryHub, IAsyncDisposable
{
    /// <summary>The stable active-call capacity diagnostic.</summary>
    private const string ActiveCallCapacityMessage = "The server stream hub is at active call capacity.";

    /// <summary>The stable active-subscription capacity diagnostic.</summary>
    private const string ActiveSubscriptionCapacityMessage = "The server stream hub is at active subscription capacity.";

    /// <summary>The stable receive retention-gap diagnostic.</summary>
    private const string RetentionGapMessage = "The requested receive cursor is outside retained server history.";

    /// <summary>The placeholder tenant used only to run structural stream identifier validation before authorization.</summary>
    private const string ValidationTenant = "tenant";

    /// <summary>The maximum optimistic commit attempts delegated to the operation processor.</summary>
    private const int MaximumCommitAttempts = 4;

    /// <summary>The maximum coalesced subscriber wakeup count.</summary>
    private const int SemaphoreMaximumCount = 1;

    /// <summary>The append journal used by publish operations.</summary>
    private readonly IServerCommitJournal _commitJournal;

    /// <summary>The receive acknowledgement journal used by subscriptions and acknowledgements.</summary>
    private readonly IServerSubscriptionAcknowledgementJournal _subscriptionJournal;

    /// <summary>The snapshot recovery journal used to read coherent views and offer recovered cursors.</summary>
    private readonly IServerSnapshotRecoveryJournal _snapshotRecoveryJournal;

    /// <summary>The owned journal resource disposed with the hub when applicable.</summary>
    private readonly IDisposable? _ownedJournal;

    /// <summary>The immutable hub options.</summary>
    private readonly ServerStreamHubOptions _options;

    /// <summary>The conflict resolving operation handler composed with the existing processor.</summary>
    private readonly ConflictResolvingServerOperationHandler _handler;

    /// <summary>The immutable processor options derived from hub options.</summary>
    private readonly ServerOperationProcessorOptions _processorOptions;

    /// <summary>The semaphore used to wake empty subscription polls after publishes or disposal.</summary>
    private readonly SemaphoreSlim _wakeup = new(0, SemaphoreMaximumCount);

    /// <summary>The cancellation source signaled when the hub begins disposal.</summary>
    private readonly CancellationTokenSource _disposeCancellation = new();

    /// <summary>The completion source signaled when active hub work drains during disposal.</summary>
    private readonly TaskCompletionSource<bool> _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Protects lifecycle admission and disposal state.</summary>
    private readonly object _lifecycleGate = new();

    /// <summary>The shared disposal task returned to all concurrent disposal callers.</summary>
    private Task? _disposeTask;

    /// <summary>The number of active effect operations currently admitted.</summary>
    private int _activeCalls;

    /// <summary>The number of active acknowledgement operations currently admitted.</summary>
    private int _activeAcknowledgements;

    /// <summary>The number of active empty-poll wait operations currently admitted.</summary>
    private int _activePolls;

    /// <summary>The number of active subscription enumerators currently admitted.</summary>
    private int _activeSubscriptions;

    /// <summary>Whether a subscriber wakeup is pending.</summary>
    private int _pendingWakeup;

    /// <summary>The monotonically increasing publish wakeup version.</summary>
    private int _wakeupVersion;

    /// <summary>Whether the hub has begun disposal.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="ServerStreamHub"/> class.</summary>
    /// <param name="options">The hub options.</param>
    /// <param name="commitJournal">The commit journal.</param>
    /// <param name="subscriptionJournal">The subscription journal.</param>
    /// <param name="snapshotRecoveryJournal">The snapshot recovery journal.</param>
    /// <param name="ownedJournal">The optional owned journal resource.</param>
    /// <param name="handler">The prepared conflict handler.</param>
    /// <param name="processorOptions">The prepared processor options.</param>
    private ServerStreamHub(
        ServerStreamHubOptions options,
        IServerCommitJournal commitJournal,
        IServerSubscriptionAcknowledgementJournal subscriptionJournal,
        IServerSnapshotRecoveryJournal snapshotRecoveryJournal,
        IDisposable? ownedJournal,
        ConflictResolvingServerOperationHandler handler,
        ServerOperationProcessorOptions processorOptions)
    {
        _options = options;
        _commitJournal = commitJournal;
        _subscriptionJournal = subscriptionJournal;
        _snapshotRecoveryJournal = snapshotRecoveryJournal;
        _ownedJournal = ownedJournal;
        _handler = handler;
        _processorOptions = processorOptions;
    }

    /// <summary>Creates a hub that owns a new in-memory server journal.</summary>
    /// <param name="options">The hub options.</param>
    /// <returns>The configured hub.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound is invalid.</exception>
    public static ServerStreamHub CreateInMemory(ServerStreamHubOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        var prepared = PrepareOptions(options);
        var journal = new InMemoryServerCommitJournal(prepared.JournalOptions);
        return new(options, journal, journal, journal, null, prepared.Handler, prepared.ProcessorOptions);
    }

    /// <summary>Creates a hub that owns a new SQLite server journal.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="options">The hub options.</param>
    /// <returns>The configured hub.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="databasePath"/> or <paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="databasePath"/> is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound is invalid.</exception>
    public static ServerStreamHub CreateSqlite(string databasePath, ServerStreamHubOptions options)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
#else
        ArgumentExceptionHelper.ThrowIfNull(databasePath);
        if (string.IsNullOrWhiteSpace(databasePath))
        {
            throw new ArgumentException("The SQLite database path cannot be empty.", nameof(databasePath));
        }
#endif

        ArgumentExceptionHelper.ThrowIfNull(options);
        var prepared = PrepareOptions(options);

        var journal = new SqliteServerCommitJournal(databasePath, prepared.JournalOptions);
        return new(options, journal, journal, journal, journal, prepared.Handler, prepared.ProcessorOptions);
    }

    /// <inheritdoc/>
    public async ValueTask<ServerSyncResult> ApplyOperationsAsync(
        SyncBatch batch,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken)
    {
        EnterActiveCall();
        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCancellation.Token);
            var token = linked.Token;
            var operations = CaptureOperations(batch, client);
            var scopes = await AuthorizePublishAsync(client, batch, operations, token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            var authorizer = new PreAuthorizedOperationAuthorizer(scopes);
            var processor = new ServerOperationProcessor(_commitJournal, authorizer, _handler, options: _processorOptions);
            var result = await processor.ProcessAsync(batch, new(client.ClientId), token).ConfigureAwait(false);
            SignalSubscribers();
            return result;
        }
        finally
        {
            ReleaseActiveCall();
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
        RemoteSubscribeRequest request,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken) =>
        new SubscriptionEnumerable(this, request, client, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask AcknowledgeAsync(
        ReceiveAcknowledgement acknowledgement,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken)
    {
        EnterActiveAcknowledgement();
        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCancellation.Token);
            var token = linked.Token;
            ValidateAcknowledgementRequest(acknowledgement, client);
            token.ThrowIfCancellationRequested();
            var publicScope = await _options.AuthorizationPolicy.AuthorizeAcknowledgeAsync(client, acknowledgement, token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            var scope = ValidateScope(client, acknowledgement.StreamId, publicScope);
            _ = _subscriptionJournal.Acknowledge(new(new(scope.TenantId, acknowledgement.StreamId), scope.ClientId, acknowledgement));
        }
        finally
        {
            ReleaseActiveAcknowledgement();
        }
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => new(BeginDispose());

    /// <summary>Creates owned-journal options from the public hub options.</summary>
    /// <param name="options">The public hub options.</param>
    /// <returns>The configured journal options.</returns>
    private static ServerCommitJournalOptions CreateJournalOptions(ServerStreamHubOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        var limits = options.JournalLimits;
        ArgumentExceptionHelper.ThrowIfNull(limits);
        return new()
        {
            MaximumStreams = limits.MaximumStreams,
            MaximumLedgerEntries = limits.MaximumLedgerEntries,
            MaximumEvents = limits.MaximumEvents,
            MaximumLogicalBytes = limits.MaximumLogicalBytes,
            MaximumOperationCaptureCount = limits.MaximumOperationCaptureCount,
            MaximumEntryEventCount = limits.MaximumEntryEventCount,
            MaximumSubscriptions = limits.MaximumSubscriptions,
            MaximumSubscriptionOffers = limits.MaximumSubscriptionOffers,
            OperationRetention = limits.OperationRetention,
            SubscriptionRetention = limits.SubscriptionRetention,
            TimeProvider = options.TimeProvider,
        };
    }

    /// <summary>Creates operation processor options from the public hub options.</summary>
    /// <param name="options">The public hub options.</param>
    /// <returns>The configured processor options.</returns>
    private static ServerOperationProcessorOptions CreateProcessorOptions(ServerStreamHubOptions options) =>
        new()
        {
            MaximumActiveRequests = options.MaximumActiveCalls,
            MaximumBatchOperations = options.MaximumBatchOperations,
            MaximumBatchLogicalBytes = options.MaximumBatchLogicalBytes,
            MaximumCanonicalOperationBytes = (int)Math.Min(int.MaxValue, options.MaximumBatchLogicalBytes),
            MaximumCommitAttempts = MaximumCommitAttempts,
            MaximumPreparedConflicts = options.ConflictHandler.MaximumProducedEvents,
            MaximumPreparedEvents = options.ConflictHandler.MaximumProducedEvents,
            MaximumPreparedLogicalBytes = options.MaximumBatchLogicalBytes,
            TimeProvider = options.TimeProvider,
        };

    /// <summary>Validates public options and prepares fallible dependencies before journal construction.</summary>
    /// <param name="options">The public hub options.</param>
    /// <returns>The prepared conflict handler, processor options and journal options.</returns>
    private static (ConflictResolvingServerOperationHandler Handler, ServerOperationProcessorOptions ProcessorOptions, ServerCommitJournalOptions JournalOptions) PrepareOptions(
        ServerStreamHubOptions options)
    {
        ValidateOptions(options);
        return (new(options.ConflictHandler), CreateProcessorOptions(options), CreateJournalOptions(options));
    }

    /// <summary>Observes a canceled secondary poll task.</summary>
    /// <param name="task">The task to observe.</param>
    /// <returns>A task that completes when the secondary task has been observed.</returns>
    private static async ValueTask ObserveSecondaryPollTaskAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>Runs an asynchronous disposal stage and preserves the first failure.</summary>
    /// <param name="firstException">The first exception already captured.</param>
    /// <param name="stage">The disposal stage.</param>
    /// <returns>The first captured exception, when any stage has failed.</returns>
    private static async ValueTask<Exception?> CaptureDisposalExceptionAsync(Exception? firstException, Func<Task> stage)
    {
        try
        {
            await stage().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            firstException ??= exception;
        }

        return firstException;
    }

    /// <summary>Rethrows a captured cleanup failure while preserving its original stack.</summary>
    /// <param name="exception">The cleanup failure, or null when cleanup succeeded.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RethrowDisposalFailure(Exception? exception) =>
        (exception is null ? null : ExceptionDispatchInfo.Capture(exception))?.Throw();

    /// <summary>Validates an authorization scope and converts it to the processor scope type.</summary>
    /// <param name="client">The authenticated client.</param>
    /// <param name="streamId">The stream identifier being authorized.</param>
    /// <param name="scope">The public authorization scope.</param>
    /// <returns>The trusted processor scope.</returns>
    /// <exception cref="UnauthorizedAccessException">The authorization scope does not match the authenticated client.</exception>
    private static ServerOperationScope ValidateScope(
        ServerAuthenticatedClient client,
        StreamId streamId,
        ServerStreamAuthorizationScope scope)
    {
        ArgumentExceptionHelper.ThrowIfNull(scope);
        ServerCommitJournalGuard.ValidateText(scope.TenantId, nameof(scope.TenantId));
        ServerCommitJournalGuard.ValidateText(scope.ClientId, nameof(scope.ClientId));
        ServerCommitJournalGuard.ValidateStreamKey(new(scope.TenantId, streamId));
        if (StringComparer.Ordinal.Equals(scope.TenantId, client.TenantId)
            && StringComparer.Ordinal.Equals(scope.ClientId, client.ClientId))
        {
            return new(scope.TenantId, scope.ClientId);
        }

        throw new UnauthorizedAccessException("The authorized scope must match the authenticated client.");
    }

    /// <summary>Validates hub options before any owned resources are created.</summary>
    /// <param name="options">The options to validate.</param>
    /// <exception cref="InvalidOperationException">A snapshot materializer is configured without a snapshot recovery authorization policy.</exception>
    private static void ValidateOptions(ServerStreamHubOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options.ConflictHandler);
        ArgumentExceptionHelper.ThrowIfNull(options.AuthorizationPolicy);
        ArgumentExceptionHelper.ThrowIfNull(options.TimeProvider);
        ArgumentExceptionHelper.ThrowIfNull(options.JournalLimits);
        ArgumentExceptionHelper.ThrowIfNull(options.SnapshotRecoveryLimits);
        options.SnapshotRecoveryLimits.Validate();
        if (options.SnapshotRecoveryMaterializer is not null && options.SnapshotRecoveryAuthorizationPolicy is null)
        {
            throw new InvalidOperationException(
                "Snapshot recovery materialization requires a snapshot recovery authorization policy.");
        }

        options.ConflictHandler.Validate();
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(options.MaximumActiveCalls);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(options.MaximumActiveSubscriptions);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(options.MaximumBatchOperations);
        ThrowIfNonPositive(options.MaximumBatchLogicalBytes, nameof(options.MaximumBatchLogicalBytes));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(options.MaximumReceiveGroups);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(options.MaximumReceiveEvents);
        ThrowIfNonPositive(options.MaximumReceiveLogicalBytes, nameof(options.MaximumReceiveLogicalBytes));
        ThrowIfInvalidDelay(options.EmptyPollDelay);
        _ = CreateJournalOptions(options);
    }

    /// <summary>Validates acknowledgement shape before authorization.</summary>
    /// <param name="acknowledgement">The acknowledgement to validate.</param>
    /// <param name="client">The authenticated client.</param>
    private static void ValidateAcknowledgementRequest(ReceiveAcknowledgement acknowledgement, ServerAuthenticatedClient client)
    {
        ArgumentExceptionHelper.ThrowIfNull(acknowledgement);
        ValidateClient(client);
        ValidateSubscriptionId(acknowledgement.SubscriptionId);
        ServerCommitJournalGuard.ValidateStreamKey(new(ValidationTenant, acknowledgement.StreamId));
        ServerCommitJournalGuard.ValidateCursor(acknowledgement.Cursor);
    }

    /// <summary>Validates subscription request shape before authorization.</summary>
    /// <param name="request">The subscription request.</param>
    /// <param name="client">The authenticated client.</param>
    private static void ValidateSubscribeRequest(RemoteSubscribeRequest request, ServerAuthenticatedClient client)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ValidateClient(client);
        ValidateSubscriptionId(request.SubscriptionId);
        ServerCommitJournalGuard.ValidateStreamKey(new(ValidationTenant, request.StreamId));
        if (request.Cursor is not null)
        {
            ServerCommitJournalGuard.ValidateCursor(request.Cursor);
        }

        ArgumentExceptionHelper.ThrowIfNull(request.InitialPosition);
    }

    /// <summary>Validates authenticated client identity shape.</summary>
    /// <param name="client">The authenticated client.</param>
    private static void ValidateClient(ServerAuthenticatedClient client)
    {
        ArgumentExceptionHelper.ThrowIfNull(client);
        ServerCommitJournalGuard.ValidateText(client.TenantId, nameof(client.TenantId));
        ServerCommitJournalGuard.ValidateText(client.ClientId, nameof(client.ClientId));
    }

    /// <summary>Validates that a subscription identifier is usable.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <exception cref="ArgumentException"><paramref name="subscriptionId"/> is empty.</exception>
    private static void ValidateSubscriptionId(SubscriptionId subscriptionId)
    {
        if (subscriptionId.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("The subscription identifier must be non-empty.", nameof(subscriptionId));
    }

    /// <summary>Throws when the empty poll delay is not finite and positive.</summary>
    /// <param name="delay">The configured delay.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="delay"/> is not finite and positive.</exception>
    private static void ThrowIfInvalidDelay(TimeSpan delay)
    {
        if (delay > TimeSpan.Zero && delay != TimeSpan.MaxValue)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(delay), delay, "Empty poll delay must be positive and finite.");
    }

    /// <summary>Throws when a configured long value is not positive.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="parameterName">The property name to report.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive.</exception>
    private static void ThrowIfNonPositive(long value, string parameterName)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, null);
    }

    /// <summary>Copies and validates operation order and uniqueness.</summary>
    /// <param name="operations">The operations to copy.</param>
    /// <returns>The validated operation snapshot.</returns>
    private static SyncOperation[] CaptureValidatedOperations(IReadOnlyList<SyncOperation> operations)
    {
        var copy = new SyncOperation[operations.Count];
        var operationIds = new HashSet<OperationId>();
        var clientSequences = new HashSet<long>();
        StreamId? streamId = null;
        var previousSequence = 0L;
        for (var index = 0; index < operations.Count; index++)
        {
            var operation = operations[index];
            ValidateOperation(operation, operationIds, clientSequences, ref streamId, ref previousSequence);
            copy[index] = operation;
        }

        return copy;
    }

    /// <summary>Validates one operation and updates publish-batch uniqueness state.</summary>
    /// <param name="operation">The operation to validate.</param>
    /// <param name="operationIds">The operation identifiers already seen.</param>
    /// <param name="clientSequences">The client sequence numbers already seen.</param>
    /// <param name="streamId">The batch stream identifier, when already established.</param>
    /// <param name="previousSequence">The previous client sequence.</param>
    /// <exception cref="SyncBatchValidationException"><paramref name="operation"/> does not satisfy batch invariants.</exception>
    private static void ValidateOperation(
        SyncOperation operation,
        HashSet<OperationId> operationIds,
        HashSet<long> clientSequences,
        ref StreamId? streamId,
        ref long previousSequence)
    {
        ArgumentExceptionHelper.ThrowIfNull(operation);
        ArgumentExceptionHelper.ThrowIfNull(operation.Payload);
        ArgumentExceptionHelper.ThrowIfNull(operation.Policy);
        if (operation.OperationId.Value == Guid.Empty || operation.StreamId.Value is null || operation.ClientSequence <= 0)
        {
            throw new SyncBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch contains a malformed operation.");
        }

        ServerCommitJournalGuard.ValidateStreamKey(new(ValidationTenant, operation.StreamId));
        operation.Policy.Validate();
        if (streamId is not null && streamId.Value != operation.StreamId)
        {
            throw new SyncBatchValidationException(SyncBatchValidationError.MixedStreams, "The synchronization batch contains operations for multiple streams.");
        }

        streamId ??= operation.StreamId;
        if (!operationIds.Add(operation.OperationId))
        {
            throw new SyncBatchValidationException(SyncBatchValidationError.DuplicateOperation, "The synchronization batch contains a duplicate operation identifier.");
        }

        if (!clientSequences.Add(operation.ClientSequence))
        {
            throw new SyncBatchValidationException(SyncBatchValidationError.DuplicateClientSequence, "The synchronization batch contains a duplicate client sequence.");
        }

        if (operation.ClientSequence >= previousSequence)
        {
            previousSequence = operation.ClientSequence;
            return;
        }

        throw new SyncBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch is not in client sequence order.");
    }

    /// <summary>Delays with the configured time provider.</summary>
    /// <param name="timeProvider">The configured time provider.</param>
    /// <param name="delay">The delay duration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the delay ends or cancellation is observed.</returns>
    private static async ValueTask DelayAsync(
        TimeProvider timeProvider,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
#if NET8_0_OR_GREATER
        await Task.Delay(delay, timeProvider, cancellationToken).ConfigureAwait(false);
#else
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = cancellationToken.UnsafeRegister(CompleteCanceledDelay, completion);
        await using var timer = timeProvider.CreateTimer(CompleteDelay, completion, delay, Timeout.InfiniteTimeSpan);
        await completion.Task.ConfigureAwait(false);
#endif
    }

#if !NET8_0_OR_GREATER
    /// <summary>Completes a time-provider delay.</summary>
    /// <param name="state">The task completion source state.</param>
    private static void CompleteDelay(object? state)
    {
        if (state is not TaskCompletionSource<bool> completion)
        {
            return;
        }

        _ = completion.TrySetResult(true);
    }

    /// <summary>Cancels a time-provider delay.</summary>
    /// <param name="state">The task completion source state.</param>
    private static void CompleteCanceledDelay(object? state)
    {
        if (state is not TaskCompletionSource<bool> completion)
        {
            return;
        }

        _ = completion.TrySetCanceled();
    }
#endif

    /// <summary>Captures and validates a publish batch before authorization.</summary>
    /// <param name="batch">The publish batch.</param>
    /// <param name="client">The authenticated client.</param>
    /// <returns>The validated operation snapshot.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="batch"/> exceeds configured operation bounds.</exception>
    /// <exception cref="SyncBatchValidationException"><paramref name="batch"/> does not satisfy publish invariants.</exception>
    private SyncOperation[] CaptureOperations(SyncBatch batch, ServerAuthenticatedClient client)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ValidateClient(client);
        if (batch.BatchId == Guid.Empty)
        {
            throw new SyncBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch identifier must be non-empty.");
        }

        ArgumentExceptionHelper.ThrowIfNull(batch.Operations);
        if (batch.Operations.Count == 0)
        {
            throw new SyncBatchValidationException(SyncBatchValidationError.MalformedBatch, "The synchronization batch must contain at least one operation.");
        }

        if (batch.Operations.Count > _options.MaximumBatchOperations)
        {
            throw new ArgumentOutOfRangeException(nameof(batch), batch.Operations.Count, "The operation count is outside the configured bounds.");
        }

        return CaptureValidatedOperations(batch.Operations);
    }

    /// <summary>Authorizes the whole publish batch and each operation before journal mutation.</summary>
    /// <param name="client">The authenticated client.</param>
    /// <param name="batch">The publish batch.</param>
    /// <param name="operations">The validated operation snapshot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The trusted operation authorizations.</returns>
    /// <exception cref="UnauthorizedAccessException">An operation authorization scope does not match the trusted batch scope.</exception>
    private async ValueTask<IReadOnlyDictionary<OperationId, ServerOperationScope>> AuthorizePublishAsync(
        ServerAuthenticatedClient client,
        SyncBatch batch,
        SyncOperation[] operations,
        CancellationToken cancellationToken)
    {
        _ = ValidateScope(
            client,
            operations[0].StreamId,
            await _options.AuthorizationPolicy.AuthorizePublishAsync(client, batch, cancellationToken).ConfigureAwait(false));
        var scopes = new Dictionary<OperationId, ServerOperationScope>(operations.Length);
        for (var index = 0; index < operations.Length; index++)
        {
            var operation = operations[index];
            var operationScope = ValidateScope(
                client,
                operation.StreamId,
                await _options.AuthorizationPolicy.AuthorizeOperationAsync(client, operation, cancellationToken).ConfigureAwait(false));
            scopes.Add(operation.OperationId, operationScope);
        }

        return scopes;
    }

    /// <summary>Reads one subscription page under the active call gate.</summary>
    /// <param name="request">The subscription request.</param>
    /// <param name="client">The authenticated client.</param>
    /// <param name="cursor">The current receive cursor.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The receive page result.</returns>
    private async ValueTask<ServerReceivePageResult> ReadSubscriptionPageAsync(
        RemoteSubscribeRequest request,
        ServerAuthenticatedClient client,
        string? cursor,
        CancellationToken cancellationToken)
    {
        EnterActiveCall();
        try
        {
            ValidateSubscribeRequest(request, client);
            cancellationToken.ThrowIfCancellationRequested();
            var publicScope = await _options.AuthorizationPolicy.AuthorizeSubscribeAsync(client, request, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            var scope = ValidateScope(client, request.StreamId, publicScope);
            var identity = new ServerSubscriptionIdentity(new(scope.TenantId, request.StreamId), scope.ClientId, request.SubscriptionId);
            _ = _subscriptionJournal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(identity, request.InitialPosition));
            return _subscriptionJournal.OfferReceivePage(new(
                identity,
                cursor,
                _options.MaximumReceiveGroups,
                _options.MaximumReceiveEvents,
                _options.MaximumReceiveLogicalBytes));
        }
        finally
        {
            ReleaseActiveCall();
        }
    }

    /// <summary>Waits for a publish wakeup, configured poll delay, cancellation or hub disposal.</summary>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>A task that completes when the next poll should run.</returns>
    private async ValueTask WaitForNextPollAsync(CancellationToken cancellationToken)
    {
        EnterActivePoll();
        var observedVersion = Volatile.Read(ref _wakeupVersion);
        try
        {
            _disposeCancellation.Token.ThrowIfCancellationRequested();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCancellation.Token);
            var wakeTask = _wakeup.WaitAsync(linked.Token);
            var delayTask = DelayAsync(_options.TimeProvider, _options.EmptyPollDelay, linked.Token).AsTask();
            var completed = await Task.WhenAny(wakeTask, delayTask).ConfigureAwait(false);
            var secondaryTask = ReferenceEquals(completed, wakeTask) ? delayTask : wakeTask;
            try
            {
                await linked.CancelAsync().ConfigureAwait(false);
            }
            finally
            {
                await ObserveSecondaryPollTaskAsync(secondaryTask).ConfigureAwait(false);
            }

            await completed.ConfigureAwait(false);
            _disposeCancellation.Token.ThrowIfCancellationRequested();
            if (ReferenceEquals(completed, wakeTask))
            {
                _ = Interlocked.Exchange(ref _pendingWakeup, 0);
                if (Volatile.Read(ref _wakeupVersion) != observedVersion)
                {
                    return;
                }
            }
        }
        finally
        {
            ReleaseActivePoll();
        }
    }

    /// <summary>Signals subscribers that new data or disposal may unblock polling.</summary>
    private void SignalSubscribers()
    {
        _ = Interlocked.Increment(ref _wakeupVersion);
        if (Interlocked.Exchange(ref _pendingWakeup, 1) != 0)
        {
            return;
        }

        _ = _wakeup.Release();
    }

    /// <summary>Admits one active journal call.</summary>
    /// <exception cref="ObjectDisposedException">The hub has been disposed.</exception>
    /// <exception cref="QueueCapacityExceededException">The active call limit has been reached.</exception>
    private void EnterActiveCall()
    {
        lock (_lifecycleGate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed != 0, this);
            if (_activeCalls >= _options.MaximumActiveCalls)
            {
                throw new QueueCapacityExceededException(ActiveCallCapacityMessage, canFitWhenEmpty: true);
            }

            _activeCalls++;
        }
    }

    /// <summary>Releases one active journal call and completes disposal drain when appropriate.</summary>
    private void ReleaseActiveCall()
    {
        TaskCompletionSource<bool>? drained = null;
        lock (_lifecycleGate)
        {
            _activeCalls--;
            if (IsDrainedUnderGate())
            {
                drained = _drained;
            }
        }

        _ = drained?.TrySetResult(true);
    }

    /// <summary>Admits one active acknowledgement call.</summary>
    /// <exception cref="ObjectDisposedException">The hub has been disposed.</exception>
    /// <exception cref="QueueCapacityExceededException">The acknowledgement call limit has been reached.</exception>
    private void EnterActiveAcknowledgement()
    {
        lock (_lifecycleGate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed != 0, this);
            if (_activeAcknowledgements >= _options.MaximumActiveCalls)
            {
                throw new QueueCapacityExceededException(ActiveCallCapacityMessage, canFitWhenEmpty: true);
            }

            _activeAcknowledgements++;
        }
    }

    /// <summary>Releases one active acknowledgement call and completes disposal drain when appropriate.</summary>
    private void ReleaseActiveAcknowledgement()
    {
        TaskCompletionSource<bool>? drained = null;
        lock (_lifecycleGate)
        {
            _activeAcknowledgements--;
            if (IsDrainedUnderGate())
            {
                drained = _drained;
            }
        }

        _ = drained?.TrySetResult(true);
    }

    /// <summary>Admits one empty-poll wait without consuming effect or acknowledgement capacity.</summary>
    /// <exception cref="OperationCanceledException">The hub has begun disposal.</exception>
    private void EnterActivePoll()
    {
        lock (_lifecycleGate)
        {
            _activePolls++;
        }
    }

    /// <summary>Releases one active empty-poll wait and completes disposal drain when appropriate.</summary>
    private void ReleaseActivePoll()
    {
        TaskCompletionSource<bool>? drained = null;
        lock (_lifecycleGate)
        {
            _activePolls--;
            if (IsDrainedUnderGate())
            {
                drained = _drained;
            }
        }

        _ = drained?.TrySetResult(true);
    }

    /// <summary>Admits one active subscription enumerator reservation.</summary>
    /// <exception cref="ObjectDisposedException">The hub has been disposed.</exception>
    /// <exception cref="QueueCapacityExceededException">The active subscription limit has been reached.</exception>
    private void EnterActiveSubscription()
    {
        lock (_lifecycleGate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed != 0, this);
            if (_activeSubscriptions >= _options.MaximumActiveSubscriptions)
            {
                throw new QueueCapacityExceededException(ActiveSubscriptionCapacityMessage, canFitWhenEmpty: true);
            }

            _activeSubscriptions++;
        }
    }

    /// <summary>Releases one active subscription enumerator reservation.</summary>
    private void ReleaseActiveSubscription()
    {
        lock (_lifecycleGate)
        {
            _activeSubscriptions--;
        }
    }

    /// <summary>Starts or joins disposal.</summary>
    /// <returns>The shared disposal task.</returns>
    private Task BeginDispose()
    {
        Task task;
        TaskCompletionSource<bool>? completion = null;
        lock (_lifecycleGate)
        {
            if (_disposeTask is not null)
            {
                task = _disposeTask;
            }
            else
            {
                _disposed = 1;
                completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _disposeTask = completion.Task;
                task = _disposeTask;
            }
        }

        if (completion is not null)
        {
            _ = CompleteDisposeAsync(completion);
        }

        return task;
    }

    /// <summary>Runs disposal outside the lifecycle gate and publishes the shared result.</summary>
    /// <param name="completion">The shared disposal completion source.</param>
    /// <returns>The asynchronous completion operation.</returns>
    private async Task CompleteDisposeAsync(TaskCompletionSource<bool> completion)
    {
        try
        {
            await DisposeAsyncCore().ConfigureAwait(false);
            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
        }
    }

    /// <summary>Completes disposal and publishes the shared outcome.</summary>
    /// <returns>The disposal task.</returns>
    private async Task DisposeAsyncCore()
    {
        Exception? firstException = null;
        firstException = await CaptureDisposalExceptionAsync(firstException, _disposeCancellation.CancelAsync).ConfigureAwait(false);
        SignalSubscribers();
        firstException = await CaptureDisposalExceptionAsync(firstException, GetActiveCallDrainTask).ConfigureAwait(false);
        try
        {
            DisposeJournal();
        }
        finally
        {
            Close();
        }

        RethrowDisposalFailure(firstException);
    }

    /// <summary>Gets the active hub work drain task.</summary>
    /// <returns>The active hub work drain task, or a completed task when no tracked work is active.</returns>
    private Task GetActiveCallDrainTask()
    {
        Task task;
        lock (_lifecycleGate)
        {
            task = IsDrainedUnderGate() ? Task.CompletedTask : _drained.Task;
        }

        return task;
    }

    /// <summary>Checks whether disposal has begun and all tracked work has drained while the lifecycle gate is held.</summary>
    /// <returns>Whether all tracked work has drained after disposal began.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsDrainedUnderGate() => _disposed != 0 && _activeCalls == 0 && _activeAcknowledgements == 0 && _activePolls == 0;

    /// <summary>Disposes the owned journal resource once.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void DisposeJournal() => _ownedJournal?.Dispose();

    /// <summary>Closes hub-managed resources after asynchronous cancellation and drain complete.</summary>
    private void Close()
    {
        _disposeCancellation.Dispose();
        _wakeup.Dispose();
    }

    /// <summary>Adapts async policy decisions into the existing processor authorizer without blocking.</summary>
    /// <param name="authorizations">The immutable per-request authorization snapshot.</param>
    private sealed class PreAuthorizedOperationAuthorizer(IReadOnlyDictionary<OperationId, ServerOperationScope> authorizations) : IServerOperationAuthorizer
    {
        /// <summary>The immutable authorization snapshot.</summary>
        private readonly ReadOnlyDictionary<OperationId, ServerOperationScope> _authorizations = new(Copy(authorizations));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation) => _authorizations[operation.OperationId];

        /// <summary>Copies authorizations on all supported target frameworks.</summary>
        /// <param name="source">The authorization source.</param>
        /// <returns>The copied authorization dictionary.</returns>
        private static Dictionary<OperationId, ServerOperationScope> Copy(IReadOnlyDictionary<OperationId, ServerOperationScope> source)
        {
            var copy = new Dictionary<OperationId, ServerOperationScope>(source.Count);
            foreach (var authorization in source)
            {
                copy.Add(authorization.Key, authorization.Value);
            }

            return copy;
        }
    }
}
