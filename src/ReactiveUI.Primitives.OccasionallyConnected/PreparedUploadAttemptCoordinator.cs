// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates one prepared remote upload attempt for an already leased operation batch.</summary>
internal static class PreparedUploadAttemptCoordinator
{
    /// <summary>Strict UTF-8 encoder used for logical metadata byte bounds.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Runs one prepared upload attempt.</summary>
    /// <param name="request">The prepared upload request.</param>
    /// <param name="cancellationToken">The token used to cancel prepare, barrier and send work before a response is obtained.</param>
    /// <returns>The upload attempt result.</returns>
    /// <exception cref="ArgumentNullException">A required request value is null.</exception>
    /// <exception cref="ArgumentException">The leased batch is malformed.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound is invalid.</exception>
    /// <exception cref="InvalidOperationException">The attempt cannot proceed safely.</exception>
    internal static async ValueTask<PreparedUploadAttemptResult> ExecuteAsync(
        PreparedUploadAttemptRequest request,
        CancellationToken cancellationToken)
    {
        ValidateRequest(request);
        var batch = CreateValidatedBatch(request.Lease, request.Options);
        var leaseId = request.Lease.LeaseId;
        using var attemptCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var renewalCancellation = new CancellationTokenSource();
        var renewal = new LeaseRenewalLoop(request.Store, leaseId, request.Lease.ExpiresAtUtc, request.Options, renewalCancellation, attemptCancellation);
        IPreparedRemotePush? prepared = null;
        PreparedUploadAttemptResult result = new([], false, false);
        ExceptionDispatchInfo? primary = null;
        try
        {
            await renewal.StartAsync().ConfigureAwait(false);
            prepared = await request.Transport.PreparePushAsync(batch, attemptCancellation.Token).ConfigureAwait(false);
            ArgumentExceptionHelper.ThrowIfNull(prepared);

            ValidatePrepared(prepared, batch, request.Options);
            var barriers = await BeginBarriersAsync(request.Store, leaseId, batch, attemptCancellation.Token).ConfigureAwait(false);
            if (HasDeniedBarrier(barriers))
            {
                result = new(barriers, false, false);
            }
            else
            {
                var remote = await prepared.SendAsync(attemptCancellation.Token).ConfigureAwait(false);
                SyncBatchValidator.Validate(batch, remote);
                await request.ReconcileAsync(new(leaseId, batch, remote), CancellationToken.None).ConfigureAwait(false);
                result = new(barriers, true, true);
            }
        }
        catch (Exception exception)
        {
            primary = CapturePrimaryException(exception, renewal, cancellationToken);
        }

        var cleanup = await CleanupAsync(prepared, renewal, request.Store, leaseId, !result.Reconciled).ConfigureAwait(false);
        primary?.Throw();
        cleanup?.Throw();
        return result;
    }

    /// <summary>Captures the exception that should win attempt precedence.</summary>
    /// <param name="exception">The caught exception.</param>
    /// <param name="renewal">The renewal loop.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The captured primary exception.</returns>
    private static ExceptionDispatchInfo CapturePrimaryException(Exception exception, LeaseRenewalLoop renewal, CancellationToken cancellationToken)
    {
        var renewalFailure = renewal.Failure;
        if (renewalFailure is null)
        {
            return ExceptionDispatchInfo.Capture(exception);
        }

        if (exception is not OperationCanceledException)
        {
            return ExceptionDispatchInfo.Capture(exception);
        }

        return cancellationToken.IsCancellationRequested
            ? ExceptionDispatchInfo.Capture(exception)
            : renewalFailure;
    }

    /// <summary>Validates request-level dependencies.</summary>
    /// <param name="request">The request to validate.</param>
    /// <exception cref="ArgumentNullException">A required request value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured bound is invalid.</exception>
    private static void ValidateRequest(PreparedUploadAttemptRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(request.Lease);
        ArgumentExceptionHelper.ThrowIfNull(request.Transport);
        ArgumentExceptionHelper.ThrowIfNull(request.Store);
        ArgumentExceptionHelper.ThrowIfNull(request.ReconcileAsync);
        ArgumentExceptionHelper.ThrowIfNull(request.Options);
        request.Options.Validate();
    }

    /// <summary>Creates the exact synchronization batch supplied to the prepared transport.</summary>
    /// <param name="lease">The leased batch.</param>
    /// <param name="options">The upload attempt options.</param>
    /// <returns>The validated synchronization batch.</returns>
    /// <exception cref="ArgumentException">The leased batch is malformed.</exception>
    /// <exception cref="InvalidOperationException">The leased batch exceeds configured bounds.</exception>
    private static SyncBatch CreateValidatedBatch(LeasedOperationBatch lease, PreparedUploadAttemptOptions options)
    {
        if (lease.LeaseId == Guid.Empty)
        {
            throw new ArgumentException("The lease identifier must be non-empty.", nameof(lease));
        }

        ValidateOperationCount(lease.Operations, options.MaximumOperations);
        var operations = ValidateOperations(lease.Operations, options.MaximumEncodedSizeBytes);
        return new(lease.LeaseId, operations);
    }

    /// <summary>Validates the number of leased operations before copying them.</summary>
    /// <param name="operations">The leased operations.</param>
    /// <param name="maximumOperations">The configured operation limit.</param>
    /// <exception cref="ArgumentException"><paramref name="operations"/> is empty.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="operations"/> exceeds the configured count.</exception>
    private static void ValidateOperationCount(IReadOnlyList<SyncOperation> operations, int maximumOperations)
    {
        ArgumentExceptionHelper.ThrowIfNull(operations);
        if (operations.Count == 0)
        {
            throw new ArgumentException("A prepared upload attempt requires at least one operation.", nameof(operations));
        }

        _ = operations.Count > maximumOperations ? throw new InvalidOperationException("The leased batch exceeds the configured operation limit.") : true;
    }

    /// <summary>Validates operation identity, ordering and logical bytes.</summary>
    /// <param name="operations">The leased operations.</param>
    /// <param name="maximumEncodedSizeBytes">The configured encoded byte limit.</param>
    /// <returns>The captured operations.</returns>
    /// <exception cref="ArgumentException">The leased operations are malformed.</exception>
    /// <exception cref="InvalidOperationException">The leased operations exceed configured bounds.</exception>
    private static List<SyncOperation> ValidateOperations(IReadOnlyList<SyncOperation> operations, long maximumEncodedSizeBytes)
    {
        List<SyncOperation> captured = [with(capacity: operations.Count)];
        var streamId = operations[0]?.StreamId ?? throw new ArgumentException("A leased operation cannot be null.", nameof(operations));
        long previousSequence = 0;
        long logicalBytes = 0;
        foreach (var operation in operations)
        {
            ValidateOperation(operation, streamId, previousSequence);
            previousSequence = operation.ClientSequence;
            logicalBytes = AddLogicalBytes(logicalBytes, operation, maximumEncodedSizeBytes);
            captured.Add(operation);
        }

        return captured;
    }

    /// <summary>Validates a leased operation header.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="streamId">The required stream identifier.</param>
    /// <param name="previousSequence">The prior client sequence.</param>
    /// <exception cref="ArgumentException">The leased operation is malformed.</exception>
    private static void ValidateOperation(SyncOperation operation, StreamId streamId, long previousSequence)
    {
        if (operation is null)
        {
            throw new ArgumentException("A leased operation cannot be null.", nameof(operation));
        }

        if (operation.OperationId.Value == Guid.Empty || operation.StreamId != streamId || operation.ClientSequence <= previousSequence)
        {
            throw new ArgumentException("Leased operations must have valid identity and increasing single-stream sequences.", nameof(operation));
        }

        ArgumentExceptionHelper.ThrowIfNull(operation.Payload);
        operation.Policy.Validate();
    }

    /// <summary>Adds one operation's logical encoded bytes.</summary>
    /// <param name="current">The current total.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="maximumEncodedSizeBytes">The configured encoded byte limit.</param>
    /// <returns>The updated total.</returns>
    /// <exception cref="InvalidOperationException">The logical size exceeds the configured limit.</exception>
    private static long AddLogicalBytes(long current, SyncOperation operation, long maximumEncodedSizeBytes)
    {
        var total = AddRequiredBytes(current, operation.Payload.PayloadLength, maximumEncodedSizeBytes);
        total = AddTextBytes(total, operation.BaseVersion, maximumEncodedSizeBytes);
        foreach (var pair in operation.Metadata)
        {
            total = AddTextBytes(AddTextBytes(total, pair.Key, maximumEncodedSizeBytes), pair.Value, maximumEncodedSizeBytes);
        }

        return total;
    }

    /// <summary>Adds UTF-8 bytes for one optional text value.</summary>
    /// <param name="current">The current total.</param>
    /// <param name="value">The optional value.</param>
    /// <param name="maximumEncodedSizeBytes">The configured encoded byte limit.</param>
    /// <returns>The updated total.</returns>
    /// <exception cref="InvalidOperationException">The logical size exceeds the configured limit.</exception>
    private static long AddTextBytes(long current, string? value, long maximumEncodedSizeBytes) =>
        value is null ? current : AddRequiredBytes(current, StrictUtf8.GetByteCount(value), maximumEncodedSizeBytes);

    /// <summary>Adds bounded logical bytes.</summary>
    /// <param name="current">The current total.</param>
    /// <param name="value">The value to add.</param>
    /// <param name="maximumEncodedSizeBytes">The configured encoded byte limit.</param>
    /// <returns>The updated total.</returns>
    /// <exception cref="InvalidOperationException">The logical size exceeds the configured limit.</exception>
    private static long AddRequiredBytes(long current, int value, long maximumEncodedSizeBytes)
    {
        if (current > maximumEncodedSizeBytes - value)
        {
            throw new InvalidOperationException("The leased batch exceeds the configured logical byte limit.");
        }

        return current + value;
    }

    /// <summary>Validates the prepared transport handle before a durable barrier is recorded.</summary>
    /// <param name="prepared">The prepared transport handle.</param>
    /// <param name="batch">The exact supplied synchronization batch.</param>
    /// <param name="options">The upload attempt options.</param>
    /// <exception cref="InvalidOperationException">The prepared handle is malformed.</exception>
    /// <exception cref="PreparedUploadSizeExceededException">The prepared handle exceeds configured bounds.</exception>
    private static void ValidatePrepared(IPreparedRemotePush prepared, SyncBatch batch, PreparedUploadAttemptOptions options)
    {
        if (!ReferenceEquals(prepared.Batch, batch))
        {
            throw new InvalidOperationException("The prepared upload handle substituted the synchronization batch.");
        }

        if (prepared.EncodedSizeBytes <= 0)
        {
            throw new InvalidOperationException("The prepared upload handle reported a non-positive encoded size.");
        }

        _ = prepared.EncodedSizeBytes > options.MaximumEncodedSizeBytes
            ? throw new PreparedUploadSizeExceededException(prepared.EncodedSizeBytes, options.MaximumEncodedSizeBytes)
            : true;
    }

    /// <summary>Records every durable attempt barrier.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="batch">The prepared batch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recorded barrier results.</returns>
    /// <exception cref="InvalidOperationException">A status or barrier receipt is malformed.</exception>
    private static async ValueTask<IReadOnlyList<AttemptBarrierResult>> BeginBarriersAsync(
        ILocalStoreAdapter store,
        Guid leaseId,
        SyncBatch batch,
        CancellationToken cancellationToken)
    {
        List<AttemptBarrierResult> barriers = [with(capacity: batch.Operations.Count)];
        foreach (var operation in batch.Operations)
        {
            var status = await store.GetOperationStatusAsync(operation.OperationId, cancellationToken).ConfigureAwait(false);
            var nextAttempt = GetNextAttempt(operation, status);
            cancellationToken.ThrowIfCancellationRequested();
            var barrier = await store.TryBeginRemoteAttemptAsync(leaseId, operation.OperationId, nextAttempt, cancellationToken).ConfigureAwait(false);
            ArgumentExceptionHelper.ThrowIfNull(barrier);

            ValidateBarrier(operation, nextAttempt, barrier);
            barriers.Add(barrier);
            if (!barrier.MaySend)
            {
                break;
            }
        }

        return barriers;
    }

    /// <summary>Computes the next checked attempt from durable status.</summary>
    /// <param name="operation">The leased operation.</param>
    /// <param name="status">The durable status.</param>
    /// <returns>The next attempt number.</returns>
    /// <exception cref="InvalidOperationException">The durable status is missing or malformed.</exception>
    private static int GetNextAttempt(SyncOperation operation, SyncOperationStatus? status)
    {
        if (status is null)
        {
            throw new InvalidOperationException("A leased operation is missing durable status.");
        }

        if (status.OperationId != operation.OperationId || status.StreamId != operation.StreamId || !CanSend(status.State))
        {
            throw new InvalidOperationException("The durable operation status does not match the leased operation.");
        }

        if (status.Attempt < 0 || status.Attempt == int.MaxValue)
        {
            throw new InvalidOperationException("The durable operation attempt is out of range.");
        }

        return status.Attempt + 1;
    }

    /// <summary>Checks whether an operation state may enter a remote attempt barrier.</summary>
    /// <param name="state">The durable operation state.</param>
    /// <returns>Whether sending may be attempted.</returns>
    private static bool CanSend(SyncOperationState state) =>
        state is SyncOperationState.SavedLocally or SyncOperationState.QueuedForUpload or SyncOperationState.Uploading
            or SyncOperationState.Conflict or SyncOperationState.Ambiguous;

    /// <summary>Validates one barrier receipt.</summary>
    /// <param name="operation">The leased operation.</param>
    /// <param name="nextAttempt">The expected attempt number.</param>
    /// <param name="barrier">The barrier receipt.</param>
    /// <exception cref="InvalidOperationException">The barrier receipt is malformed.</exception>
    private static void ValidateBarrier(SyncOperation operation, int nextAttempt, AttemptBarrierResult barrier)
    {
        if (barrier.OperationId == operation.OperationId && barrier.Attempt == nextAttempt)
        {
            return;
        }

        throw new InvalidOperationException("The local store returned a malformed attempt barrier receipt.");
    }

    /// <summary>Checks whether any barrier denied the send.</summary>
    /// <param name="barriers">The barrier receipts.</param>
    /// <returns>Whether sending was denied.</returns>
    private static bool HasDeniedBarrier(IReadOnlyList<AttemptBarrierResult> barriers) =>
        !barriers[barriers.Count - 1].MaySend;

    /// <summary>Cleans up the prepared handle, renewal loop and owned lease.</summary>
    /// <param name="prepared">The prepared handle.</param>
    /// <param name="renewal">The renewal loop.</param>
    /// <param name="store">The local store.</param>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="releaseLease">Whether the coordinator still owns the lease.</param>
    /// <returns>The first cleanup exception, if any.</returns>
    private static async ValueTask<ExceptionDispatchInfo?> CleanupAsync(
        IPreparedRemotePush? prepared,
        LeaseRenewalLoop? renewal,
        ILocalStoreAdapter store,
        Guid leaseId,
        bool releaseLease)
    {
        ExceptionDispatchInfo? failure = null;
        if (releaseLease)
        {
            failure = await CaptureCleanupAsync(failure, () => DisposePreparedAsync(prepared)).ConfigureAwait(false);
            failure = await CaptureCleanupAsync(failure, () => DisposeRenewalAsync(renewal)).ConfigureAwait(false);
            failure = await CaptureCleanupAsync(failure, () => store.ReleaseLeaseAsync(leaseId, CancellationToken.None)).ConfigureAwait(false);
            return failure;
        }

        failure = await CaptureCleanupAsync(failure, () => DisposeTransferredRenewalAsync(renewal)).ConfigureAwait(false);
        failure = await CaptureCleanupAsync(failure, () => DisposePreparedAsync(prepared)).ConfigureAwait(false);
        return failure;
    }

    /// <summary>Disposes the prepared handle when it exists.</summary>
    /// <param name="prepared">The prepared handle.</param>
    /// <returns>The disposal task.</returns>
    private static async ValueTask DisposePreparedAsync(IPreparedRemotePush? prepared)
    {
        if (prepared is not null)
        {
            await prepared.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Disposes the renewal loop when it exists.</summary>
    /// <param name="renewal">The renewal loop.</param>
    /// <returns>The disposal task.</returns>
    private static async ValueTask DisposeRenewalAsync(LeaseRenewalLoop? renewal)
    {
        if (renewal is not null)
        {
            await renewal.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Disposes the renewal loop after durable reconciliation consumed lease ownership.</summary>
    /// <param name="renewal">The renewal loop.</param>
    /// <returns>The disposal task.</returns>
    private static async ValueTask DisposeTransferredRenewalAsync(LeaseRenewalLoop? renewal)
    {
        if (renewal is not null)
        {
            await renewal.DisposeAfterOwnershipTransferAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Captures the first cleanup exception.</summary>
    /// <param name="existing">The existing cleanup failure.</param>
    /// <param name="operation">The cleanup operation.</param>
    /// <returns>The first cleanup exception.</returns>
    private static async ValueTask<ExceptionDispatchInfo?> CaptureCleanupAsync(
        ExceptionDispatchInfo? existing,
        Func<ValueTask> operation)
    {
        try
        {
            await operation().ConfigureAwait(false);
            return existing;
        }
        catch (Exception exception)
        {
            return existing ?? ExceptionDispatchInfo.Capture(exception);
        }
    }

    /// <summary>Runs bounded lease renewal until the attempt drains.</summary>
    private sealed class LeaseRenewalLoop : IAsyncDisposable
    {
        /// <summary>The local store.</summary>
        private readonly ILocalStoreAdapter _store;

        /// <summary>The lease identifier.</summary>
        private readonly Guid _leaseId;

        /// <summary>The upload attempt options.</summary>
        private readonly PreparedUploadAttemptOptions _options;

        /// <summary>The cancellation source that stops lease renewal.</summary>
        private readonly CancellationTokenSource _renewalCancellation;

        /// <summary>The cancellation source shared with the transport attempt.</summary>
        private readonly CancellationTokenSource _attemptCancellation;

        /// <summary>Synchronizes tick state.</summary>
#if NET9_0_OR_GREATER
        private readonly System.Threading.Lock _gate = new();
#else
        private readonly object _gate = new();
#endif

        /// <summary>The tracked lease expiry.</summary>
        private DateTimeOffset _expiresAtUtc;

        /// <summary>The current tick signal.</summary>
        private TaskCompletionSource<bool> _tick = CreateTickSignal();

        /// <summary>The renewal timer.</summary>
        private ITimer? _timer;

        /// <summary>The renewal task.</summary>
        private Task? _renewalTask;

        /// <summary>The first renewal failure.</summary>
        private ExceptionDispatchInfo? _failure;

        /// <summary>Whether the loop is stopping.</summary>
        private bool _stopping;

        /// <summary>Initializes a new instance of the <see cref="LeaseRenewalLoop"/> class.</summary>
        /// <param name="store">The local store.</param>
        /// <param name="leaseId">The lease identifier.</param>
        /// <param name="expiresAtUtc">The initial lease expiry.</param>
        /// <param name="options">The upload attempt options.</param>
        /// <param name="renewalCancellation">The cancellation source that stops lease renewal.</param>
        /// <param name="attemptCancellation">The cancellation source shared with the attempt.</param>
        internal LeaseRenewalLoop(
            ILocalStoreAdapter store,
            Guid leaseId,
            DateTimeOffset expiresAtUtc,
            PreparedUploadAttemptOptions options,
            CancellationTokenSource renewalCancellation,
            CancellationTokenSource attemptCancellation)
        {
            _store = store;
            _leaseId = leaseId;
            _expiresAtUtc = expiresAtUtc;
            _options = options;
            _renewalCancellation = renewalCancellation;
            _attemptCancellation = attemptCancellation;
        }

        /// <summary>Gets the first renewal failure.</summary>
        internal ExceptionDispatchInfo? Failure
        {
            get
            {
                lock (_gate)
                {
                    return _failure;
                }
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync() => await DisposeCoreAsync(true).ConfigureAwait(false);

        /// <summary>Disposes the renewal loop after reconciliation consumed the lease.</summary>
        /// <returns>The disposal task.</returns>
        internal async ValueTask DisposeAfterOwnershipTransferAsync() => await DisposeCoreAsync(false).ConfigureAwait(false);

        /// <summary>Starts the renewal loop.</summary>
        /// <returns>The start task.</returns>
        internal async ValueTask StartAsync()
        {
            var timer = _options.TimeProvider.CreateTimer(
                SignalTimerTick,
                this,
                Timeout.InfiniteTimeSpan,
                Timeout.InfiniteTimeSpan);
            _timer = timer;
            _renewalTask = RunAsync(timer);
            if (IsRenewalDue(_options.TimeProvider.GetUtcNow()))
            {
                await RenewTowardTargetAsync(timer).ConfigureAwait(false);
            }
            else
            {
                ScheduleNextRenewal(timer);
            }
        }

        /// <summary>Creates one tick signal.</summary>
        /// <returns>The tick signal.</returns>
        private static TaskCompletionSource<bool> CreateTickSignal() =>
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Signals the renewal loop from timer state.</summary>
        /// <param name="state">The timer state.</param>
        /// <exception cref="InvalidOperationException">The timer state is invalid.</exception>
        private static void SignalTimerTick(object? state)
        {
            if (state is LeaseRenewalLoop loop)
            {
                loop.SignalTick();
                return;
            }

            throw new InvalidOperationException("The lease renewal timer received invalid state.");
        }

        /// <summary>Adds time while preserving a protocol-oriented failure.</summary>
        /// <param name="value">The base time.</param>
        /// <param name="duration">The duration.</param>
        /// <param name="parameterName">The parameter name.</param>
        /// <returns>The resulting time.</returns>
        /// <exception cref="InvalidOperationException">The lease expiry overflowed.</exception>
        private static DateTimeOffset CheckedAdd(DateTimeOffset value, TimeSpan duration, string parameterName)
        {
            try
            {
                return value.Add(duration);
            }
            catch (ArgumentOutOfRangeException exception)
            {
                throw new InvalidOperationException($"The lease expiry cannot be extended by {parameterName}.", exception);
            }
        }

        /// <summary>Stops and drains the renewal loop.</summary>
        /// <param name="throwRenewalFailure">Whether renewal failures remain ownership failures.</param>
        /// <returns>The disposal task.</returns>
        private async ValueTask DisposeCoreAsync(bool throwRenewalFailure)
        {
            Task? renewalTask;
            TaskCompletionSource<bool> completion;
            ExceptionDispatchInfo? cleanupFailure = null;
            lock (_gate)
            {
                _stopping = true;
                completion = _tick;
                renewalTask = _renewalTask;
            }

            cleanupFailure = await CaptureCleanupAsync(cleanupFailure, DisposeTimerAsync).ConfigureAwait(false);
            cleanupFailure = await CaptureCleanupAsync(cleanupFailure, CancelRenewalAsync).ConfigureAwait(false);
            _ = completion.TrySetResult(false);
            if (renewalTask is not null)
            {
                cleanupFailure = await CaptureCleanupAsync(cleanupFailure, () => new(renewalTask)).ConfigureAwait(false);
            }

            var failure = _failure;
            if (throwRenewalFailure && failure is not null)
            {
                failure.Throw();
            }

            if (cleanupFailure is not null)
            {
                cleanupFailure.Throw();
            }
        }

        /// <summary>Disposes the timer when it exists.</summary>
        /// <returns>The completed disposal task.</returns>
        private ValueTask DisposeTimerAsync()
        {
            _timer?.Dispose();
            return default;
        }

        /// <summary>Cancels renewal callbacks.</summary>
        /// <returns>The cancellation task.</returns>
        private async ValueTask CancelRenewalAsync() => await _renewalCancellation.CancelAsync().ConfigureAwait(false);

        /// <summary>Runs the sequential renewal loop.</summary>
        /// <param name="timer">The renewal timer.</param>
        /// <returns>The renewal task.</returns>
        private async Task RunAsync(ITimer timer)
        {
            try
            {
                while (await WaitForTickAsync().ConfigureAwait(false))
                {
                    await RenewTowardTargetAsync(timer).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_renewalCancellation.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                CaptureFailure(exception);
                await _attemptCancellation.CancelAsync().ConfigureAwait(false);
            }
        }

        /// <summary>Captures a renewal failure.</summary>
        /// <param name="exception">The failure.</param>
        private void CaptureFailure(Exception exception)
        {
            lock (_gate)
            {
                _failure ??= ExceptionDispatchInfo.Capture(exception);
            }
        }

        /// <summary>Renews the lease only to the configured forward horizon.</summary>
        /// <param name="timer">The renewal timer.</param>
        /// <returns>The renewal task.</returns>
        /// <exception cref="InvalidOperationException">The lease expired before renewal or its target expiry overflowed.</exception>
        private async ValueTask RenewTowardTargetAsync(ITimer timer)
        {
            _renewalCancellation.Token.ThrowIfCancellationRequested();
            var now = _options.TimeProvider.GetUtcNow();
            if (_expiresAtUtc <= now)
            {
                throw new InvalidOperationException("The upload lease expired before renewal.");
            }

            var targetExpiry = CheckedAdd(now, _options.LeaseRenewalDuration, nameof(_options.LeaseRenewalDuration));
            var extension = targetExpiry - _expiresAtUtc;
            if (extension > TimeSpan.Zero)
            {
                await _store.RenewLeaseAsync(_leaseId, extension, _renewalCancellation.Token).ConfigureAwait(false);
                _expiresAtUtc = CheckedAdd(_expiresAtUtc, extension, nameof(extension));
            }

            ScheduleNextRenewal(timer);
        }

        /// <summary>Checks if renewal is due now.</summary>
        /// <param name="now">The current time.</param>
        /// <returns>Whether renewal is due.</returns>
        private bool IsRenewalDue(DateTimeOffset now) =>
            _expiresAtUtc - now <= _options.LeaseRenewalInterval;

        /// <summary>Schedules the next one-shot renewal before the tracked expiry.</summary>
        /// <param name="timer">The renewal timer.</param>
        private void ScheduleNextRenewal(ITimer timer)
        {
            var dueTime = GetNextRenewalDelay(_options.TimeProvider.GetUtcNow());
            lock (_gate)
            {
                if (!_stopping)
                {
                    _ = timer.Change(dueTime, Timeout.InfiniteTimeSpan);
                }
            }
        }

        /// <summary>Gets the next renewal delay from the tracked expiry.</summary>
        /// <param name="now">The current time.</param>
        /// <returns>The timer due time.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private TimeSpan GetNextRenewalDelay(DateTimeOffset now) =>
            TimeSpan.FromTicks(Math.Max(0L, (_expiresAtUtc - now - _options.LeaseRenewalInterval).Ticks));

        /// <summary>Waits for the next timer tick.</summary>
        /// <returns>Whether the loop should renew.</returns>
        private Task<bool> WaitForTickAsync()
        {
            Task<bool> task;
            lock (_gate)
            {
                task = _tick.Task;
            }

            return task;
        }

        /// <summary>Signals one timer tick.</summary>
        private void SignalTick()
        {
            TaskCompletionSource<bool>? completion = null;
            lock (_gate)
            {
                if (!_stopping)
                {
                    completion = _tick;
                    _tick = CreateTickSignal();
                }
            }

            _ = completion?.TrySetResult(true);
        }
    }
}
