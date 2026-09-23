// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Upload pump implementation for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>The stable local reason used when a head can never fit the negotiated upload envelope.</summary>
    private const string OversizedUploadReasonCode = "OC.Engine.UploadOversized";

    /// <summary>The divisor used to shrink a batch after exact prepared encoding exceeds the negotiated limit.</summary>
    private const int PrefixSplitDivisor = 2;

    /// <summary>The upload attempt fault code.</summary>
    private const string UploadAttemptFaultCode = "OC.Engine.UploadAttempt";

    /// <summary>The upload attempt fault message.</summary>
    private const string UploadAttemptFaultMessage = "A synchronization upload attempt failed.";

    /// <summary>Checks whether a remote upload result consumes pending queue work.</summary>
    /// <param name="kind">The result kind.</param>
    /// <returns>Whether the result is terminal for queue accounting.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsTerminalUploadResult(OperationResultKind kind) =>
        kind is OperationResultKind.Accepted or OperationResultKind.Rejected;

    /// <summary>Creates bounded batch-selection metadata for a leased batch.</summary>
    /// <param name="lease">The lease.</param>
    /// <returns>The candidate metadata.</returns>
    private static BatchSelectionItem[] CreateBatchSelectionItems(LeasedOperationBatch lease)
    {
        var items = new BatchSelectionItem[lease.Operations.Count];
        for (var i = 0; i < items.Length; i++)
        {
            var operation = lease.Operations[i];
            items[i] = new(operation.ClientSequence, Math.Max(UnknownProducerRetainedBytes, operation.Payload.PayloadLength));
        }

        return items;
    }

    /// <summary>Finds an upload result by operation identity without depending on result ordering.</summary>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="result">The upload result.</param>
    /// <param name="operationResult">The matching result.</param>
    /// <returns>Whether the result contains the operation.</returns>
    private static bool TryFindOperationResult(
        OperationId operationId,
        RemoteSyncResult result,
        out OperationSyncResult? operationResult)
    {
        foreach (var candidate in result.Operations)
        {
            if (candidate.OperationId != operationId)
            {
                continue;
            }

            operationResult = candidate;
            return true;
        }

        operationResult = null;
        return false;
    }

    /// <summary>Checks whether an exception came from capability request validation.</summary>
    /// <param name="exception">The observed exception.</param>
    /// <returns>Whether the exception is a capability validation failure.</returns>
    private static bool IsCapabilityValidationException(Exception exception) =>
        exception is InvalidOperationException or ArgumentException;

    /// <summary>Combines normalized upload capabilities for a mixed-policy lease.</summary>
    /// <param name="left">The first normalized capability set.</param>
    /// <param name="right">The next normalized capability set.</param>
    /// <returns>The strictest normalized capability set shared by both operations.</returns>
    private static NegotiatedCapabilities CombineLeasedUploadCapabilities(NegotiatedCapabilities left, NegotiatedCapabilities right) =>
        left with
        {
            Features = left.Features & right.Features,
            MaximumBatchOperations = Math.Min(left.MaximumBatchOperations, right.MaximumBatchOperations),
            MaximumBatchBytes = Math.Min(left.MaximumBatchBytes, right.MaximumBatchBytes),
            EffectiveExactlyOnceWindow = GetShortestRetentionWindow(left.EffectiveExactlyOnceWindow, right.EffectiveExactlyOnceWindow),
        };

    /// <summary>Gets the shortest non-null retention window from two normalized operation policies.</summary>
    /// <param name="left">The first retention window.</param>
    /// <param name="right">The second retention window.</param>
    /// <returns>The shortest effective window, if either policy requires one.</returns>
    private static TimeSpan? GetShortestRetentionWindow(TimeSpan? left, TimeSpan? right) =>
        (left, right) switch
        {
            ({ } first, { } second) => first <= second ? first : second,
            ({ } first, null) => first,
            (null, { } second) => second,
            _ => null,
        };

    /// <summary>Checks whether a missing retry anchor belongs to a never-attempted operation.</summary>
    /// <param name="operation">The leased operation.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="status">The durable operation status.</param>
    /// <returns>Whether a fresh pre-send anchor may be created.</returns>
    private static bool CanCreateFreshUploadRetryAnchor(
        SyncOperation operation,
        StreamId streamId,
        SyncOperationStatus? status) =>
        status is { Attempt: 0 }
        && status.OperationId == operation.OperationId
        && status.StreamId == streamId;

    /// <summary>Runs the single bounded upload control pump for the active session.</summary>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The pump task.</returns>
    private async Task RunUploadPumpAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                TrackAvailableUploadAttempts(cancellationToken);
                var wait = GetUploadPumpWait(cancellationToken);
                try
                {
                    if (await WaitForUploadPumpWaitAsync(wait).ConfigureAwait(false))
                    {
                        return;
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    await WaitForUploadDrainAfterCancellationAsync().ConfigureAwait(false);
                    return;
                }
            }
        }
        catch (Exception exception)
        {
            PublishFault("OC.Engine.UploadPump", "The synchronization upload pump failed.", null, exception);
        }
    }

    /// <summary>Starts all currently acquirable upload attempts.</summary>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    private void TrackAvailableUploadAttempts(CancellationToken cancellationToken)
    {
        while (TryAcquireUpload(cancellationToken, out var acquisition, out var context, out var reservation)
            && acquisition is not null)
        {
            TrackUploadAttempt(reservation, RunUploadAttemptAsync(acquisition, context, cancellationToken));
        }
    }

    /// <summary>Gets the next upload pump wait.</summary>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The next wait.</returns>
    private UploadPumpWait GetUploadPumpWait(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                var drainTask = GetUploadDrainTaskLocked();
                if (drainTask.IsCompleted)
                {
                    TryCompleteSyncCycleLocked();
                    return UploadPumpWait.Completed;
                }

                return new(drainTask, AwaitingDrain: true);
            }

            return new(TakeUploadSignalTaskLocked(cancellationToken), AwaitingDrain: false);
        }
    }

    /// <summary>Waits for the next upload pump signal.</summary>
    /// <param name="wait">The wait to observe.</param>
    /// <returns>Whether the pump should stop.</returns>
    private async ValueTask<bool> WaitForUploadPumpWaitAsync(UploadPumpWait wait)
    {
        if (wait.IsCompleted)
        {
            return true;
        }

        await wait.Task.ConfigureAwait(false);
        if (!wait.AwaitingDrain)
        {
            return false;
        }

        lock (_gate)
        {
            TryCompleteSyncCycleLocked();
        }

        return true;
    }

    /// <summary>Waits for active upload attempts to drain after cancellation.</summary>
    /// <returns>The drain task.</returns>
    private async ValueTask WaitForUploadDrainAfterCancellationAsync()
    {
        Task drainTask;
        lock (_gate)
        {
            drainTask = GetUploadDrainTaskLocked();
        }

        await drainTask.ConfigureAwait(false);
        lock (_gate)
        {
            TryCompleteSyncCycleLocked();
        }
    }

    /// <summary>Attempts to acquire one stream head for a bounded upload attempt.</summary>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <param name="acquisition">The acquired stream head.</param>
    /// <param name="context">The acquired upload attempt context.</param>
    /// <param name="reservation">The active upload reservation.</param>
    /// <returns><see langword="true"/> when one stream was acquired.</returns>
    private bool TryAcquireUpload(
        CancellationToken cancellationToken,
        out FairStreamAcquisition? acquisition,
        out UploadAttemptContext context,
        out UploadAttemptReservation reservation)
    {
        lock (_gate)
        {
            acquisition = null;
            context = default;
            reservation = default;

            if (cancellationToken.IsCancellationRequested
                || _admissionState != EngineAdmissionState.Running
                || _activeUploadAttempts >= _options.Options.MaxConcurrentStreams
                || _session is not IRemoteTransportBatchPreparer
                || !_scheduler.TryAcquire(out acquisition))
            {
                return false;
            }

            if (!_uploadHeads.TryGetValue(acquisition.StreamId, out var head))
            {
                var now = _options.TimeProvider.GetUtcNow();
                head = new(0, now, now, 0, DeadLetterOversizedHead: false, ForceReady: false, Inflight: false);
            }

            if (!IsStreamRemoteActiveLocked(acquisition.StreamId))
            {
                _ = CompleteSchedulerAcquisitionLocked(acquisition);
                _ = _scheduledStreams.Remove(acquisition.StreamId);
                _ = _uploadHeads.Remove(acquisition.StreamId);
                DeferStreamScheduleLocked(acquisition.StreamId, ToReschedule(head));
                TryCompleteSyncCycleLocked();
                acquisition = null;
                return false;
            }

            if (!TryCreateUploadAttemptContextLocked(acquisition, head, out context))
            {
                acquisition = null;
                return false;
            }

            _uploadHeads[acquisition.StreamId] = head with { Inflight = true };
            _activeUploadAttempts++;
            reservation = new(acquisition.StreamId, CreateCompletion());
            _activeUploadAttemptByStream[acquisition.StreamId] = reservation.Completion.Task;
            return true;
        }
    }

    /// <summary>Tries to create immutable upload state for an acquired scheduler head.</summary>
    /// <param name="acquisition">The acquired scheduler head.</param>
    /// <param name="head">The upload head metadata.</param>
    /// <param name="context">The upload context.</param>
    /// <returns>Whether the context was created.</returns>
    private bool TryCreateUploadAttemptContextLocked(
        FairStreamAcquisition acquisition,
        UploadHead head,
        out UploadAttemptContext context)
    {
        context = default;
        SharedSessionLease sessionLease = default;
        var leaseAcquired = false;
        try
        {
            if (!TryAcquireSharedSessionLeaseLocked(out sessionLease)
                || sessionLease.Session is not IRemoteTransportBatchPreparer leasedPreparer)
            {
                _ = CompleteSchedulerAcquisitionLocked(acquisition);
                return false;
            }

            leaseAcquired = true;
            context = CreateUploadAttemptContextLocked(sessionLease, leasedPreparer, head);
            return true;
        }
        catch
        {
            if (leaseAcquired)
            {
                _ = sessionLease.State.ReleaseReference();
            }

            _ = CompleteSchedulerAcquisitionLocked(acquisition);
            throw;
        }
    }

    /// <summary>Creates immutable upload attempt state for an acquired shared session.</summary>
    /// <param name="sessionLease">The acquired shared-session generation lease.</param>
    /// <param name="activePreparer">The prepared-upload seam exposed by the active session.</param>
    /// <param name="head">The acquired upload head.</param>
    /// <returns>The upload attempt context.</returns>
    private UploadAttemptContext CreateUploadAttemptContextLocked(
        SharedSessionLease sessionLease,
        IRemoteTransportBatchPreparer activePreparer,
        UploadHead head)
    {
        var acquiredCapabilities = sessionLease.Session.NegotiatedCapabilities;
        var uploadLimits = CapabilityNegotiator.Negotiate(CreateUploadCapabilityRequest(
            OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce, Durability = OperationDurability.Volatile },
            acquiredCapabilities));
        var maximumOperations = head.MaximumOperations > 0
            ? Math.Min(uploadLimits.MaximumBatchOperations, head.MaximumOperations)
            : uploadLimits.MaximumBatchOperations;
        var attemptOptions = new PreparedUploadAttemptOptions { MaximumOperations = maximumOperations, MaximumEncodedSizeBytes = uploadLimits.MaximumBatchBytes, TimeProvider = _options.TimeProvider };

        attemptOptions.Validate();
        return new(
            activePreparer,
            attemptOptions,
            attemptOptions.MaximumOperations,
            attemptOptions.MaximumEncodedSizeBytes,
            head,
            acquiredCapabilities,
            sessionLease);
    }

    /// <summary>Tracks one bounded upload attempt task until it completes.</summary>
    /// <param name="reservation">The stream reservation owned by the attempt.</param>
    /// <param name="task">The attempt task.</param>
    private void TrackUploadAttempt(UploadAttemptReservation reservation, Task task)
    {
        lock (_gate)
        {
            _ = _uploadAttemptTasks.Add(task);
            _uploadAttemptReservations[task] = reservation;
        }

        _ = task.ContinueWith(
            static (completed, state) =>
            {
                ArgumentExceptionHelper.ThrowIfNull(state);
                ((SyncEngine)state).ObserveCompletedUploadAttempt(completed);
            },
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>Observes and removes a completed upload attempt task.</summary>
    /// <param name="task">The completed attempt task.</param>
    private void ObserveCompletedUploadAttempt(Task task)
    {
        TaskCompletionSource<bool>? drainWaiter = null;
        _ = task.Exception;
        lock (_gate)
        {
            _ = _uploadAttemptTasks.Remove(task);
            if (_uploadAttemptReservations.TryGetValue(task, out var reservation))
            {
                _ = _uploadAttemptReservations.Remove(task);
                if (_activeUploadAttemptByStream.TryGetValue(reservation.StreamId, out var current)
                    && ReferenceEquals(current, reservation.Completion.Task))
                {
                    _ = _activeUploadAttemptByStream.Remove(reservation.StreamId);
                }

                reservation.Complete();
            }

            if (_uploadAttemptTasks.Count == 0 && _activeUploadAttempts == 0)
            {
                drainWaiter = _uploadDrainWaiter;
                _uploadDrainWaiter = null;
            }
        }

        _ = drainWaiter?.TrySetResult(true);
    }

    /// <summary>Stores a per-stream completion reserved before an upload attempt starts external work.</summary>
    /// <param name="StreamId">The stream that owns the attempt.</param>
    /// <param name="Completion">The reservation completion observed by snapshot recovery.</param>
    private readonly record struct UploadAttemptReservation(StreamId StreamId, TaskCompletionSource<bool> Completion)
    {
        /// <summary>Completes the lifetime join after the attempt releases its shared session lease.</summary>
        internal void Complete() => _ = Completion.TrySetResult(true);
    }
}
