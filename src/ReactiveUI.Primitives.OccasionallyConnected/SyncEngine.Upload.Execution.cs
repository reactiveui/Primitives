// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Upload pump implementation for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Completes an upload acquisition and reports completion faults.</summary>
    /// <param name="acquisition">The acquisition to complete.</param>
    /// <param name="reschedule">The optional reschedule request.</param>
    private void CompleteUploadAcquisitionSafely(FairStreamAcquisition acquisition, UploadReschedule? reschedule)
    {
        try
        {
            CompleteUploadAcquisition(acquisition, reschedule);
        }
        catch (Exception exception)
        {
            PublishFault("OC.Engine.UploadCompletion", "An upload scheduler completion failed.", acquisition.StreamId, exception);
        }
    }

    /// <summary>Plans a leased upload before recording any send barrier.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="head">The upload head metadata.</param>
    /// <param name="maximumOperations">The operation count lease ceiling.</param>
    /// <param name="maximumBytes">The byte lease ceiling.</param>
    /// <returns>The planned outcome.</returns>
    private async Task<PlannedUploadOutcome> PlanLeasedUploadAsync(LeasedOperationBatch lease, UploadHead head, int maximumOperations, long maximumBytes)
    {
        var plan = BatchSelectionPlanner.Plan(CreateBatchSelectionItems(lease), new()
        {
            Batching = _options.Options.Batching,
            NegotiatedMaximumOperations = maximumOperations,
            NegotiatedMaximumBytes = maximumBytes,
            EnvelopeBytes = 0,
            FirstEligibleElapsed = GetNonNegativeElapsed(head.ReadySinceUtc),
            ForceReady = head.ForceReady,
        });
        return await ApplyPlanAsync(lease, head, plan).ConfigureAwait(false);
    }

    /// <summary>Executes a prepared upload and maps exact encoded-size failures to bounded rescheduling.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="execution">The prepared upload execution context.</param>
    /// <param name="head">The upload head metadata.</param>
    /// <param name="streamId">The acquired stream identity.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The optional reschedule request.</returns>
    private async Task<UploadReschedule?> ExecutePreparedUploadAsync(
        LeasedOperationBatch lease,
        PreparedUploadExecution execution,
        UploadHead head,
        StreamId streamId,
        CancellationToken cancellationToken)
    {
        try
        {
            var startedTimestamp = GetDiagnosticTimestamp();
            using var activity = StartDiagnosticActivity(OccasionallyConnectedActivityName.SyncPush);
            var result = await PreparedUploadAttemptCoordinator.ExecuteAsync(
                new(lease, execution.Preparer, _options.Store, ReconcileUploadAsync, execution.AttemptOptions),
                cancellationToken).ConfigureAwait(false);
            if (result.Sent)
            {
                RecordSyncBatchSize(lease.Operations.Count);
                RecordSyncDuration(startedTimestamp);
            }

            if (result.Reconciled)
            {
                await PublishPostReconcileAsync(lease).ConfigureAwait(false);
                RecordRemoteProgress(execution.SessionGeneration);
            }

            return result.Sent ? CreateImmediateReschedule(head, maximumOperations: 0) : null;
        }
        catch (PreparedUploadSizeExceededException exception)
        {
            PublishFault("OC.Engine.UploadOversized", "A prepared upload exceeded the negotiated byte limit.", streamId, exception);
            return CreateOversizedPreparedReschedule(lease, head);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            return await HandleUploadAttemptFailureAsync(
                    lease,
                    head,
                    streamId,
                    execution,
                    exception,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Applies retry policy to a failed leased upload attempt.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="head">The upload head metadata.</param>
    /// <param name="streamId">The acquired stream identity.</param>
    /// <param name="execution">The immutable upload attempt context.</param>
    /// <param name="exception">The observed failure.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The optional retry reschedule.</returns>
    private async Task<UploadReschedule?> HandleUploadAttemptFailureAsync(
        LeasedOperationBatch lease,
        UploadHead head,
        StreamId streamId,
        PreparedUploadExecution execution,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var failure = ClassifyRetryFailure(exception);
        if (failure.Kind == RetryFailureKind.RemoteSessionExpired)
        {
            return await HandleRemoteSessionExpiredUploadAsync(
                    head,
                    streamId,
                    execution.SessionGeneration,
                    exception,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (!IsRetryableFailure(failure))
        {
            PublishUploadAttemptFault(streamId, exception);
            return null;
        }

        var retryPolicy = new RetryPolicy(execution.RetryOptions, _options.TimeProvider, _options.RetryRandomSource);
        DateTimeOffset? latestDueUtc = null;
        for (var i = 0; i < lease.Operations.Count; i++)
        {
            var operationId = lease.Operations[i].OperationId;
            var state = await GetUploadFailureRetryStateAsync(operationId, streamId, execution.RequiresDurableRetryAnchor).ConfigureAwait(false);
            if (state is null)
            {
                return null;
            }

            var decision = retryPolicy.GetDecision(failure, state);
            if (decision.Kind == RetryDecisionKind.Stop)
            {
                PublishUploadAttemptFault(streamId, exception);
                return null;
            }

            await _options.Store.SaveRetryStateAsync(operationId, decision.NextState, CancellationToken.None).ConfigureAwait(false);
            if (decision.DueUtc is { } dueUtc && (latestDueUtc is null || dueUtc > latestDueUtc.Value))
            {
                latestDueUtc = dueUtc;
            }
        }

        RecordRetry(lease.Operations.Count);
        return new(
            head.Priority,
            head.ReadySinceUtc,
            latestDueUtc ?? _options.TimeProvider.GetUtcNow(),
            MaximumOperations: 0,
            DeadLetterOversizedHead: false,
            ForceReady: false) { IsRetryBackoff = true };
    }

    /// <summary>Renews an expired shared remote session before durable retry policy is consumed.</summary>
    /// <param name="head">The failed upload head.</param>
    /// <param name="streamId">The stream whose upload failed.</param>
    /// <param name="sessionGeneration">The shared session generation that observed expiry.</param>
    /// <param name="exception">The observed transport failure.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The immediate reschedule request, if renewal succeeded.</returns>
    private async Task<UploadReschedule?> HandleRemoteSessionExpiredUploadAsync(
        UploadHead head,
        StreamId streamId,
        long sessionGeneration,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (await TryRenewSharedSessionAsync(sessionGeneration, cancellationToken).ConfigureAwait(false))
        {
            return CreateImmediateReschedule(head, maximumOperations: 0);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return null;
        }

        PublishUploadAttemptFault(streamId, exception);
        return null;
    }

    /// <summary>Gets retry state after a retryable upload failure without creating exact anchors after remote effects.</summary>
    /// <param name="operationId">The leased operation identity.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="requiresDurableRetryAnchor">Whether retry state must already carry a pre-send anchor.</param>
    /// <returns>The retry state, or null when the upload has been faulted.</returns>
    private async Task<RetryState?> GetUploadFailureRetryStateAsync(
        OperationId operationId,
        StreamId streamId,
        bool requiresDurableRetryAnchor)
    {
        var state = await _options.Store.GetRetryStateAsync(operationId, CancellationToken.None).ConfigureAwait(false);
        if (state is not null)
        {
            return state;
        }

        if (!requiresDurableRetryAnchor)
        {
            return RetryState.Start(_options.TimeProvider.GetUtcNow());
        }

        PublishUploadAttemptFault(
            streamId,
            new InvalidOperationException("The exactly-once upload retry anchor is missing after a remote attempt."));
        return null;
    }

    /// <summary>Leases a single pending batch for one stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="maximumOperations">The operation count ceiling.</param>
    /// <param name="maximumBytes">The byte ceiling.</param>
    /// <param name="leaseDuration">The requested lease duration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The leased batch, or null when no pending work exists.</returns>
    private async Task<LeasedOperationBatch?> LeaseOneBatchAsync(
        StreamId streamId,
        int maximumOperations,
        long maximumBytes,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken)
    {
        var request = new OutboxLeaseRequest(streamId, maximumOperations, maximumBytes, leaseDuration);
        await using var enumerator = _options.Store.LeasePendingOperationsAsync(request, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        return await enumerator.MoveNextAsync().ConfigureAwait(false) ? enumerator.Current : null;
    }

    /// <summary>Applies a remote upload result through the owning stream participant.</summary>
    /// <param name="reconciliation">The upload reconciliation request.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The reconciliation task.</returns>
    private async ValueTask ReconcileUploadAsync(PreparedUploadReconciliation reconciliation, CancellationToken cancellationToken)
    {
        var streamId = reconciliation.Batch.Operations[0].StreamId;
        var participant = GetParticipant(streamId);
        var transition = await participant.ApplySyncResultAsync(reconciliation.Batch, reconciliation.Result, cancellationToken).ConfigureAwait(false);
        if (!RecordParticipantQueueSnapshot(streamId, transition.QueueSnapshot))
        {
            RecordTerminalUploadQueueRelease(reconciliation.Batch, reconciliation.Result);
        }

        RecordUploadResultMetrics(reconciliation.Result);
    }

    /// <summary>Records queue release for terminal durable upload outcomes.</summary>
    /// <param name="batch">The uploaded batch.</param>
    /// <param name="result">The durable remote result.</param>
    private void RecordTerminalUploadQueueRelease(SyncBatch batch, RemoteSyncResult result)
    {
        var operationCount = 0L;
        var byteCount = 0L;
        foreach (var operation in batch.Operations)
        {
            if (!TryFindOperationResult(operation.OperationId, result, out var operationResult) || operationResult is null)
            {
                continue;
            }

            if (!IsTerminalUploadResult(operationResult.Kind))
            {
                continue;
            }

            operationCount++;
            byteCount += GetOperationRetainedBytes(operation);
        }

        if (operationCount == 0)
        {
            return;
        }

        RecordQueueDelta(batch.Operations[0].StreamId, -operationCount, -byteCount);
    }

    /// <summary>Runs best-effort post-commit publication after durable lease ownership transferred to the store.</summary>
    /// <param name="lease">The consumed lease.</param>
    /// <returns>The post-commit task.</returns>
    private async Task PublishPostReconcileAsync(LeasedOperationBatch lease)
    {
        var streamId = lease.Operations[0].StreamId;
        NotifyCapacityReleased(streamId);
        try
        {
            var batch = new SyncBatch(lease.LeaseId, lease.Operations);
            await PublishReconciledStatusesAsync(batch).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            PublishFault("OC.Engine.UploadPostCommit", "Post-commit upload notification failed.", streamId, exception);
        }
    }

    /// <summary>Publishes durable operation statuses after upload reconciliation.</summary>
    /// <param name="batch">The reconciled batch.</param>
    /// <returns>The status publication task.</returns>
    /// <exception cref="InvalidOperationException">The local store does not return a durable status for a reconciled operation.</exception>
    private async Task PublishReconciledStatusesAsync(SyncBatch batch)
    {
        for (var i = 0; i < batch.Operations.Count; i++)
        {
            var operation = batch.Operations[i];
            var status = await _options.Store.GetOperationStatusAsync(operation.OperationId, CancellationToken.None).ConfigureAwait(false)
                ?? throw new InvalidOperationException("The local store did not return a durable status after upload reconciliation.");

            _operationStates.Publish(status);
        }
    }

    /// <summary>Applies a local dead-letter transition for the oversized head of a lease.</summary>
    /// <param name="lease">The active lease.</param>
    /// <returns>The dead-letter task.</returns>
    private async Task DeadLetterOversizedLeaseHeadAsync(LeasedOperationBatch lease)
    {
        var operation = lease.Operations[0];
        var participant = GetParticipant(operation.StreamId);
        var transition = await participant
            .DeadLetterOperationAsync(lease.LeaseId, operation.OperationId, OversizedUploadReasonCode, CancellationToken.None)
            .ConfigureAwait(false);
        RecordDeadLetter();
        RecordOperationRejected();
        if (!RecordParticipantQueueSnapshot(operation.StreamId, transition.QueueSnapshot))
        {
            RecordQueueDelta(operation.StreamId, -1, -GetOperationRetainedBytes(operation));
        }

        if (lease.Operations.Count > 1)
        {
            await _options.Store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None).ConfigureAwait(false);
        }

        await PublishPostReconcileAsync(new(lease.LeaseId, lease.ExpiresAtUtc, [operation])).ConfigureAwait(false);
    }

    /// <summary>Applies a local plan outcome before a prepared upload begins.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="head">The acquired head metadata.</param>
    /// <param name="plan">The selection plan.</param>
    /// <returns>The handled outcome and optional reschedule request.</returns>
    private async Task<PlannedUploadOutcome> ApplyPlanAsync(LeasedOperationBatch lease, UploadHead head, BatchSelectionResult plan)
    {
        if (plan.Kind == BatchSelectionResultKind.Ready && plan.PrefixCount == lease.Operations.Count)
        {
            return new(Handled: false, Reschedule: null);
        }

        if (plan.Kind == BatchSelectionResultKind.OversizedHead || plan.PrefixCount == 0)
        {
            await DeadLetterOversizedLeaseHeadAsync(lease).ConfigureAwait(false);
            return new(Handled: true, CreateImmediateReschedule(head, maximumOperations: 0));
        }

        await _options.Store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None).ConfigureAwait(false);
        if (plan.Kind == BatchSelectionResultKind.WaitForDwell)
        {
            var dueUtc = head.ReadySinceUtc + _options.Options.Batching.MaximumDwellTime;
            return new(Handled: true, new(head.Priority, head.ReadySinceUtc, dueUtc, head.MaximumOperations, DeadLetterOversizedHead: false, ForceReady: false));
        }

        return new(Handled: true, CreateImmediateReschedule(head, plan.PrefixCount));
    }
}
