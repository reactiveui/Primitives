// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <content>Applies DropOldest, DropNewest, and custom outbox overflow strategies inside the serialized stream lane.</content>
internal sealed partial class OccasionallyConnectedStream<TState, TInput>
{
    /// <summary>The stable fault code for an outbox overflow handled by a dropping or custom strategy.</summary>
    private const string OutboxOverflowFaultCode = "OC.Stream.OutboxOverflow";

    /// <summary>The stable fault code for observer input overflow.</summary>
    private const string InputOverflowFaultCode = "OC.Stream.InputOverflow";

    /// <summary>The dead-letter reason code for an operation evicted by DropOldest.</summary>
    private const string DroppedOldestReasonCode = "OC.Overflow.DroppedOldest";

    /// <summary>The dead-letter reason code for an operation evicted by a custom policy.</summary>
    private const string CustomEvictedReasonCode = "OC.Overflow.CustomEvicted";

    /// <summary>The maximum number of candidates exposed to a custom policy.</summary>
    private const int MaximumOverflowCandidates = 256;

    /// <summary>The lease duration used while an eviction owns the outbox prefix.</summary>
    private static readonly TimeSpan OverflowLeaseDuration = TimeSpan.FromSeconds(30);

    /// <summary>Invokes a custom policy and converts its failures into policy errors.</summary>
    /// <param name="policy">The registered policy.</param>
    /// <param name="context">The overflow context.</param>
    /// <returns>The non-null decision.</returns>
    /// <exception cref="InvalidOperationException">The policy failed or returned null.</exception>
    private static BufferOverflowDecision InvokeOverflowPolicy(IBufferOverflowPolicy policy, BufferOverflowContext context)
    {
        BufferOverflowDecision? decision;
        try
        {
            decision = policy.Decide(context);
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException("The buffer overflow policy failed.", exception);
        }

        return decision ?? throw new InvalidOperationException("The buffer overflow policy returned no decision.");
    }

    /// <summary>Finds the pending index of a policy-selected candidate.</summary>
    /// <param name="context">The context given to the policy.</param>
    /// <param name="pending">The pending stream operations.</param>
    /// <param name="selected">The selected operation.</param>
    /// <returns>The pending index of the selected candidate.</returns>
    /// <exception cref="InvalidOperationException">The selection is not a listed candidate.</exception>
    private static int FindSelectedCandidate(
        BufferOverflowContext context,
        IReadOnlyList<SyncOperation> pending,
        OperationId? selected)
    {
        if (selected is { } operationId)
        {
            for (var i = 0; i < context.Candidates.Count; i++)
            {
                if (context.Candidates[i].OperationId == operationId)
                {
                    return FindPendingIndex(pending, operationId);
                }
            }
        }

        throw new InvalidOperationException("The buffer overflow policy selected an operation that is not an eligible non-durable candidate.");
    }

    /// <summary>Finds the pending index of an operation.</summary>
    /// <param name="pending">The pending stream operations.</param>
    /// <param name="operationId">The operation.</param>
    /// <returns>The pending index.</returns>
    /// <exception cref="InvalidOperationException">The operation is not pending.</exception>
    private static int FindPendingIndex(IReadOnlyList<SyncOperation> pending, OperationId operationId)
    {
        for (var i = 0; i < pending.Count; i++)
        {
            if (pending[i].OperationId == operationId)
            {
                return i;
            }
        }

        throw new InvalidOperationException("The selected overflow candidate is no longer pending.");
    }

    /// <summary>Finds the oldest pending non-durable operation.</summary>
    /// <param name="pending">The pending stream operations.</param>
    /// <returns>The pending index, or -1 when none exists.</returns>
    private static int FindOldestVolatileOperation(IReadOnlyList<SyncOperation> pending)
    {
        for (var i = 0; i < pending.Count; i++)
        {
            if (pending[i].Policy.Durability == OperationDurability.Volatile)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>Creates the bounded metadata-only candidate view for a custom policy.</summary>
    /// <param name="pending">The pending stream operations.</param>
    /// <returns>The oldest non-durable candidates.</returns>
    private static List<BufferOverflowCandidate> CreateOverflowCandidates(IReadOnlyList<SyncOperation> pending)
    {
        List<BufferOverflowCandidate> candidates = [];
        for (var i = 0; i < pending.Count && candidates.Count < MaximumOverflowCandidates; i++)
        {
            var operation = pending[i];
            if (operation.Policy.Durability != OperationDurability.Volatile)
            {
                continue;
            }

            candidates.Add(new(
                operation.OperationId,
                operation.ClientSequence,
                operation.Payload.PayloadLength,
                operation.Policy.Priority,
                operation.TimestampUtc));
        }

        return candidates;
    }

    /// <summary>Commits a local publication and applies the dropping or custom overflow strategy when the outbox is full.</summary>
    /// <param name="committer">The initialized committer.</param>
    /// <param name="commit">The commit attempt.</param>
    /// <param name="options">The effective publish options.</param>
    /// <param name="incomingRetainedBytes">The declared retained bytes of the incoming publication.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The local commit result.</returns>
    private async ValueTask<LocalStreamCommitResult<TState, TInput>> CommitWithOverflowAsync(
        LocalStreamCommitter<TState, TInput> committer,
        Func<CancellationToken, ValueTask<LocalStreamCommitResult<TState, TInput>>> commit,
        RemotePublishOptions? options,
        long incomingRetainedBytes,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                return await commit(cancellationToken).ConfigureAwait(false);
            }
            catch (QueueCapacityExceededException exception) when (
                exception.CanFitWhenEmpty
                && options is { AdmissionStrategy: BufferStrategy.DropOldest or BufferStrategy.DropNewest or BufferStrategy.Custom })
            {
                var evicted = await HandleOverflowAsync(committer, options, incomingRetainedBytes, exception, cancellationToken)
                    .ConfigureAwait(false);
                if (!evicted)
                {
                    throw;
                }
            }
        }
    }

    /// <summary>Applies the configured overflow strategy once.</summary>
    /// <param name="committer">The initialized committer.</param>
    /// <param name="options">The effective publish options.</param>
    /// <param name="incomingRetainedBytes">The declared retained bytes of the incoming publication.</param>
    /// <param name="exception">The capacity failure.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when an operation was evicted and the commit should be retried.</returns>
    private async ValueTask<bool> HandleOverflowAsync(
        LocalStreamCommitter<TState, TInput> committer,
        RemotePublishOptions options,
        long incomingRetainedBytes,
        QueueCapacityExceededException exception,
        CancellationToken cancellationToken)
    {
        if (options.AdmissionStrategy == BufferStrategy.DropNewest)
        {
            ReportOverflow("The outbox is full; the incoming non-durable publication was dropped.", null, exception);
            return false;
        }

        var pending = await RecoverPendingOperationsAsync(committer, cancellationToken).ConfigureAwait(false);
        if (options.AdmissionStrategy == BufferStrategy.DropOldest)
        {
            var index = FindOldestVolatileOperation(pending);
            if (index >= 0 && await TryEvictAsync(committer, pending, index, DroppedOldestReasonCode, cancellationToken).ConfigureAwait(false))
            {
                return true;
            }

            ReportOverflow("The outbox is full and holds no evictable non-durable operation.", null, exception);
            return false;
        }

        return await ApplyCustomPolicyAsync(committer, options, incomingRetainedBytes, pending, exception, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Invokes the registered custom policy and applies its decision.</summary>
    /// <param name="committer">The initialized committer.</param>
    /// <param name="options">The effective publish options.</param>
    /// <param name="incomingRetainedBytes">The declared retained bytes of the incoming publication.</param>
    /// <param name="pending">The pending stream operations in client sequence order.</param>
    /// <param name="exception">The capacity failure.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when an operation was evicted and the commit should be retried.</returns>
    /// <exception cref="InvalidOperationException">The policy is missing, fails, or selects an ineligible operation.</exception>
    /// <exception cref="BufferOverflowBlockedException">The policy chose to wait for capacity.</exception>
    private async ValueTask<bool> ApplyCustomPolicyAsync(
        LocalStreamCommitter<TState, TInput> committer,
        RemotePublishOptions options,
        long incomingRetainedBytes,
        IReadOnlyList<SyncOperation> pending,
        QueueCapacityExceededException exception,
        CancellationToken cancellationToken)
    {
        var policy = _options.BufferOverflowPolicy
            ?? throw new InvalidOperationException("Custom admission requires a registered IBufferOverflowPolicy.");
        var context = new BufferOverflowContext(StreamId, options.Durable, options.Priority, incomingRetainedBytes, CreateOverflowCandidates(pending));
        var decision = InvokeOverflowPolicy(policy, context);
        switch (decision.Kind)
        {
            case BufferOverflowDecisionKind.Block:
            {
                throw new BufferOverflowBlockedException("The buffer overflow policy chose to wait for outbox capacity.", exception);
            }

            case BufferOverflowDecisionKind.Reject:
            {
                ReportOverflow("The buffer overflow policy rejected the incoming publication.", null, exception);
                return false;
            }

            case BufferOverflowDecisionKind.Evict:
            {
                var index = FindSelectedCandidate(context, pending, decision.EvictOperationId);
                if (await TryEvictAsync(committer, pending, index, CustomEvictedReasonCode, cancellationToken).ConfigureAwait(false))
                {
                    return true;
                }

                throw new InvalidOperationException("The buffer overflow policy selected an operation that is leased or blocked for upload.");
            }

            default:
            {
                throw new InvalidOperationException("The buffer overflow policy returned an undefined decision.");
            }
        }
    }

    /// <summary>Reads the pending operations of this stream in client sequence order.</summary>
    /// <param name="committer">The initialized committer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The pending operations.</returns>
    private async ValueTask<IReadOnlyList<SyncOperation>> RecoverPendingOperationsAsync(
        LocalStreamCommitter<TState, TInput> committer,
        CancellationToken cancellationToken)
    {
        var recovered = await _options.Store
            .RecoverStreamAsync(StreamId, committer.Current.SubscriptionId, cancellationToken)
            .ConfigureAwait(false);
        return recovered.PendingOperations;
    }

    /// <summary>Leases the outbox prefix ending at one non-durable operation and dead-letters that operation.</summary>
    /// <param name="committer">The initialized committer.</param>
    /// <param name="pending">The pending stream operations.</param>
    /// <param name="index">The pending index of the operation to evict.</param>
    /// <param name="reasonCode">The stable dead-letter reason code.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when the operation was evicted; <see langword="false"/> when it is leased or blocked.</returns>
    /// <exception cref="InvalidOperationException">The target operation is durable.</exception>
    private async ValueTask<bool> TryEvictAsync(
        LocalStreamCommitter<TState, TInput> committer,
        IReadOnlyList<SyncOperation> pending,
        int index,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        var target = pending[index];
        if (target.Policy.Durability != OperationDurability.Volatile)
        {
            throw new InvalidOperationException("Durable operations cannot be evicted.");
        }

        var lease = await LeaseOverflowPrefixAsync(pending, index, cancellationToken).ConfigureAwait(false);
        if (lease is null)
        {
            return false;
        }

        if (lease.Operations.Count != index + 1 || lease.Operations[index].OperationId != target.OperationId)
        {
            await ReleaseOverflowLeaseAsync(lease.LeaseId).ConfigureAwait(false);
            return false;
        }

        LocalStreamCommitterState<TState> state;
        try
        {
            state = await committer.DeadLetterOperationAsync(lease.LeaseId, target.OperationId, reasonCode, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            await ReleaseOverflowLeaseAsync(lease.LeaseId).ConfigureAwait(false);
            throw;
        }

        if (lease.Operations.Count > 1)
        {
            await ReleaseOverflowLeaseAsync(lease.LeaseId).ConfigureAwait(false);
            _options.Coordinator.NotifyRecoveredLocalWorkReady(StreamId, pending[0].Policy.Priority);
        }

        var queueSnapshot = committer.RecoveredQueueSnapshot;
        var status = await PublishDeadLetterTransitionAsync(state, queueSnapshot, target.OperationId).ConfigureAwait(false);
        PublishOverflowFault(
            "The outbox is full; the oldest eligible non-durable operation was evicted.",
            target.OperationId,
            new QueueCapacityExceededException("The unresolved outbox capacity was exceeded.", canFitWhenEmpty: true));
        _options.Coordinator.RecordQueueOverflow(StreamId, queueSnapshot, status);
        return true;
    }

    /// <summary>Leases the unleased outbox prefix of this stream through one target operation.</summary>
    /// <param name="pending">The pending stream operations.</param>
    /// <param name="index">The pending index of the last operation to lease.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The lease, or null when the stream head is leased or blocked.</returns>
    private async ValueTask<LeasedOperationBatch?> LeaseOverflowPrefixAsync(
        IReadOnlyList<SyncOperation> pending,
        int index,
        CancellationToken cancellationToken)
    {
        var bytes = 0L;
        for (var i = 0; i <= index; i++)
        {
            bytes = checked(bytes + pending[i].Payload.PayloadLength);
        }

        var request = new OutboxLeaseRequest(StreamId, index + 1, Math.Max(1L, bytes), OverflowLeaseDuration);
        await using var enumerator = _options.Store.LeasePendingOperationsAsync(request, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        return await enumerator.MoveNextAsync().ConfigureAwait(false) ? enumerator.Current : null;
    }

    /// <summary>Releases an eviction lease without letting release failures hide the eviction outcome.</summary>
    /// <param name="leaseId">The lease.</param>
    /// <returns>The release task.</returns>
    private async ValueTask ReleaseOverflowLeaseAsync(Guid leaseId)
    {
        try
        {
            await _options.Store.ReleaseLeaseAsync(leaseId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            PublishFault("OC.Stream.OverflowLease", "The outbox overflow lease could not be released; it will expire.", null, exception);
        }
    }

    /// <summary>Reports an overflow that evicted nothing.</summary>
    /// <param name="message">The diagnostic message.</param>
    /// <param name="operationId">The optional operation identity.</param>
    /// <param name="exception">The capacity failure.</param>
    private void ReportOverflow(string message, OperationId? operationId, Exception exception)
    {
        PublishOverflowFault(message, operationId, exception);
        _options.Coordinator.RecordQueueOverflow(StreamId, null, null);
    }

    /// <summary>Publishes a capacity fault for a handled overflow.</summary>
    /// <param name="message">The diagnostic message.</param>
    /// <param name="operationId">The optional operation identity.</param>
    /// <param name="exception">The capacity failure.</param>
    private void PublishOverflowFault(string message, OperationId? operationId, Exception exception)
    {
        var diagnostic = CreateDiagnosticException(exception);
        var fault = new OccasionallyConnectedFault(
            OutboxOverflowFaultCode,
            message,
            _options.TimeProvider.GetUtcNow(),
            StreamId,
            operationId,
            diagnostic)
        { Category = FaultCategory.Capacity, Severity = FaultSeverity.Warning, IsTransient = true };
        _ = _faults.PublishEvent(fault, GetFaultNotificationSize(fault, diagnostic));
    }

    /// <summary>Publishes an observer input fault and records input overflows as queue overflows.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="message">The diagnostic message.</param>
    /// <param name="operationId">The optional operation identity.</param>
    /// <param name="exception">The local exception.</param>
    private void PublishInputFaultCore(string code, string message, OperationId? operationId, Exception exception)
    {
        PublishFault(code, message, operationId, exception);
        if (!string.Equals(code, InputOverflowFaultCode, StringComparison.Ordinal))
        {
            return;
        }

        _options.Coordinator.RecordQueueOverflow(StreamId, null, null);
    }

    /// <summary>Determines whether custom admission is available for publish options.</summary>
    /// <param name="options">The effective publish options.</param>
    /// <returns><see langword="true"/> when a policy is registered and no custom conflict policy is requested.</returns>
    private bool SupportsCustomAdmission(RemotePublishOptions options) =>
        _options.BufferOverflowPolicy is not null && options.ConflictPolicy != ConflictPolicy.Custom;
}
