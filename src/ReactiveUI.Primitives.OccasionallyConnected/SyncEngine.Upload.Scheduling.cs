// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Upload pump implementation for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Converts a current upload head to a reschedule request.</summary>
    /// <param name="head">The current upload head.</param>
    /// <returns>The reschedule request.</returns>
    private static UploadReschedule ToReschedule(UploadHead head) =>
        new(head.Priority, head.ReadySinceUtc, head.DueUtc, head.MaximumOperations, head.DeadLetterOversizedHead, head.ForceReady) { IsRetryBackoff = head.IsRetryBackoff };

    /// <summary>Merges two upload reschedule requests while preserving retry backoff and explicit flush markers.</summary>
    /// <param name="existing">The existing request.</param>
    /// <param name="requested">The new request.</param>
    /// <returns>The merged request.</returns>
    private static UploadReschedule MergeUploadReschedule(UploadReschedule existing, UploadReschedule requested)
    {
        if (existing.IsRetryBackoff)
        {
            return existing;
        }

        if (requested.IsRetryBackoff)
        {
            return requested;
        }

        return new(
            existing.Priority >= requested.Priority ? existing.Priority : requested.Priority,
            existing.ReadySinceUtc <= requested.ReadySinceUtc ? existing.ReadySinceUtc : requested.ReadySinceUtc,
            existing.DueUtc <= requested.DueUtc ? existing.DueUtc : requested.DueUtc,
            existing.MaximumOperations > 0 ? existing.MaximumOperations : requested.MaximumOperations,
            existing.DeadLetterOversizedHead || requested.DeadLetterOversizedHead,
            existing.ForceReady || requested.ForceReady);
    }

    /// <summary>Creates a reschedule request for an immediate retry while preserving head age.</summary>
    /// <param name="head">The prior head metadata.</param>
    /// <param name="maximumOperations">The adaptive operation ceiling, or zero for the negotiated default.</param>
    /// <returns>The reschedule request.</returns>
    private UploadReschedule CreateImmediateReschedule(UploadHead head, int maximumOperations) =>
        new(head.Priority, head.ReadySinceUtc, _options.TimeProvider.GetUtcNow(), maximumOperations, DeadLetterOversizedHead: false, ForceReady: head.ForceReady);

    /// <summary>Merges an explicit trigger queued during a successful attempt into the continuation request.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="requested">The successful continuation request.</param>
    /// <returns>The merged request.</returns>
    private UploadReschedule MergePendingExplicitFlushLocked(StreamId streamId, UploadReschedule requested)
    {
        if (requested.IsRetryBackoff || !_rescheduleStreams.TryGetValue(streamId, out var pending))
        {
            _ = _rescheduleStreams.Remove(streamId);
            return requested;
        }

        _ = _rescheduleStreams.Remove(streamId);
        return MergeUploadReschedule(requested, pending);
    }

    /// <summary>Creates a reschedule request after exact prepared encoding exceeded the negotiated byte limit.</summary>
    /// <param name="lease">The lease that was released by the prepared coordinator.</param>
    /// <param name="head">The prior head metadata.</param>
    /// <returns>The next bounded reschedule request.</returns>
    private UploadReschedule CreateOversizedPreparedReschedule(LeasedOperationBatch lease, UploadHead head)
    {
        if (lease.Operations.Count <= 1)
        {
            return new(head.Priority, head.ReadySinceUtc, _options.TimeProvider.GetUtcNow(), MaximumOperations: 1, DeadLetterOversizedHead: true, ForceReady: head.ForceReady);
        }

        var nextMaximumOperations = Math.Max(1, lease.Operations.Count / PrefixSplitDivisor);
        return new(head.Priority, head.ReadySinceUtc, _options.TimeProvider.GetUtcNow(), nextMaximumOperations, DeadLetterOversizedHead: false, ForceReady: head.ForceReady);
    }

    /// <summary>Defers a stopped upload wake after an inflight attempt releases ownership.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="reschedule">The optional retry reschedule.</param>
    /// <param name="hadHead">Whether the upload had scheduler head metadata.</param>
    /// <param name="priorHead">The prior scheduler head metadata.</param>
    private void DeferStoppingUploadWakeLocked(
        StreamId streamId,
        UploadReschedule? reschedule,
        bool hadHead,
        UploadHead priorHead)
    {
        _ = _rescheduleStreams.Remove(streamId);
        if (!_participants.ContainsKey(streamId))
        {
            return;
        }

        if (reschedule is { } requested)
        {
            DeferStreamScheduleLocked(streamId, requested);
            return;
        }

        if (!hadHead)
        {
            return;
        }

        DeferStreamScheduleLocked(streamId, ToReschedule(priorHead));
    }

    /// <summary>Completes scheduler ownership after one upload attempt.</summary>
    /// <param name="acquisition">The acquisition to complete.</param>
    /// <param name="reschedule">The optional reschedule request.</param>
    private void CompleteUploadAcquisition(FairStreamAcquisition acquisition, UploadReschedule? reschedule)
    {
        TaskCompletionSource<bool>? drainWaiter = null;
        lock (_gate)
        {
            _activeUploadAttempts--;
            if (_activeUploadAttempts == 0 && _uploadAttemptTasks.Count == 0)
            {
                drainWaiter = _uploadDrainWaiter;
                _uploadDrainWaiter = null;
            }

            var completedActiveHead = CompleteSchedulerAcquisitionLocked(acquisition);
            if (completedActiveHead)
            {
                CompleteActiveUploadAcquisitionLocked(acquisition, reschedule);
            }
            else
            {
                TryCompleteSyncCycleLocked();
            }
        }

        _ = drainWaiter?.TrySetResult(true);
    }

    /// <summary>Completes a scheduler-owned active upload head while the engine lock is held.</summary>
    /// <param name="acquisition">The active scheduler acquisition.</param>
    /// <param name="reschedule">The optional reschedule request.</param>
    private void CompleteActiveUploadAcquisitionLocked(FairStreamAcquisition acquisition, UploadReschedule? reschedule)
    {
        _ = _scheduledStreams.Remove(acquisition.StreamId);
        var hadHead = _uploadHeads.TryGetValue(acquisition.StreamId, out var priorHead);
        _ = _uploadHeads.Remove(acquisition.StreamId);
        if (_admissionState == EngineAdmissionState.Disposed)
        {
            _ = _rescheduleStreams.Remove(acquisition.StreamId);
            TryCompleteSyncCycleLocked();
        }
        else if (_admissionState == EngineAdmissionState.Stopping || !IsStreamRemoteActiveLocked(acquisition.StreamId))
        {
            DeferStoppingUploadWakeLocked(acquisition.StreamId, reschedule, hadHead, priorHead);
            TryCompleteSyncCycleLocked();
        }
        else if (reschedule is { } requested)
        {
            requested = MergePendingExplicitFlushLocked(acquisition.StreamId, requested);
            ScheduleStreamLocked(acquisition.StreamId, requested);
        }
        else if (_rescheduleStreams.TryGetValue(acquisition.StreamId, out var pending))
        {
            _ = _rescheduleStreams.Remove(acquisition.StreamId);
            ScheduleStreamLocked(acquisition.StreamId, pending);
        }
        else
        {
            TryCompleteSyncCycleLocked();
        }
    }

    /// <summary>Gets the current active upload drain task while the engine lock is held.</summary>
    /// <returns>The upload drain task.</returns>
    private Task GetUploadDrainTaskLocked()
    {
        if (_activeUploadAttempts == 0 && _uploadAttemptTasks.Count == 0)
        {
            return Task.CompletedTask;
        }

        _uploadDrainWaiter ??= CreateCompletion();
        return _uploadDrainWaiter.Task;
    }

    /// <summary>Gets a non-negative elapsed duration for ready-head metadata.</summary>
    /// <param name="readySinceUtc">The ready timestamp.</param>
    /// <returns>The elapsed duration.</returns>
    private TimeSpan GetNonNegativeElapsed(DateTimeOffset readySinceUtc)
    {
        var elapsed = _options.TimeProvider.GetUtcNow() - readySinceUtc;
        return elapsed < TimeSpan.Zero ? TimeSpan.Zero : elapsed;
    }

    /// <summary>Completes an acquired scheduler head when it still owns the active head.</summary>
    /// <param name="acquisition">The acquisition.</param>
    /// <returns><see langword="true"/> when the active head was completed; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CompleteSchedulerAcquisitionLocked(FairStreamAcquisition acquisition) =>
        _scheduler.TryComplete(acquisition);

    /// <summary>Publishes an upload attempt fault.</summary>
    /// <param name="streamId">The related stream.</param>
    /// <param name="exception">The observed exception.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PublishUploadAttemptFault(StreamId streamId, Exception exception) =>
        PublishFault(UploadAttemptFaultCode, UploadAttemptFaultMessage, streamId, exception);

    /// <summary>Publishes a sanitized engine fault notification.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="message">The stable diagnostic message.</param>
    /// <param name="streamId">The related stream, if any.</param>
    /// <param name="exception">The observed exception.</param>
    private void PublishFault(string code, string message, StreamId? streamId, Exception exception)
    {
        const int maximumDiagnosticTypeNameLength = 256;
        var diagnosticType = exception.GetType().ToString();
        if (diagnosticType.Length > maximumDiagnosticTypeNameLength)
        {
            diagnosticType = diagnosticType.Remove(maximumDiagnosticTypeNameLength);
        }

        var fault = new OccasionallyConnectedFault(
            code,
            message,
            _options.TimeProvider.GetUtcNow(),
            streamId,
            OperationId: null,
            new InvalidOperationException(diagnosticType)) { Category = FaultCategory.Transport, Severity = FaultSeverity.Warning, IsTransient = true };
        _faults.Publish(fault);
    }

    /// <summary>Stores bounded upload ready-head timing metadata.</summary>
    /// <param name="Priority">The head priority.</param>
    /// <param name="ReadySinceUtc">The UTC time the head became ready to the engine.</param>
    /// <param name="DueUtc">The UTC time the head becomes schedulable.</param>
    /// <param name="MaximumOperations">The adaptive operation ceiling, or zero for the negotiated default.</param>
    /// <param name="DeadLetterOversizedHead">Whether the next single-operation lease must be dead-lettered.</param>
    /// <param name="ForceReady">Whether the head should bypass dwell planning.</param>
    /// <param name="Inflight">Whether the head is currently acquired by an upload attempt.</param>
    private readonly record struct UploadHead(
        int Priority,
        DateTimeOffset ReadySinceUtc,
        DateTimeOffset DueUtc,
        int MaximumOperations,
        bool DeadLetterOversizedHead,
        bool ForceReady,
        bool Inflight)
    {
        /// <summary>Gets a value indicating whether this head is waiting for retry-policy backoff.</summary>
        public bool IsRetryBackoff { get; init; }
    }

    /// <summary>Describes how an acquired upload head should be scheduled again.</summary>
    /// <param name="Priority">The head priority.</param>
    /// <param name="ReadySinceUtc">The original batching readiness timestamp.</param>
    /// <param name="DueUtc">The next scheduler due time.</param>
    /// <param name="MaximumOperations">The adaptive operation ceiling, or zero for the negotiated default.</param>
    /// <param name="DeadLetterOversizedHead">Whether the next single-operation lease must be dead-lettered.</param>
    /// <param name="ForceReady">Whether the head should bypass dwell planning.</param>
    private readonly record struct UploadReschedule(
        int Priority,
        DateTimeOffset ReadySinceUtc,
        DateTimeOffset DueUtc,
        int MaximumOperations,
        bool DeadLetterOversizedHead,
        bool ForceReady)
    {
        /// <summary>Gets a value indicating whether this request is a retry-policy backoff.</summary>
        public bool IsRetryBackoff { get; init; }
    }

    /// <summary>Stores immutable upload attempt state captured with the acquired session.</summary>
    /// <param name="Preparer">The prepared-upload transport seam from the acquired session.</param>
    /// <param name="AttemptOptions">The prepared attempt options.</param>
    /// <param name="MaximumOperations">The operation count lease ceiling.</param>
    /// <param name="MaximumBytes">The byte lease ceiling.</param>
    /// <param name="Head">The acquired upload-head metadata.</param>
    /// <param name="NegotiatedCapabilities">The capabilities negotiated by the acquired session.</param>
    /// <param name="SessionLease">The acquired shared-session generation lease.</param>
    private readonly record struct UploadAttemptContext(
        IRemoteTransportBatchPreparer Preparer,
        PreparedUploadAttemptOptions AttemptOptions,
        int MaximumOperations,
        long MaximumBytes,
        UploadHead Head,
        NegotiatedCapabilities NegotiatedCapabilities,
        SharedSessionLease SessionLease);

    /// <summary>Stores prepared-upload execution options derived from the leased operation policies.</summary>
    /// <param name="Preparer">The prepared-upload transport seam from the acquired session.</param>
    /// <param name="AttemptOptions">The prepared attempt options.</param>
    /// <param name="RetryOptions">The retry options bounded by leased capabilities.</param>
    /// <param name="RequiresDurableRetryAnchor">Whether retry state must already carry a pre-send anchor.</param>
    /// <param name="SessionGeneration">The shared session generation captured with the acquired session.</param>
    private readonly record struct PreparedUploadExecution(
        IRemoteTransportBatchPreparer Preparer,
        PreparedUploadAttemptOptions AttemptOptions,
        RetryOptions RetryOptions,
        bool RequiresDurableRetryAnchor,
        long SessionGeneration);

    /// <summary>Describes the outcome of a shared-session renewal attempt.</summary>
    /// <param name="Renewed">Whether renewal succeeded or another generation already replaced the observed session.</param>
    private readonly record struct SessionRenewalResult(bool Renewed);

    /// <summary>Describes whether planning handled an acquired lease without a remote send.</summary>
    /// <param name="Handled">Whether the lease was handled before upload.</param>
    /// <param name="Reschedule">The optional reschedule request.</param>
    private readonly record struct PlannedUploadOutcome(bool Handled, UploadReschedule? Reschedule);

    /// <summary>Stores a retry anchor lookup result.</summary>
    /// <param name="Faulted">Whether the lookup faulted and released the lease.</param>
    /// <param name="RetryState">The stored retry state.</param>
    private readonly record struct UploadRetryAnchorLookup(bool Faulted, RetryState? RetryState);

    /// <summary>Stores a durable status lookup result.</summary>
    /// <param name="Faulted">Whether the lookup faulted and released the lease.</param>
    /// <param name="Status">The stored operation status.</param>
    private readonly record struct UploadStatusLookup(bool Faulted, SyncOperationStatus? Status);

    /// <summary>Describes the upload pump's next wait.</summary>
    /// <param name="Task">The task to await.</param>
    /// <param name="AwaitingDrain">Whether the task represents drain completion.</param>
    private readonly record struct UploadPumpWait(Task Task, bool AwaitingDrain)
    {
        /// <summary>Gets the completed terminal drain wait.</summary>
        internal static UploadPumpWait Completed { get; } = new(Task.CompletedTask, AwaitingDrain: true);

        /// <summary>Gets a value indicating whether the terminal drain wait is already complete.</summary>
        internal bool IsCompleted => AwaitingDrain && Task.IsCompleted;
    }
}
