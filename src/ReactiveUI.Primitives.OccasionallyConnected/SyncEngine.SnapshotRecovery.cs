// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Snapshot recovery coordination for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Stores the reservation completed by each running upload attempt task.</summary>
    private readonly Dictionary<Task, UploadAttemptReservation> _uploadAttemptReservations = [];

    /// <summary>Stores currently running upload attempt tasks by stream identity.</summary>
    private readonly Dictionary<StreamId, Task> _activeUploadAttemptByStream = [];

    /// <summary>Tracks streams whose receive pump is applying snapshot recovery.</summary>
    private readonly HashSet<StreamId> _snapshotRecoveryStreams = [];

    /// <summary>Stores upload heads parked while a stream is applying snapshot recovery.</summary>
    private readonly Dictionary<StreamId, UploadHead> _snapshotRecoveryUploadHeads = [];

    /// <summary>Owns bounded FIFO snapshot recovery permits under the engine gate.</summary>
    private readonly SnapshotRecoveryAdmissionQueue _snapshotRecoveryAdmissionQueue;

    /// <summary>Gets finite structural limits for one snapshot recovery transaction.</summary>
    private static SnapshotRecoveryLimits DefaultSnapshotRecoveryLimits { get; } = new();

    /// <summary>Checks whether a retained-history gap belongs to the active subscription.</summary>
    /// <param name="exception">The retained-history gap.</param>
    /// <param name="subscription">The active subscription.</param>
    /// <returns>Whether the gap matches the active subscription.</returns>
    private static bool IsRecoveryGapForSubscription(
        RemoteSubscriptionRetentionGapException exception,
        ReceiveStreamSubscription subscription) =>
        exception.StreamId == subscription.StreamId
        && exception.SubscriptionId == subscription.SubscriptionId;

    /// <summary>Acknowledges a recovered cursor when receive acknowledgements were negotiated.</summary>
    /// <param name="session">The active session.</param>
    /// <param name="acknowledgement">The recovered cursor acknowledgement.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The acknowledgement task.</returns>
    private static async ValueTask AcknowledgeSnapshotRecoveryAsync(
        IRemoteTransportSession session,
        ReceiveAcknowledgement acknowledgement,
        CancellationToken cancellationToken)
    {
        if ((session.NegotiatedCapabilities.Features & RemoteTransportCapabilities.ReceiveAcknowledgements) == 0)
        {
            return;
        }

        await session.AcknowledgeAsync(acknowledgement, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Validates and gets the snapshot recovery facet from a negotiated session.</summary>
    /// <param name="session">The active session.</param>
    /// <returns>The validated snapshot recovery session.</returns>
    /// <exception cref="InvalidOperationException">The session did not negotiate and implement snapshot recovery.</exception>
    private static IRemoteSnapshotRecoverySession ValidateSnapshotRecoverySession(IRemoteTransportSession session)
    {
        if ((session.NegotiatedCapabilities.Features & RemoteTransportCapabilities.SnapshotRecovery) != 0
            && session is IRemoteSnapshotRecoverySession snapshotSession)
        {
            return snapshotSession;
        }

        throw new InvalidOperationException("The receive session does not support snapshot recovery.");
    }

    /// <summary>Creates upload head metadata from a reschedule request.</summary>
    /// <param name="requested">The reschedule request.</param>
    /// <returns>The upload head.</returns>
    private static UploadHead ToUploadHead(UploadReschedule requested) =>
        new(
            requested.Priority,
            requested.ReadySinceUtc,
            requested.DueUtc,
            requested.MaximumOperations,
            requested.DeadLetterOversizedHead,
            requested.ForceReady,
            Inflight: false) { IsRetryBackoff = requested.IsRetryBackoff };

    /// <summary>Recovers one receive gap through bounded snapshot recovery.</summary>
    /// <param name="registration">The participant registration whose receive cursor has a gap.</param>
    /// <param name="session">The active remote session.</param>
    /// <param name="exception">The retained-history gap.</param>
    /// <param name="sharedSessionGeneration">The generation of the receive session.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The recovery task.</returns>
    private async ValueTask<bool> RecoverReceiveSnapshotAsync(
        ParticipantRegistration registration,
        IRemoteTransportSession session,
        RemoteSubscriptionRetentionGapException exception,
        long sharedSessionGeneration,
        CancellationToken cancellationToken)
    {
        var participant = registration.Participant;
        var recovery = BeginSnapshotRecovery(
            registration,
            sharedSessionGeneration,
            cancellationToken,
            out var staleCancellation,
            out var staleDrainTask);
        if (staleCancellation is { } cancellation)
        {
            await CompleteRenewedReceiveCancellationsAsync([cancellation]).ConfigureAwait(false);
        }
        else if (staleDrainTask is not null)
        {
            await staleDrainTask.ConfigureAwait(false);
        }

        if (recovery is not { } activeRecovery)
        {
            return false;
        }

        var completedRecovery = false;
        try
        {
            var snapshotSession = ValidateSnapshotRecoverySession(session);
            await activeRecovery.ActiveUploadAttempt.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            await activeRecovery.Admission.ConfigureAwait(false);
            var limits = CreateSnapshotRecoveryLimits();
            var result = await participant
                .RecoverSnapshotAsync(snapshotSession, exception.ExpiredCursor, limits, cancellationToken)
                .ConfigureAwait(false);
            _ = RecordParticipantQueueSnapshot(participant.StreamId, result.QueueSnapshot);
            NotifyCapacityReleased(participant.StreamId);
            CompleteSnapshotRecovery(participant.StreamId, activeRecovery, releaseParkedUploads: true);
            completedRecovery = true;
            await AcknowledgeSnapshotRecoveryAsync(session, result.Acknowledgement, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            if (!completedRecovery)
            {
                CompleteSnapshotRecovery(participant.StreamId, activeRecovery, releaseParkedUploads: false);
            }

            throw;
        }
    }

    /// <summary>Starts snapshot recovery coordination for one stream.</summary>
    /// <param name="registration">The stream registration being recovered.</param>
    /// <param name="sharedSessionGeneration">The generation of the receive session.</param>
    /// <param name="cancellationToken">The recovery cancellation token.</param>
    /// <param name="staleCancellation">Cancellation captured if renewal already retired the receive session.</param>
    /// <param name="staleDrainTask">An existing cancellation callback drain to join if another stop owns it.</param>
    /// <returns>The active upload attempt to join, if any.</returns>
    private SnapshotRecoveryAdmission? BeginSnapshotRecovery(
        ParticipantRegistration registration,
        long sharedSessionGeneration,
        CancellationToken cancellationToken,
        out ReceiveRenewalCancellation? staleCancellation,
        out Task? staleDrainTask)
    {
        SnapshotRecoveryAdmissionQueue.Admission waiter;
        Task activeUpload;
        staleCancellation = null;
        staleDrainTask = null;
        lock (_gate)
        {
            if (sharedSessionGeneration > 0
                && registration.ActiveSharedReceiveGeneration == sharedSessionGeneration
                && sharedSessionGeneration != _sessionGeneration)
            {
                staleCancellation = CaptureReceiveRenewalCancellationLocked(registration, out staleDrainTask);
                return null;
            }

            var streamId = registration.Participant.StreamId;
            _ = _snapshotRecoveryStreams.Add(streamId);
            ParkScheduledSnapshotRecoveryUploadLocked(streamId);
            activeUpload = _activeUploadAttemptByStream.TryGetValue(streamId, out var uploadAttempt)
                ? uploadAttempt
                : Task.CompletedTask;
            waiter = _snapshotRecoveryAdmissionQueue.EnqueueLocked(cancellationToken);
            SignalUploadPumpLocked();
        }

        waiter.PublishInitial();
        return new(activeUpload, waiter);
    }

    /// <summary>Completes snapshot recovery coordination for one stream.</summary>
    /// <param name="streamId">The recovered stream.</param>
    /// <param name="admission">The recovery admission.</param>
    /// <param name="releaseParkedUploads">Whether parked uploads are safe to release.</param>
    private void CompleteSnapshotRecovery(
        StreamId streamId,
        SnapshotRecoveryAdmission admission,
        bool releaseParkedUploads)
    {
        SnapshotRecoveryAdmissionQueue.CompletionActions completion;
        lock (_gate)
        {
            completion = _snapshotRecoveryAdmissionQueue.CompleteLocked(admission.Waiter);

            if (releaseParkedUploads || _admissionState != EngineAdmissionState.Running || !_participants.ContainsKey(streamId))
            {
                _ = _snapshotRecoveryStreams.Remove(streamId);
                ScheduleSnapshotRecoveryParkedUploadLocked(streamId);
            }

            TryCompleteSyncCycleLocked();
            SignalUploadPumpLocked();
        }

        completion.Publish();
    }

    /// <summary>Parks one scheduled upload head while snapshot recovery owns the stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    private void ParkScheduledSnapshotRecoveryUploadLocked(StreamId streamId)
    {
        if (!_uploadHeads.TryGetValue(streamId, out var head) || head.Inflight || !_scheduler.RemovePendingHead(streamId))
        {
            return;
        }

        ParkSnapshotRecoveryHeadLocked(streamId, head);
        _ = _scheduledStreams.Remove(streamId);
        _ = _uploadHeads.Remove(streamId);
        TryCompleteSyncCycleLocked();
    }

    /// <summary>Parks a reschedule request while snapshot recovery owns the stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="requested">The upload request.</param>
    private void ParkSnapshotRecoveryUploadLocked(StreamId streamId, UploadReschedule requested)
    {
        var head = ToUploadHead(requested);
        ParkSnapshotRecoveryHeadLocked(streamId, head);
        TryCompleteSyncCycleLocked();
    }

    /// <summary>Parks a completed active upload continuation while snapshot recovery owns the stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="reschedule">The optional continuation request.</param>
    /// <param name="hadHead">Whether prior head metadata existed.</param>
    /// <param name="priorHead">The prior head metadata.</param>
    private void CompleteSnapshotRecoveryUploadAcquisitionLocked(
        StreamId streamId,
        UploadReschedule? reschedule,
        bool hadHead,
        UploadHead priorHead)
    {
        if (reschedule is { } requested)
        {
            ParkSnapshotRecoveryUploadLocked(streamId, MergePendingExplicitFlushLocked(streamId, requested));
            return;
        }

        if (_rescheduleStreams.TryGetValue(streamId, out var pending))
        {
            _ = _rescheduleStreams.Remove(streamId);
            ParkSnapshotRecoveryUploadLocked(streamId, pending);
            return;
        }

        if (hadHead)
        {
            ParkSnapshotRecoveryHeadLocked(streamId, priorHead with { Inflight = false });
        }
    }

    /// <summary>Stores or merges a snapshot recovery parked head.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="head">The head to park.</param>
    private void ParkSnapshotRecoveryHeadLocked(StreamId streamId, UploadHead head)
    {
        if (_snapshotRecoveryUploadHeads.TryGetValue(streamId, out var existing))
        {
            head = ToUploadHead(MergeUploadReschedule(ToReschedule(existing), ToReschedule(head)));
        }

        _snapshotRecoveryUploadHeads[streamId] = head with { Inflight = false };
    }

    /// <summary>Reschedules a head parked during snapshot recovery.</summary>
    /// <param name="streamId">The stream identity.</param>
    private void ScheduleSnapshotRecoveryParkedUploadLocked(StreamId streamId)
    {
        if (!_snapshotRecoveryUploadHeads.TryGetValue(streamId, out var head))
        {
            return;
        }

        _ = _snapshotRecoveryUploadHeads.Remove(streamId);
        if (_admissionState == EngineAdmissionState.Disposed || !_participants.ContainsKey(streamId))
        {
            return;
        }

        if (_admissionState != EngineAdmissionState.Running || !IsStreamRemoteActiveLocked(streamId))
        {
            DeferStreamScheduleLocked(streamId, ToReschedule(head));
            return;
        }

        ScheduleStreamLocked(streamId, ToReschedule(head));
    }

    /// <summary>Creates finite recovery limits from the existing configured message budgets.</summary>
    /// <returns>The recovery limits for one request/response transaction.</returns>
    private SnapshotRecoveryLimits CreateSnapshotRecoveryLimits()
    {
        var security = _options.Options.Security;
        var maximumMessageBytes = security.MaximumMessageBytes;
        return DefaultSnapshotRecoveryLimits with
        {
            MaximumPayloadBytes = Math.Min(DefaultSnapshotRecoveryLimits.MaximumPayloadBytes, security.MaximumPayloadBytes),
            MaximumMetadataEntries = Math.Min(DefaultSnapshotRecoveryLimits.MaximumMetadataEntries, security.MaximumMetadataEntries),
            MaximumMetadataBytes = Math.Min(DefaultSnapshotRecoveryLimits.MaximumMetadataBytes, maximumMessageBytes),
            MaximumCursorUtf8Bytes = Math.Min(DefaultSnapshotRecoveryLimits.MaximumCursorUtf8Bytes, maximumMessageBytes),
            MaximumStreamIdUtf8Bytes = Math.Min(DefaultSnapshotRecoveryLimits.MaximumStreamIdUtf8Bytes, maximumMessageBytes),
            MaximumContractUtf8Bytes = Math.Min(DefaultSnapshotRecoveryLimits.MaximumContractUtf8Bytes, maximumMessageBytes),
            MaximumReasonCodeUtf8Bytes = Math.Min(DefaultSnapshotRecoveryLimits.MaximumReasonCodeUtf8Bytes, maximumMessageBytes),
            MaximumLogicalBytes = Math.Min(DefaultSnapshotRecoveryLimits.MaximumLogicalBytes, maximumMessageBytes),
        };
    }

    /// <summary>Stores snapshot recovery admission state.</summary>
    /// <param name="ActiveUploadAttempt">The active upload attempt to join before capture.</param>
    /// <param name="Waiter">The bounded recovery admission waiter.</param>
    private readonly record struct SnapshotRecoveryAdmission(
        Task ActiveUploadAttempt,
        SnapshotRecoveryAdmissionQueue.Admission Waiter)
    {
        /// <summary>Gets the bounded recovery admission task.</summary>
        internal Task Admission => Waiter.Task;
    }
}
