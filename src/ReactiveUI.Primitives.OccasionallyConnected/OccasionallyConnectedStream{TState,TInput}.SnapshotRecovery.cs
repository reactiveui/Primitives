// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides bounded snapshot recovery for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
internal sealed partial class OccasionallyConnectedStream<TState, TInput>
{
    /// <inheritdoc/>
    async ValueTask<ParticipantSnapshotRecoveryTransitionResult> IOccasionallyConnectedStreamParticipant.RecoverSnapshotAsync(
        IRemoteSnapshotRecoverySession session,
        string? expiredCursor,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(session);
        ArgumentExceptionHelper.ThrowIfNull(limits);
        var result = await RecoverSnapshotAsync(session, expiredCursor, limits, cancellationToken).ConfigureAwait(false);
        return new(result.Acknowledgement, result.QueueSnapshot);
    }

    /// <summary>Recovers a retained-history gap without holding the stream work lane over remote I/O.</summary>
    /// <param name="session">The remote snapshot recovery session.</param>
    /// <param name="expiredCursor">The expired cursor, if known.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The durable recovery transition.</returns>
    internal async ValueTask<SnapshotRecoveryStreamCommitResult<TState>> RecoverSnapshotAsync(
        IRemoteSnapshotRecoverySession session,
        string? expiredCursor,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        var capture = await CaptureSnapshotRecoveryAsync(expiredCursor, limits, cancellationToken).ConfigureAwait(false);
        var result = await session.GetSnapshotAsync(capture.Request, cancellationToken).ConfigureAwait(false);
        return await CommitSnapshotRecoveryAsync(capture, result, limits, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Captures the bounded local snapshot recovery request inside the stream work lane.</summary>
    /// <param name="expiredCursor">The expired cursor, if known.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The capture result.</returns>
    private async ValueTask<SnapshotRecoveryCaptureResult> CaptureSnapshotRecoveryAsync(
        string? expiredCursor,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        Task<SnapshotRecoveryCaptureResult> task;
        lock (_gate)
        {
            ThrowIfDisposed();
            task = _workLane.EnqueueAsync(
                token => CaptureSnapshotRecoveryCoreAsync(expiredCursor, limits, token),
                cancellationToken);
        }

        return await task.ConfigureAwait(false);
    }

    /// <summary>Commits the bounded snapshot recovery response inside the stream work lane.</summary>
    /// <param name="capture">The prior local capture and request.</param>
    /// <param name="result">The remote recovery result.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed recovery result.</returns>
    private async ValueTask<SnapshotRecoveryStreamCommitResult<TState>> CommitSnapshotRecoveryAsync(
        SnapshotRecoveryCaptureResult capture,
        RemoteSnapshotRecoveryResult result,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        Task<SnapshotRecoveryStreamCommitResult<TState>> task;
        lock (_gate)
        {
            ThrowIfDisposed();
            task = _workLane.EnqueueAsync(
                token => CommitSnapshotRecoveryCoreAsync(capture, result, limits, token),
                cancellationToken);
        }

        return await task.ConfigureAwait(false);
    }

    /// <summary>Runs one bounded snapshot recovery capture inside the serialized stream lane.</summary>
    /// <param name="expiredCursor">The expired cursor, if known.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The bounded capture.</returns>
    private async ValueTask<SnapshotRecoveryCaptureResult> CaptureSnapshotRecoveryCoreAsync(
        string? expiredCursor,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        var committer = await EnsureInitializedCoreAsync(cancellationToken).ConfigureAwait(false);
        return await committer.CaptureSnapshotRecoveryAsync(expiredCursor, limits, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs one bounded snapshot recovery commit inside the serialized stream lane.</summary>
    /// <param name="capture">The capture that fenced the remote request.</param>
    /// <param name="result">The remote recovery result.</param>
    /// <param name="limits">The finite recovery limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The committed recovery result.</returns>
    private async ValueTask<SnapshotRecoveryStreamCommitResult<TState>> CommitSnapshotRecoveryCoreAsync(
        SnapshotRecoveryCaptureResult capture,
        RemoteSnapshotRecoveryResult result,
        SnapshotRecoveryLimits limits,
        CancellationToken cancellationToken)
    {
        var committer = await EnsureInitializedCoreAsync(cancellationToken).ConfigureAwait(false);
        var committed = await committer.ApplySnapshotRecoveryAsync(capture, result, limits, cancellationToken).ConfigureAwait(false);
        var committedPayload = await PublishLocalAsync(committed.State, null, CancellationToken.None).ConfigureAwait(false);
        if (committedPayload is not null)
        {
            PublishCommittedStateQueueSnapshot(committedPayload, committed.QueueSnapshot);
        }

        await PublishSnapshotRecoveryOperationStatusesAsync(result.OperationDispositions).ConfigureAwait(false);
        return committed;
    }

    /// <summary>Publishes terminal operation statuses proven by snapshot recovery.</summary>
    /// <param name="dispositions">The validated recovery operation dispositions.</param>
    /// <returns>The best-effort notification task.</returns>
    private async ValueTask PublishSnapshotRecoveryOperationStatusesAsync(IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        for (var index = 0; index < dispositions.Count; index++)
        {
            var disposition = dispositions[index];
            if (disposition.Kind == SnapshotOperationDispositionKind.Unknown)
            {
                continue;
            }

            try
            {
                var status = await _options.Store.GetOperationStatusAsync(disposition.OperationId, CancellationToken.None).ConfigureAwait(false);
                if (status is null)
                {
                    PublishFault(
                        OperationStatusFaultCode,
                        "The durable operation status was unavailable after snapshot recovery.",
                        disposition.OperationId,
                        new InvalidOperationException("The durable operation status was unavailable."));
                    continue;
                }

                _ = _operationStates.PublishEvent(status, GetOperationStatusNotificationSize(status));
            }
            catch (Exception exception)
            {
                PublishFault(
                    OperationStatusFaultCode,
                    "The durable operation status could not be read after snapshot recovery.",
                    disposition.OperationId,
                    exception);
            }
        }
    }
}
