// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Remote receive pump implementation for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>The receive pump fault code.</summary>
    private const string ReceivePumpFaultCode = "OC.Engine.ReceivePump";

    /// <summary>The receive pump fault message.</summary>
    private const string ReceivePumpFaultMessage = "The synchronization receive pump failed.";

    /// <summary>Validates that a transport session satisfies all requested delivery guarantees.</summary>
    /// <param name="session">The connected session.</param>
    /// <param name="deliveryGuarantees">The requested delivery guarantees.</param>
    /// <exception cref="InvalidOperationException">The session does not satisfy the guarantee.</exception>
    private static void ValidateTransportSessionGuarantees(
        IRemoteTransportSession session,
        IReadOnlyCollection<DeliveryGuarantee> deliveryGuarantees)
    {
        foreach (var deliveryGuarantee in deliveryGuarantees)
        {
            ValidateTransportSessionGuarantee(session, deliveryGuarantee);
        }
    }

    /// <summary>Validates that a transport session satisfies one requested delivery guarantee.</summary>
    /// <param name="session">The connected session.</param>
    /// <param name="deliveryGuarantee">The requested delivery guarantee.</param>
    /// <exception cref="InvalidOperationException">The session does not satisfy the guarantee.</exception>
    private static void ValidateTransportSessionGuarantee(IRemoteTransportSession session, DeliveryGuarantee deliveryGuarantee)
    {
        if (deliveryGuarantee == DeliveryGuarantee.AtMostOnce)
        {
            return;
        }

        var features = session.NegotiatedCapabilities.Features;
        if (deliveryGuarantee == DeliveryGuarantee.AtLeastOnce
            && (features & RemoteTransportCapabilities.ServerIdempotency) != 0)
        {
            return;
        }

        const RemoteTransportCapabilities exactlyOnceFeatures = RemoteTransportCapabilities.ServerIdempotency
            | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
            | RemoteTransportCapabilities.ReceiveAcknowledgements;
        if (deliveryGuarantee == DeliveryGuarantee.ExactlyOnce
            && (features & exactlyOnceFeatures) == exactlyOnceFeatures
            && session.NegotiatedCapabilities.ServerIdempotencyRetention is { } retention
            && retention > TimeSpan.Zero
            && retention != TimeSpan.MaxValue)
        {
            return;
        }

        throw new InvalidOperationException("The receive session does not satisfy the requested delivery guarantee.");
    }

    /// <summary>Disposes a session owned only by the receive pump.</summary>
    /// <param name="session">The session to dispose.</param>
    /// <param name="ownsSession">Whether the receive pump owns the session.</param>
    /// <returns>The disposal task.</returns>
    private static async ValueTask DisposeReceiveOwnedSessionAsync(IRemoteTransportSession session, bool ownsSession)
    {
        if (!ownsSession)
        {
            return;
        }

        await session.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Acknowledges a durably applied remote cursor when the transport negotiated acknowledgements.</summary>
    /// <param name="session">The active remote session.</param>
    /// <param name="subscriptionId">The durable subscription identity.</param>
    /// <param name="batch">The applied remote batch.</param>
    /// <param name="result">The durable local apply result.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The acknowledgement task.</returns>
    private static async ValueTask AcknowledgeReceiveAsync(
        IRemoteTransportSession session,
        SubscriptionId subscriptionId,
        RemoteEventBatch batch,
        RemoteApplyResult result,
        CancellationToken cancellationToken)
    {
        if ((session.NegotiatedCapabilities.Features & RemoteTransportCapabilities.ReceiveAcknowledgements) == 0)
        {
            return;
        }

        var acknowledgement = new ReceiveAcknowledgement(subscriptionId, batch.StreamId, result.NextCursor);
        await session.AcknowledgeAsync(acknowledgement, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Releases the active receive session before a retry reconnects.</summary>
    /// <param name="registration">The participant that owns the active receive lease.</param>
    /// <param name="state">The mutable receive state.</param>
    /// <returns>The release task.</returns>
    private async ValueTask ReleaseReceiveRetrySessionAsync(ParticipantRegistration registration, ReceivePumpState state)
    {
        lock (_gate)
        {
            registration.SetActiveSharedReceiveGeneration(0);
        }

        if (state.ActiveSession is null)
        {
            return;
        }

        var sessionToRelease = state.ActiveSession;
        var ownsSessionToRelease = state.OwnsActiveSession;
        var sharedLease = state.ActiveSharedSessionLease;
        state.ActiveSession = null;
        state.OwnsActiveSession = false;
        state.ActiveSharedSessionLease = null;
        if (ownsSessionToRelease)
        {
            await DisposeReceiveOwnedSessionAsync(sessionToRelease, ownsSessionToRelease).ConfigureAwait(false);
        }
        else if (sharedLease is { } lease)
        {
            await ReleaseSharedSessionLeaseAsync(lease).ConfigureAwait(false);
        }
    }

    /// <summary>Disposes a rejected receive session without masking the validation failure.</summary>
    /// <param name="session">The rejected session.</param>
    /// <returns>The disposal task.</returns>
    private async ValueTask DisposeRejectedReceiveSessionAsync(IRemoteTransportSession session)
    {
        try
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            PublishFault("OC.Engine.RejectedSessionDisposal", "Disposing a rejected transport session failed.", null, exception);
        }
    }

    /// <summary>Disposes a rejected receive session when a candidate was already connected.</summary>
    /// <param name="session">The optional rejected session.</param>
    /// <returns>The disposal task.</returns>
    private async ValueTask DisposeRejectedReceiveSessionIfPresentAsync(IRemoteTransportSession? session)
    {
        if (session is null)
        {
            return;
        }

        await DisposeRejectedReceiveSessionAsync(session).ConfigureAwait(false);
    }

    /// <summary>Starts receive pumps for registered participants while the engine lock is held.</summary>
    /// <param name="previousCancellations">The replaced cancellation sources to dispose outside the engine gate.</param>
    private void StartReceivePumpsLocked(List<CancellationTokenSource> previousCancellations)
    {
        if (_session is null || _uploadCancellation is null || _admissionState != EngineAdmissionState.Running)
        {
            return;
        }

        foreach (var registration in _participants.Values)
        {
            if (!registration.RemoteActive)
            {
                continue;
            }

            var previousCancellation = StartReceivePumpForRegistrationLocked(registration);
            if (previousCancellation is not null)
            {
                previousCancellations.Add(previousCancellation);
            }
        }
    }

    /// <summary>Starts a receive pump for one participant registration while the engine lock is held.</summary>
    /// <param name="registration">The participant registration.</param>
    /// <returns>The replaced cancellation source to dispose outside the engine gate.</returns>
    private CancellationTokenSource? StartReceivePumpForRegistrationLocked(ParticipantRegistration registration)
    {
        var uploadCancellation = _uploadCancellation;
        if (uploadCancellation is null || _admissionState != EngineAdmissionState.Running)
        {
            return null;
        }

        if (!registration.RemoteActive)
        {
            return null;
        }

        if (!_receivePumpStreams.Add(registration.Participant.StreamId))
        {
            registration.ReceiveRestartRequested = true;
            return null;
        }

        if (!TryAcquireSharedSessionLeaseLocked(out var sessionLease))
        {
            _ = _receivePumpStreams.Remove(registration.Participant.StreamId);
            registration.ReceiveRestartRequested = true;
            return null;
        }

        var pumpLease = registration.BeginReceivePump(out var previousCancellation);
        if (pumpLease is null)
        {
            _ = sessionLease.State.ReleaseReference();
            _ = _receivePumpStreams.Remove(registration.Participant.StreamId);
            registration.ReceiveRestartRequested = true;
            return previousCancellation;
        }

        registration.ReceiveRestartRequested = false;
        registration.SetActiveSharedReceiveGeneration(sessionLease.Generation);
        var task = RunReceivePumpAsync(registration, pumpLease.Value.Generation, sessionLease, uploadCancellation.Token, pumpLease.Value.Token);
        registration.ReceiveTask = task;
        registration.ReceiveTaskGeneration = pumpLease.Value.Generation;
        _receivePumpTasks.Add(task);
        _ = task.ContinueWith(
            static (completed, state) =>
            {
                ArgumentExceptionHelper.ThrowIfNull(state);
                ((SyncEngine)state).ObserveCompletedReceivePump(completed);
            },
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        return previousCancellation;
    }

    /// <summary>Runs remote receive for one registered participant.</summary>
    /// <param name="registration">The participant registration that owns the receive pump.</param>
    /// <param name="generation">The receive generation captured for this pump.</param>
    /// <param name="sessionLease">The active shared session lease.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <param name="registrationCancellationToken">The registration-owned receive cancellation token captured for this pump.</param>
    /// <returns>The receive pump task.</returns>
    private async Task RunReceivePumpAsync(
        ParticipantRegistration registration,
        long generation,
        SharedSessionLease sessionLease,
        CancellationToken cancellationToken,
        CancellationToken registrationCancellationToken)
    {
        using var receiveCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, registrationCancellationToken);
        var receiveToken = receiveCancellation.Token;
        await Task.Yield();
        var state = CreateReceivePumpState(sessionLease);
        try
        {
            await RunReceivePumpLoopAsync(registration, state, receiveToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (receiveToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            PublishReceivePumpFault(registration.Participant, exception);
        }
        finally
        {
            await CompleteReceivePumpAsync(registration, generation, state).ConfigureAwait(false);
        }
    }

    /// <summary>Creates initial receive pump state.</summary>
    /// <param name="sessionLease">The active shared session lease.</param>
    /// <returns>The receive pump state.</returns>
    private ReceivePumpState CreateReceivePumpState(SharedSessionLease sessionLease) =>
        new(sessionLease, RetryState.Start(_options.TimeProvider.GetUtcNow()));

    /// <summary>Runs receive work until stopped, unregistered, or permanently failed.</summary>
    /// <param name="registration">The participant registration that owns the receive pump.</param>
    /// <param name="state">The mutable receive state.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The receive loop task.</returns>
    private async ValueTask RunReceivePumpLoopAsync(
        ParticipantRegistration registration,
        ReceivePumpState state,
        CancellationToken cancellationToken)
    {
        var retryPolicy = new RetryPolicy(_options.Options.Retry, _options.TimeProvider, _options.RetryRandomSource);
        while (!cancellationToken.IsCancellationRequested && IsActiveRegisteredParticipant(registration))
        {
            var result = await RunReceivePumpIterationAsync(registration, state, cancellationToken).ConfigureAwait(false);
            if (!result.ShouldContinue)
            {
                return;
            }

            if (result.Failure is null)
            {
                continue;
            }

            var retry = await TryPrepareReceiveRetryAsync(
                    registration,
                    result.Failure,
                    retryPolicy,
                    state.RetryState,
                    state.SharedSessionGeneration,
                    cancellationToken)
                .ConfigureAwait(false);
            state.RetryState = retry.NextState;
            if (!retry.ShouldRetry)
            {
                return;
            }

            await ReleaseReceiveRetrySessionAsync(registration, state).ConfigureAwait(false);
            if (!retry.UseRenewedSharedSession || !TryAcquireReceiveSharedSessionLease(registration, out var renewedLease))
            {
                continue;
            }

            state.ActiveSession = renewedLease.Session;
            state.OwnsActiveSession = false;
            state.ActiveSharedSessionLease = renewedLease;
            state.SharedSessionGeneration = renewedLease.Generation;
        }
    }

    /// <summary>Runs one receive pump iteration.</summary>
    /// <param name="registration">The participant registration that owns the receive pump.</param>
    /// <param name="state">The mutable receive state.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The iteration result.</returns>
    private async ValueTask<ReceiveIterationResult> RunReceivePumpIterationAsync(
        ParticipantRegistration registration,
        ReceivePumpState state,
        CancellationToken cancellationToken)
    {
        var result = state.ActiveSession is null
            ? await ConnectReceiveIterationAsync(state, cancellationToken).ConfigureAwait(false)
            : await RunReceiveSubscriptionIterationAsync(registration, state, cancellationToken).ConfigureAwait(false);
        ResetReceiveRetryAfterProgress(state);
        return result;
    }

    /// <summary>Connects a receive session for a retry iteration.</summary>
    /// <param name="state">The mutable receive state.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The iteration result.</returns>
    private async ValueTask<ReceiveIterationResult> ConnectReceiveIterationAsync(ReceivePumpState state, CancellationToken cancellationToken)
    {
        try
        {
            state.ActiveSession = await ConnectReceiveSessionAsync(state.ActiveGuarantee, cancellationToken).ConfigureAwait(false);
            state.OwnsActiveSession = true;
            state.ActiveSharedSessionLease = null;
            state.SharedSessionGeneration = GetSharedSessionGeneration();
            return ReceiveIterationResult.ContinueWithoutFailure;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return ReceiveIterationResult.StopWithoutFailure;
        }
        catch (Exception exception)
        {
            return new(true, exception);
        }
    }

    /// <summary>Runs an active receive subscription iteration.</summary>
    /// <param name="registration">The participant registration that owns the receive pump.</param>
    /// <param name="state">The mutable receive state.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The iteration result.</returns>
    private async ValueTask<ReceiveIterationResult> RunReceiveSubscriptionIterationAsync(
        ParticipantRegistration registration,
        ReceivePumpState state,
        CancellationToken cancellationToken)
    {
        try
        {
            var shouldRetry = await RunReceiveSubscriptionAsync(registration, state, cancellationToken).ConfigureAwait(false);
            return shouldRetry ? new(true, new IOException("The receive subscription ended before engine stop.")) : ReceiveIterationResult.StopWithoutFailure;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return ReceiveIterationResult.StopWithoutFailure;
        }
        catch (Exception exception) when (IsActiveRegisteredParticipant(registration))
        {
            return new(true, exception);
        }
    }

    /// <summary>Resets retry exhaustion after durable receive progress.</summary>
    /// <param name="state">The mutable receive state.</param>
    private void ResetReceiveRetryAfterProgress(ReceivePumpState state)
    {
        if (!state.MadeProgress)
        {
            return;
        }

        state.RetryState = RetryState.Start(_options.TimeProvider.GetUtcNow());
        state.MadeProgress = false;
    }

    /// <summary>Runs one receive subscription pass against the supplied session.</summary>
    /// <param name="registration">The participant registration that owns the receive pump.</param>
    /// <param name="state">The mutable receive state.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns><see langword="true"/> when an active subscription completed and should be retried; otherwise, <see langword="false"/>.</returns>
    private async ValueTask<bool> RunReceiveSubscriptionAsync(
        ParticipantRegistration registration,
        ReceivePumpState state,
        CancellationToken cancellationToken)
    {
        var participant = registration.Participant;
        var subscription = await participant.PrepareReceiveAsync(cancellationToken).ConfigureAwait(false);
        if (subscription is null || state.ActiveSession is null || !IsActiveRegisteredParticipant(registration))
        {
            return false;
        }

        state.ActiveGuarantee = subscription.DeliveryGuarantee;
        var request = new RemoteSubscribeRequest(subscription.StreamId, subscription.SubscriptionId, subscription.Cursor, subscription.InitialPosition);
        var session = state.ActiveSession;
        var sessionGeneration = state.SharedSessionGeneration;
        try
        {
            await foreach (var batch in session.SubscribeAsync(request, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (!IsActiveRegisteredParticipant(registration))
                {
                    return false;
                }

                await ApplyReceiveBatchAsync(
                        participant,
                        session,
                        subscription.SubscriptionId,
                        batch,
                        sessionGeneration,
                        cancellationToken)
                    .ConfigureAwait(false);
                state.MadeProgress = true;
            }
        }
        catch (RemoteSubscriptionRetentionGapException exception) when (IsRecoveryGapForSubscription(exception, subscription))
        {
            if (!await RecoverReceiveSnapshotAsync(registration, session, exception, sessionGeneration, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }

            RecordRemoteProgress(sessionGeneration);
            state.MadeProgress = true;
            return IsActiveRegisteredParticipant(registration);
        }

        return IsActiveRegisteredParticipant(registration);
    }

    /// <summary>Applies and acknowledges one remote receive batch.</summary>
    /// <param name="participant">The participant that owns the serialized stream lane.</param>
    /// <param name="session">The active receive session.</param>
    /// <param name="subscriptionId">The active subscription identity.</param>
    /// <param name="batch">The remote batch.</param>
    /// <param name="sharedSessionGeneration">The shared session generation that received the batch.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The apply task.</returns>
    private async ValueTask ApplyReceiveBatchAsync(
        IOccasionallyConnectedStreamParticipant participant,
        IRemoteTransportSession session,
        SubscriptionId subscriptionId,
        RemoteEventBatch batch,
        long sharedSessionGeneration,
        CancellationToken cancellationToken)
    {
        var startedTimestamp = GetDiagnosticTimestamp();
        using var activity = StartDiagnosticActivity(OccasionallyConnectedActivityName.SyncReceive);
        var result = await participant.ApplyRemoteBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        _ = RecordParticipantQueueSnapshot(batch.StreamId, result.QueueSnapshot);
        NotifyCapacityReleased(batch.StreamId);
        if (result.CursorAdvanced)
        {
            RecordRemoteProgress(sharedSessionGeneration);
        }

        await AcknowledgeReceiveAsync(session, subscriptionId, batch, result.Receipt, cancellationToken).ConfigureAwait(false);
        RecordSyncBatchSize(batch.Events.Count);
        RecordSyncDuration(startedTimestamp);
        RecordDuplicate(result.Receipt.DuplicateCount);
    }

    /// <summary>Computes and waits for the next receive retry.</summary>
    /// <param name="registration">The participant registration whose pump failed.</param>
    /// <param name="exception">The observed failure.</param>
    /// <param name="retryPolicy">The retry policy.</param>
    /// <param name="retryState">The current retry state.</param>
    /// <param name="sessionGeneration">The shared session generation associated with the failed attempt.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The retry decision.</returns>
    private async ValueTask<ReceiveRetryResult> TryPrepareReceiveRetryAsync(
        ParticipantRegistration registration,
        Exception exception,
        RetryPolicy retryPolicy,
        RetryState retryState,
        long sessionGeneration,
        CancellationToken cancellationToken)
    {
        var participant = registration.Participant;
        var failure = ClassifyRetryFailure(exception);
        if (failure.Kind == RetryFailureKind.RemoteSessionExpired)
        {
            if (await TryRenewSharedSessionAsync(sessionGeneration, cancellationToken).ConfigureAwait(false))
            {
                return new(true, retryState, UseRenewedSharedSession: true);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return new(false, retryState, UseRenewedSharedSession: false);
            }

            PublishReceivePumpFault(participant, exception);
            return new(false, retryState, UseRenewedSharedSession: false);
        }

        if (!IsRetryableFailure(failure))
        {
            PublishReceivePumpFault(participant, exception);
            return new(false, retryState, UseRenewedSharedSession: false);
        }

        var decision = retryPolicy.GetDecision(failure, retryState);
        if (decision.Kind == RetryDecisionKind.Stop)
        {
            PublishReceivePumpFault(participant, exception);
            return new(false, decision.NextState, UseRenewedSharedSession: false);
        }

        if (decision.Delay is { } delay && delay > TimeSpan.Zero)
        {
            await DelayReceiveRetryAsync(delay, cancellationToken).ConfigureAwait(false);
        }

        RecordRetry();
        return new(true, decision.NextState, UseRenewedSharedSession: false);
    }

    /// <summary>Opens a new receive session for a retry pass.</summary>
    /// <param name="deliveryGuarantee">The delivery guarantee required by the active stream.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The connected session.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private ValueTask<IRemoteTransportSession> ConnectReceiveSessionAsync(
        DeliveryGuarantee deliveryGuarantee,
        CancellationToken cancellationToken) =>
        ConnectValidatedSessionAsync([deliveryGuarantee], cancellationToken);

    /// <summary>Connects and validates a session, releasing it if capability validation fails.</summary>
    /// <param name="requiredGuarantees">The delivery guarantees required by the registered work.</param>
    /// <param name="cancellationToken">The connection cancellation token.</param>
    /// <returns>The validated session.</returns>
    private async ValueTask<IRemoteTransportSession> ConnectValidatedSessionAsync(
        IReadOnlyCollection<DeliveryGuarantee> requiredGuarantees,
        CancellationToken cancellationToken)
    {
        var request = CreateTransportConnectRequest(requiredGuarantees);
        using var activity = StartDiagnosticActivity(OccasionallyConnectedActivityName.TransportConnect);
        AcquireCircuitBreaker();
        IRemoteTransportSession? session = null;
        try
        {
            session = await ConnectThroughCircuitBreakerAsync(request, cancellationToken).ConfigureAwait(false);
            ValidateTransportSessionGuarantees(session, requiredGuarantees);
            ValidateTransportSessionClientInboxRequirement(session);
            return session;
        }
        catch
        {
            if (session is not null)
            {
                await DisposeRejectedReceiveSessionAsync(session).ConfigureAwait(false);
            }

            throw;
        }
    }

    /// <summary>Validates that the configured local inbox satisfies the peer retention requirement.</summary>
    /// <param name="session">The connected session.</param>
    /// <exception cref="InvalidOperationException">The local inbox cannot satisfy the peer requirement.</exception>
    private void ValidateTransportSessionClientInboxRequirement(IRemoteTransportSession session)
    {
        if (session.NegotiatedCapabilities.ClientInboxRetentionRequired is not { } required)
        {
            return;
        }

        if (required <= TimeSpan.Zero || required == TimeSpan.MaxValue)
        {
            throw new InvalidOperationException("Peer inbox retention requirements must be positive and finite.");
        }

        if ((_options.Store.Capabilities & LocalStoreCapabilities.DurableInbox) == 0)
        {
            throw new InvalidOperationException("The store must support durable inbox retention required by the peer.");
        }

        if (_options.Options.Retention.InboxDeduplicationRetention >= required)
        {
            return;
        }

        throw new InvalidOperationException("The client's inbox retention is shorter than the peer requires.");
    }

    /// <summary>Creates a validated transport connection request.</summary>
    /// <param name="requiredGuarantees">The delivery guarantees required by the caller.</param>
    /// <returns>The connection request.</returns>
    private TransportConnectRequest CreateTransportConnectRequest(IReadOnlyCollection<DeliveryGuarantee> requiredGuarantees) =>
        new(_options.SupportedProtocolVersions, _options.Client, requiredGuarantees);

    /// <summary>Gets the transport guarantees required by registered upload and receive work.</summary>
    /// <param name="cancellationToken">The startup cancellation token.</param>
    /// <returns>The required guarantees.</returns>
    private async ValueTask<IReadOnlyCollection<DeliveryGuarantee>> GetRequiredTransportGuaranteesAsync(CancellationToken cancellationToken)
    {
        var participants = GetRegisteredParticipantsSnapshot();
        var guarantees = new List<DeliveryGuarantee>();
        for (var i = 0; i < participants.Length; i++)
        {
            if (IsActiveRegisteredParticipant(participants[i]))
            {
                await AddParticipantReceiveGuaranteeAsync(participants[i], guarantees, cancellationToken).ConfigureAwait(false);
            }
        }

        return guarantees;
    }

    /// <summary>Gets registered participants without holding the engine gate while user code runs.</summary>
    /// <returns>The registered participant snapshot.</returns>
    private ParticipantRegistration[] GetRegisteredParticipantsSnapshot()
    {
        lock (_gate)
        {
            var participants = new ParticipantRegistration[_participants.Count];
            var index = 0;
            foreach (var registration in _participants.Values)
            {
                participants[index] = registration;
                index++;
            }

            return participants;
        }
    }

    /// <summary>Adds a participant receive guarantee when it has an active subscription.</summary>
    /// <param name="registration">The participant registration to inspect.</param>
    /// <param name="guarantees">The collected guarantees.</param>
    /// <param name="cancellationToken">The startup cancellation token.</param>
    /// <returns>The inspection task.</returns>
    private async ValueTask AddParticipantReceiveGuaranteeAsync(
        ParticipantRegistration registration,
        List<DeliveryGuarantee> guarantees,
        CancellationToken cancellationToken)
    {
        var subscription = await registration.Participant.PrepareReceiveAsync(cancellationToken).ConfigureAwait(false);
        if (subscription is not null && IsActiveRegisteredParticipant(registration) && !guarantees.Contains(subscription.DeliveryGuarantee))
        {
            guarantees.Add(subscription.DeliveryGuarantee);
        }
    }

    /// <summary>Waits for a receive retry using the injected engine clock.</summary>
    /// <param name="delay">The bounded retry delay.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The delay task.</returns>
    private async ValueTask DelayReceiveRetryAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        var completion = CreateCompletion();
        await using var timer = _options.TimeProvider.CreateTimer(
            static state =>
            {
                ArgumentExceptionHelper.ThrowIfNull(state);
                _ = ((TaskCompletionSource<bool>)state).TrySetResult(true);
            },
            completion,
            delay,
            Timeout.InfiniteTimeSpan);
        await using var registration = UnsafeRegisterDelayCancellation(completion, cancellationToken);
        await completion.Task.ConfigureAwait(false);
    }

    /// <summary>Publishes a receive pump fault.</summary>
    /// <param name="participant">The participant whose pump failed.</param>
    /// <param name="exception">The observed exception.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void PublishReceivePumpFault(IOccasionallyConnectedStreamParticipant participant, Exception exception) =>
        PublishFault(ReceivePumpFaultCode, ReceivePumpFaultMessage, participant.StreamId, exception);

    /// <summary>Completes a receive pump and releases any owned retry session.</summary>
    /// <param name="registration">The participant registration whose pump completed.</param>
    /// <param name="generation">The completed receive pump generation.</param>
    /// <param name="state">The mutable receive state.</param>
    /// <returns>The completion task.</returns>
    private async ValueTask CompleteReceivePumpAsync(ParticipantRegistration registration, long generation, ReceivePumpState state)
    {
        try
        {
            await ReleaseReceiveRetrySessionAsync(registration, state).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            PublishReceivePumpFault(registration.Participant, exception);
        }
        finally
        {
            CompleteReceivePump(registration, generation);
        }
    }

    /// <summary>Determines whether a participant registration remains current and active with this engine.</summary>
    /// <param name="registration">The registration to check.</param>
    /// <returns><see langword="true"/> when the registration is still current and remote active.</returns>
    private bool IsActiveRegisteredParticipant(ParticipantRegistration registration)
    {
        lock (_gate)
        {
            return _participants.TryGetValue(registration.Participant.StreamId, out var current)
                && ReferenceEquals(current, registration)
                && registration.RemoteActive;
        }
    }

    /// <summary>Removes a completed receive pump from the active receive stream set.</summary>
    /// <param name="registration">The registration whose pump completed.</param>
    /// <param name="generation">The completed receive pump generation.</param>
    private void CompleteReceivePump(ParticipantRegistration registration, long generation)
    {
        var disposeReceiveCancellation = CompleteReceivePumpLocked(registration, generation, out var previousCancellation);
        previousCancellation?.Dispose();
        if (disposeReceiveCancellation)
        {
            registration.DisposeReceiveCancellation();
        }
    }

    /// <summary>Completes receive pump bookkeeping while the engine lock is held.</summary>
    /// <param name="registration">The registration whose pump completed.</param>
    /// <param name="generation">The completed receive pump generation.</param>
    /// <param name="previousCancellation">The replaced cancellation source to dispose outside the engine gate.</param>
    /// <returns><see langword="true"/> when the registration cancellation source should be disposed.</returns>
    private bool CompleteReceivePumpLocked(
        ParticipantRegistration registration,
        long generation,
        out CancellationTokenSource? previousCancellation)
    {
        previousCancellation = null;
        lock (_gate)
        {
            if ((!_participants.TryGetValue(registration.Participant.StreamId, out var current)
                || ReferenceEquals(current, registration))
                && registration.ReceiveTaskGeneration == generation)
            {
                _ = _receivePumpStreams.Remove(registration.Participant.StreamId);
                registration.ReceiveTask = null;
                registration.ReceiveTaskGeneration = 0;
            }

            var disposeReceiveCancellation = !_participants.TryGetValue(registration.Participant.StreamId, out current)
                || !ReferenceEquals(current, registration);
            if (!disposeReceiveCancellation && registration.RemoteActive && registration.ReceiveRestartRequested)
            {
                previousCancellation = StartReceivePumpForRegistrationLocked(registration);
            }

            return disposeReceiveCancellation;
        }
    }

    /// <summary>Retries a pending receive restart after a stopped generation has finished running cancellation callbacks.</summary>
    /// <param name="registration">The registration whose cancellation just completed.</param>
    private void ReconcilePendingReceiveRestartAfterCancellation(ParticipantRegistration registration)
    {
        CancellationTokenSource? previousCancellation = null;
        lock (_gate)
        {
            if (_participants.TryGetValue(registration.Participant.StreamId, out var current)
                && ReferenceEquals(current, registration)
                && registration.RemoteActive
                && registration.ReceiveRestartRequested)
            {
                previousCancellation = StartReceivePumpForRegistrationLocked(registration);
            }
        }

        previousCancellation?.Dispose();
    }

    /// <summary>Observes and removes a completed receive pump task.</summary>
    /// <param name="task">The completed receive pump task.</param>
    private void ObserveCompletedReceivePump(Task task)
    {
        _ = task.Exception;
        lock (_gate)
        {
            _ = _receivePumpTasks.Remove(task);
        }
    }

    /// <summary>Describes one receive loop iteration.</summary>
    /// <param name="ShouldContinue">Whether the receive loop should continue.</param>
    /// <param name="Failure">The failure to classify for retry, if any.</param>
    private readonly record struct ReceiveIterationResult(bool ShouldContinue, Exception? Failure)
    {
        /// <summary>Gets the result for a completed loop.</summary>
        internal static ReceiveIterationResult StopWithoutFailure { get; } = new(false, null);

        /// <summary>Gets the result for another loop iteration without retry classification.</summary>
        internal static ReceiveIterationResult ContinueWithoutFailure { get; } = new(true, null);
    }

    /// <summary>Describes a receive retry decision.</summary>
    /// <param name="ShouldRetry">Whether the pump should retry.</param>
    /// <param name="NextState">The next retry state.</param>
    /// <param name="UseRenewedSharedSession">Whether the next iteration should use the renewed shared session.</param>
    private readonly record struct ReceiveRetryResult(bool ShouldRetry, RetryState NextState, bool UseRenewedSharedSession);

    /// <summary>Mutable state for one receive pump.</summary>
    /// <param name="sessionLease">The active shared session lease.</param>
    /// <param name="retryState">The initial retry state.</param>
    private sealed class ReceivePumpState(SharedSessionLease sessionLease, RetryState retryState)
    {
        /// <summary>Gets or sets the active receive session.</summary>
        internal IRemoteTransportSession? ActiveSession { get; set; } = sessionLease.Session;

        /// <summary>Gets or sets a value indicating whether the pump owns the active session.</summary>
        internal bool OwnsActiveSession { get; set; }

        /// <summary>Gets or sets the active shared session lease when the receive session is shared.</summary>
        internal SharedSessionLease? ActiveSharedSessionLease { get; set; } = sessionLease;

        /// <summary>Gets or sets the shared session generation associated with the active receive session.</summary>
        internal long SharedSessionGeneration { get; set; } = sessionLease.Generation;

        /// <summary>Gets or sets the active delivery guarantee.</summary>
        internal DeliveryGuarantee ActiveGuarantee { get; set; } = DeliveryGuarantee.AtLeastOnce;

        /// <summary>Gets or sets a value indicating whether receive work made durable progress.</summary>
        internal bool MadeProgress { get; set; }

        /// <summary>Gets or sets the current retry state.</summary>
        internal RetryState RetryState { get; set; } = retryState;
    }
}
