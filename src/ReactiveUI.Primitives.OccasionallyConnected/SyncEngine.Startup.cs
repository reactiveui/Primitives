// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Shared transport startup helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Publishes the connected startup session while the engine lock is held.</summary>
    /// <param name="session">The connected transport session.</param>
    /// <param name="cancellation">The startup cancellation owner.</param>
    /// <param name="uploadCancellation">The created upload cancellation source.</param>
    /// <returns>Whether the session became active.</returns>
    private bool TryPublishStartedSession(
        IRemoteTransportSession session,
        StartupCancellationOwner cancellation,
        out CancellationTokenSource? uploadCancellation)
    {
        uploadCancellation = null;
        lock (_gate)
        {
            if (_disposed || cancellation.StopRequested || _admissionState == EngineAdmissionState.Stopping)
            {
                return false;
            }

            uploadCancellation = PublishSessionLocked(session);
            return true;
        }
    }

    /// <summary>Publishes a connected session and opens upload admission while the engine lock is held.</summary>
    /// <param name="session">The connected transport session.</param>
    /// <returns>The created upload cancellation source.</returns>
    private CancellationTokenSource PublishSessionLocked(IRemoteTransportSession session)
    {
        _session = session;
        _sessionGeneration++;
        _sharedSessionLease = new(session, _sessionGeneration);
        _remoteSessionRenewalUsedSinceProgress = false;
        var uploadCancellation = new CancellationTokenSource();
        _uploadCancellation = uploadCancellation;
        _admissionState = EngineAdmissionState.Running;
        ScheduleDeferredUploadHeadsLocked();
        return uploadCancellation;
    }

    /// <summary>Starts shared store and transport state.</summary>
    /// <param name="cancellation">The engine-owned startup cancellation.</param>
    /// <returns>The startup task.</returns>
    /// <exception cref="ObjectDisposedException">The engine is disposed before startup can complete.</exception>
    /// <exception cref="OperationCanceledException">An accepted stop superseded startup.</exception>
    private async ValueTask StartWithCancellationAsync(StartupCancellationOwner cancellation)
    {
        await EnsureStoreInitializedAsync(CancellationToken.None).ConfigureAwait(false);
        await PrepareParticipantsForEngineStartAsync().ConfigureAwait(false);
        var requiredGuarantees = await GetRequiredTransportGuaranteesAsync(cancellation.Token).ConfigureAwait(false);
        IRemoteTransportSession session;
        try
        {
            session = await ConnectValidatedSessionAsync(requiredGuarantees, cancellation.Token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!cancellation.Token.IsCancellationRequested && IsTransientConnectFailure(exception))
        {
            BeginOfflineConnect(exception, cancellation);
            return;
        }

        if (!TryPublishStartedSession(session, cancellation, out var uploadCancellation))
        {
            await session.DisposeAsync().ConfigureAwait(false);
            ThrowIfDisposed();
            throw new OperationCanceledException("Startup was superseded by an accepted stop.", cancellation.Token);
        }

        StartSessionPumps(uploadCancellation);
    }

    /// <summary>Initializes and activates typed participants before startup exposes remote receive or upload work.</summary>
    /// <returns>The participant preparation operation.</returns>
    private async ValueTask PrepareParticipantsForEngineStartAsync()
    {
        var registrations = CaptureParticipantsPendingEngineStart();
        if (registrations is null)
        {
            return;
        }

        for (var i = 0; i < registrations.Count; i++)
        {
            if (registrations[i].Participant is IEngineStartupParticipant participant)
            {
                await participant.InitializeForEngineStartAsync().ConfigureAwait(false);
            }
        }

        ActivatePreparedParticipants(registrations);
    }

    /// <summary>Captures participants that need initialization before global startup activates remote work.</summary>
    /// <returns>The pending participant registrations, or null when none need preparation.</returns>
    private List<ParticipantRegistration>? CaptureParticipantsPendingEngineStart()
    {
        List<ParticipantRegistration>? registrations = null;
        lock (_gate)
        {
            foreach (var registration in _participants.Values)
            {
                if (registration.ActivateOnEngineStart
                    && registration.Participant is IEngineStartupParticipant { InitializeOnEngineStart: true })
                {
                    (registrations ??= []).Add(registration);
                }
            }
        }

        return registrations;
    }

    /// <summary>Activates initialized participants while the engine gate is held.</summary>
    /// <param name="registrations">The registrations prepared for this startup generation.</param>
    private void ActivatePreparedParticipants(List<ParticipantRegistration> registrations)
    {
        lock (_gate)
        {
            if (_admissionState is EngineAdmissionState.Stopping or EngineAdmissionState.Disposed)
            {
                return;
            }

            for (var i = 0; i < registrations.Count; i++)
            {
                var registration = registrations[i];
                if (!_participants.TryGetValue(registration.Participant.StreamId, out var current)
                    || !ReferenceEquals(current, registration)
                    || !registration.ActivateOnEngineStart)
                {
                    continue;
                }

                registration.RemoteActive = true;
                registration.ActivateOnEngineStart = false;
                ScheduleDeferredUploadHeadLocked(registration.Participant.StreamId);
            }
        }
    }

    /// <summary>Starts upload and receive pumps for a newly published session and reports the online state.</summary>
    /// <param name="uploadCancellation">The upload cancellation created when the session was published.</param>
    private void StartSessionPumps(CancellationTokenSource? uploadCancellation)
    {
        if (uploadCancellation is not null)
        {
            var pumpTask = RunUploadPumpAsync(uploadCancellation.Token);
            List<CancellationTokenSource>? previousReceiveCancellations = null;
            lock (_gate)
            {
                if (_uploadCancellation == uploadCancellation)
                {
                    previousReceiveCancellations = [];
                    _uploadPumpTask = pumpTask;
                    if (_scheduledStreams.Count != 0)
                    {
                        SignalUploadPumpLocked();
                    }

                    StartReceivePumpsLocked(previousReceiveCancellations);
                }
            }

            if (previousReceiveCancellations is not null)
            {
                for (var i = 0; i < previousReceiveCancellations.Count; i++)
                {
                    previousReceiveCancellations[i].Dispose();
                }
            }
        }

        PublishLifecycleState(SyncLifecycleStatus.Online, networkAvailable: true);
    }
}
