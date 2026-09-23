// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Shared remote-session renewal helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Checks whether a renewed session would weaken the current shared session capabilities.</summary>
    /// <param name="currentSupportsPreparedUpload">Whether the current shared session supports prepared upload.</param>
    /// <param name="current">The current shared session capabilities.</param>
    /// <param name="candidateSupportsPreparedUpload">Whether the candidate renewed session supports prepared upload.</param>
    /// <param name="candidate">The candidate renewed session capabilities.</param>
    /// <returns>Whether the renewed session is a downgrade.</returns>
    private static bool IsRenewedSessionDowngrade(
        bool currentSupportsPreparedUpload,
        NegotiatedCapabilities current,
        bool candidateSupportsPreparedUpload,
        NegotiatedCapabilities candidate)
    {
        if (currentSupportsPreparedUpload && !candidateSupportsPreparedUpload)
        {
            return true;
        }

        return (candidate.Features & current.Features) != current.Features
            || candidate.MaximumBatchOperations < current.MaximumBatchOperations
            || candidate.MaximumBatchBytes < current.MaximumBatchBytes
            || IsShorter(candidate.ServerIdempotencyRetention, current.ServerIdempotencyRetention)
            || IsShorter(candidate.EffectiveExactlyOnceWindow, current.EffectiveExactlyOnceWindow);
    }

    /// <summary>Checks whether a candidate retention window is shorter than the current one.</summary>
    /// <param name="candidate">The candidate retention window.</param>
    /// <param name="current">The current retention window.</param>
    /// <returns>Whether the candidate is shorter.</returns>
    private static bool IsShorter(TimeSpan? candidate, TimeSpan? current) =>
        current is not null && (candidate is null || candidate < current);

    /// <summary>Records meaningful remote progress for the current shared session generation.</summary>
    /// <param name="sessionGeneration">The session generation that made progress.</param>
    private void RecordRemoteProgress(long sessionGeneration)
    {
        lock (_gate)
        {
            if (sessionGeneration == _sessionGeneration)
            {
                _remoteSessionRenewalUsedSinceProgress = false;
            }
        }
    }

    /// <summary>Gets the current shared session generation.</summary>
    /// <returns>The current shared session generation.</returns>
    private long GetSharedSessionGeneration()
    {
        lock (_gate)
        {
            return _sessionGeneration;
        }
    }

    /// <summary>Tries to acquire a lease for the current shared session.</summary>
    /// <param name="lease">The acquired shared-session lease.</param>
    /// <returns>Whether the lease was acquired.</returns>
    private bool TryAcquireSharedSessionLease(out SharedSessionLease lease)
    {
        lock (_gate)
        {
            return TryAcquireSharedSessionLeaseLocked(out lease);
        }
    }

    /// <summary>Tries to acquire a lease for the current shared session while the engine lock is held.</summary>
    /// <param name="lease">The acquired shared-session lease.</param>
    /// <returns>Whether the lease was acquired.</returns>
    private bool TryAcquireSharedSessionLeaseLocked(out SharedSessionLease lease)
    {
        if (_admissionState != EngineAdmissionState.Running
            || _disposed
            || _sharedSessionLease is not { } state
            || _session != state.Session)
        {
            lease = default;
            return false;
        }

        state.AddReference();
        lease = new(state.Session, state.Generation, state);
        return true;
    }

    /// <summary>Releases a shared-session lease and disposes a retired generation when the last holder leaves.</summary>
    /// <param name="lease">The shared-session lease.</param>
    /// <returns>The release task.</returns>
    private async ValueTask ReleaseSharedSessionLeaseAsync(SharedSessionLease lease)
    {
        IRemoteTransportSession? sessionToDispose = null;
        lock (_gate)
        {
            if (lease.State.ReleaseReference() && !lease.State.StopOwned)
            {
                _ = _retiredSessionLeases.Remove(lease.State);
                sessionToDispose = lease.Session;
            }
        }

        if (sessionToDispose is not null)
        {
            await DisposeRetiredSharedSessionAsync(sessionToDispose).ConfigureAwait(false);
        }
    }

    /// <summary>Disposes a retired shared session outside the engine lock.</summary>
    /// <param name="session">The retired shared session.</param>
    /// <returns>The disposal task.</returns>
    private async ValueTask DisposeRetiredSharedSessionAsync(IRemoteTransportSession session)
    {
        try
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            PublishFault("OC.Engine.RetiredSessionDisposal", "Disposing a retired transport session failed.", null, exception);
        }
    }

    /// <summary>Tries to renew the shared remote session after the remote reports that the observed generation expired.</summary>
    /// <param name="observedGeneration">The shared session generation that observed expiry.</param>
    /// <param name="cancellationToken">The caller-owned wait cancellation token.</param>
    /// <returns>Whether the caller may retry against a renewed or already replaced generation.</returns>
    private async Task<bool> TryRenewSharedSessionAsync(long observedGeneration, CancellationToken cancellationToken)
    {
        while (true)
        {
            var renewal = CaptureSharedSessionRenewal(observedGeneration);
            if (renewal.ImmediateResult is { } immediateResult)
            {
                return immediateResult;
            }

            if (renewal.RenewalCompletion is not null)
            {
                _ = CompleteSharedSessionRenewalAsync(
                    observedGeneration,
                    renewal.RenewalCompletion,
                    renewal.RenewalCancellationToken);
            }

            if (renewal.RenewalTask is null)
            {
                return false;
            }

            var result = await renewal.RenewalTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            if (renewal.RenewalGeneration == observedGeneration)
            {
                return result.Renewed;
            }
        }
    }

    /// <summary>Captures or registers the shared renewal wait under the engine lock.</summary>
    /// <param name="observedGeneration">The shared session generation that observed expiry.</param>
    /// <returns>The captured renewal wait, or an immediate result.</returns>
    private (
        Task<SessionRenewalResult>? RenewalTask,
        long RenewalGeneration,
        TaskCompletionSource<SessionRenewalResult>? RenewalCompletion,
        CancellationToken RenewalCancellationToken,
        bool? ImmediateResult) CaptureSharedSessionRenewal(long observedGeneration)
    {
        lock (_gate)
        {
            if (_sessionRenewalTask is { } activeRenewalTask)
            {
                return (activeRenewalTask, _sessionRenewalGeneration, null, CancellationToken.None, null);
            }

            if (_admissionState != EngineAdmissionState.Running || _disposed)
            {
                return (null, 0, null, CancellationToken.None, false);
            }

            if (observedGeneration != _sessionGeneration)
            {
                return (null, 0, null, CancellationToken.None, true);
            }

            if (_remoteSessionRenewalUsedSinceProgress)
            {
                return (null, 0, null, CancellationToken.None, false);
            }

            _remoteSessionRenewalUsedSinceProgress = true;
            _sessionRenewalGeneration = observedGeneration;
            var renewalCancellationToken = _uploadCancellation?.Token ?? CancellationToken.None;
            var renewalCompletion = new TaskCompletionSource<SessionRenewalResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var renewalTask = renewalCompletion.Task;
            _sessionRenewalTask = renewalTask;
            return (renewalTask, observedGeneration, renewalCompletion, renewalCancellationToken, null);
        }
    }

    /// <summary>Drives the registered renewal work outside the engine lock.</summary>
    /// <param name="observedGeneration">The shared session generation that observed expiry.</param>
    /// <param name="renewalCompletion">The registered shared completion for waiters.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The renewal result.</returns>
    private async Task<SessionRenewalResult> CompleteSharedSessionRenewalAsync(
        long observedGeneration,
        TaskCompletionSource<SessionRenewalResult> renewalCompletion,
        CancellationToken cancellationToken)
    {
        try
        {
            var result = await RenewSharedSessionAsync(observedGeneration, cancellationToken).ConfigureAwait(false);
            ClearRenewalTask(observedGeneration);
            _ = renewalCompletion.TrySetResult(result);
            return result;
        }
        catch (Exception exception)
        {
            PublishFault("OC.Engine.SessionRenewal", "Driving the shared remote session renewal failed.", null, exception);
            var failed = new SessionRenewalResult(Renewed: false);
            ClearRenewalTask(observedGeneration);
            _ = renewalCompletion.TrySetResult(failed);
            return failed;
        }
    }

    /// <summary>Connects and publishes a replacement shared session for one observed generation.</summary>
    /// <param name="observedGeneration">The shared session generation that observed expiry.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The renewal result.</returns>
    private async Task<SessionRenewalResult> RenewSharedSessionAsync(long observedGeneration, CancellationToken cancellationToken)
    {
        IRemoteTransportSession? newSession = null;
        try
        {
            var requiredGuarantees = await GetRequiredTransportGuaranteesAsync(cancellationToken).ConfigureAwait(false);
            newSession = await ConnectValidatedSessionAsync(requiredGuarantees, cancellationToken).ConfigureAwait(false);
            if (!TryCaptureRenewalValidation(
                    observedGeneration,
                    out var currentSupportsPreparedUpload,
                    out var currentCapabilities)
                || !TryPublishRenewedSession(
                    observedGeneration,
                    newSession,
                    currentSupportsPreparedUpload,
                    currentCapabilities,
                    newSession is IRemoteTransportBatchPreparer,
                    newSession.NegotiatedCapabilities,
                    out var retiredSessionToDispose))
            {
                var rejectedSession = newSession;
                newSession = null;
                await DisposeRejectedReceiveSessionAsync(rejectedSession).ConfigureAwait(false);
                return new(false);
            }

            newSession = null;
            if (retiredSessionToDispose is not null)
            {
                await DisposeRetiredSharedSessionAsync(retiredSessionToDispose).ConfigureAwait(false);
            }

            return new(true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await DisposeRejectedReceiveSessionIfPresentAsync(newSession).ConfigureAwait(false);
            return new(false);
        }
        catch (Exception exception)
        {
            if (newSession is not null)
            {
                await DisposeRejectedReceiveSessionAsync(newSession).ConfigureAwait(false);
            }

            PublishFault("OC.Engine.SessionRenewal", "Renewing the shared remote session failed.", null, exception);
            return new(false);
        }
    }

    /// <summary>Captures current session data needed to validate a renewed session outside the engine lock.</summary>
    /// <param name="observedGeneration">The shared session generation that observed expiry.</param>
    /// <param name="supportsPreparedUpload">Whether the current shared session supports prepared upload.</param>
    /// <param name="capabilities">The current shared session capabilities.</param>
    /// <returns>Whether current session data was captured.</returns>
    private bool TryCaptureRenewalValidation(
        long observedGeneration,
        out bool supportsPreparedUpload,
        out NegotiatedCapabilities capabilities)
    {
        IRemoteTransportSession? currentSession;
        lock (_gate)
        {
            if (_admissionState != EngineAdmissionState.Running
                || _disposed
                || observedGeneration != _sessionGeneration
                || _session is null)
            {
                supportsPreparedUpload = false;
                capabilities = new(new(0, 0), RemoteTransportCapabilities.None, 0, 0, null, null);
                return false;
            }

            currentSession = _session;
        }

        supportsPreparedUpload = currentSession is IRemoteTransportBatchPreparer;
        capabilities = currentSession.NegotiatedCapabilities;
        return true;
    }

    /// <summary>Publishes a renewed shared session when the observed generation is still current.</summary>
    /// <param name="observedGeneration">The shared session generation that observed expiry.</param>
    /// <param name="newSession">The validated replacement session.</param>
    /// <param name="currentSupportsPreparedUpload">Whether the current session supports prepared upload.</param>
    /// <param name="currentCapabilities">The current session capabilities captured outside the engine lock.</param>
    /// <param name="candidateSupportsPreparedUpload">Whether the candidate session supports prepared upload.</param>
    /// <param name="candidateCapabilities">The candidate session capabilities captured outside the engine lock.</param>
    /// <param name="retiredSessionToDispose">The old generation to dispose outside the lock.</param>
    /// <returns>Whether the session became the active shared session.</returns>
    private bool TryPublishRenewedSession(
        long observedGeneration,
        IRemoteTransportSession newSession,
        bool currentSupportsPreparedUpload,
        NegotiatedCapabilities currentCapabilities,
        bool candidateSupportsPreparedUpload,
        NegotiatedCapabilities candidateCapabilities,
        out IRemoteTransportSession? retiredSessionToDispose)
    {
        retiredSessionToDispose = null;
        lock (_gate)
        {
            if (_admissionState != EngineAdmissionState.Running
                || _disposed
                || observedGeneration != _sessionGeneration
                || IsRenewedSessionDowngrade(
                    currentSupportsPreparedUpload,
                    currentCapabilities,
                    candidateSupportsPreparedUpload,
                    candidateCapabilities))
            {
                return false;
            }

            if (_sharedSessionLease is { } oldSession)
            {
                if (oldSession.Retire())
                {
                    retiredSessionToDispose = oldSession.Session;
                }
                else
                {
                    _retiredSessionLeases.Add(oldSession);
                }
            }

            _session = newSession;
            _sessionGeneration++;
            _sharedSessionLease = new(newSession, _sessionGeneration);
            SignalUploadPumpLocked();
            return true;
        }
    }

    /// <summary>Clears a completed renewal task if it still represents the observed generation.</summary>
    /// <param name="observedGeneration">The renewal generation.</param>
    private void ClearRenewalTask(long observedGeneration)
    {
        lock (_gate)
        {
            ClearRenewalTaskLocked(observedGeneration);
        }
    }

    /// <summary>Clears a completed renewal task while the engine gate is held.</summary>
    /// <param name="observedGeneration">The renewal generation.</param>
    private void ClearRenewalTaskLocked(long observedGeneration)
    {
        if (_sessionRenewalGeneration != observedGeneration)
        {
            return;
        }

        _sessionRenewalTask = null;
        _sessionRenewalGeneration = 0;
    }

    /// <summary>Stores an acquired shared-session generation lease.</summary>
    /// <param name="Session">The leased shared session.</param>
    /// <param name="Generation">The leased shared session generation.</param>
    /// <param name="State">The mutable lease state.</param>
    private readonly record struct SharedSessionLease(
        IRemoteTransportSession Session,
        long Generation,
        SharedSessionLeaseState State);

    /// <summary>Tracks references to one shared-session generation.</summary>
    /// <param name="session">The shared session.</param>
    /// <param name="generation">The shared session generation.</param>
    private sealed class SharedSessionLeaseState(IRemoteTransportSession session, long generation)
    {
        /// <summary>Stores the live lease reference count.</summary>
        private int _referenceCount;

        /// <summary>Gets the shared session.</summary>
        internal IRemoteTransportSession Session { get; } = session;

        /// <summary>Gets the shared session generation.</summary>
        internal long Generation { get; } = generation;

        /// <summary>Gets a value indicating whether stop or dispose owns final disposal.</summary>
        internal bool StopOwned { get; private set; }

        /// <summary>Gets or sets a value indicating whether the generation has been retired.</summary>
        private bool IsRetired { get; set; }

        /// <summary>Marks the generation as retired.</summary>
        /// <returns>Whether this generation has no live references.</returns>
        internal bool Retire()
        {
            IsRetired = true;
            return _referenceCount == 0;
        }

        /// <summary>Adds a live reference.</summary>
        internal void AddReference() => _referenceCount++;

        /// <summary>Releases a live reference.</summary>
        /// <returns>Whether this release should dispose the retired session.</returns>
        internal bool ReleaseReference()
        {
            if (_referenceCount > 0)
            {
                _referenceCount--;
            }

            return IsRetired && _referenceCount == 0;
        }

        /// <summary>Marks the generation as owned by stop or dispose cleanup.</summary>
        internal void MarkStopOwned() => StopOwned = true;
    }
}
