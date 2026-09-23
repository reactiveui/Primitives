// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

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

            _session = session;
            _sessionGeneration++;
            _sharedSessionLease = new(session, _sessionGeneration);
            _remoteSessionRenewalUsedSinceProgress = false;
            uploadCancellation = new();
            _uploadCancellation = uploadCancellation;
            _admissionState = EngineAdmissionState.Running;
            ScheduleDeferredUploadHeadsLocked();
            return true;
        }
    }

    /// <summary>Starts shared store and transport state.</summary>
    /// <param name="cancellation">The engine-owned startup cancellation.</param>
    /// <returns>The startup task.</returns>
    /// <exception cref="ObjectDisposedException">The engine is disposed before startup can complete.</exception>
    /// <exception cref="OperationCanceledException">An accepted stop superseded startup.</exception>
    private async ValueTask StartWithCancellationAsync(StartupCancellationOwner cancellation)
    {
        await EnsureStoreInitializedAsync(CancellationToken.None).ConfigureAwait(false);
        var requiredGuarantees = await GetRequiredTransportGuaranteesAsync(cancellation.Token).ConfigureAwait(false);
        var session = await ConnectValidatedSessionAsync(requiredGuarantees, cancellation.Token).ConfigureAwait(false);

        if (!TryPublishStartedSession(session, cancellation, out var uploadCancellation))
        {
            await session.DisposeAsync().ConfigureAwait(false);
            ThrowIfDisposed();
            throw new OperationCanceledException("Startup was superseded by an accepted stop.", cancellation.Token);
        }

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
