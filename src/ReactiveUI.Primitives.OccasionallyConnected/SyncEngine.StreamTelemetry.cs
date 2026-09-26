// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Builds engine and stream diagnostics from one protected lifecycle and queue snapshot.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Creates the current aggregate engine state while the engine gate is held.</summary>
    /// <returns>The current aggregate state.</returns>
    private SyncState CreateAggregateSyncStateLocked()
    {
        long pendingOperations = 0;
        long pendingBytes = 0;
        foreach (var snapshot in _queueDiagnosticSnapshots.Values)
        {
            pendingOperations = checked(pendingOperations + snapshot.PendingOperations);
            pendingBytes = checked(pendingBytes + snapshot.PendingBytes);
        }

        return CreateSyncState(_diagnosticLifecycleStatus, _diagnosticNetworkAvailable, pendingOperations, pendingBytes);
    }

    /// <summary>Creates one stream state while the engine gate is held.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="registration">The registered participant.</param>
    /// <returns>The stream-owned state.</returns>
    private SyncState CreateStreamSyncStateLocked(StreamId streamId, ParticipantRegistration registration)
    {
        _ = _queueDiagnosticSnapshots.TryGetValue(streamId, out var queue);
        var status = registration.RemoteActive ? _diagnosticLifecycleStatus : SyncLifecycleStatus.Stopped;
        var networkAvailable = registration.RemoteActive && _diagnosticNetworkAvailable;
        return CreateSyncState(status, networkAvailable, queue.PendingOperations, queue.PendingBytes);
    }

    /// <summary>Creates one synchronization state from captured diagnostic values.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <param name="networkAvailable">Whether the network path is available.</param>
    /// <param name="pendingOperations">The pending operation count.</param>
    /// <param name="pendingBytes">The pending byte count.</param>
    /// <returns>The immutable synchronization state.</returns>
    private SyncState CreateSyncState(SyncLifecycleStatus status, bool networkAvailable, long pendingOperations, long pendingBytes) =>
        new(status, networkAvailable, checked((int)pendingOperations), pendingBytes, _options.TimeProvider.GetUtcNow(), null, _diagnosticRetryAfter, _diagnosticReasonCode);

    /// <summary>Captures all registered stream states while the engine gate is held.</summary>
    /// <returns>The captured stream states and sinks.</returns>
    private (IOccasionallyConnectedStreamDiagnosticsSink Sink, SyncState State)[] CaptureStreamSyncStatesLocked()
    {
        var states = new List<(IOccasionallyConnectedStreamDiagnosticsSink Sink, SyncState State)>(_participants.Count);
        foreach (var pair in _participants)
        {
            if (pair.Value.Participant is IOccasionallyConnectedStreamDiagnosticsSink sink)
            {
                states.Add((sink, CreateStreamSyncStateLocked(pair.Key, pair.Value)));
            }
        }

        return [.. states];
    }

    /// <summary>Publishes current queue diagnostics after a committed queue mutation.</summary>
    /// <param name="streamId">The stream whose queue changed.</param>
    private void PublishQueueSyncState(StreamId streamId)
    {
        SyncState aggregate;
        SyncState? stream = null;
        IOccasionallyConnectedStreamDiagnosticsSink? sink = null;
        long revision;
        lock (_gate)
        {
            if (!_participants.TryGetValue(streamId, out var registration))
            {
                return;
            }

            revision = ++_diagnosticRevision;
            aggregate = CreateAggregateSyncStateLocked();
            if (registration.Participant is IOccasionallyConnectedStreamDiagnosticsSink diagnosticsSink)
            {
                sink = diagnosticsSink;
                stream = CreateStreamSyncStateLocked(streamId, registration);
            }
        }

        PublishGlobalSyncState(aggregate, revision);
        if (sink is not null && stream is not null)
        {
            PublishStreamDiagnostic(sink, stream, revision);
        }
    }

    /// <summary>Publishes the remaining aggregate after a stream is unregistered.</summary>
    private void PublishCurrentAggregateSyncState()
    {
        SyncState state;
        long revision;
        lock (_gate)
        {
            revision = ++_diagnosticRevision;
            state = CreateAggregateSyncStateLocked();
        }

        PublishGlobalSyncState(state, revision);
    }

    /// <summary>Publishes one stream's current lifecycle state outside the engine gate.</summary>
    /// <param name="streamId">The stream identity.</param>
    private void PublishStreamSyncState(StreamId streamId)
    {
        SyncState state;
        IOccasionallyConnectedStreamDiagnosticsSink sink;
        long revision;
        lock (_gate)
        {
            if (!_participants.TryGetValue(streamId, out var registration)
                || registration.Participant is not IOccasionallyConnectedStreamDiagnosticsSink diagnosticsSink)
            {
                return;
            }

            revision = ++_diagnosticRevision;
            sink = diagnosticsSink;
            state = CreateStreamSyncStateLocked(streamId, registration);
        }

        PublishStreamDiagnostic(sink, state, revision);
    }

    /// <summary>Delivers one stream diagnostic without affecting the durable operation that produced it.</summary>
    /// <param name="sink">The stream diagnostic sink.</param>
    /// <param name="state">The captured stream state.</param>
    /// <param name="revision">The captured engine diagnostic revision.</param>
    private void PublishStreamDiagnostic(IOccasionallyConnectedStreamDiagnosticsSink sink, SyncState state, long revision)
    {
        try
        {
            sink.PublishSyncState(state, revision);
        }
        catch (Exception exception)
        {
            try
            {
                PublishFault("OC.Engine.StreamSyncState", "A stream synchronization state notification failed.", null, exception);
            }
            catch (Exception faultObserverException)
            {
                _ = faultObserverException;
            }
        }
    }

    /// <summary>Queues the latest global state for serialized delivery outside the engine gate.</summary>
    /// <param name="state">The captured state.</param>
    /// <param name="revision">The captured diagnostic revision.</param>
    private void PublishGlobalSyncState(SyncState state, long revision)
    {
        var startWorker = false;
        lock (_gate)
        {
            if (revision <= _latestGlobalSyncRevision)
            {
                return;
            }

            _latestGlobalSyncRevision = revision;
            _pendingGlobalSyncState = state;
            if (!_globalSyncDeliveryActive)
            {
                _globalSyncDeliveryActive = true;
                startWorker = true;
            }
        }

        if (startWorker)
        {
            // Even an inline public sequencer cannot invoke observers on the mutation caller's stack.
            _ = Task.Run(ScheduleGlobalSyncStateDrain);
        }
    }

    /// <summary>Hands the single diagnostic drain to the configured observer scheduler.</summary>
    private void ScheduleGlobalSyncStateDrain()
    {
        try
        {
            _options.NotificationScheduler.Schedule(new GlobalSyncStateDrainWorkItem(this));
        }
        catch (Exception exception)
        {
            lock (_gate)
            {
                // Keep the latest pending snapshot so a later mutation can retry scheduling.
                _globalSyncDeliveryActive = false;
            }

            ReportGlobalSyncDeliveryFault(exception);
        }
    }

    /// <summary>Delivers captured global states in revision order, coalescing only pending intermediate states.</summary>
    private void DrainGlobalSyncStates()
    {
        while (true)
        {
            SyncState state;
            lock (_gate)
            {
                if (_pendingGlobalSyncState is not { } pending)
                {
                    _globalSyncDeliveryActive = false;
                    return;
                }

                state = pending;
                _pendingGlobalSyncState = null;
            }

            try
            {
                _syncStates.Publish(state);
            }
            catch (Exception exception)
            {
                ReportGlobalSyncDeliveryFault(exception);
            }
        }
    }

    /// <summary>Reports a global diagnostic delivery failure without affecting engine work.</summary>
    /// <param name="exception">The observer or worker failure.</param>
    private void ReportGlobalSyncDeliveryFault(Exception exception)
    {
        try
        {
            PublishFault("OC.Engine.SyncStateObserver", "A synchronization state observer failed.", null, exception);
        }
        catch (Exception faultObserverException)
        {
            _ = faultObserverException;
        }
    }

    /// <summary>Runs the bounded global delivery loop on the configured scheduler.</summary>
    /// <param name="owner">The owning engine.</param>
    private sealed class GlobalSyncStateDrainWorkItem(SyncEngine owner) : IWorkItem
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => owner.DrainGlobalSyncStates();
    }
}
