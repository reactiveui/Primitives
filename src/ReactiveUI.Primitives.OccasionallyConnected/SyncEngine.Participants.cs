// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Participant registration helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Creates a stop wait task that joins receive completion and cancellation callback drain.</summary>
    /// <param name="receiveTask">The receive pump task for the stopped generation.</param>
    /// <param name="cancellationTask">The cancellation callback drain task.</param>
    /// <param name="completionTask">The cancellation completion driver task.</param>
    /// <returns>The joined stop wait task, if there is asynchronous stop work.</returns>
    private static Task? CreateReceiveStopTask(Task? receiveTask, Task? cancellationTask, Task? completionTask)
    {
        var stopTask = receiveTask;
        if (cancellationTask is not null)
        {
            stopTask = stopTask is null ? cancellationTask : Task.WhenAll(stopTask, cancellationTask);
        }

        if (completionTask is null)
        {
            return stopTask;
        }

        return stopTask is null ? completionTask : Task.WhenAll(stopTask, completionTask);
    }

    /// <summary>Starts remote work for one registered stream without changing global lifecycle state.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private void StartStream(StreamId streamId, CancellationToken cancellationToken)
    {
        CancellationTokenSource? previousCancellation;
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            var registration = GetParticipantLocked(streamId);
            if (!registration.RemoteActive)
            {
                registration.RemoteActive = true;
            }

            ScheduleDeferredUploadHeadLocked(streamId);
            previousCancellation = StartReceivePumpForRegistrationLocked(registration);
        }

        previousCancellation?.Dispose();
        PublishStreamSyncState(streamId);
    }

    /// <summary>Stops remote work for one registered stream without changing global lifecycle state.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The receive-pump stop wait task, if an active pump was canceled.</returns>
    private Task? StopStream(StreamId streamId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ParticipantRegistration registration;
        Task? stopTask;
        TaskCompletionSource<bool>? cancellationCompletion = null;
        (
            long Generation,
            CancellationTokenSource Source,
            TaskCompletionSource<bool> DrainCompletion)? receiveCancellation;
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            registration = GetParticipantLocked(streamId);
            if (registration.RemoteActive)
            {
                registration.RemoteActive = false;
                registration.ReceiveRestartRequested = false;
                if (_admissionState == EngineAdmissionState.Stopping && _stopTask is { IsCompleted: false } acceptedStop)
                {
                    stopTask = acceptedStop;
                    receiveCancellation = null;
                }
                else
                {
                    receiveCancellation = registration.BeginCancelReceive(out var cancellationDrainTask);
                    if (receiveCancellation is not null)
                    {
                        cancellationCompletion = CreateCompletion();
                    }

                    stopTask = CreateReceiveStopTask(registration.ReceiveTask, cancellationDrainTask, cancellationCompletion?.Task);
                    if (stopTask is not null)
                    {
                        RegisterReceiveStopTaskLocked(stopTask);
                    }
                }

                registration.StopReceiveTask = stopTask;
                ParkStreamUploadLocked(streamId);
            }
            else
            {
                stopTask = registration.StopReceiveTask;
                receiveCancellation = null;
            }
        }

        if (receiveCancellation is not null && cancellationCompletion is not null)
        {
            var cancellationLease = receiveCancellation.Value;
            _ = Task.Run(
                () => CompleteReceiveCancellationDriver(registration, cancellationLease, cancellationCompletion),
                CancellationToken.None);
        }

        PublishStreamSyncState(streamId);
        return stopTask;
    }

    /// <summary>Completes receive cancellation outside the caller path and publishes any cancellation failure.</summary>
    /// <param name="registration">The registration whose receive work is being canceled.</param>
    /// <param name="receiveCancellation">The captured receive cancellation ownership.</param>
    /// <param name="completion">The completion joined by stop waiters.</param>
    private void CompleteReceiveCancellationDriver(
        ParticipantRegistration registration,
        (
            long Generation,
            CancellationTokenSource Source,
            TaskCompletionSource<bool> DrainCompletion) receiveCancellation,
        TaskCompletionSource<bool> completion)
    {
        try
        {
            var failure = registration.CompleteCancelReceive(receiveCancellation);
            ReconcilePendingReceiveRestartAfterCancellation(registration);
            if (failure is not null)
            {
                PublishReceivePumpFault(registration.Participant, failure);
            }

            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
        }
    }

    /// <summary>Tracks a receive stop task until it finishes or global cleanup takes ownership.</summary>
    /// <param name="task">The receive stop task.</param>
    private void RegisterReceiveStopTaskLocked(Task task)
    {
        _receiveStopTasks.Add(task);
        _ = task.ContinueWith(
            static (completed, state) =>
            {
                ArgumentExceptionHelper.ThrowIfNull(state);
                ((SyncEngine)state).ObserveCompletedReceiveStop(completed);
            },
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>Observes and removes a completed receive stop task.</summary>
    /// <param name="task">The completed receive stop task.</param>
    private void ObserveCompletedReceiveStop(Task task)
    {
        _ = task.Exception;
        lock (_gate)
        {
            _ = _receiveStopTasks.Remove(task);
        }
    }

    /// <summary>Determines whether a stream remains registered and active while the engine lock is held.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns><see langword="true"/> when the stream has active remote work.</returns>
    private bool IsStreamRemoteActiveLocked(StreamId streamId) =>
        _participants.TryGetValue(streamId, out var registration) && registration.RemoteActive;

    /// <summary>Unregisters a participant.</summary>
    /// <param name="registration">The participant registration.</param>
    private void Unregister(ParticipantRegistration registration)
    {
        CapacityWaiter[] waiters;
        QueueDiagnosticSnapshot queueSnapshot = default;
        var hasQueueSnapshot = false;
        var receivePumpActive = false;
        lock (_gate)
        {
            var streamId = registration.Participant.StreamId;

            // The registration owns the sole removal and admits Dispose exactly once.
            _ = _participants.Remove(streamId);
            var signal = _capacitySignals[streamId];
            waiters = signal.TakeWaiters();
            _ = _capacitySignals.Remove(streamId);
            _ = _scheduler.Remove(streamId);
            _ = _scheduledStreams.Remove(streamId);
            _ = _rescheduleStreams.Remove(streamId);
            _ = _uploadHeads.Remove(streamId);
            _ = _deferredUploadHeads.Remove(streamId);
            receivePumpActive = _receivePumpStreams.Remove(streamId);
            if (_queueDiagnosticSnapshots.TryGetValue(streamId, out queueSnapshot))
            {
                hasQueueSnapshot = true;
                _ = _queueDiagnosticSnapshots.Remove(streamId);
            }

            TryCompleteSyncCycleLocked();
        }

        var receiveCancellationFailure = registration.CancelReceive();
        if (!receivePumpActive)
        {
            registration.DisposeReceiveCancellation();
        }

        if (receiveCancellationFailure is not null)
        {
            PublishReceivePumpFault(registration.Participant, receiveCancellationFailure);
        }

        if (hasQueueSnapshot)
        {
            RecordQueueDiagnostics(-queueSnapshot.PendingOperations, -queueSnapshot.PendingBytes);
            PublishCurrentAggregateSyncState();
        }

        var exception = new ObjectDisposedException(nameof(IOccasionallyConnectedStreamParticipant));
        for (var i = 0; i < waiters.Length; i++)
        {
            waiters[i].Release(exception);
        }
    }

    /// <summary>Throws when the engine is disposed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

    /// <summary>Throws when the engine is disposed while the lock is held.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposedLocked() => ThrowIfDisposed();
}
