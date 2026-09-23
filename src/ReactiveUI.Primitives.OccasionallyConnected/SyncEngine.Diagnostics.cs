// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Diagnostics helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Validates a queue diagnostic snapshot.</summary>
    /// <param name="snapshot">The snapshot.</param>
    /// <exception cref="ArgumentOutOfRangeException">The snapshot contains a negative value.</exception>
    private static void ValidateQueueSnapshot(QueueDiagnosticSnapshot snapshot)
    {
        if (snapshot.PendingOperations < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(snapshot), snapshot.PendingOperations, "Pending operation count must not be negative.");
        }

        if (snapshot.PendingBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(snapshot), snapshot.PendingBytes, "Pending byte count must not be negative.");
        }

        if (snapshot.Revision >= 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(snapshot), snapshot.Revision, "Queue diagnostic revision must not be negative.");
    }

    /// <summary>Starts a diagnostics activity without allowing listener failures to affect engine work.</summary>
    /// <param name="activityName">The activity name.</param>
    /// <returns>The started activity, or null when diagnostics are disabled or rejected.</returns>
    private SafeDiagnosticActivity? StartDiagnosticActivity(OccasionallyConnectedActivityName activityName)
    {
        try
        {
            var activity = _activities.Start(activityName);
            return activity is null ? null : new SafeDiagnosticActivity(activity);
        }
        catch (Exception exception)
        {
            _ = exception;
            return null;
        }
    }

    /// <summary>Gets a diagnostic timestamp from the injected time provider.</summary>
    /// <returns>The current monotonic timestamp.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetDiagnosticTimestamp() => _options.TimeProvider.GetTimestamp();

    /// <summary>Gets elapsed diagnostic time from the injected time provider.</summary>
    /// <param name="startedTimestamp">The monotonic start timestamp.</param>
    /// <returns>The elapsed time.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private TimeSpan GetDiagnosticElapsed(long startedTimestamp) =>
        _options.TimeProvider.GetElapsedTime(startedTimestamp);

    /// <summary>Records a local operation publication.</summary>
    private void RecordOperationPublished()
    {
        try
        {
            _metrics.RecordOperationPublished();
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records a rejected operation.</summary>
    private void RecordOperationRejected()
    {
        try
        {
            _metrics.RecordOperationRejected();
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records queue diagnostics.</summary>
    /// <param name="pendingDelta">The pending operation delta.</param>
    /// <param name="byteDelta">The retained byte delta.</param>
    private void RecordQueueDiagnostics(long pendingDelta, long byteDelta)
    {
        try
        {
            _metrics.RecordQueuePending(pendingDelta);
            _metrics.RecordQueueBytes(byteDelta);
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records reconciled remote upload results.</summary>
    /// <param name="result">The durable remote result.</param>
    private void RecordUploadResultMetrics(RemoteSyncResult result)
    {
        try
        {
            var synchronized = 0;
            var rejected = 0;
            var conflicts = 0;
            for (var i = 0; i < result.Operations.Count; i++)
            {
                var operationResult = result.Operations[i];
                switch (operationResult.Kind)
                {
                    case OperationResultKind.Accepted:
                    {
                        synchronized++;
                        break;
                    }

                    case OperationResultKind.Rejected:
                    {
                        rejected++;
                        break;
                    }

                    case OperationResultKind.Conflict:
                    {
                        conflicts++;
                        break;
                    }

                    default:
                    {
                        break;
                    }
                }
            }

            _metrics.RecordOperationSynchronized(synchronized);
            _metrics.RecordOperationRejected(rejected);
            _metrics.RecordConflict(conflicts);
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records a retry decision.</summary>
    /// <param name="count">The retry count.</param>
    private void RecordRetry(long count = 1)
    {
        try
        {
            _metrics.RecordRetry(count);
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records duplicate remote receive events.</summary>
    /// <param name="count">The duplicate count.</param>
    private void RecordDuplicate(long count)
    {
        try
        {
            _metrics.RecordDuplicate(count);
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records a synchronization batch size.</summary>
    /// <param name="count">The batch operation or event count.</param>
    private void RecordSyncBatchSize(long count)
    {
        try
        {
            _metrics.RecordSyncBatchSize(count);
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records synchronization duration.</summary>
    /// <param name="startedTimestamp">The monotonic start timestamp.</param>
    private void RecordSyncDuration(long startedTimestamp)
    {
        try
        {
            _metrics.RecordSyncDuration(GetDiagnosticElapsed(startedTimestamp));
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records store commit duration.</summary>
    /// <param name="startedTimestamp">The monotonic start timestamp.</param>
    private void RecordStoreCommitDuration(long startedTimestamp)
    {
        try
        {
            _metrics.RecordStoreCommitDuration(GetDiagnosticElapsed(startedTimestamp));
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records a connection state transition.</summary>
    private void RecordConnectionStateChange()
    {
        try
        {
            _metrics.RecordConnectionStateChange();
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Records dead-lettered operations.</summary>
    /// <param name="count">The number of operations.</param>
    private void RecordDeadLetter(long count = 1)
    {
        try
        {
            _metrics.RecordDeadLetter(count);
        }
        catch (Exception exception)
        {
            _ = exception;
        }
    }

    /// <summary>Disposes a diagnostic activity without allowing listener failures to affect engine work.</summary>
    /// <param name="activity">The started activity.</param>
    private sealed class SafeDiagnosticActivity(IDisposable activity) : IDisposable
    {
        /// <summary>Tracks whether the activity has been disposed.</summary>
        private int _disposed;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                activity.Dispose();
            }
            catch (Exception exception)
            {
                _ = exception;
            }
        }
    }
}
