// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Observes stop intent while forwarding recovery stream coordination to the real engine.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Signals when a typed stream sends stop intent to its coordinator.</summary>
    /// <param name="inner">The real engine coordinator.</param>
    private sealed class StopObservingStreamCoordinator(IOccasionallyConnectedStreamCoordinator inner) : IOccasionallyConnectedStreamCoordinator
    {
        /// <summary>Gets the stop-entry signal.</summary>
        public TaskCompletionSource StopEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable RegisterParticipant(IOccasionallyConnectedStreamParticipant participant) =>
            inner.RegisterParticipant(participant);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> EnsureSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            inner.EnsureSubscriptionIdAsync(streamId, preferredId, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<LocalCommitAdmission> EnterLocalCommitAsync(
            StreamId streamId,
            long retainedBytes,
            CancellationToken cancellationToken) =>
            inner.EnterLocalCommitAsync(streamId, retainedBytes, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompleteLocalCommit(LocalCommitAdmission admission) => inner.CompleteLocalCommit(admission);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetCapacityReleaseGeneration(StreamId streamId) => inner.GetCapacityReleaseGeneration(streamId);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask WaitForCapacityReleaseAsync(
            StreamId streamId,
            long observedGeneration,
            long retainedBytes,
            CancellationToken cancellationToken) =>
            inner.WaitForCapacityReleaseAsync(streamId, observedGeneration, retainedBytes, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartStreamAsync(StreamId streamId, CancellationToken cancellationToken) =>
            inner.StartStreamAsync(streamId, cancellationToken);

        /// <inheritdoc/>
        public ValueTask StopStreamAsync(StreamId streamId, CancellationToken cancellationToken)
        {
            var stop = inner.StopStreamAsync(streamId, cancellationToken);
            _ = StopEntered.TrySetResult();
            return stop;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordRecoveredQueueAggregate(StreamId streamId, QueueDiagnosticSnapshot snapshot) =>
            inner.RecordRecoveredQueueAggregate(streamId, snapshot);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void NotifyRecoveredLocalWorkReady(StreamId streamId, int priority, DateTimeOffset? notBeforeUtc) =>
            inner.NotifyRecoveredLocalWorkReady(streamId, priority, notBeforeUtc);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RecordSavedLocalCommit(
            StreamId streamId,
            SyncOperation operation,
            QueueDiagnosticSnapshot snapshot,
            PublishReceipt receipt) =>
            inner.RecordSavedLocalCommit(streamId, operation, snapshot, receipt);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void NotifyLocalCommitReady(StreamId streamId, SyncOperation operation) =>
            inner.NotifyLocalCommitReady(streamId, operation);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void NotifyCapacityReleased(StreamId streamId) => inner.NotifyCapacityReleased(streamId);
    }
}
