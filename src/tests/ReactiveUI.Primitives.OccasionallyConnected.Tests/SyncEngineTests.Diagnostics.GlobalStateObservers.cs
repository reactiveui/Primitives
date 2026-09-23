// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Observer fixtures for global synchronization state ordering.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Rejects global state delivery to exercise isolation in the serialized worker.</summary>
    private sealed class ThrowingGlobalSyncStateObserver : IObserver<SyncState>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        public void OnNext(SyncState value)
        {
            _ = value;
            throw new InvalidOperationException("Global state observer failure.");
        }
    }

    /// <summary>Delegates durable work while deliberately rejecting stream diagnostic delivery.</summary>
    private sealed class ThrowingStreamDiagnosticParticipant : IOccasionallyConnectedStreamParticipant, IOccasionallyConnectedStreamDiagnosticsSink
    {
        /// <summary>The durable participant used by the test.</summary>
        private readonly RecordingParticipant _inner = new();

        /// <inheritdoc />
        public StreamId StreamId => _inner.StreamId;

        /// <summary>Gets the number of attempted notifications.</summary>
        public int NotificationCount { get; private set; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ReceiveStreamSubscription?> PrepareReceiveAsync(CancellationToken cancellationToken) =>
            _inner.PrepareReceiveAsync(cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> CommitSerializedAsync(SyncOperation operation, CancellationToken cancellationToken) =>
            _inner.CommitSerializedAsync(operation, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantQueueTransitionResult> ApplySyncResultAsync(SyncBatch batch, RemoteSyncResult result, CancellationToken cancellationToken) =>
            _inner.ApplySyncResultAsync(batch, result, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantRemoteApplyResult> ApplyRemoteBatchAsync(RemoteEventBatch batch, CancellationToken cancellationToken) =>
            _inner.ApplyRemoteBatchAsync(batch, cancellationToken);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ParticipantQueueTransitionResult> DeadLetterOperationAsync(Guid leaseId, OperationId operationId, string reasonCode, CancellationToken cancellationToken) =>
            _inner.DeadLetterOperationAsync(leaseId, operationId, reasonCode, cancellationToken);

        /// <inheritdoc />
        public void PublishSyncState(SyncState state, long revision)
        {
            _ = state;
            _ = revision;
            NotificationCount++;
            throw new InvalidOperationException("Stream diagnostic observer failure.");
        }
    }

    /// <summary>Rejects fault delivery so the engine can prove its secondary diagnostic isolation.</summary>
    private sealed class ThrowingFaultObserver : IObserver<OccasionallyConnectedFault>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        public void OnNext(OccasionallyConnectedFault value)
        {
            _ = value;
            throw new InvalidOperationException("Fault observer failure.");
        }
    }

    /// <summary>Records states while reentering the engine during the first callback.</summary>
    /// <param name="onFirst">The first-callback action.</param>
    private sealed class ReentrantGlobalSyncStateObserver(Action onFirst) : IObserver<SyncState>
    {
        /// <summary>Tracks callback count.</summary>
        private int _callbacks;

        /// <summary>Gets callback completion order.</summary>
        public ConcurrentQueue<int> PendingCounts { get; } = new();

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        public void OnNext(SyncState value)
        {
            if (Interlocked.Increment(ref _callbacks) == 1)
            {
                onFirst();
            }

            PendingCounts.Enqueue(value.PendingOperations);
        }
    }

    /// <summary>Blocks the first global observer callback until a newer queue revision is captured.</summary>
    private sealed class BlockingGlobalSyncStateObserver : IObserver<SyncState>
    {
        /// <summary>Tracks the number of callbacks.</summary>
        private int _callbacks;

        /// <summary>Gets the first callback entry signal.</summary>
        public TaskCompletionSource FirstEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the first callback release signal.</summary>
        public TaskCompletionSource ReleaseFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets observed pending counts in callback completion order.</summary>
        public ConcurrentQueue<int> PendingCounts { get; } = new();

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        public void OnNext(SyncState value)
        {
            if (Interlocked.Increment(ref _callbacks) == 1)
            {
                _ = FirstEntered.TrySetResult();
                ReleaseFirst.Task.GetAwaiter().GetResult();
            }

            PendingCounts.Enqueue(value.PendingOperations);
        }
    }
}
