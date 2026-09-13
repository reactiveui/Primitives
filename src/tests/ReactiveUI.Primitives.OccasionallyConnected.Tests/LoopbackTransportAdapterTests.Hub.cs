// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LoopbackTransportAdapter"/>.</summary>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>Records loopback hub calls.</summary>
    private sealed class RecordingHub : IServerStreamHub
    {
        /// <summary>The operation identifiers already applied by the simulated server.</summary>
        private readonly HashSet<OperationId> _appliedOperations = [];

        /// <summary>Gets the applied batches.</summary>
        public List<SyncBatch> ApplyBatches { get; } = [];

        /// <summary>Gets or sets a custom apply handler.</summary>
        public Func<SyncBatch, ClientIdentity, CancellationToken, ValueTask<ServerSyncResult>>? ApplyHandler { get; init; }

        /// <summary>Gets or sets a custom subscribe handler.</summary>
        public Func<RemoteSubscribeRequest, ClientIdentity, CancellationToken, IAsyncEnumerable<RemoteEventBatch>>? SubscribeHandler { get; init; }

        /// <summary>Gets or sets a value indicating whether the next apply response is dropped.</summary>
        public bool DropNextApplyResponse { get; set; }

        /// <summary>Gets the apply call count.</summary>
        public int ApplyCalls { get; private set; }

        /// <summary>Gets the subscribe call count.</summary>
        public int SubscribeCalls { get; private set; }

        /// <summary>Gets the acknowledge call count.</summary>
        public int AcknowledgeCalls { get; private set; }

        /// <summary>Gets the unique simulated server effects.</summary>
        public int UniqueServerEffects { get; private set; }

        /// <summary>Gets the last applied batch.</summary>
        public SyncBatch? ApplyBatch { get; private set; }

        /// <summary>Gets the last apply client identity.</summary>
        public ClientIdentity? ApplyClient { get; private set; }

        /// <summary>Gets the last subscribe client identity.</summary>
        public ClientIdentity? SubscribeClient { get; private set; }

        /// <summary>Gets the last acknowledgement.</summary>
        public ReceiveAcknowledgement? Acknowledgement { get; private set; }

        /// <summary>Gets the last acknowledgement client identity.</summary>
        public ClientIdentity? AcknowledgeClient { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(
            SyncBatch batch,
            ClientIdentity client,
            CancellationToken cancellationToken)
        {
            ApplyCalls++;
            ApplyBatch = batch;
            ApplyClient = client;
            ApplyBatches.Add(batch);
            for (var index = 0; index < batch.Operations.Count; index++)
            {
                if (_appliedOperations.Add(batch.Operations[index].OperationId))
                {
                    UniqueServerEffects++;
                }
            }

            if (DropNextApplyResponse)
            {
                DropNextApplyResponse = false;
                return ValueTask.FromException<ServerSyncResult>(new InvalidOperationException("response lost"));
            }

            return ApplyHandler is null
                ? ValueTask.FromResult(new ServerSyncResult(CreateResult(batch), []))
                : ApplyHandler(batch, client, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            ClientIdentity client,
            CancellationToken cancellationToken)
        {
            AcknowledgeCalls++;
            Acknowledgement = acknowledgement;
            AcknowledgeClient = client;
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(
            RemoteSubscribeRequest request,
            ClientIdentity client,
            CancellationToken cancellationToken)
        {
            SubscribeCalls++;
            SubscribeClient = client;
            return SubscribeHandler is null ? YieldBatches() : SubscribeHandler(request, client, cancellationToken);
        }
    }

    /// <summary>Provides a sequence whose enumerator faults during disposal.</summary>
    /// <param name="batch">The batch to yield.</param>
    private sealed class ThrowingDisposeEnumerable(RemoteEventBatch batch) : IAsyncEnumerable<RemoteEventBatch>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IAsyncEnumerator<RemoteEventBatch> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            new ThrowingDisposeEnumerator(batch);
    }

    /// <summary>Yields one batch and faults during disposal.</summary>
    /// <param name="batch">The batch to yield.</param>
    private sealed class ThrowingDisposeEnumerator(RemoteEventBatch batch) : IAsyncEnumerator<RemoteEventBatch>
    {
        /// <summary>Whether the batch has been yielded.</summary>
        private int _moved;

        /// <inheritdoc/>
        public RemoteEventBatch Current => batch;

        /// <inheritdoc/>
        public ValueTask<bool> MoveNextAsync() =>
            Interlocked.Exchange(ref _moved, 1) == 0 ? ValueTask.FromResult(true) : ValueTask.FromResult(false);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() =>
            ValueTask.FromException(new InvalidOperationException("dispose failed"));
    }

    /// <summary>Stores state for a cancellation callback re-entry test.</summary>
    /// <param name="Session">The session captured by the callback.</param>
    /// <param name="Acknowledgement">The acknowledgement used by the callback.</param>
    /// <param name="Completed">Signals callback completion.</param>
    private sealed record ReentrantAcknowledgeContext(
        IRemoteTransportSession? Session,
        ReceiveAcknowledgement Acknowledgement,
        TaskCompletionSource Completed);
}
