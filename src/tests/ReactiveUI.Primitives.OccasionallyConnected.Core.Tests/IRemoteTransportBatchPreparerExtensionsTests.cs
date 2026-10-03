// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IRemoteTransportBatchPreparerExtensions"/>.</summary>
public sealed class IRemoteTransportBatchPreparerExtensionsTests
{
    /// <summary>Verifies the prepare overload forwards the batch with <see cref="CancellationToken.None"/>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PreparePushAsyncForwardsBatchWithNoneToken()
    {
        var batch = new SyncBatch(Guid.NewGuid(), [CreateOperation()]);
        var prepared = new Prepared(batch);
        var preparer = new Preparer(prepared);

        var actual = await preparer.PreparePushAsync(batch);

        await Assert.That(actual).IsSameReferenceAs(prepared);
        await Assert.That(preparer.Batch).IsSameReferenceAs(batch);
        await Assert.That(preparer.CancellationToken).IsEqualTo(CancellationToken.None);
    }

    /// <summary>Verifies the prepared send overload forwards <see cref="CancellationToken.None"/>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SendAsyncForwardsNoneToken()
    {
        var batch = new SyncBatch(Guid.NewGuid(), [CreateOperation()]);
        var prepared = new Prepared(batch);

        var result = await prepared.SendAsync();

        await Assert.That(result.BatchId).IsEqualTo(batch.BatchId);
        await Assert.That(prepared.CancellationToken).IsEqualTo(CancellationToken.None);
        await Assert.That(prepared.SendCalls).IsEqualTo(1);
    }

    /// <summary>Creates a synchronization operation for forwarding assertions.</summary>
    /// <returns>A synchronization operation.</returns>
    private static SyncOperation CreateOperation() =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = new("stream"),
            ClientSequence = 1,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Append,
            Payload = new("contract", 1, "json", ReadOnlyMemory<byte>.Empty, "hash"),
            Policy = OperationPolicy.Default,
            Metadata = new Dictionary<string, string>(),
        };

    /// <summary>Records batch preparation calls.</summary>
    /// <param name="prepared">The prepared handle to return.</param>
    private sealed class Preparer(IPreparedRemotePush prepared) : IRemoteTransportBatchPreparer
    {
        /// <summary>Gets the batch supplied by the extension method.</summary>
        public SyncBatch? Batch { get; private set; }

        /// <summary>Gets the cancellation token supplied by the extension method.</summary>
        public CancellationToken CancellationToken { get; private set; }

        /// <inheritdoc/>
        public ValueTask<IPreparedRemotePush> PreparePushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            Batch = batch;
            CancellationToken = cancellationToken;
            return new(prepared);
        }
    }

    /// <summary>Records prepared push send calls.</summary>
    /// <param name="batch">The prepared batch.</param>
    private sealed class Prepared(SyncBatch batch) : IPreparedRemotePush
    {
        /// <inheritdoc/>
        public SyncBatch Batch { get; } = batch;

        /// <inheritdoc/>
        public long EncodedSizeBytes => 1;

        /// <summary>Gets the cancellation token supplied by the extension method.</summary>
        public CancellationToken CancellationToken { get; private set; }

        /// <summary>Gets the send call count.</summary>
        public int SendCalls { get; private set; }

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> SendAsync(CancellationToken cancellationToken)
        {
            SendCalls++;
            CancellationToken = cancellationToken;
            return new(new RemoteSyncResult(Batch.BatchId, [], null, null));
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
