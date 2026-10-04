// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackPushVerifier"/>.</summary>
public sealed class CrdtLoopbackPushVerifierTests
{
    /// <summary>The test client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The test reason code.</summary>
    private const string ReasonCode = "server-busy";

    /// <summary>The test server version.</summary>
    private const string ServerVersion = "server-v1";

    /// <summary>The test stream identifier.</summary>
    private static readonly StreamId Stream = new("resilience/pusher");

    /// <summary>Verifies conflict results are terminal and remain accepted by the lab pusher.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushAcceptedReturnsConflictResult()
    {
        var batch = CreateBatch();
        var result = CreateResult(batch, OperationResultKind.Conflict, "merged");
        await using var session = new ScriptedSession(result);

        var actual = await CrdtLoopbackPushVerifier.PushAcceptedAsync(
            session,
            batch,
            CancellationToken.None);

        await Assert.That(actual.Operations[0].Kind).IsEqualTo(OperationResultKind.Conflict);
        await Assert.That(actual.Operations[0].ReasonCode).IsEqualTo("merged");
    }

    /// <summary>Verifies pushed operation/result count mismatches fail as a protocol violation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushAcceptedThrowsWhenResultCountDoesNotMatchBatch()
    {
        var batch = CreateBatch();
        var result = new RemoteSyncResult(batch.BatchId, [], null, null);
        await using var session = new ScriptedSession(result);

        var exception = await Assert.That(
                async () => await CrdtLoopbackPushVerifier.PushAcceptedAsync(
                    session,
                    batch,
                    CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(exception?.Message).Contains("unexpected operation result count");
    }

    /// <summary>Verifies a same-count result for a different batch cannot report acceptance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushAcceptedThrowsWhenResultBatchIdentifierDoesNotMatchBatch()
    {
        var batch = CreateBatch();
        var result = new RemoteSyncResult(
            Guid.Parse("00000000-0000-0000-0000-000000000803"),
            [new(batch.Operations[0].OperationId, OperationResultKind.Accepted, null, ServerVersion)],
            ServerVersion,
            null);

        await AssertPushFailureAsync(batch, result);
    }

    /// <summary>Verifies a same-count result for an unknown operation cannot report acceptance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushAcceptedThrowsWhenResultOperationIsUnknown()
    {
        var batch = CreateBatch();
        var result = new RemoteSyncResult(
            batch.BatchId,
            [
                new(
                    new OperationId(Guid.Parse("00000000-0000-0000-0000-000000000603")),
                    OperationResultKind.Accepted,
                    null,
                    ServerVersion),
            ],
            ServerVersion,
            null);

        await AssertPushFailureAsync(batch, result);
    }

    /// <summary>Verifies same-count duplicate operation results cannot report acceptance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushAcceptedThrowsWhenResultOperationsAreDuplicate()
    {
        var firstOperationId = new OperationId(Guid.Parse("00000000-0000-0000-0000-000000000604"));
        var secondOperationId = new OperationId(Guid.Parse("00000000-0000-0000-0000-000000000605"));
        var batch = CreateBatch(firstOperationId, secondOperationId);
        var result = new RemoteSyncResult(
            batch.BatchId,
            [
                new(firstOperationId, OperationResultKind.Accepted, null, ServerVersion),
                new(firstOperationId, OperationResultKind.Accepted, null, ServerVersion),
            ],
            ServerVersion,
            null);

        await AssertPushFailureAsync(batch, result);
    }

    /// <summary>Verifies non-terminal push results fail with their stable server reason code.</summary>
    /// <param name="kind">The operation result kind.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(OperationResultKind.Rejected)]
    [Arguments(OperationResultKind.Retryable)]
    public async Task PushAcceptedThrowsWhenOperationIsNotAccepted(OperationResultKind kind)
    {
        var batch = CreateBatch();
        var result = CreateResult(batch, kind, ReasonCode);
        await using var session = new ScriptedSession(result);

        var exception = await Assert.That(
                async () => await CrdtLoopbackPushVerifier.PushAcceptedAsync(
                    session,
                    batch,
                    CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(exception?.Message).Contains(ReasonCode);
    }

    /// <summary>Creates one public sync batch.</summary>
    /// <returns>The sync batch.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncBatch CreateBatch() =>
        CreateBatch(new OperationId(Guid.Parse("00000000-0000-0000-0000-000000000602")));

    /// <summary>Creates one public sync batch.</summary>
    /// <param name="operationIds">The operation identifiers.</param>
    /// <returns>The sync batch.</returns>
    private static SyncBatch CreateBatch(params OperationId[] operationIds)
    {
        var operations = new SyncOperation[operationIds.Length];
        for (var index = 0; index < operationIds.Length; index++)
        {
            var payload = CrdtServerPayloads.CreateInput(
                CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, index + 1)),
                CrdtBounds.Default);
            operations[index] = new()
            {
                OperationId = operationIds[index],
                StreamId = Stream,
                ClientSequence = index + 1,
                TimestampUtc = DateTimeOffset.UnixEpoch,
                Type = SyncOperationType.Update,
                Payload = payload,
                Policy = OperationPolicy.Default,
            };
        }

        return new(Guid.Parse("00000000-0000-0000-0000-000000000802"), operations);
    }

    /// <summary>Creates a public remote sync result.</summary>
    /// <param name="batch">The source batch.</param>
    /// <param name="kind">The operation result kind.</param>
    /// <param name="reasonCode">The reason code.</param>
    /// <returns>The remote sync result.</returns>
    private static RemoteSyncResult CreateResult(
        SyncBatch batch,
        OperationResultKind kind,
        string reasonCode) =>
        new(
            batch.BatchId,
            [new(batch.Operations[0].OperationId, kind, reasonCode, ServerVersion)],
            ServerVersion,
            null);

    /// <summary>Asserts a malformed same-count push result does not report success.</summary>
    /// <param name="batch">The source batch.</param>
    /// <param name="result">The malformed result.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertPushFailureAsync(SyncBatch batch, RemoteSyncResult result)
    {
        await using var session = new ScriptedSession(result);

        await Assert.That(
                async () => await CrdtLoopbackPushVerifier.PushAcceptedAsync(
                    session,
                    batch,
                    CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Scripted public transport session for push responses.</summary>
    /// <param name="result">The push result to return.</param>
    private sealed class ScriptedSession(RemoteSyncResult result) : IRemoteTransportSession
    {
        /// <summary>The maximum negotiated payload size.</summary>
        private const int MaximumPayloadBytes = 1024;

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities { get; } = new(
            new(1, 0),
            RemoteTransportCapabilities.None,
            1,
            MaximumPayloadBytes,
            null,
            null);

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new(result);
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException("The pusher test does not subscribe to streams.");

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            throw new NotSupportedException("The pusher test does not acknowledge receives.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
