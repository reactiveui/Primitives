// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackReceiver"/>.</summary>
public sealed class CrdtLoopbackReceiverTests
{
    /// <summary>The test client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The test cursor.</summary>
    private const string Cursor = "cursor-1";

    /// <summary>The requested cursor.</summary>
    private const string RequestedCursor = "cursor-requested";

    /// <summary>The different cursor.</summary>
    private const string OtherCursor = "cursor-other";

    /// <summary>The count that exceeds the lab receive limits.</summary>
    private const int OverLimitCount = 33;

    /// <summary>The first generated operation seed.</summary>
    private const int GeneratedOperationSeed = 602;

    /// <summary>The first generated event seed.</summary>
    private const int GeneratedEventSeed = 702;

    /// <summary>The first generated completion operation seed.</summary>
    private const int GeneratedCompletionOperationSeed = 902;

    /// <summary>The deterministic GUID prefix.</summary>
    private const string GuidPrefix = "00000000-0000-0000-0000-";

    /// <summary>The deterministic GUID tail format.</summary>
    private const string GuidSeedFormat = "x12";

    /// <summary>The test stream identifier.</summary>
    private static readonly StreamId Stream = new("resilience/receiver");

    /// <summary>The different stream identifier.</summary>
    private static readonly StreamId OtherStream = new("resilience/receiver-other");

    /// <summary>The test subscription identifier.</summary>
    private static readonly SubscriptionId Subscription = new(Guid.Parse("00000000-0000-0000-0000-000000000501"));

    /// <summary>Verifies empty subscriptions fail instead of acknowledging an absent frontier.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveStreamFromCursorThrowsWhenSubscriptionProducesNoBatch()
    {
        await using var session = new ScriptedSession([]);

        var exception = await Assert.That(
                async () => await CrdtLoopbackReceiver.ReceiveStreamFromCursorAsync(
                    session,
                    Stream,
                    Subscription,
                    null,
                    CrdtBounds.Default,
                    CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(exception?.Message).Contains("produced no authoritative CRDT batch");
        await Assert.That(session.Acknowledgements).IsEmpty();
    }

    /// <summary>Verifies non-authoritative CRDT payloads fail before the cursor is ACKed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveStreamFromCursorRejectsNonAuthoritativePayload()
    {
        var batch = CreateMutationBatch();
        await using var session = new ScriptedSession([batch]);

        var exception = await Assert.That(
                async () => await CrdtLoopbackReceiver.ReceiveStreamFromCursorAsync(
                    session,
                    Stream,
                    Subscription,
                    null,
                    CrdtBounds.Default,
                    CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(exception?.Message).Contains("did not contain an authoritative state");
        await Assert.That(session.Acknowledgements).IsEmpty();
    }

    /// <summary>Verifies a public batch for another stream fails before the cursor is ACKed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveStreamFromCursorRejectsMismatchedStreamBeforeAcknowledgement()
    {
        await using var session = new ScriptedSession([CreateAuthoritativeBatch(OtherStream, null, Cursor)]);

        await AssertReceiveFailureAsync(
            session,
            Stream,
            cursor: null);
    }

    /// <summary>Verifies a public batch for another previous cursor fails before the cursor is ACKed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveStreamFromCursorRejectsMismatchedPreviousCursorBeforeAcknowledgement()
    {
        await using var session = new ScriptedSession([CreateAuthoritativeBatch(Stream, OtherCursor, Cursor)]);

        await AssertReceiveFailureAsync(
            session,
            Stream,
            RequestedCursor);
    }

    /// <summary>Verifies receive pages over the event limit fail before the cursor is ACKed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveStreamFromCursorRejectsEventCountOverLimitBeforeAcknowledgement()
    {
        await using var session = new ScriptedSession([CreateAuthoritativeBatch(Stream, null, Cursor, OverLimitCount)]);

        await AssertReceiveFailureAsync(
            session,
            Stream,
            cursor: null);
    }

    /// <summary>Verifies receive pages over the completed-operation limit fail before the cursor is ACKed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveStreamFromCursorRejectsCompletedOperationCountOverLimitBeforeAcknowledgement()
    {
        var batch = CreateAuthoritativeBatch(Stream, null, Cursor) with { CompletedOperations = CreateCompletions(OverLimitCount) };
        await using var session = new ScriptedSession([batch]);

        await AssertReceiveFailureAsync(
            session,
            Stream,
            cursor: null);
    }

    /// <summary>Verifies successful receives acknowledge the requested subscription identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReceiveStreamFromCursorAcknowledgesRequestedSubscription()
    {
        await using var session = new ScriptedSession([CreateAuthoritativeBatch(Stream, RequestedCursor, Cursor)]);

        _ = await CrdtLoopbackReceiver.ReceiveStreamFromCursorAsync(
            session,
            Stream,
            Subscription,
            RequestedCursor,
            CrdtBounds.Default,
            CancellationToken.None);

        await Assert.That(session.Requests[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.Requests[0].StreamId).IsEqualTo(Stream);
        await Assert.That(session.Requests[0].Cursor).IsEqualTo(RequestedCursor);
        await Assert.That(session.Acknowledgements[0].SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(session.Acknowledgements[0].StreamId).IsEqualTo(Stream);
        await Assert.That(session.Acknowledgements[0].Cursor).IsEqualTo(Cursor);
    }

    /// <summary>Creates a public remote event batch carrying a mutation payload instead of authoritative state.</summary>
    /// <returns>The remote event batch.</returns>
    private static RemoteEventBatch CreateMutationBatch()
    {
        var operationId = new OperationId(Guid.Parse("00000000-0000-0000-0000-000000000601"));
        var payload = CrdtServerPayloads.CreateInput(
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, 1)),
            CrdtBounds.Default);
        var remoteEvent = new RemoteEvent(
            Guid.Parse("00000000-0000-0000-0000-000000000701"),
            Stream,
            Cursor,
            DateTimeOffset.UnixEpoch,
            operationId,
            payload,
            new Dictionary<string, string>());
        return new(
            Guid.Parse("00000000-0000-0000-0000-000000000801"),
            Stream,
            null,
            Cursor,
            [remoteEvent]);
    }

    /// <summary>Creates a public remote event batch carrying an authoritative state payload.</summary>
    /// <param name="streamId">The batch stream identifier.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <returns>The remote event batch.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static RemoteEventBatch CreateAuthoritativeBatch(
        StreamId streamId,
        string? previousCursor,
        string nextCursor) =>
        CreateAuthoritativeBatch(streamId, previousCursor, nextCursor, eventCount: 1);

    /// <summary>Creates a public remote event batch carrying authoritative state payloads.</summary>
    /// <param name="streamId">The batch stream identifier.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="eventCount">The event count.</param>
    /// <returns>The remote event batch.</returns>
    private static RemoteEventBatch CreateAuthoritativeBatch(
        StreamId streamId,
        string? previousCursor,
        string nextCursor,
        int eventCount)
    {
        List<RemoteEvent> events = [];
        for (var index = 0; index < eventCount; index++)
        {
            events.Add(CreateAuthoritativeEvent(streamId, nextCursor, index));
        }

        return new(
            Guid.Parse("00000000-0000-0000-0000-000000000802"),
            streamId,
            previousCursor,
            nextCursor,
            events);
    }

    /// <summary>Creates a public remote event carrying an authoritative state payload.</summary>
    /// <param name="streamId">The event stream identifier.</param>
    /// <param name="cursor">The server cursor.</param>
    /// <param name="index">The event index.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateAuthoritativeEvent(StreamId streamId, string cursor, int index)
    {
        var state = new CrdtState { Kind = CrdtKind.GCounter, GCounterComponents = new Dictionary<string, long> { [ClientId] = 1 } };
        var operationId = new OperationId(CreateGuid(GeneratedOperationSeed + index));
        var payload = CrdtServerPayloads.CreateInput(CrdtInput.ForAuthoritativeState(state), CrdtBounds.Default);
        return new(
            CreateGuid(GeneratedEventSeed + index),
            streamId,
            cursor,
            DateTimeOffset.UnixEpoch,
            operationId,
            payload,
            new Dictionary<string, string>());
    }

    /// <summary>Creates public completed operation declarations.</summary>
    /// <param name="count">The completion count.</param>
    /// <returns>The completed operations.</returns>
    private static List<RemoteOperationCompletion> CreateCompletions(int count)
    {
        List<RemoteOperationCompletion> completions = [];
        for (var index = 0; index < count; index++)
        {
            var operationId = new OperationId(CreateGuid(GeneratedCompletionOperationSeed + index));
            completions.Add(new(new(ClientId, operationId), []));
        }

        return completions;
    }

    /// <summary>Creates a deterministic GUID from a small positive seed.</summary>
    /// <param name="seed">The seed.</param>
    /// <returns>The deterministic GUID.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Guid CreateGuid(int seed) =>
        new($"{GuidPrefix}{seed.ToString(GuidSeedFormat, CultureInfo.InvariantCulture)}");

    /// <summary>Asserts that malformed receive identity fails without ACK.</summary>
    /// <param name="session">The scripted session.</param>
    /// <param name="streamId">The requested stream identifier.</param>
    /// <param name="cursor">The requested cursor.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertReceiveFailureAsync(
        ScriptedSession session,
        StreamId streamId,
        string? cursor)
    {
        await Assert.That(
                async () => await CrdtLoopbackReceiver.ReceiveStreamFromCursorAsync(
                    session,
                    streamId,
                    Subscription,
                    cursor,
                    CrdtBounds.Default,
                    CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(session.Acknowledgements).IsEmpty();
    }

    /// <summary>Scripted public transport session for malformed receive responses.</summary>
    /// <param name="batches">The batches to stream.</param>
    private sealed class ScriptedSession(IReadOnlyList<RemoteEventBatch> batches) : IRemoteTransportSession
    {
        /// <summary>The maximum negotiated payload size.</summary>
        private const int MaximumPayloadBytes = 1024;

        /// <summary>Gets acknowledgements sent through the session.</summary>
        public List<ReceiveAcknowledgement> Acknowledgements { get; } = [];

        /// <summary>Gets subscribe requests sent through the session.</summary>
        public List<RemoteSubscribeRequest> Requests { get; } = [];

        /// <inheritdoc/>
        public NegotiatedCapabilities NegotiatedCapabilities { get; } = new(
            new(1, 0),
            RemoteTransportCapabilities.None,
            1,
            MaximumPayloadBytes,
            null,
            null);

        /// <inheritdoc/>
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            throw new NotSupportedException("The receiver test does not push operations.");

        /// <inheritdoc/>
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Requests.Add(request);
            await Task.Yield();
            for (var index = 0; index < batches.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return batches[index];
            }
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken)
        {
            Acknowledgements.Add(acknowledgement);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
