// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Receives authoritative CRDT loopback stream pages and acknowledges their cursor frontier.</summary>
internal static class CrdtLoopbackReceiver
{
    /// <summary>Receives one complete stream page from the beginning and ACKs its frontier.</summary>
    /// <param name="session">The client session.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The deterministic subscription id.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The received states and cursor frontier.</returns>
    internal static async ValueTask<CrdtLoopbackReceivedStream> ReceiveStreamAsync(
        IRemoteTransportSession session,
        StreamId streamId,
        SubscriptionId subscriptionId,
        CrdtBounds bounds,
        CancellationToken cancellationToken) =>
        await ReceiveStreamFromCursorAsync(
            session,
            streamId,
            subscriptionId,
            null,
            bounds,
            cancellationToken).ConfigureAwait(false);

    /// <summary>Receives one complete stream page from a cursor and ACKs its frontier.</summary>
    /// <param name="session">The client session.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The deterministic subscription id.</param>
    /// <param name="cursor">The optional resume cursor.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The received states and cursor frontier.</returns>
    /// <exception cref="InvalidOperationException">The stream produced no authoritative CRDT batch.</exception>
    internal static async ValueTask<CrdtLoopbackReceivedStream> ReceiveStreamFromCursorAsync(
        IRemoteTransportSession session,
        StreamId streamId,
        SubscriptionId subscriptionId,
        string? cursor,
        CrdtBounds bounds,
        CancellationToken cancellationToken)
    {
        var request = new RemoteSubscribeRequest(streamId, subscriptionId, cursor, StartPosition.FromSequence(0));
        await using var enumerator = session
            .SubscribeAsync(request, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
        {
            throw new InvalidOperationException($"The stream '{streamId}' produced no authoritative CRDT batch.");
        }

        var batch = enumerator.Current;
        ValidateBatch(request, batch);
        var states = DecodeStates(batch, bounds);
        var acknowledgement = new ReceiveAcknowledgement(subscriptionId, streamId, batch.NextCursor);
        await session.AcknowledgeAsync(acknowledgement, cancellationToken).ConfigureAwait(false);
        return new(
            states,
            batch.PreviousCursor,
            batch.NextCursor,
            GetEventIds(batch),
            batch.Events.Count,
            batch.CompletedOperations.Count);
    }

    /// <summary>Decodes authoritative CRDT states from a remote event batch.</summary>
    /// <param name="batch">The remote event batch.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The decoded authoritative states.</returns>
    /// <exception cref="InvalidOperationException">The event was not an authoritative CRDT state.</exception>
    internal static List<CrdtState> DecodeStates(RemoteEventBatch batch, CrdtBounds bounds)
    {
        List<CrdtState> states = [];
        for (var index = 0; index < batch.Events.Count; index++)
        {
            var input = CrdtCodec.DecodeInput(batch.Events[index].Payload.Payload, bounds);
            if (input.Kind != CrdtInputKind.AuthoritativeState || input.State is null)
            {
                throw new InvalidOperationException("The CRDT loopback event did not contain an authoritative state.");
            }

            states.Add(input.State);
        }

        return states;
    }

    /// <summary>Copies event identifiers from a remote event batch.</summary>
    /// <param name="batch">The remote event batch.</param>
    /// <returns>The copied event identifiers.</returns>
    internal static List<Guid> GetEventIds(RemoteEventBatch batch)
    {
        List<Guid> eventIds = [];
        for (var index = 0; index < batch.Events.Count; index++)
        {
            eventIds.Add(batch.Events[index].EventId);
        }

        return eventIds;
    }

    /// <summary>Validates that a received batch belongs to the active request before decode and ACK.</summary>
    /// <param name="request">The active subscribe request.</param>
    /// <param name="batch">The received batch.</param>
    /// <exception cref="InvalidOperationException">The batch did not belong to the active request.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateBatch(RemoteSubscribeRequest request, RemoteEventBatch batch)
    {
        try
        {
            RemoteEventBatchValidator.Validate(
                batch,
                CrdtLoopbackScenario.MaximumReceiveEvents,
                CrdtLoopbackScenario.MaximumCompletedOperations);
        }
        catch (ArgumentException exception)
        {
            throw new InvalidOperationException("The CRDT loopback receive batch was malformed.", exception);
        }

        if (batch.StreamId != request.StreamId)
        {
            throw new InvalidOperationException("The CRDT loopback receive batch stream did not match the request.");
        }

        if (request.Cursor is null || string.Equals(batch.PreviousCursor, request.Cursor, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("The CRDT loopback receive batch previous cursor did not match the requested cursor.");
    }
}
