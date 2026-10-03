// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Validates bounded complete operation groups before remote projection.</summary>
public static class RemoteEventBatchValidator
{
    /// <summary>Validates a received batch and its complete operation groups.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="maximumEvents">The positive event and total declared identifier bound.</param>
    /// <param name="maximumCompletedOperations">The positive completed operation bound.</param>
    /// <exception cref="ArgumentNullException">The batch is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A bound is not positive.</exception>
    /// <exception cref="ArgumentException">The batch is malformed or exceeds a bound.</exception>
    /// <remarks>This validates grouping only. Authentication, cursor continuity and payload validation remain caller responsibilities.</remarks>
    public static void Validate(RemoteEventBatch batch, int maximumEvents, int maximumCompletedOperations)
    {
        ArgumentExceptionHelper.ThrowIfNull(batch);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumEvents);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumCompletedOperations);

        ValidateBoundsAndHeader(batch, maximumEvents, maximumCompletedOperations);
        var events = IndexEvents(batch);
        var declaredEvents = ValidateCompletions(batch, events);
        foreach (var remoteEvent in batch.Events)
        {
            if (remoteEvent.Origin is not null && !declaredEvents.Contains(remoteEvent.EventId))
            {
                throw new ArgumentException("An originating operation is not completely declared.", nameof(batch));
            }
        }
    }

    /// <summary>Checks all counts before lookup structures are allocated.</summary>
    /// <param name="batch">The batch.</param>
    /// <param name="maximumEvents">The event and declared identifier bound.</param>
    /// <param name="maximumCompletedOperations">The completion bound.</param>
    /// <exception cref="ArgumentException">The batch header or declared counts are invalid.</exception>
    private static void ValidateBoundsAndHeader(RemoteEventBatch batch, int maximumEvents, int maximumCompletedOperations)
    {
        if (batch.BatchId == Guid.Empty || batch.StreamId.Value is null || string.IsNullOrWhiteSpace(batch.NextCursor))
        {
            throw new ArgumentException("The receive batch header is malformed.", nameof(batch));
        }

        if (batch.Events.Count > maximumEvents || batch.CompletedOperations.Count > maximumCompletedOperations)
        {
            throw new ArgumentException("The receive batch exceeds its configured count bounds.", nameof(batch));
        }

        var remaining = maximumEvents;
        foreach (var completion in batch.CompletedOperations)
        {
            if (completion is null || completion.EventIds.Count > remaining)
            {
                throw new ArgumentException("The receive completion declarations are malformed or exceed the identifier bound.", nameof(batch));
            }

            remaining -= completion.EventIds.Count;
        }
    }

    /// <summary>Indexes distinct valid events in the batch.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The event index.</returns>
    /// <exception cref="ArgumentException">An event is malformed, duplicated, or belongs to another stream.</exception>
    private static Dictionary<Guid, RemoteEvent> IndexEvents(RemoteEventBatch batch)
    {
        Dictionary<Guid, RemoteEvent> events = [with(capacity: batch.Events.Count)];
        foreach (var remoteEvent in batch.Events)
        {
            if (remoteEvent is null || remoteEvent.EventId == Guid.Empty || remoteEvent.StreamId != batch.StreamId)
            {
                throw new ArgumentException("The receive batch contains a malformed event.", nameof(batch));
            }

            if (events.ContainsKey(remoteEvent.EventId))
            {
                throw new ArgumentException("The receive batch contains a duplicate event identifier.", nameof(batch));
            }

            events.Add(remoteEvent.EventId, remoteEvent);
        }

        return events;
    }

    /// <summary>Validates complete origin groups against the event index.</summary>
    /// <param name="batch">The batch.</param>
    /// <param name="events">The event index.</param>
    /// <returns>The distinct declared event identifiers.</returns>
    /// <exception cref="ArgumentException">A completion repeats an origin or references an invalid event.</exception>
    private static HashSet<Guid> ValidateCompletions(RemoteEventBatch batch, Dictionary<Guid, RemoteEvent> events)
    {
        HashSet<RemoteEventOrigin> origins = [];
        HashSet<Guid> declaredEvents = [];
        foreach (var completion in batch.CompletedOperations)
        {
            if (!origins.Add(completion.Origin))
            {
                throw new ArgumentException("The receive batch repeats a completed operation.", nameof(batch));
            }

            foreach (var eventId in completion.EventIds)
            {
                if (!events.TryGetValue(eventId, out var remoteEvent) || remoteEvent.Origin != completion.Origin || !declaredEvents.Add(eventId))
                {
                    throw new ArgumentException("A completion references a missing, foreign, or repeated event.", nameof(batch));
                }
            }
        }

        return declaredEvents;
    }
}
