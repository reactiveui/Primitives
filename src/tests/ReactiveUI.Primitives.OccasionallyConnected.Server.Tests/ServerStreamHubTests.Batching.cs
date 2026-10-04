// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerStreamHub"/>.</summary>
/// <content>Verifies journal validation cannot discard a previously valid prepared prefix.</content>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The domain event limit that deliberately exceeds the journal's per-entry event limit.</summary>
    private const int HigherPreparedEventLimit = 2;

    /// <summary>Verifies a valid first operation survives a later plan rejected by narrower journal limits.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ApplyOperationsAsyncPreservesPrefixWhenLaterPlanExceedsJournalEventLimit()
    {
        using var database = new SqliteLease();
        var handler = new DifferingEventCountsDomainHandler();
        var configured = Options(new AllowPolicy(Tenant), handler);
        var options = configured with
        {
            ConflictHandler = configured.ConflictHandler with { MaximumProducedEvents = HigherPreparedEventLimit },
            JournalLimits = new ServerCommitJournalLimits { MaximumEntryEventCount = SingleCount },
        };
        var first = Operation(1, PayloadA);
        var second = Operation(SecondOperationSeed, PayloadB);
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, options);
        _ = await Assert.ThrowsExactlyAsync<ArgumentOutOfRangeException>(
            () => hub.ApplyOperationsAsync(Batch(first, second), new(Tenant, Client), CancellationToken.None).AsTask());

        using var journal = new SqliteServerCommitJournal(database.Path, new() { MaximumEntryEventCount = SingleCount });
        var snapshot = journal.Read(new(Tenant, Stream), [new(Client, first.OperationId), new(Client, second.OperationId)]);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(snapshot.LastEventSequence).IsEqualTo(SingleCount);
        var retained = snapshot.Entries[0];
        await Assert.That(retained.OperationKey.OperationId).IsEqualTo(first.OperationId);
        await Assert.That(retained.Events).Count().IsEqualTo(SingleCount);
        var replay = await hub.ApplyOperationsAsync(Batch(first), new(Tenant, Client), CancellationToken.None);
        await Assert.That(replay.Result.Operations[0]).IsEqualTo(retained.Result);
        await Assert.That(replay.ProducedEvents).Count().IsEqualTo(SingleCount);
        var replayedEvent = replay.ProducedEvents[0];
        var originalEvent = retained.Events[0];
        await Assert.That(replayedEvent.EventId).IsEqualTo(originalEvent.EventId);
        await Assert.That(replayedEvent.ServerCursor).IsEqualTo(originalEvent.ServerCursor);
        await Assert.That(replayedEvent.CommittedAtUtc).IsEqualTo(originalEvent.CommittedAtUtc);
        await Assert.That(replayedEvent.Origin).IsEqualTo(originalEvent.Origin);
        await Assert.That(replayedEvent.Payload.PayloadHash).IsEqualTo(originalEvent.Payload.PayloadHash);
        await Assert.That(replayedEvent.Payload.Payload.Span.SequenceEqual(originalEvent.Payload.Payload.Span)).IsTrue();
        await Assert.That(handler.CallCount).IsEqualTo(HigherPreparedEventLimit);
        await Assert.That(journal.Read(new(Tenant, Stream), [new(Client, second.OperationId)]).Entries).IsEmpty();
    }

    /// <summary>Proposes one event for the first operation and two for the second.</summary>
    private sealed class DifferingEventCountsDomainHandler : IServerDomainHandler
    {
        /// <summary>Gets the number of preparations, including the rejected second plan.</summary>
        internal int CallCount { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerDomainApplyResult> ApplyAsync(ServerDomainApplyContext context, CancellationToken cancellationToken)
        {
            CallCount++;
            List<ServerProducedEvent> events = [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }];
            if (context.Operation.ClientSequence == SecondOperationSeed)
            {
                events.Add(new() { EventId = Guid.NewGuid(), Payload = context.Operation.Payload });
            }

            var state = new ServerState(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload);
            return ValueTask.FromResult(new ServerDomainApplyResult { NewState = state, Events = events });
        }
    }
}
