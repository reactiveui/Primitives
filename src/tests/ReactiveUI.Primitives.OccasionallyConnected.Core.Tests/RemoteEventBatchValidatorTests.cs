// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RemoteEventBatchValidator"/>.</summary>
public sealed class RemoteEventBatchValidatorTests
{
    /// <summary>The test cursor.</summary>
    private const string Cursor = "cursor";

    /// <summary>The test client identity.</summary>
    private const string Client = "client";

    /// <summary>The receive event bound.</summary>
    private const int MaximumEvents = 4;

    /// <summary>The receive completion bound.</summary>
    private const int MaximumCompletions = 2;

    /// <summary>The test stream.</summary>
    private static readonly StreamId Stream = new("counter");

    /// <summary>Verifies a first fragment cannot declare an entire multi-event operation complete.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PartialOperationGroupCannotCompleteOptimisticInput()
    {
        var origin = new RemoteEventOrigin(Client, OperationId.New());
        var first = CreateEvent(origin);
        var second = CreateEvent(origin);
        var batch = new RemoteEventBatch(Guid.NewGuid(), Stream, null, Cursor, [first, second]) { CompletedOperations = [new(origin, [first.EventId])] };

        Action validate = () => RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);
        await Assert.That(validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies complete groups, zero-event acceptance and legacy events coexist without ordering opaque cursors.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompleteGroupsAllowZeroEventOperationsAndLegacyEvents()
    {
        var operationId = OperationId.New();
        var firstOrigin = new RemoteEventOrigin("first-client", operationId);
        var secondOrigin = new RemoteEventOrigin("second-client", operationId);
        var first = CreateEvent(firstOrigin);
        var second = CreateEvent(firstOrigin);
        var legacy = CreateEvent(secondOrigin) with { Origin = null };
        var batch = new RemoteEventBatch(Guid.NewGuid(), Stream, Cursor, Cursor, [first, second, legacy])
        {
            CompletedOperations = [new(firstOrigin, [first.EventId, second.EventId]), new(secondOrigin, [])],
        };

        RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);

        await Assert.That(batch.CompletedOperations.Count).IsEqualTo(MaximumCompletions);
        await Assert.That(batch.CompletedOperations[1].EventIds.Count).IsEqualTo(0);
    }

    /// <summary>Verifies an empty receive checkpoint is valid.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EmptyCheckpointHasNoInventedCompletion()
    {
        var batch = CreateBatch([], []);
        RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);
        await Assert.That(batch.CompletedOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies two clients sharing an operation identifier remain distinct at exact receive limits.</summary>
    /// <param name="firstClient">The first ordinal client identity.</param>
    /// <param name="secondClient">The second ordinal client identity.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments("client", "CLIENT")]
    [Arguments("\u00e9", "e\u0301")]
    public async Task ExactBoundsPreserveOrdinalClientGroups(string firstClient, string secondClient)
    {
        var operationId = OperationId.New();
        var firstOrigin = new RemoteEventOrigin(firstClient, operationId);
        var secondOrigin = new RemoteEventOrigin(secondClient, operationId);
        var first = CreateEvent(firstOrigin);
        var second = CreateEvent(firstOrigin);
        var third = CreateEvent(secondOrigin);
        var fourth = CreateEvent(secondOrigin);
        var batch = CreateBatch(
            [first, second, third, fourth],
            [new(firstOrigin, [first.EventId, second.EventId]), new(secondOrigin, [third.EventId, fourth.EventId])]);

        RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);

        await Assert.That(batch.Events.Count).IsEqualTo(MaximumEvents);
        await Assert.That(batch.CompletedOperations.Count).IsEqualTo(MaximumCompletions);
    }

    /// <summary>Verifies the total declaration bound is checked before inspecting malformed event contents.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CombinedDeclarationLimitPrecedesEventInspection()
    {
        var first = new RemoteOperationCompletion(new("first", OperationId.New()), new Guid[MaximumEvents]);
        var second = new RemoteOperationCompletion(new("second", OperationId.New()), [Guid.NewGuid()]);
        var batch = CreateBatch(new RemoteEvent[1], [first, second]);
        Action validate = () => RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);

        var exception = await Assert.That(validate).ThrowsExactly<ArgumentException>();
        await Assert.That(exception?.Message).Contains("identifier bound");
    }

    /// <summary>Verifies a missing batch fails at the public validation boundary.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NullBatchFailsBeforeProjection()
    {
        Action<RemoteEventBatch> validate = static batch => RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);
        var exception = await Assert.That(() => validate.DynamicInvoke([null])).ThrowsExactly<System.Reflection.TargetInvocationException>();
        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
    }

    /// <summary>Verifies malformed operation groups cannot advance a receive checkpoint.</summary>
    /// <param name="scenario">The invalid completion shape.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments("missing")]
    [Arguments("empty-id")]
    [Arguments("foreign-client")]
    [Arguments("repeated-id")]
    [Arguments("repeated-origin")]
    [Arguments("undeclared")]
    [Arguments("legacy")]
    public async Task InvalidCompletionCannotAdvanceCheckpoint(string scenario)
    {
        var origin = new RemoteEventOrigin(Client, OperationId.New());
        var remoteEvent = CreateEvent(origin);
        var completions = scenario switch
        {
            "missing" => new RemoteOperationCompletion[] { new(origin, [Guid.NewGuid()]) },
            "empty-id" => [new(origin, [Guid.Empty])],
            "foreign-client" => [new(new("other-client", origin.OperationId), [remoteEvent.EventId])],
            "repeated-id" => [new(origin, [remoteEvent.EventId, remoteEvent.EventId])],
            "repeated-origin" => [new(origin, [remoteEvent.EventId]), new(origin, [])],
            "undeclared" => [],
            _ => [new(origin, [remoteEvent.EventId])],
        };
        var batch = CreateBatch([scenario == "legacy" ? remoteEvent with { Origin = null } : remoteEvent], completions);

        Action validate = () => RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);
        await Assert.That(validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies events must be unique, non-null, and belong to the batch stream.</summary>
    /// <param name="scenario">The invalid event shape.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments("null")]
    [Arguments("duplicate")]
    [Arguments("empty-id")]
    [Arguments("other-stream")]
    public async Task InvalidEventCannotAdvanceCheckpoint(string scenario)
    {
        var origin = new RemoteEventOrigin(Client, OperationId.New());
        var remoteEvent = CreateEvent(origin) with { Origin = null };
        var malformed = new RemoteEvent(
            scenario == "empty-id" ? Guid.Empty : Guid.NewGuid(),
            scenario == "other-stream" ? new("other-stream") : Stream,
            Cursor,
            DateTimeOffset.UnixEpoch,
            null,
            remoteEvent.Payload,
            remoteEvent.Metadata);
        var events = scenario switch
        {
            "null" => new RemoteEvent[1],
            "duplicate" => [remoteEvent, remoteEvent],
            _ => [malformed],
        };

        Action validate = () => RemoteEventBatchValidator.Validate(CreateBatch(events, []), MaximumEvents, MaximumCompletions);
        await Assert.That(validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies positive receive limits are required.</summary>
    /// <param name="events">The event limit.</param>
    /// <param name="completions">The completion limit.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(0, MaximumCompletions)]
    [Arguments(-1, MaximumCompletions)]
    [Arguments(MaximumEvents, 0)]
    [Arguments(MaximumEvents, -1)]
    public async Task NonpositiveLimitsFail(int events, int completions)
    {
        Action validate = () => RemoteEventBatchValidator.Validate(CreateBatch([], []), events, completions);
        await Assert.That(validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies finite receive count and declaration limits.</summary>
    /// <param name="scenario">The exceeded bound or absent completion.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments("events")]
    [Arguments("completions")]
    [Arguments("identifiers")]
    [Arguments("null-completion")]
    public async Task InvalidReceiveBoundsFail(string scenario)
    {
        var origin = new RemoteEventOrigin("bounded-client", OperationId.New());
        var remoteEvent = CreateEvent(origin);
        var events = scenario == "events" ? Enumerable.Repeat(remoteEvent, MaximumEvents + 1).ToArray() : [];
        var completions = scenario switch
        {
            "completions" => Enumerable.Repeat(new RemoteOperationCompletion(origin, []), MaximumCompletions + 1).ToArray(),
            "identifiers" => [new(origin, Enumerable.Repeat(Guid.NewGuid(), MaximumEvents + 1).ToArray())],
            "null-completion" => new RemoteOperationCompletion[1],
            _ => [],
        };

        Action validate = () => RemoteEventBatchValidator.Validate(CreateBatch(events, completions), MaximumEvents, MaximumCompletions);
        await Assert.That(validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies malformed batch headers fail before projection.</summary>
    /// <param name="scenario">The malformed header field.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments("batch-id")]
    [Arguments("stream-id")]
    [Arguments("cursor")]
    public async Task InvalidHeaderFails(string scenario)
    {
        var batch = new RemoteEventBatch(
            scenario == "batch-id" ? Guid.Empty : Guid.NewGuid(),
            scenario == "stream-id" ? default : Stream,
            null,
            scenario == "cursor" ? " " : Cursor,
            []);
        Action validate = () => RemoteEventBatchValidator.Validate(batch, MaximumEvents, MaximumCompletions);
        await Assert.That(validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Creates a batch with complete operation declarations.</summary>
    /// <param name="events">The events.</param>
    /// <param name="completions">The declarations.</param>
    /// <returns>The batch.</returns>
    private static RemoteEventBatch CreateBatch(IReadOnlyList<RemoteEvent> events, IReadOnlyList<RemoteOperationCompletion> completions) =>
        new(Guid.NewGuid(), Stream, null, Cursor, events) { CompletedOperations = completions };

    /// <summary>Creates an event belonging to a client operation.</summary>
    /// <param name="origin">The client operation.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent CreateEvent(RemoteEventOrigin origin) =>
        new(
            Guid.NewGuid(),
            Stream,
            Cursor,
            DateTimeOffset.UnixEpoch,
            origin.OperationId,
            new("counter", 1, "application/json", ReadOnlyMemory<byte>.Empty, "hash"),
            new Dictionary<string, string>()) { Origin = origin };
}
