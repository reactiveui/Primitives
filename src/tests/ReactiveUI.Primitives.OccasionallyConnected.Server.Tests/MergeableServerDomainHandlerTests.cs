// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="MergeableServerDomainHandler"/>.</summary>
public sealed class MergeableServerDomainHandlerTests
{
    /// <summary>The authenticated client.</summary>
    private const string Client = "client-a";

    /// <summary>The tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The initial CRDT version.</summary>
    private const string InitialVersion = "crdt-v0";

    /// <summary>The first CRDT version.</summary>
    private const string FirstVersion = "crdt-v1";

    /// <summary>The merge resolution reason.</summary>
    private const string MergeReason = "crdt.merge";

    /// <summary>The expected single item count.</summary>
    private const int SingleCount = 1;

    /// <summary>The invalid CRDT kind backing value.</summary>
    private const int InvalidKindValue = -1;

    /// <summary>The accepted counter component.</summary>
    private const int Component = 7;

    /// <summary>The stream.</summary>
    private static readonly StreamId Stream = new("crdt/domain");

    /// <summary>The server time.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 13, 11, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies the domain handler persists state and emits an authoritative CRDT input.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyAsyncEmitsAuthoritativeStateEveryAcceptedOperation()
    {
        var operation = Operation(OperationId.New());
        var state = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(Client, Component)),
            Client,
            1,
            MergeableBounds.Default);
        var payload = MergeableServerPayloads.CreateState(state);
        var context = new ServerDomainApplyContext
        {
            Client = new(Client, Tenant),
            Operation = operation,
            Conflict = new(
                new(Stream, InitialVersion, MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter))),
                [operation],
                new(Client, Tenant),
                new() { CandidateWrite = new() { ClientId = Client, OperationId = operation.OperationId, CommittedAtUtc = Start } }),
            Resolution = new(
                [operation.OperationId],
                [],
                [new(operation.OperationId, MergeReason, payload)],
                [],
                FirstVersion),
        };
        var handler = new MergeableServerDomainHandler(new() { Kind = MergeableKind.IncrementOnlyCounter });

        var result = await handler.ApplyAsync(context, CancellationToken.None);
        var eventInput = MergeableCodec.DecodeInput(result.Events[0].Payload.Payload);

        await Assert.That(result.NewState.Version).IsEqualTo(FirstVersion);
        await Assert.That(result.NewState.State).IsEqualTo(payload);
        await Assert.That(result.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(result.Events[0].EventId).IsNotEqualTo(Guid.Empty);
        await Assert.That(eventInput.Kind).IsEqualTo(MergeableInputKind.AuthoritativeState);
        await Assert.That((eventInput.Value as MergeableState)?.Value.Counter).IsEqualTo(Component);
    }

    /// <summary>Verifies missing CRDT resolved payloads are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyAsyncRejectsMissingResolvedPayload()
    {
        var operation = Operation(OperationId.New());
        var context = Context(operation, new(operation.OperationId, MergeReason, null));
        var handler = new MergeableServerDomainHandler(new() { Kind = MergeableKind.IncrementOnlyCounter });

        async Task Act() => _ = await handler.ApplyAsync(context, CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies invalid CRDT resolved payloads are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyAsyncRejectsInvalidResolvedPayload()
    {
        var operation = Operation(OperationId.New());
        var payload = MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.Counter));
        var context = Context(operation, new(operation.OperationId, MergeReason, payload));
        var handler = new MergeableServerDomainHandler(new() { Kind = MergeableKind.IncrementOnlyCounter });

        async Task Act() => _ = await handler.ApplyAsync(context, CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies resolutions without a matching CRDT merge conflict are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyAsyncRejectsMissingMergeConflict()
    {
        var operation = Operation(OperationId.New());
        var context = Context(operation, new(OperationId.New(), MergeReason, MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter))));
        var handler = new MergeableServerDomainHandler(new() { Kind = MergeableKind.IncrementOnlyCounter });

        async Task Act() => _ = await handler.ApplyAsync(context, CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies invalid domain handler kind configuration is rejected during construction.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConstructorRejectsInvalidKind()
    {
        static void Act() => _ = new MergeableServerDomainHandler(new() { Kind = (MergeableKind)InvalidKindValue });

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Creates a domain apply context.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="conflict">The resolved conflict.</param>
    /// <returns>The context.</returns>
    private static ServerDomainApplyContext Context(SyncOperation operation, ResolvedConflict conflict) =>
        new()
        {
            Client = new(Client, Tenant),
            Operation = operation,
            Conflict = new(
                new(Stream, InitialVersion, MergeableServerPayloads.CreateState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter))),
                [operation],
                new(Client, Tenant),
                new() { CandidateWrite = new() { ClientId = Client, OperationId = operation.OperationId, CommittedAtUtc = Start } }),
            Resolution = new([operation.OperationId], [], [conflict], [], FirstVersion),
        };

    /// <summary>Creates an operation.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation Operation(OperationId operationId) =>
        new()
        {
            OperationId = operationId,
            StreamId = Stream,
            ClientSequence = 1,
            TimestampUtc = Start,
            BaseVersion = InitialVersion,
            Type = SyncOperationType.Update,
            Payload = MergeableServerPayloads.CreateInput(
                MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(Client, Component))),
        };
}
