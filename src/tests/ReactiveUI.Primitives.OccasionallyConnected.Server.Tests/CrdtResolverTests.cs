// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="CrdtResolver"/>.</summary>
public sealed partial class CrdtResolverTests
{
    /// <summary>The authenticated tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The authenticated client.</summary>
    private const string Client = "client-a";

    /// <summary>The foreign client.</summary>
    private const string ForeignClient = "client-b";

    /// <summary>The initial CRDT version.</summary>
    private const string InitialVersion = "crdt-v0";

    /// <summary>The first CRDT version.</summary>
    private const string FirstVersion = "crdt-v1";

    /// <summary>The expected single item count.</summary>
    private const int SingleCount = 1;

    /// <summary>The invalid CRDT kind backing value.</summary>
    private const int InvalidKindValue = -1;

    /// <summary>The malformed byte value.</summary>
    private const byte MalformedByte = 255;

    /// <summary>The CRDT merge reason.</summary>
    private const string MergeReason = "crdt.merge";

    /// <summary>The invalid CRDT mutation reason.</summary>
    private const string InvalidMutationReason = "crdt-invalid-mutation";

    /// <summary>The durable sequence used by resolver tests.</summary>
    private const int DurableSequence = 2;

    /// <summary>The dominated component value.</summary>
    private const int DominatedComponent = 3;

    /// <summary>The current component value.</summary>
    private const int CurrentComponent = 5;

    /// <summary>The CRDT stream.</summary>
    private static readonly StreamId Stream = new("crdt/counter");

    /// <summary>The fixed server time.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 13, 10, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies mismatched trusted write provenance is rejected before merge.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsMismatchedTrustedProvenance()
    {
        var operation = Operation(
            OperationId.New(),
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, 1)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });
        var context = new ConflictContext(
            State(CrdtFunctions.Empty(CrdtKind.GCounter), InitialVersion),
            [operation],
            new(Client, Tenant),
            new() { CandidateWrite = new() { ClientId = ForeignClient, OperationId = operation.OperationId, CommittedAtUtc = Start } });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("crdt-missing-server-provenance");
        await Assert.That(result.ServerVersion).IsEqualTo(InitialVersion);
    }

    /// <summary>Verifies dominated counter writes are accepted and return the complete state payload.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncAcceptsDominatedGCounterAsSnapshotConflict()
    {
        var operation = Operation(
            OperationId.New(),
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, DominatedComponent)));
        var current = CrdtFunctions.ApplyLocal(
            CrdtFunctions.Empty(CrdtKind.GCounter),
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, CurrentComponent)),
            Client,
            1,
            CrdtBounds.Default);
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });
        var context = new ConflictContext(
            State(current, InitialVersion),
            [operation],
            new(Client, Tenant),
            new() { CandidateWrite = new() { ClientId = Client, OperationId = operation.OperationId, CommittedAtUtc = Start } });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);
        var resolved = DecodeState(result.Conflicts[0].ResolvedPayload);

        await Assert.That(result.AcceptedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.AcceptedOperations[0]).IsEqualTo(operation.OperationId);
        await Assert.That(result.RejectedOperations).IsEmpty();
        await Assert.That(result.Conflicts[0].ResolutionCode).IsEqualTo(MergeReason);
        await Assert.That(result.ServerVersion).IsEqualTo(FirstVersion);
        await Assert.That(resolved.Value.Counter).IsEqualTo(CurrentComponent);
    }

    /// <summary>Verifies empty incoming operation sets are rejected with the empty operation identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsEmptyIncomingOperations()
    {
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });
        var context = new ConflictContext(
            State(CrdtFunctions.Empty(CrdtKind.GCounter), InitialVersion),
            [],
            new(Client, Tenant),
            new() { CandidateWrite = new() { ClientId = Client, OperationId = OperationId.New(), CommittedAtUtc = Start } });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.RejectedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.RejectedOperations[0].OperationId.Value).IsEqualTo(Guid.Empty);
    }

    /// <summary>Verifies multi-operation resolver calls are rejected before merge.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsMultipleIncomingOperations()
    {
        var first = Operation(OperationId.New(), CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, CurrentComponent)));
        var second = Operation(OperationId.New(), CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, DominatedComponent)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });
        var context = new ConflictContext(
            State(CrdtFunctions.Empty(CrdtKind.GCounter), InitialVersion),
            [first, second],
            new(Client, Tenant),
            new() { CandidateWrite = new() { ClientId = Client, OperationId = first.OperationId, CommittedAtUtc = Start } });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.RejectedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.RejectedOperations[0].OperationId).IsEqualTo(first.OperationId);
    }

    /// <summary>Verifies invalid current CRDT state payloads reject the candidate operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsInvalidCurrentStatePayload()
    {
        var operation = Operation(OperationId.New(), CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, CurrentComponent)));
        var payload = new[] { MalformedByte };
        var envelope = new PayloadEnvelope(
            CrdtContracts.StateContractId,
            CrdtContracts.SchemaVersion,
            CrdtServerPayloads.ContentType,
            payload,
            CrdtServerPayloads.ComputePayloadHash(payload));
        var state = new ServerState(Stream, InitialVersion, envelope);
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(state, operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("crdt-invalid-state");
    }

    /// <summary>Verifies authoritative-state inputs are not accepted as client mutations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsAuthoritativeStateInput()
    {
        var operation = Operation(OperationId.New(), CrdtInput.ForAuthoritativeState(CrdtFunctions.Empty(CrdtKind.GCounter)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(CrdtFunctions.Empty(CrdtKind.GCounter), operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("crdt-input-kind-mismatch");
    }

    /// <summary>Verifies mutation kinds that do not match the registered CRDT kind are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsMutationKindMismatch()
    {
        var operation = Operation(OperationId.New(), CrdtInput.ForMutation(CrdtMutation.PNCounterSet(Client, CurrentComponent, DominatedComponent)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(CrdtFunctions.Empty(CrdtKind.GCounter), operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("crdt-state-kind-mismatch");
    }

    /// <summary>Verifies G-counter actors must match the trusted server client.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsForeignGCounterActor()
    {
        var operation = Operation(OperationId.New(), CrdtInput.ForMutation(CrdtMutation.GCounterSet(ForeignClient, CurrentComponent)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(CrdtFunctions.Empty(CrdtKind.GCounter), operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("crdt-invalid-mutation");
    }

    /// <summary>Verifies PN-counter actors must match the trusted server client.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsForeignPnCounterActor()
    {
        var operation = Operation(OperationId.New(), CrdtInput.ForMutation(CrdtMutation.PNCounterSet(ForeignClient, CurrentComponent, DominatedComponent)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.PNCounter, VersionFactory = new CrdtSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(CrdtFunctions.Empty(CrdtKind.PNCounter), operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("crdt-invalid-mutation");
    }

    /// <summary>Verifies OR-set registrations reject non-OR-set mutations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncRejectsOrSetMutationKindMismatch()
    {
        var operation = Operation(OperationId.New(), CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, CurrentComponent)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.ORSet, VersionFactory = new CrdtSequentialVersionFactory() });

        var result = await resolver.ResolveAsync(Context(CrdtFunctions.Empty(CrdtKind.ORSet), operation, Client), CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("crdt-state-kind-mismatch");
    }

    /// <summary>Verifies invalid resolver kind configuration is rejected during construction.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConstructorRejectsInvalidKind()
    {
        static void Act() => _ = new CrdtResolver(new() { Kind = (CrdtKind)InvalidKindValue, VersionFactory = new CrdtSequentialVersionFactory() });

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies host version factory errors remain host errors after CRDT mutation validation succeeds.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveAsyncPropagatesVersionFactoryFailure()
    {
        var operation = Operation(
            OperationId.New(),
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(Client, CurrentComponent)));
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new ThrowingVersionFactory() });

        async Task Act() => _ = await resolver.ResolveAsync(
            new(
                State(CrdtFunctions.Empty(CrdtKind.GCounter), InitialVersion),
                [operation],
                new(Client, Tenant),
                new() { CandidateWrite = new() { ClientId = Client, OperationId = operation.OperationId, CommittedAtUtc = Start } }),
            CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a conflict context from an explicit server state.</summary>
    /// <param name="state">The server state.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="clientId">The authenticated client id.</param>
    /// <returns>The conflict context.</returns>
    private static ConflictContext Context(ServerState state, SyncOperation operation, string clientId) =>
        new(
            state,
            [operation],
            new(clientId, Tenant),
            new() { CandidateWrite = new() { ClientId = clientId, OperationId = operation.OperationId, CommittedAtUtc = Start } });

    /// <summary>Creates a CRDT operation.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="input">The CRDT input.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation Operation(OperationId operationId, CrdtInput input) =>
        new()
        {
            OperationId = operationId,
            StreamId = Stream,
            ClientSequence = DurableSequence,
            TimestampUtc = Start.AddDays(1),
            BaseVersion = InitialVersion,
            Type = SyncOperationType.Update,
            Payload = CrdtServerPayloads.CreateInput(input),
        };

    /// <summary>Creates a server state.</summary>
    /// <param name="state">The CRDT state.</param>
    /// <param name="version">The version.</param>
    /// <returns>The server state.</returns>
    private static ServerState State(CrdtState state, string version) =>
        new(Stream, version, CrdtServerPayloads.CreateState(state));

    /// <summary>Decodes a resolved state payload.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The CRDT state.</returns>
    /// <exception cref="InvalidOperationException">The resolved payload is missing.</exception>
    private static CrdtState DecodeState(PayloadEnvelope? payload)
    {
        if (payload is null)
        {
            throw new InvalidOperationException("The resolved state payload is missing.");
        }

        return CrdtCodec.DecodeState(payload.Payload);
    }

    /// <summary>Version factory that fails after mutation validation.</summary>
    private sealed class ThrowingVersionFactory : IServerConflictVersionFactory
    {
        /// <inheritdoc/>
        public string CreateNextVersion(ConflictContext context, SyncOperation operation) =>
            throw new InvalidOperationException("The host version factory failed.");
    }
}
