// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>CRDT composition tests for <see cref="ServerStreamHub"/> through its public surface.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The second authenticated client used by CRDT provenance tests.</summary>
    private const string CrdtOtherClient = "client-b";

    /// <summary>The first committed CRDT server version.</summary>
    private const string CrdtFirstVersion = "crdt-v1";

    /// <summary>The second committed CRDT server version.</summary>
    private const string CrdtSecondVersion = "crdt-v2";

    /// <summary>The third committed CRDT server version.</summary>
    private const string CrdtThirdVersion = "crdt-v3";

    /// <summary>The stable reason for a CRDT payload whose hash does not match its bytes.</summary>
    private const string CrdtHashMismatchReason = "crdt-payload-hash-mismatch";

    /// <summary>The stable reason for a CRDT payload that cannot be decoded or applied.</summary>
    private const string CrdtInvalidMutationReason = "crdt-invalid-mutation";

    /// <summary>The stable reason for a CRDT payload with the wrong contract metadata.</summary>
    private const string CrdtContractMismatchReason = "crdt-contract-mismatch";

    /// <summary>The stable reason for a CRDT mutation that targets another CRDT kind.</summary>
    private const string CrdtKindMismatchReason = "crdt-state-kind-mismatch";

    /// <summary>The foreign payload contract used by CRDT rejection tests.</summary>
    private const string CrdtForeignContract = "contract-foreign";

    /// <summary>The first counter component written by the first client.</summary>
    private const long CrdtFirstComponent = 5;

    /// <summary>The counter component written by the second client.</summary>
    private const long CrdtOtherComponent = 2;

    /// <summary>The raised counter component written by the first client.</summary>
    private const long CrdtRaisedComponent = 7;

    /// <summary>The counter component written by the second client's later operation.</summary>
    private const long CrdtOtherRaisedComponent = 3;

    /// <summary>The spoofed counter component a client tries to write for another actor.</summary>
    private const long CrdtSpoofedComponent = 100;

    /// <summary>The second operation sequence.</summary>
    private const int CrdtSecondSequence = 2;

    /// <summary>The third operation sequence.</summary>
    private const int CrdtThirdSequence = 3;

    /// <summary>The fourth operation sequence.</summary>
    private const int CrdtFourthSequence = 4;

    /// <summary>The fifth operation sequence.</summary>
    private const int CrdtFifthSequence = 5;

    /// <summary>The sixth operation sequence.</summary>
    private const int CrdtSixthSequence = 6;

    /// <summary>The expected double item count.</summary>
    private const int CrdtDoubleCount = 2;

    /// <summary>The expected triple item count.</summary>
    private const int CrdtTripleCount = 3;

    /// <summary>The operation identifier discriminator that keeps CRDT identifiers distinct from other hub tests.</summary>
    private const short CrdtOperationDiscriminator = 0x0C;

    /// <summary>The maximum snapshot response bytes requested by CRDT recovery tests.</summary>
    private const int CrdtSnapshotMaximumResponseBytes = 16_384;

    /// <summary>The CRDT stream used by hub composition tests.</summary>
    private static readonly StreamId CrdtStream = new("crdt/hub");

    /// <summary>The bounded wait used for every subscription read.</summary>
    private static readonly TimeSpan CrdtGuardTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Verifies accepted CRDT operations publish canonical events that read back identically after reopening SQLite.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithCrdtRegistrationPublishesCanonicalEventsThatSurviveSqliteReopen()
    {
        using var database = new SqliteLease();
        var first = CrdtOperation(1, 1, CrdtCounter(Client, CrdtFirstComponent));
        var second = CrdtOperation(CrdtSecondSequence, 1, CrdtCounter(CrdtOtherClient, CrdtOtherComponent));
        var third = CrdtOperation(CrdtThirdSequence, CrdtSecondSequence, CrdtCounter(Client, CrdtRaisedComponent));
        var produced = new List<RemoteEvent>();
        RemoteEventBatch before;
        await using (var hub = ServerStreamHub.CreateSqlite(database.Path, CrdtOptions(composeFromParts: true)))
        {
            var firstResult = await hub.ApplyOperationsAsync(Batch(first), new(Tenant, Client), CancellationToken.None);
            var secondResult = await hub.ApplyOperationsAsync(Batch(second), new(Tenant, CrdtOtherClient), CancellationToken.None);
            var thirdResult = await hub.ApplyOperationsAsync(Batch(third), new(Tenant, Client), CancellationToken.None);
            await AssertCrdtAcceptedAsync(firstResult, CrdtFirstVersion);
            await AssertCrdtAcceptedAsync(secondResult, CrdtSecondVersion);
            await AssertCrdtAcceptedAsync(thirdResult, CrdtThirdVersion);
            produced.AddRange(firstResult.ProducedEvents);
            produced.AddRange(secondResult.ProducedEvents);
            produced.AddRange(thirdResult.ProducedEvents);
            before = await ReadCrdtPageAsync(hub, new(Tenant, Client), SubscriptionId.New(), null);
        }

        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, CrdtOptions(composeFromParts: false));
        var after = await ReadCrdtPageAsync(reopened, new(Tenant, Client), SubscriptionId.New(), null);

        await Assert.That(before.Events).Count().IsEqualTo(CrdtTripleCount);
        await Assert.That(CrdtCursorSequence(before.Events[0].ServerCursor)).IsLessThan(CrdtCursorSequence(before.Events[1].ServerCursor));
        await Assert.That(CrdtCursorSequence(before.Events[1].ServerCursor)).IsLessThan(CrdtCursorSequence(before.Events[CrdtDoubleCount].ServerCursor));
        await AssertCrdtEventsEqualAsync(produced, before.Events);
        await AssertCrdtEventsEqualAsync(before.Events, after.Events);
        await Assert.That(before.Events[0].CausedByOperationId).IsEqualTo(first.OperationId);
        await Assert.That(before.Events[1].CausedByOperationId).IsEqualTo(second.OperationId);
        await Assert.That(before.Events[CrdtDoubleCount].CausedByOperationId).IsEqualTo(third.OperationId);
        await Assert.That(DecodeCrdtEventState(after.Events[0]).Value.Counter).IsEqualTo(CrdtFirstComponent);
        await Assert.That(DecodeCrdtEventState(after.Events[1]).Value.Counter).IsEqualTo(CrdtFirstComponent + CrdtOtherComponent);
        await Assert.That(DecodeCrdtEventState(after.Events[CrdtDoubleCount]).Value.Counter).IsEqualTo(CrdtRaisedComponent + CrdtOtherComponent);
    }

    /// <summary>Verifies CRDT events carry the authenticated client and causing operation, and never borrow another client's identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithCrdtRegistrationAttributesEventsToAuthenticatedClient()
    {
        using var database = new SqliteLease();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, CrdtOptions(composeFromParts: true));
        var clientOperation = CrdtOperation(1, 1, CrdtCounter(Client, CrdtFirstComponent));
        var sharedIdentifier = clientOperation with { Payload = CrdtCounter(CrdtOtherClient, CrdtOtherComponent) };
        var spoofed = CrdtOperation(CrdtSecondSequence, CrdtSecondSequence, CrdtCounter(Client, CrdtSpoofedComponent));
        var otherOperation = CrdtOperation(CrdtThirdSequence, CrdtThirdSequence, CrdtCounter(CrdtOtherClient, CrdtOtherRaisedComponent));

        var clientResult = await hub.ApplyOperationsAsync(Batch(clientOperation), new(Tenant, Client), CancellationToken.None);
        var sharedResult = await hub.ApplyOperationsAsync(Batch(sharedIdentifier), new(Tenant, CrdtOtherClient), CancellationToken.None);
        var spoofedResult = await hub.ApplyOperationsAsync(Batch(spoofed), new(Tenant, CrdtOtherClient), CancellationToken.None);
        var otherResult = await hub.ApplyOperationsAsync(Batch(otherOperation), new(Tenant, CrdtOtherClient), CancellationToken.None);
        var page = await ReadCrdtPageAsync(hub, new(Tenant, Client), SubscriptionId.New(), null);

        await AssertCrdtAcceptedAsync(clientResult, CrdtFirstVersion);
        await AssertCrdtAcceptedAsync(sharedResult, CrdtSecondVersion);
        await AssertCrdtRejectedAsync(spoofedResult, CrdtInvalidMutationReason, CrdtSecondVersion);
        await AssertCrdtAcceptedAsync(otherResult, CrdtThirdVersion);
        await Assert.That(page.Events).Count().IsEqualTo(CrdtTripleCount);
        await AssertCrdtOriginAsync(page.Events[0], Client, clientOperation.OperationId);
        await AssertCrdtOriginAsync(page.Events[1], CrdtOtherClient, clientOperation.OperationId);
        await AssertCrdtOriginAsync(page.Events[CrdtDoubleCount], CrdtOtherClient, otherOperation.OperationId);
        await Assert.That(page.Events[0].EventId).IsNotEqualTo(page.Events[1].EventId);
        await Assert.That(page.Events.Any(e => e.CausedByOperationId == spoofed.OperationId)).IsFalse();
        var finalState = DecodeCrdtEventState(page.Events[CrdtDoubleCount]);
        await Assert.That(finalState.GCounterComponents[Client]).IsEqualTo(CrdtFirstComponent);
        await Assert.That(finalState.GCounterComponents[CrdtOtherClient]).IsEqualTo(CrdtOtherRaisedComponent);
    }

    /// <summary>Verifies malformed CRDT payloads are rejected with stable reasons and produce no event or state change.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithCrdtRegistrationRejectsInvalidPayloadsWithoutEventsOrStateChange()
    {
        using var database = new SqliteLease();
        var materializer = new CrdtStateMaterializer();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, CrdtOptions(composeFromParts: true, materializer));
        var accepted = CrdtOperation(1, 1, CrdtCounter(Client, CrdtFirstComponent));
        var valid = CrdtCounter(Client, CrdtSpoofedComponent);
        var garbage = new byte[] { 0xFF, 0xFE, 0xFD };
        var badHash = CrdtOperation(CrdtSecondSequence, CrdtSecondSequence, valid with { PayloadHash = CrdtPayloadHash(garbage) });
        var invalidPayload = CrdtOperation(
            CrdtThirdSequence,
            CrdtThirdSequence,
            new(CrdtContracts.InputContractId, CrdtContracts.SchemaVersion, CrdtServerPayloads.ContentType, garbage, CrdtPayloadHash(garbage)));
        var wrongContract = CrdtOperation(CrdtFourthSequence, CrdtFourthSequence, valid with { ContractId = CrdtForeignContract });
        var wrongKind = CrdtOperation(
            CrdtFifthSequence,
            CrdtFifthSequence,
            CrdtServerPayloads.CreateInput(CrdtInput.ForMutation(CrdtMutation.PNCounterSet(Client, CrdtSpoofedComponent, 0))));
        var later = CrdtOperation(CrdtSixthSequence, CrdtSixthSequence, CrdtCounter(Client, CrdtRaisedComponent));
        var subscriptionId = SubscriptionId.New();

        await AssertCrdtAcceptedAsync(await hub.ApplyOperationsAsync(Batch(accepted), new(Tenant, Client), CancellationToken.None), CrdtFirstVersion);
        await AssertCrdtRejectedAsync(await hub.ApplyOperationsAsync(Batch(badHash), new(Tenant, Client), CancellationToken.None), CrdtHashMismatchReason, CrdtFirstVersion);
        await AssertCrdtRejectedAsync(await hub.ApplyOperationsAsync(Batch(invalidPayload), new(Tenant, Client), CancellationToken.None), CrdtInvalidMutationReason, CrdtFirstVersion);
        await AssertCrdtRejectedAsync(await hub.ApplyOperationsAsync(Batch(wrongContract), new(Tenant, Client), CancellationToken.None), CrdtContractMismatchReason, CrdtFirstVersion);
        await AssertCrdtRejectedAsync(await hub.ApplyOperationsAsync(Batch(wrongKind), new(Tenant, Client), CancellationToken.None), CrdtKindMismatchReason, CrdtFirstVersion);
        var afterRejections = await ReadCrdtPageAsync(hub, new(Tenant, Client), subscriptionId, null);
        await AssertCrdtAcceptedAsync(await hub.ApplyOperationsAsync(Batch(later), new(Tenant, Client), CancellationToken.None), CrdtSecondVersion);
        var recovered = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            CrdtSnapshotRequest(subscriptionId, afterRejections.NextCursor),
            new(Tenant, Client),
            CancellationToken.None);
        var all = await ReadCrdtPageAsync(hub, new(Tenant, Client), SubscriptionId.New(), null);

        await Assert.That(afterRejections.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(afterRejections.Events[0].CausedByOperationId).IsEqualTo(accepted.OperationId);
        await Assert.That(all.Events).Count().IsEqualTo(CrdtDoubleCount);
        await Assert.That(all.Events[0].CausedByOperationId).IsEqualTo(accepted.OperationId);
        await Assert.That(all.Events[1].CausedByOperationId).IsEqualTo(later.OperationId);
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint?.ServerVersion).IsEqualTo(CrdtSecondVersion);
        var state = DecodeCrdtCheckpointState(recovered);
        await Assert.That(state.Value.Counter).IsEqualTo(CrdtRaisedComponent);
        await Assert.That(state.GCounterComponents).Count().IsEqualTo(SingleCount);
        await Assert.That(state.PNCounterPositiveComponents).IsEmpty();
    }

    /// <summary>Verifies a CRDT snapshot materializer returns merged state and a frontier that resumes without duplicate events.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The recovered snapshot has no frontier cursor.</exception>
    [Test]
    public async Task GetSnapshotAsyncWithCrdtMaterializerReturnsMergedStateAndResumesWithoutDuplicates()
    {
        using var database = new SqliteLease();
        var materializer = new CrdtStateMaterializer();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, CrdtOptions(composeFromParts: true, materializer));
        var subscriptionId = SubscriptionId.New();
        var first = CrdtOperation(1, 1, CrdtCounter(Client, CrdtFirstComponent));
        var second = CrdtOperation(CrdtSecondSequence, 1, CrdtCounter(CrdtOtherClient, CrdtOtherComponent));
        var third = CrdtOperation(CrdtThirdSequence, CrdtSecondSequence, CrdtCounter(Client, CrdtRaisedComponent));
        _ = await hub.ApplyOperationsAsync(Batch(first), new(Tenant, Client), CancellationToken.None);
        var expired = await ReadCrdtPageAsync(hub, new(Tenant, Client), subscriptionId, null);
        _ = await hub.ApplyOperationsAsync(Batch(second), new(Tenant, CrdtOtherClient), CancellationToken.None);

        var recovered = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            CrdtSnapshotRequest(subscriptionId, expired.NextCursor),
            new(Tenant, Client),
            CancellationToken.None);
        var frontier = recovered.Checkpoint?.FrontierCursor ?? throw new InvalidOperationException("A recovered CRDT snapshot must include a frontier cursor.");
        await hub.AcknowledgeAsync(new(subscriptionId, CrdtStream, frontier), new(Tenant, Client), CancellationToken.None);
        var thirdResult = await hub.ApplyOperationsAsync(Batch(third), new(Tenant, Client), CancellationToken.None);
        var resumed = await ReadCrdtPageAsync(hub, new(Tenant, Client), subscriptionId, frontier);

        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint?.ServerVersion).IsEqualTo(CrdtSecondVersion);
        await Assert.That(recovered.Checkpoint?.ClientState.ContractId).IsEqualTo(CrdtContracts.StateContractId);
        await Assert.That(materializer.CallCount).IsEqualTo(SingleCount);
        var state = DecodeCrdtCheckpointState(recovered);
        await Assert.That(state.Kind).IsEqualTo(CrdtKind.GCounter);
        await Assert.That(state.GCounterComponents[Client]).IsEqualTo(CrdtFirstComponent);
        await Assert.That(state.GCounterComponents[CrdtOtherClient]).IsEqualTo(CrdtOtherComponent);
        await Assert.That(state.Value.Counter).IsEqualTo(CrdtFirstComponent + CrdtOtherComponent);
        await Assert.That(resumed.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(resumed.Events[0].CausedByOperationId).IsEqualTo(third.OperationId);
        await Assert.That(resumed.Events[0].EventId).IsEqualTo(thirdResult.ProducedEvents[0].EventId);
        await Assert.That(DecodeCrdtEventState(resumed.Events[0]).Value.Counter).IsEqualTo(CrdtRaisedComponent + CrdtOtherComponent);
    }

    /// <summary>Verifies a duplicate CRDT operation after SQLite reopen returns the original result without a second effect.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithCrdtRegistrationReplaysDuplicateOperationAfterSqliteReopen()
    {
        using var database = new SqliteLease();
        var operation = CrdtOperation(1, 1, CrdtCounter(Client, CrdtFirstComponent));
        var next = CrdtOperation(CrdtSecondSequence, CrdtSecondSequence, CrdtCounter(Client, CrdtRaisedComponent));
        ServerSyncResult original;
        await using (var hub = ServerStreamHub.CreateSqlite(database.Path, CrdtOptions(composeFromParts: true)))
        {
            original = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        }

        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, CrdtOptions(composeFromParts: true));
        var replay = await reopened.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        var nextResult = await reopened.ApplyOperationsAsync(Batch(next), new(Tenant, Client), CancellationToken.None);
        var page = await ReadCrdtPageAsync(reopened, new(Tenant, Client), SubscriptionId.New(), null);

        await AssertCrdtAcceptedAsync(original, CrdtFirstVersion);
        await AssertCrdtAcceptedAsync(replay, CrdtFirstVersion);
        await AssertCrdtAcceptedAsync(nextResult, CrdtSecondVersion);
        await AssertCrdtEventsEqualAsync(original.ProducedEvents, replay.ProducedEvents);
        await Assert.That(page.Events).Count().IsEqualTo(CrdtDoubleCount);
        await Assert.That(page.Events[0].EventId).IsEqualTo(original.ProducedEvents[0].EventId);
        await Assert.That(page.Events[1].CausedByOperationId).IsEqualTo(next.OperationId);
        await Assert.That(DecodeCrdtEventState(page.Events[1]).Value.Counter).IsEqualTo(CrdtRaisedComponent);
    }

    /// <summary>Creates hub options that register the built-in CRDT stream.</summary>
    /// <param name="composeFromParts">Whether to compose the public CRDT parts directly instead of using the registration factory.</param>
    /// <param name="materializer">The optional snapshot materializer.</param>
    /// <returns>The hub options.</returns>
    private static ServerStreamHubOptions CrdtOptions(bool composeFromParts, IServerSnapshotMaterializer? materializer = null) =>
        new()
        {
            AuthorizationPolicy = new AllowPolicy(Tenant),
            ConflictHandler = new() { Streams = [composeFromParts ? CrdtRegistrationFromParts() : CrdtServerStreamRegistration.Create(new() { StreamId = CrdtStream, Kind = CrdtKind.GCounter })] },
            SnapshotRecoveryAuthorizationPolicy = materializer is null ? null : new AllowSnapshotRecoveryPolicy(Tenant),
            SnapshotRecoveryMaterializer = materializer,
            EmptyPollDelay = TimeSpan.FromMilliseconds(PollDelayMilliseconds),
            TimeProvider = new FixedTimeProvider(Start),
            JournalLimits = new() { OperationRetention = TimeSpan.FromMinutes(OperationRetentionMinutes), SubscriptionRetention = TimeSpan.FromMinutes(SubscriptionRetentionMinutes) },
        };

    /// <summary>Composes a CRDT stream registration from the public resolver, domain handler, initial state and version factory types.</summary>
    /// <returns>The stream registration.</returns>
    private static ServerConflictStreamRegistration CrdtRegistrationFromParts()
    {
        var resolver = new CrdtResolver(new() { Kind = CrdtKind.GCounter, VersionFactory = new CrdtSequentialVersionFactory(new()) });
        return new()
        {
            StreamId = CrdtStream,
            InitialStateFactory = new CrdtInitialStateFactory(new() { Kind = CrdtKind.GCounter }),
            LastWriterWinsResolver = resolver,
            MergeResolver = resolver,
            CustomResolver = resolver,
            DomainHandler = new CrdtServerDomainHandler(new() { Kind = CrdtKind.GCounter }),
        };
    }

    /// <summary>Creates a CRDT operation for the hub stream.</summary>
    /// <param name="seed">The deterministic operation identifier seed.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="payload">The operation payload.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CrdtOperation(int seed, long sequence, PayloadEnvelope payload) =>
        new()
        {
            OperationId = new(new Guid(seed, CrdtOperationDiscriminator, 0, [0, 0, 0, 0, 0, 0, 0, 1])),
            StreamId = CrdtStream,
            ClientSequence = sequence,
            TimestampUtc = Start,
            BaseVersion = null,
            Type = SyncOperationType.Update,
            Payload = payload,
        };

    /// <summary>Creates a grow-only counter mutation payload.</summary>
    /// <param name="actor">The mutation actor.</param>
    /// <param name="component">The component value.</param>
    /// <returns>The payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CrdtCounter(string actor, long component) =>
        CrdtServerPayloads.CreateInput(CrdtInput.ForMutation(CrdtMutation.GCounterSet(actor, component)));

    /// <summary>Computes a well-formed payload hash for arbitrary bytes.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The encoded payload hash.</returns>
    private static string CrdtPayloadHash(byte[] payload) => $"sha256-{Convert.ToBase64String(SHA256.HashData(payload))}";

    /// <summary>Creates a snapshot recovery request for the CRDT stream state contract.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="expiredCursor">The expired cursor.</param>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest CrdtSnapshotRequest(SubscriptionId subscriptionId, string? expiredCursor) =>
        new()
        {
            StreamId = CrdtStream,
            SubscriptionId = subscriptionId,
            ExpiredCursor = expiredCursor,
            ClientStateContractId = CrdtContracts.StateContractId,
            ClientStateSchemaVersion = CrdtContracts.SchemaVersion,
            SnapshotFormatVersion = SingleCount,
            PendingOperations = [],
            ReplayOperations = [],
            MaximumResponseBytes = CrdtSnapshotMaximumResponseBytes,
        };

    /// <summary>Reads one CRDT subscription page with a bounded wait.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="client">The authenticated client.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="resumeCursor">The optional resume cursor.</param>
    /// <returns>The page.</returns>
    /// <exception cref="InvalidOperationException">No page is available.</exception>
    private static async Task<RemoteEventBatch> ReadCrdtPageAsync(
        ServerStreamHub hub,
        ServerAuthenticatedClient client,
        SubscriptionId subscriptionId,
        string? resumeCursor)
    {
        var enumerable = hub.SubscribeStreamAsync(
            new(CrdtStream, subscriptionId, resumeCursor, StartPosition.FromSequence(0)),
            client,
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var hasPage = await enumerator.MoveNextAsync().AsTask().WaitAsync(CrdtGuardTimeout);
        return hasPage ? enumerator.Current : throw new InvalidOperationException("A CRDT subscription page must be available.");
    }

    /// <summary>Extracts the numeric sequence from a server cursor.</summary>
    /// <param name="cursor">The server cursor.</param>
    /// <returns>The cursor sequence.</returns>
    private static long CrdtCursorSequence(string cursor)
    {
        var segments = cursor.Split(':');
        return long.Parse(segments[^CrdtDoubleCount], CultureInfo.InvariantCulture);
    }

    /// <summary>Decodes the authoritative CRDT state carried by a canonical event.</summary>
    /// <param name="remoteEvent">The event.</param>
    /// <returns>The CRDT state.</returns>
    /// <exception cref="InvalidOperationException">The event does not carry authoritative state.</exception>
    private static CrdtState DecodeCrdtEventState(RemoteEvent remoteEvent)
    {
        var input = CrdtCodec.DecodeInput(remoteEvent.Payload.Payload);
        return input.Kind == CrdtInputKind.AuthoritativeState && input.State is { } state
            ? state
            : throw new InvalidOperationException("A CRDT event must carry authoritative state.");
    }

    /// <summary>Decodes the CRDT state materialized into a recovered checkpoint.</summary>
    /// <param name="result">The recovery result.</param>
    /// <returns>The CRDT state.</returns>
    /// <exception cref="InvalidOperationException">The result has no checkpoint.</exception>
    private static CrdtState DecodeCrdtCheckpointState(RemoteSnapshotRecoveryResult result)
    {
        var checkpoint = result.Checkpoint ?? throw new InvalidOperationException("A recovered CRDT snapshot must include a checkpoint.");
        return CrdtCodec.DecodeState(checkpoint.ClientState.Payload);
    }

    /// <summary>Asserts a single accepted CRDT operation and its single canonical event.</summary>
    /// <param name="result">The sync result.</param>
    /// <param name="version">The expected server version.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertCrdtAcceptedAsync(ServerSyncResult result, string version)
    {
        await Assert.That(result.Result.Operations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(result.Result.Operations[0].ServerVersion).IsEqualTo(version);
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(result.ProducedEvents[0].StreamId).IsEqualTo(CrdtStream);
    }

    /// <summary>Asserts a single rejected CRDT operation that produced no event.</summary>
    /// <param name="result">The sync result.</param>
    /// <param name="reason">The expected stable reason.</param>
    /// <param name="version">The expected unchanged server version.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertCrdtRejectedAsync(ServerSyncResult result, string reason, string version)
    {
        await Assert.That(result.Result.Operations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsEqualTo(reason);
        await Assert.That(result.Result.Operations[0].ServerVersion).IsEqualTo(version);
        await Assert.That(result.ProducedEvents).IsEmpty();
    }

    /// <summary>Asserts event provenance.</summary>
    /// <param name="remoteEvent">The event.</param>
    /// <param name="clientId">The expected client.</param>
    /// <param name="operationId">The expected causing operation.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertCrdtOriginAsync(RemoteEvent remoteEvent, string clientId, OperationId operationId)
    {
        await Assert.That(remoteEvent.CausedByOperationId).IsEqualTo(operationId);
        await Assert.That(remoteEvent.Origin?.ClientId).IsEqualTo(clientId);
        await Assert.That(remoteEvent.Origin?.OperationId).IsEqualTo(operationId);
    }

    /// <summary>Asserts two canonical event lists are identical.</summary>
    /// <param name="expected">The expected events.</param>
    /// <param name="actual">The actual events.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertCrdtEventsEqualAsync(IReadOnlyList<RemoteEvent> expected, IReadOnlyList<RemoteEvent> actual)
    {
        await Assert.That(actual).Count().IsEqualTo(expected.Count);
        for (var index = 0; index < expected.Count; index++)
        {
            await Assert.That(actual[index].EventId).IsEqualTo(expected[index].EventId);
            await Assert.That(actual[index].StreamId).IsEqualTo(expected[index].StreamId);
            await Assert.That(actual[index].ServerCursor).IsEqualTo(expected[index].ServerCursor);
            await Assert.That(actual[index].CommittedAtUtc).IsEqualTo(expected[index].CommittedAtUtc);
            await Assert.That(actual[index].CausedByOperationId).IsEqualTo(expected[index].CausedByOperationId);
            await Assert.That(actual[index].Origin).IsEqualTo(expected[index].Origin);
            await Assert.That(actual[index].Payload.ContractId).IsEqualTo(expected[index].Payload.ContractId);
            await Assert.That(actual[index].Payload.PayloadHash).IsEqualTo(expected[index].Payload.PayloadHash);
            await Assert.That(actual[index].Payload.Payload.ToArray().SequenceEqual(expected[index].Payload.Payload.ToArray())).IsTrue();
        }
    }

    /// <summary>Materializes captured CRDT server state into the CRDT state contract.</summary>
    private sealed class CrdtStateMaterializer : IServerSnapshotMaterializer
    {
        /// <summary>The number of materialization calls.</summary>
        private int _callCount;

        /// <summary>Gets the number of materialization calls.</summary>
        internal int CallCount => Volatile.Read(ref _callCount);

        /// <inheritdoc/>
        public ValueTask<ServerSnapshotMaterializationResult> MaterializeAsync(
            ServerSnapshotMaterializationContext context,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _ = Interlocked.Increment(ref _callCount);
            if (!string.Equals(context.ClientStateContractId, CrdtContracts.StateContractId, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(new ServerSnapshotMaterializationResult { Status = ServerSnapshotMaterializationStatus.UnsupportedProjection });
            }

            var state = CrdtCodec.DecodeState(context.CapturedServerState.State.Payload);
            var clientState = CrdtServerPayloads.CreateState(state);
            return ValueTask.FromResult(new ServerSnapshotMaterializationResult { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = clientState });
        }
    }
}
