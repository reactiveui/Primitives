// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>CRDT composition tests for <see cref="ServerStreamHub"/> through its public surface.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The second authenticated client used by CRDT provenance tests.</summary>
    private const string MergeableOtherClient = "client-b";

    /// <summary>The first committed CRDT server version.</summary>
    private const string MergeableFirstVersion = "crdt-v1";

    /// <summary>The second committed CRDT server version.</summary>
    private const string MergeableSecondVersion = "crdt-v2";

    /// <summary>The third committed CRDT server version.</summary>
    private const string MergeableThirdVersion = "crdt-v3";

    /// <summary>The stable reason for a CRDT payload whose hash does not match its bytes.</summary>
    private const string MergeableHashMismatchReason = "crdt-payload-hash-mismatch";

    /// <summary>The stable reason for a CRDT payload that cannot be decoded or applied.</summary>
    private const string MergeableInvalidMutationReason = "crdt-invalid-mutation";

    /// <summary>The stable reason for a CRDT payload with the wrong contract metadata.</summary>
    private const string MergeableContractMismatchReason = "crdt-contract-mismatch";

    /// <summary>The stable reason for a CRDT mutation that targets another CRDT kind.</summary>
    private const string MergeableKindMismatchReason = "crdt-state-kind-mismatch";

    /// <summary>The foreign payload contract used by CRDT rejection tests.</summary>
    private const string MergeableForeignContract = "contract-foreign";

    /// <summary>The first counter component written by the first client.</summary>
    private const long MergeableFirstComponent = 5;

    /// <summary>The counter component written by the second client.</summary>
    private const long MergeableOtherComponent = 2;

    /// <summary>The raised counter component written by the first client.</summary>
    private const long MergeableRaisedComponent = 7;

    /// <summary>The counter component written by the second client's later operation.</summary>
    private const long MergeableOtherRaisedComponent = 3;

    /// <summary>The spoofed counter component a client tries to write for another actor.</summary>
    private const long MergeableSpoofedComponent = 100;

    /// <summary>The second operation sequence.</summary>
    private const int MergeableSecondSequence = 2;

    /// <summary>The third operation sequence.</summary>
    private const int MergeableThirdSequence = 3;

    /// <summary>The fourth operation sequence.</summary>
    private const int MergeableFourthSequence = 4;

    /// <summary>The fifth operation sequence.</summary>
    private const int MergeableFifthSequence = 5;

    /// <summary>The sixth operation sequence.</summary>
    private const int MergeableSixthSequence = 6;

    /// <summary>The expected double item count.</summary>
    private const int MergeableDoubleCount = 2;

    /// <summary>The expected triple item count.</summary>
    private const int MergeableTripleCount = 3;

    /// <summary>The operation identifier discriminator that keeps CRDT identifiers distinct from other hub tests.</summary>
    private const short MergeableOperationDiscriminator = 0x0C;

    /// <summary>The maximum snapshot response bytes requested by CRDT recovery tests.</summary>
    private const int MergeableSnapshotMaximumResponseBytes = 16_384;

    /// <summary>The CRDT stream used by hub composition tests.</summary>
    private static readonly StreamId MergeableStream = new("crdt/hub");

    /// <summary>The bounded wait used for every subscription read.</summary>
    private static readonly TimeSpan MergeableGuardTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Verifies accepted CRDT operations publish canonical events that read back identically after reopening SQLite.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithMergeableRegistrationPublishesCanonicalEventsThatSurviveSqliteReopen()
    {
        using var database = new SqliteLease();
        var first = MergeableOperation(1, 1, MergeableCounter(Client, MergeableFirstComponent));
        var second = MergeableOperation(MergeableSecondSequence, 1, MergeableCounter(MergeableOtherClient, MergeableOtherComponent));
        var third = MergeableOperation(MergeableThirdSequence, MergeableSecondSequence, MergeableCounter(Client, MergeableRaisedComponent));
        var produced = new List<RemoteEvent>();
        RemoteEventBatch before;
        await using (var hub = ServerStreamHub.CreateSqlite(database.Path, MergeableOptions(composeFromParts: true)))
        {
            var firstResult = await hub.ApplyOperationsAsync(Batch(first), new(Tenant, Client), CancellationToken.None);
            var secondResult = await hub.ApplyOperationsAsync(Batch(second), new(Tenant, MergeableOtherClient), CancellationToken.None);
            var thirdResult = await hub.ApplyOperationsAsync(Batch(third), new(Tenant, Client), CancellationToken.None);
            await AssertMergeableAcceptedAsync(firstResult, MergeableFirstVersion);
            await AssertMergeableAcceptedAsync(secondResult, MergeableSecondVersion);
            await AssertMergeableAcceptedAsync(thirdResult, MergeableThirdVersion);
            produced.AddRange(firstResult.ProducedEvents);
            produced.AddRange(secondResult.ProducedEvents);
            produced.AddRange(thirdResult.ProducedEvents);
            before = await ReadMergeablePageAsync(hub, new(Tenant, Client), SubscriptionId.New(), null);
        }

        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, MergeableOptions(composeFromParts: false));
        var after = await ReadMergeablePageAsync(reopened, new(Tenant, Client), SubscriptionId.New(), null);

        await Assert.That(before.Events).Count().IsEqualTo(MergeableTripleCount);
        await Assert.That(MergeableCursorSequence(before.Events[0].ServerCursor)).IsLessThan(MergeableCursorSequence(before.Events[1].ServerCursor));
        await Assert.That(MergeableCursorSequence(before.Events[1].ServerCursor)).IsLessThan(MergeableCursorSequence(before.Events[MergeableDoubleCount].ServerCursor));
        await AssertMergeableEventsEqualAsync(produced, before.Events);
        await AssertMergeableEventsEqualAsync(before.Events, after.Events);
        await Assert.That(before.Events[0].CausedByOperationId).IsEqualTo(first.OperationId);
        await Assert.That(before.Events[1].CausedByOperationId).IsEqualTo(second.OperationId);
        await Assert.That(before.Events[MergeableDoubleCount].CausedByOperationId).IsEqualTo(third.OperationId);
        await Assert.That(DecodeMergeableEventState(after.Events[0]).Value.Counter).IsEqualTo(MergeableFirstComponent);
        await Assert.That(DecodeMergeableEventState(after.Events[1]).Value.Counter).IsEqualTo(MergeableFirstComponent + MergeableOtherComponent);
        await Assert.That(DecodeMergeableEventState(after.Events[MergeableDoubleCount]).Value.Counter).IsEqualTo(MergeableRaisedComponent + MergeableOtherComponent);
    }

    /// <summary>Verifies CRDT events carry the authenticated client and causing operation, and never borrow another client's identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithMergeableRegistrationAttributesEventsToAuthenticatedClient()
    {
        using var database = new SqliteLease();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, MergeableOptions(composeFromParts: true));
        var clientOperation = MergeableOperation(1, 1, MergeableCounter(Client, MergeableFirstComponent));
        var sharedIdentifier = clientOperation with { Payload = MergeableCounter(MergeableOtherClient, MergeableOtherComponent) };
        var spoofed = MergeableOperation(MergeableSecondSequence, MergeableSecondSequence, MergeableCounter(Client, MergeableSpoofedComponent));
        var otherOperation = MergeableOperation(MergeableThirdSequence, MergeableThirdSequence, MergeableCounter(MergeableOtherClient, MergeableOtherRaisedComponent));

        var clientResult = await hub.ApplyOperationsAsync(Batch(clientOperation), new(Tenant, Client), CancellationToken.None);
        var sharedResult = await hub.ApplyOperationsAsync(Batch(sharedIdentifier), new(Tenant, MergeableOtherClient), CancellationToken.None);
        var spoofedResult = await hub.ApplyOperationsAsync(Batch(spoofed), new(Tenant, MergeableOtherClient), CancellationToken.None);
        var otherResult = await hub.ApplyOperationsAsync(Batch(otherOperation), new(Tenant, MergeableOtherClient), CancellationToken.None);
        var page = await ReadMergeablePageAsync(hub, new(Tenant, Client), SubscriptionId.New(), null);

        await AssertMergeableAcceptedAsync(clientResult, MergeableFirstVersion);
        await AssertMergeableAcceptedAsync(sharedResult, MergeableSecondVersion);
        await AssertMergeableRejectedAsync(spoofedResult, MergeableInvalidMutationReason, MergeableSecondVersion);
        await AssertMergeableAcceptedAsync(otherResult, MergeableThirdVersion);
        await Assert.That(page.Events).Count().IsEqualTo(MergeableTripleCount);
        await AssertMergeableOriginAsync(page.Events[0], Client, clientOperation.OperationId);
        await AssertMergeableOriginAsync(page.Events[1], MergeableOtherClient, clientOperation.OperationId);
        await AssertMergeableOriginAsync(page.Events[MergeableDoubleCount], MergeableOtherClient, otherOperation.OperationId);
        await Assert.That(page.Events[0].EventId).IsNotEqualTo(page.Events[1].EventId);
        await Assert.That(page.Events.Any(e => e.CausedByOperationId == spoofed.OperationId)).IsFalse();
        var finalState = DecodeMergeableEventState(page.Events[MergeableDoubleCount]);
        await Assert.That(finalState.IncrementOnlyTotalsByClient[Client]).IsEqualTo(MergeableFirstComponent);
        await Assert.That(finalState.IncrementOnlyTotalsByClient[MergeableOtherClient]).IsEqualTo(MergeableOtherRaisedComponent);
    }

    /// <summary>Verifies malformed CRDT payloads are rejected with stable reasons and produce no event or state change.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithMergeableRegistrationRejectsInvalidPayloadsWithoutEventsOrStateChange()
    {
        using var database = new SqliteLease();
        var materializer = new MergeableStateMaterializer();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, MergeableOptions(composeFromParts: true, materializer));
        var accepted = MergeableOperation(1, 1, MergeableCounter(Client, MergeableFirstComponent));
        var valid = MergeableCounter(Client, MergeableSpoofedComponent);
        var garbage = new byte[] { 0xFF, 0xFE, 0xFD };
        var badHash = MergeableOperation(MergeableSecondSequence, MergeableSecondSequence, valid with { PayloadHash = MergeablePayloadHash(garbage) });
        var invalidPayload = MergeableOperation(
            MergeableThirdSequence,
            MergeableThirdSequence,
            new(MergeableContracts.InputContractId, MergeableContracts.SchemaVersion, MergeableServerPayloads.ContentType, garbage, MergeablePayloadHash(garbage)));
        var wrongContract = MergeableOperation(MergeableFourthSequence, MergeableFourthSequence, valid with { ContractId = MergeableForeignContract });
        var wrongKind = MergeableOperation(
            MergeableFifthSequence,
            MergeableFifthSequence,
            MergeableServerPayloads.CreateInput(MergeableInput.ForChange(MergeableChange.CounterSet(Client, MergeableSpoofedComponent, 0))));
        var later = MergeableOperation(MergeableSixthSequence, MergeableSixthSequence, MergeableCounter(Client, MergeableRaisedComponent));
        var subscriptionId = SubscriptionId.New();

        await AssertMergeableAcceptedAsync(await hub.ApplyOperationsAsync(Batch(accepted), new(Tenant, Client), CancellationToken.None), MergeableFirstVersion);
        await AssertMergeableRejectedAsync(await hub.ApplyOperationsAsync(Batch(badHash), new(Tenant, Client), CancellationToken.None), MergeableHashMismatchReason, MergeableFirstVersion);
        await AssertMergeableRejectedAsync(await hub.ApplyOperationsAsync(Batch(invalidPayload), new(Tenant, Client), CancellationToken.None), MergeableInvalidMutationReason, MergeableFirstVersion);
        await AssertMergeableRejectedAsync(await hub.ApplyOperationsAsync(Batch(wrongContract), new(Tenant, Client), CancellationToken.None), MergeableContractMismatchReason, MergeableFirstVersion);
        await AssertMergeableRejectedAsync(await hub.ApplyOperationsAsync(Batch(wrongKind), new(Tenant, Client), CancellationToken.None), MergeableKindMismatchReason, MergeableFirstVersion);
        var afterRejections = await ReadMergeablePageAsync(hub, new(Tenant, Client), subscriptionId, null);
        await AssertMergeableAcceptedAsync(await hub.ApplyOperationsAsync(Batch(later), new(Tenant, Client), CancellationToken.None), MergeableSecondVersion);
        var recovered = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            MergeableSnapshotRequest(subscriptionId, afterRejections.NextCursor),
            new(Tenant, Client),
            CancellationToken.None);
        var all = await ReadMergeablePageAsync(hub, new(Tenant, Client), SubscriptionId.New(), null);

        await Assert.That(afterRejections.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(afterRejections.Events[0].CausedByOperationId).IsEqualTo(accepted.OperationId);
        await Assert.That(all.Events).Count().IsEqualTo(MergeableDoubleCount);
        await Assert.That(all.Events[0].CausedByOperationId).IsEqualTo(accepted.OperationId);
        await Assert.That(all.Events[1].CausedByOperationId).IsEqualTo(later.OperationId);
        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint?.ServerVersion).IsEqualTo(MergeableSecondVersion);
        var state = DecodeMergeableCheckpointState(recovered);
        await Assert.That(state.Value.Counter).IsEqualTo(MergeableRaisedComponent);
        await Assert.That(state.IncrementOnlyTotalsByClient).Count().IsEqualTo(SingleCount);
        await Assert.That(state.IncrementsByClient).IsEmpty();
    }

    /// <summary>Verifies a CRDT snapshot materializer returns merged state and a frontier that resumes without duplicate events.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The recovered snapshot has no frontier cursor.</exception>
    [Test]
    public async Task GetSnapshotAsyncWithMergeableMaterializerReturnsMergedStateAndResumesWithoutDuplicates()
    {
        using var database = new SqliteLease();
        var materializer = new MergeableStateMaterializer();
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, MergeableOptions(composeFromParts: true, materializer));
        var subscriptionId = SubscriptionId.New();
        var first = MergeableOperation(1, 1, MergeableCounter(Client, MergeableFirstComponent));
        var second = MergeableOperation(MergeableSecondSequence, 1, MergeableCounter(MergeableOtherClient, MergeableOtherComponent));
        var third = MergeableOperation(MergeableThirdSequence, MergeableSecondSequence, MergeableCounter(Client, MergeableRaisedComponent));
        _ = await hub.ApplyOperationsAsync(Batch(first), new(Tenant, Client), CancellationToken.None);
        var expired = await ReadMergeablePageAsync(hub, new(Tenant, Client), subscriptionId, null);
        _ = await hub.ApplyOperationsAsync(Batch(second), new(Tenant, MergeableOtherClient), CancellationToken.None);

        var recovered = await ((IServerSnapshotRecoveryHub)hub).GetSnapshotAsync(
            MergeableSnapshotRequest(subscriptionId, expired.NextCursor),
            new(Tenant, Client),
            CancellationToken.None);
        var frontier = recovered.Checkpoint?.FrontierCursor ?? throw new InvalidOperationException("A recovered CRDT snapshot must include a frontier cursor.");
        await hub.AcknowledgeAsync(new(subscriptionId, MergeableStream, frontier), new(Tenant, Client), CancellationToken.None);
        var thirdResult = await hub.ApplyOperationsAsync(Batch(third), new(Tenant, Client), CancellationToken.None);
        var resumed = await ReadMergeablePageAsync(hub, new(Tenant, Client), subscriptionId, frontier);

        await Assert.That(recovered.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(recovered.Checkpoint?.ServerVersion).IsEqualTo(MergeableSecondVersion);
        await Assert.That(recovered.Checkpoint?.ClientState.ContractId).IsEqualTo(MergeableContracts.StateContractId);
        await Assert.That(materializer.CallCount).IsEqualTo(SingleCount);
        var state = DecodeMergeableCheckpointState(recovered);
        await Assert.That(state.Kind).IsEqualTo(MergeableKind.IncrementOnlyCounter);
        await Assert.That(state.IncrementOnlyTotalsByClient[Client]).IsEqualTo(MergeableFirstComponent);
        await Assert.That(state.IncrementOnlyTotalsByClient[MergeableOtherClient]).IsEqualTo(MergeableOtherComponent);
        await Assert.That(state.Value.Counter).IsEqualTo(MergeableFirstComponent + MergeableOtherComponent);
        await Assert.That(resumed.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(resumed.Events[0].CausedByOperationId).IsEqualTo(third.OperationId);
        await Assert.That(resumed.Events[0].EventId).IsEqualTo(thirdResult.ProducedEvents[0].EventId);
        await Assert.That(DecodeMergeableEventState(resumed.Events[0]).Value.Counter).IsEqualTo(MergeableRaisedComponent + MergeableOtherComponent);
    }

    /// <summary>Verifies a duplicate CRDT operation after SQLite reopen returns the original result without a second effect.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithMergeableRegistrationReplaysDuplicateOperationAfterSqliteReopen()
    {
        using var database = new SqliteLease();
        var operation = MergeableOperation(1, 1, MergeableCounter(Client, MergeableFirstComponent));
        var next = MergeableOperation(MergeableSecondSequence, MergeableSecondSequence, MergeableCounter(Client, MergeableRaisedComponent));
        ServerSyncResult original;
        await using (var hub = ServerStreamHub.CreateSqlite(database.Path, MergeableOptions(composeFromParts: true)))
        {
            original = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        }

        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, MergeableOptions(composeFromParts: true));
        var replay = await reopened.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        var nextResult = await reopened.ApplyOperationsAsync(Batch(next), new(Tenant, Client), CancellationToken.None);
        var page = await ReadMergeablePageAsync(reopened, new(Tenant, Client), SubscriptionId.New(), null);

        await AssertMergeableAcceptedAsync(original, MergeableFirstVersion);
        await AssertMergeableAcceptedAsync(replay, MergeableFirstVersion);
        await AssertMergeableAcceptedAsync(nextResult, MergeableSecondVersion);
        await AssertMergeableEventsEqualAsync(original.ProducedEvents, replay.ProducedEvents);
        await Assert.That(page.Events).Count().IsEqualTo(MergeableDoubleCount);
        await Assert.That(page.Events[0].EventId).IsEqualTo(original.ProducedEvents[0].EventId);
        await Assert.That(page.Events[1].CausedByOperationId).IsEqualTo(next.OperationId);
        await Assert.That(DecodeMergeableEventState(page.Events[1]).Value.Counter).IsEqualTo(MergeableRaisedComponent);
    }

    /// <summary>Creates hub options that register the built-in CRDT stream.</summary>
    /// <param name="composeFromParts">Whether to compose the public CRDT parts directly instead of using the registration factory.</param>
    /// <param name="materializer">The optional snapshot materializer.</param>
    /// <returns>The hub options.</returns>
    private static ServerStreamHubOptions MergeableOptions(bool composeFromParts, IServerSnapshotMaterializer? materializer = null) =>
        new()
        {
            AuthorizationPolicy = new AllowPolicy(Tenant),
            ConflictHandler = new() { Streams = [composeFromParts ? MergeableRegistrationFromParts() : MergeableServerStreamRegistration.Create(new() { StreamId = MergeableStream, Kind = MergeableKind.IncrementOnlyCounter })] },
            SnapshotRecoveryAuthorizationPolicy = materializer is null ? null : new AllowSnapshotRecoveryPolicy(Tenant),
            SnapshotRecoveryMaterializer = materializer,
            EmptyPollDelay = TimeSpan.FromMilliseconds(PollDelayMilliseconds),
            TimeProvider = new FixedTimeProvider(Start),
            JournalLimits = new() { OperationRetention = TimeSpan.FromMinutes(OperationRetentionMinutes), SubscriptionRetention = TimeSpan.FromMinutes(SubscriptionRetentionMinutes) },
        };

    /// <summary>Composes a CRDT stream registration from the public resolver, domain handler, initial state and version factory types.</summary>
    /// <returns>The stream registration.</returns>
    private static ServerConflictStreamRegistration MergeableRegistrationFromParts()
    {
        var resolver = new MergeableResolver(new() { Kind = MergeableKind.IncrementOnlyCounter, VersionFactory = new MergeableSequentialVersionFactory(new()) });
        return new()
        {
            StreamId = MergeableStream,
            InitialStateFactory = new MergeableInitialStateFactory(new() { Kind = MergeableKind.IncrementOnlyCounter }),
            LastWriterWinsResolver = resolver,
            MergeResolver = resolver,
            CustomResolver = resolver,
            DomainHandler = new MergeableServerDomainHandler(new() { Kind = MergeableKind.IncrementOnlyCounter }),
        };
    }

    /// <summary>Creates a CRDT operation for the hub stream.</summary>
    /// <param name="seed">The deterministic operation identifier seed.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="payload">The operation payload.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation MergeableOperation(int seed, long sequence, PayloadEnvelope payload) =>
        new()
        {
            OperationId = new(new Guid(seed, MergeableOperationDiscriminator, 0, [0, 0, 0, 0, 0, 0, 0, 1])),
            StreamId = MergeableStream,
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
    private static PayloadEnvelope MergeableCounter(string actor, long component) =>
        MergeableServerPayloads.CreateInput(MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(actor, component)));

    /// <summary>Computes a well-formed payload hash for arbitrary bytes.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The encoded payload hash.</returns>
    private static string MergeablePayloadHash(byte[] payload) => $"sha256-{Convert.ToBase64String(SHA256.HashData(payload))}";

    /// <summary>Creates a snapshot recovery request for the CRDT stream state contract.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="expiredCursor">The expired cursor.</param>
    /// <returns>The recovery request.</returns>
    private static RemoteSnapshotRecoveryRequest MergeableSnapshotRequest(SubscriptionId subscriptionId, string? expiredCursor) =>
        new()
        {
            StreamId = MergeableStream,
            SubscriptionId = subscriptionId,
            ExpiredCursor = expiredCursor,
            ClientStateContractId = MergeableContracts.StateContractId,
            ClientStateSchemaVersion = MergeableContracts.SchemaVersion,
            SnapshotFormatVersion = SingleCount,
            PendingOperations = [],
            ReplayOperations = [],
            MaximumResponseBytes = MergeableSnapshotMaximumResponseBytes,
        };

    /// <summary>Reads one CRDT subscription page with a bounded wait.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="client">The authenticated client.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="resumeCursor">The optional resume cursor.</param>
    /// <returns>The page.</returns>
    /// <exception cref="InvalidOperationException">No page is available.</exception>
    private static async Task<RemoteEventBatch> ReadMergeablePageAsync(
        ServerStreamHub hub,
        ServerAuthenticatedClient client,
        SubscriptionId subscriptionId,
        string? resumeCursor)
    {
        var enumerable = hub.SubscribeStreamAsync(
            new(MergeableStream, subscriptionId, resumeCursor, StartPosition.FromSequence(0)),
            client,
            CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var hasPage = await enumerator.MoveNextAsync().AsTask().WaitAsync(MergeableGuardTimeout);
        return hasPage ? enumerator.Current : throw new InvalidOperationException("A CRDT subscription page must be available.");
    }

    /// <summary>Extracts the numeric sequence from a server cursor.</summary>
    /// <param name="cursor">The server cursor.</param>
    /// <returns>The cursor sequence.</returns>
    private static long MergeableCursorSequence(string cursor)
    {
        var segments = cursor.Split(':');
        return long.Parse(segments[^MergeableDoubleCount], CultureInfo.InvariantCulture);
    }

    /// <summary>Decodes the authoritative CRDT state carried by a canonical event.</summary>
    /// <param name="remoteEvent">The event.</param>
    /// <returns>The CRDT state.</returns>
    /// <exception cref="InvalidOperationException">The event does not carry authoritative state.</exception>
    private static MergeableState DecodeMergeableEventState(RemoteEvent remoteEvent)
    {
        var input = MergeableCodec.DecodeInput(remoteEvent.Payload.Payload);
        return input.Value is MergeableState state
            ? state
            : throw new InvalidOperationException("A CRDT event must carry authoritative state.");
    }

    /// <summary>Decodes the CRDT state materialized into a recovered checkpoint.</summary>
    /// <param name="result">The recovery result.</param>
    /// <returns>The CRDT state.</returns>
    /// <exception cref="InvalidOperationException">The result has no checkpoint.</exception>
    private static MergeableState DecodeMergeableCheckpointState(RemoteSnapshotRecoveryResult result)
    {
        var checkpoint = result.Checkpoint ?? throw new InvalidOperationException("A recovered CRDT snapshot must include a checkpoint.");
        return MergeableCodec.DecodeState(checkpoint.ClientState.Payload);
    }

    /// <summary>Asserts a single accepted CRDT operation and its single canonical event.</summary>
    /// <param name="result">The sync result.</param>
    /// <param name="version">The expected server version.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMergeableAcceptedAsync(ServerSyncResult result, string version)
    {
        await Assert.That(result.Result.Operations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(result.Result.Operations[0].ServerVersion).IsEqualTo(version);
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(result.ProducedEvents[0].StreamId).IsEqualTo(MergeableStream);
    }

    /// <summary>Asserts a single rejected CRDT operation that produced no event.</summary>
    /// <param name="result">The sync result.</param>
    /// <param name="reason">The expected stable reason.</param>
    /// <param name="version">The expected unchanged server version.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMergeableRejectedAsync(ServerSyncResult result, string reason, string version)
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
    private static async Task AssertMergeableOriginAsync(RemoteEvent remoteEvent, string clientId, OperationId operationId)
    {
        await Assert.That(remoteEvent.CausedByOperationId).IsEqualTo(operationId);
        await Assert.That(remoteEvent.Origin?.ClientId).IsEqualTo(clientId);
        await Assert.That(remoteEvent.Origin?.OperationId).IsEqualTo(operationId);
    }

    /// <summary>Asserts two canonical event lists are identical.</summary>
    /// <param name="expected">The expected events.</param>
    /// <param name="actual">The actual events.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMergeableEventsEqualAsync(IReadOnlyList<RemoteEvent> expected, IReadOnlyList<RemoteEvent> actual)
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
    private sealed class MergeableStateMaterializer : IServerSnapshotMaterializer
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
            if (!string.Equals(context.ClientStateContractId, MergeableContracts.StateContractId, StringComparison.Ordinal))
            {
                return ValueTask.FromResult(new ServerSnapshotMaterializationResult { Status = ServerSnapshotMaterializationStatus.UnsupportedProjection });
            }

            var state = MergeableCodec.DecodeState(context.CapturedServerState.State.Payload);
            var clientState = MergeableServerPayloads.CreateState(state);
            return ValueTask.FromResult(new ServerSnapshotMaterializationResult { Status = ServerSnapshotMaterializationStatus.Materialized, ClientState = clientState });
        }
    }
}
