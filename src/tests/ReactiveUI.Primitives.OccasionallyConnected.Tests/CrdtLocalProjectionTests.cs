// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="CrdtLocalProjection"/>.</summary>
public sealed partial class CrdtLocalProjectionTests
{
    /// <summary>The CRDT client id.</summary>
    private const string ClientId = "client-a";

    /// <summary>The server actor id.</summary>
    private const string ServerId = "server";

    /// <summary>The SQLite store identity.</summary>
    private const string StoreIdentity = "crdt-client-a";

    /// <summary>The first cursor.</summary>
    private const string CursorOne = "cursor-1";

    /// <summary>The alpha element text.</summary>
    private const string AlphaText = "alpha";

    /// <summary>The CRDT binary content type.</summary>
    private const string CrdtContentType = "application/vnd.reactiveui.oc.crdt+binary";

    /// <summary>An invalid content type.</summary>
    private const string InvalidContentType = "application/octet-stream";

    /// <summary>An unknown contract id.</summary>
    private const string UnknownContractId = "reactiveui.oc.unknown";

    /// <summary>An invalid payload hash.</summary>
    private const string InvalidPayloadHash = "sha256-invalid";

    /// <summary>The server version.</summary>
    private const string ServerVersion = "server-v1";

    /// <summary>The first durable sequence.</summary>
    private const long FirstSequence = 1;

    /// <summary>The recovered next durable sequence.</summary>
    private const long RecoveredNextSequence = 2;

    /// <summary>The OR-set local sequence.</summary>
    private const long ORSetLocalSequence = 7;

    /// <summary>The optimistic counter value.</summary>
    private const long OptimisticCounterValue = 9;

    /// <summary>The local committed counter value.</summary>
    private const long LocalCounterValue = 5;

    /// <summary>The remote authoritative counter value.</summary>
    private const long RemoteCounterValue = 2;

    /// <summary>The empty authoritative counter value.</summary>
    private const long EmptyCounterValue = 0;

    /// <summary>The store format version.</summary>
    private const int StoreVersion = 1;

    /// <summary>The first binding index.</summary>
    private const int FirstBindingIndex = 0;

    /// <summary>The stream id.</summary>
    private static readonly StreamId Stream = new("crdt/set");

    /// <summary>Verifies local OR-set adds derive dots from the committer operation sequence.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyLocalDerivesORSetDotFromOperationSequence()
    {
        CrdtLocalProjection projection = new(ClientId, CrdtKind.ORSet);
        var operation = CreateOperation(ORSetLocalSequence, CrdtInput.ForMutation(CrdtMutation.ORSetAdd(Bytes(AlphaText))));

        var state = projection.ApplyLocal(projection.InitialState, CrdtInput.ForMutation(CrdtMutation.ORSetAdd(Bytes(AlphaText))), operation);

        await Assert.That(state.DotBindings[FirstBindingIndex].Dot.ClientId).IsEqualTo(ClientId);
        await Assert.That(state.DotBindings[FirstBindingIndex].Dot.ClientSequence).IsEqualTo(ORSetLocalSequence);
    }

    /// <summary>Verifies remote CRDT inputs replace the authoritative state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ApplyRemoteReplacesAuthoritativeState()
    {
        CrdtLocalProjection projection = new(ClientId, CrdtKind.GCounter);
        var optimistic = CrdtFunctions.ApplyLocal(
            projection.InitialState,
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, OptimisticCounterValue)),
            ClientId,
            FirstSequence,
            CrdtBounds.Default);
        var authoritative = CreateRemoteCounter(RemoteCounterValue);
        var remote = CreateRemoteEvent(Guid.NewGuid(), CursorOne, CrdtInput.ForAuthoritativeState(authoritative), null);

        var replaced = projection.ApplyRemote(optimistic, CrdtInput.ForAuthoritativeState(authoritative), remote);

        await Assert.That(replaced.Value.Counter).IsEqualTo(RemoteCounterValue);
        await Assert.That(replaced.GCounterComponents.ContainsKey(ClientId)).IsFalse();
    }

    /// <summary>Verifies projection rejects remote mutation inputs and reconcile preserves state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ProjectionRejectsRemoteMutationsAndReconcilesState()
    {
        CrdtLocalProjection projection = new(ClientId, CrdtKind.GCounter);
        var state = CreateRemoteCounter(LocalCounterValue);
        var mutationInput = CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, LocalCounterValue));
        var remote = CreateRemoteEvent(Guid.NewGuid(), CursorOne, mutationInput, null);
        var resolution = new ConflictResolutionResult([], [], [], [], ServerVersion);

        await Assert.That(() => projection.ApplyRemote(state, mutationInput, remote)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(projection.Reconcile(state, resolution).Value.Counter).IsEqualTo(LocalCounterValue);
    }

    /// <summary>Verifies CRDT binary serializer rejects mismatched schema, type, content, and hash data.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SerializerRejectsMismatchedSchemaTypeContentAndHash()
    {
        var serializer = new CrdtPayloadSerializer();
        var state = CreateRemoteCounter(LocalCounterValue);
        var input = CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, LocalCounterValue));
        var stateEnvelope = await serializer.SerializeAsync(CrdtContracts.StateContractId, CrdtContracts.SchemaVersion, state, CancellationToken.None);
        var inputEnvelope = await serializer.SerializeAsync(CrdtContracts.InputContractId, CrdtContracts.SchemaVersion, input, CancellationToken.None);
        var differentPayload = CrdtCodec.EncodeState(CreateRemoteCounter(RemoteCounterValue));

        await Assert.That(serializer.ContentType).IsEqualTo(CrdtContentType);
        await Assert.That((await serializer.DeserializeAsync(inputEnvelope, typeof(CrdtInput), CancellationToken.None) as CrdtInput)?.Mutation?.ActorId).IsEqualTo(ClientId);
        await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() =>
            serializer.SerializeAsync(CrdtContracts.StateContractId, CrdtContracts.SchemaVersion, input, CancellationToken.None).AsTask());
        await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() =>
            serializer.SerializeAsync(UnknownContractId, CrdtContracts.SchemaVersion, state, CancellationToken.None).AsTask());
        await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() =>
            serializer.SerializeAsync(CrdtContracts.StateContractId, CrdtContracts.SchemaVersion + StoreVersion, state, CancellationToken.None).AsTask());
        await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() =>
            serializer.DeserializeAsync(stateEnvelope, typeof(CrdtInput), CancellationToken.None).AsTask());
        await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() =>
            serializer.DeserializeAsync(stateEnvelope with { ContentType = InvalidContentType }, typeof(CrdtState), CancellationToken.None).AsTask());
        await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() =>
            serializer.DeserializeAsync(stateEnvelope with { PayloadHash = InvalidPayloadHash }, typeof(CrdtState), CancellationToken.None).AsTask());
        await Assert.ThrowsExactlyAsync<PayloadSchemaException>(() =>
            serializer.DeserializeAsync(
                stateEnvelope with { PayloadHash = JsonPayloadSerializer.ComputePayloadHash(differentPayload) },
                typeof(CrdtState),
                CancellationToken.None).AsTask());
    }

    /// <summary>Verifies CRDT state uses the real SQLite committer recovery and authoritative checkpoint flow.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SqliteCommitterRecoversTypedStateAndPreservesAuthoritativeCheckpoint()
    {
        var directory = Directory.CreateTempSubdirectory("oc-crdt-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, "local.db");
            var serializer = new CrdtPayloadSerializer();
            var projection = new CrdtLocalProjection(ClientId, CrdtKind.GCounter);
            OperationId operationId;
            SubscriptionId subscriptionId;
            await using (var store = new SqliteLocalStoreAdapter(databasePath))
            {
                subscriptionId = await InitializeAsync(store);
                var committer = CreateCommitter(store, subscriptionId, serializer, projection);
                _ = await committer.RecoverAsync(CancellationToken.None);
                var local = await committer.CommitAsync(
                    CrdtInput.ForMutation(CrdtMutation.GCounterSet(ClientId, LocalCounterValue)),
                    OperationPolicy.Default,
                    CancellationToken.None);
                operationId = local.Operation.OperationId;
                var authoritativeBeforeRemote = await DecodeAuthoritativeAsync(serializer, local.State.AuthoritativePayload);
                var authoritative = CreateRemoteCounter(RemoteCounterValue);
                var eventId = Guid.NewGuid();
                var remote = new RemoteEventBatch(
                    Guid.NewGuid(),
                    Stream,
                    null,
                    CursorOne,
                    [CreateRemoteEvent(eventId, CursorOne, CrdtInput.ForAuthoritativeState(authoritative), operationId)])
                {
                    CompletedOperations = [new(new RemoteEventOrigin(ClientId, operationId), [eventId])],
                };

                var applied = await committer.ApplyRemoteBatchAsync(remote, CancellationToken.None);

                await Assert.That(authoritativeBeforeRemote.Value.Counter).IsEqualTo(EmptyCounterValue);
                await Assert.That(applied.State.State.Value.Counter).IsEqualTo(RemoteCounterValue);
                await Assert.That(applied.State.ServerCursor).IsEqualTo(CursorOne);
            }

            await using var reopened = new SqliteLocalStoreAdapter(databasePath);
            await reopened.InitializeAsync(new(StoreIdentity, StoreVersion, false), CancellationToken.None);
            var recoveredCommitter = CreateCommitter(reopened, subscriptionId, serializer, projection);
            var recovered = await recoveredCommitter.RecoverAsync(CancellationToken.None);

            await Assert.That(recovered.State.Value.Counter).IsEqualTo(RemoteCounterValue);
            await Assert.That(recovered.NextClientSequence).IsEqualTo(RecoveredNextSequence);
            await Assert.That(recovered.ServerCursor).IsEqualTo(CursorOne);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Creates committer options.</summary>
    /// <param name="store">The store.</param>
    /// <param name="subscriptionId">The subscription id.</param>
    /// <param name="serializer">The serializer.</param>
    /// <param name="projection">The projection.</param>
    /// <returns>The committer.</returns>
    private static LocalStreamCommitter<CrdtState, CrdtInput> CreateCommitter(
        ILocalStoreAdapter store,
        SubscriptionId subscriptionId,
        IPayloadSerializer serializer,
        ILocalProjection<CrdtState, CrdtInput> projection) =>
        new(new()
        {
            StreamId = Stream,
            SubscriptionId = subscriptionId,
            ClientId = ClientId,
            Contracts = new()
            {
                InputContractId = CrdtContracts.InputContractId,
                InputSchemaVersion = CrdtContracts.SchemaVersion,
                StateContractId = CrdtContracts.StateContractId,
                StateSchemaVersion = CrdtContracts.SchemaVersion,
                SnapshotFormatVersion = CrdtContracts.SchemaVersion,
            },
            Dependencies = new() { Store = store, Serializer = serializer, Projection = projection, TimeProvider = TimeProvider.System },
        });

    /// <summary>Initializes a SQLite store and subscription.</summary>
    /// <param name="store">The store.</param>
    /// <returns>The subscription id.</returns>
    private static async ValueTask<SubscriptionId> InitializeAsync(SqliteLocalStoreAdapter store)
    {
        await store.InitializeAsync(new(StoreIdentity, StoreVersion, false), CancellationToken.None);
        return await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
    }

    /// <summary>Creates a remote counter state.</summary>
    /// <param name="component">The component.</param>
    /// <returns>The CRDT state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CrdtState CreateRemoteCounter(long component) =>
        CrdtFunctions.ApplyLocal(CrdtFunctions.Empty(CrdtKind.GCounter), CrdtInput.ForMutation(CrdtMutation.GCounterSet(ServerId, component)), ServerId, FirstSequence, CrdtBounds.Default);

    /// <summary>Creates a local operation.</summary>
    /// <param name="clientSequence">The client sequence.</param>
    /// <param name="input">The input.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(long clientSequence, CrdtInput input)
    {
        var payload = CrdtCodec.EncodeInput(input, CrdtBounds.Default);
        return new()
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = clientSequence,
            TimestampUtc = DateTimeOffset.UnixEpoch,
            Type = SyncOperationType.Update,
            Payload = new(CrdtContracts.InputContractId, CrdtContracts.SchemaVersion, CrdtContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload)),
        };
    }

    /// <summary>Creates a remote event.</summary>
    /// <param name="eventId">The event id.</param>
    /// <param name="cursor">The cursor.</param>
    /// <param name="input">The input.</param>
    /// <param name="operationId">The causing operation id.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(Guid eventId, string cursor, CrdtInput input, OperationId? operationId)
    {
        var payload = CrdtCodec.EncodeInput(input, CrdtBounds.Default);
        var envelope = new PayloadEnvelope(CrdtContracts.InputContractId, CrdtContracts.SchemaVersion, CrdtContentType, payload, JsonPayloadSerializer.ComputePayloadHash(payload));
        var remoteEvent = new RemoteEvent(eventId, Stream, cursor, DateTimeOffset.UnixEpoch, operationId, envelope, new Dictionary<string, string>());
        return operationId.HasValue
            ? remoteEvent with { Origin = new(ClientId, operationId.Value) }
            : remoteEvent;
    }

    /// <summary>Decodes an authoritative payload.</summary>
    /// <param name="serializer">The serializer.</param>
    /// <param name="payload">The payload.</param>
    /// <returns>The decoded CRDT state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the authoritative payload is missing.</exception>
    private static async ValueTask<CrdtState> DecodeAuthoritativeAsync(CrdtPayloadSerializer serializer, PayloadEnvelope? payload)
    {
        if (payload is null)
        {
            throw new InvalidOperationException("Expected authoritative payload.");
        }

        var value = await serializer.DeserializeAsync(payload, typeof(CrdtState), CancellationToken.None);
        return (CrdtState)value;
    }

    /// <summary>Creates UTF-8 bytes.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);
}
