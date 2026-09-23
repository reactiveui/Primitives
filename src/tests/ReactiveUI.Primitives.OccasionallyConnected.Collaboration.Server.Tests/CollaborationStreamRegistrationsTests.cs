// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="CollaborationStreamRegistrations"/>.</summary>
public sealed class CollaborationStreamRegistrationsTests
{
    /// <summary>The version used for activity resolver tests.</summary>
    private const string InitialActivityVersion = "activity-v0";

    /// <summary>The client identifier used by activity resolver tests.</summary>
    private const string ClientId = "client-a";

    /// <summary>The tenant hint used by activity resolver tests.</summary>
    private const string TenantHint = "tenant-a";

    /// <summary>The initial client sequence used by activity operations.</summary>
    private const long InitialClientSequence = 1;

    /// <summary>The stale client sequence used by activity patch operations.</summary>
    private const long StalePatchClientSequence = 2;

    /// <summary>The current activity JSON used before patch operations.</summary>
    private const string InitialActivityPayloadJson = """{"status":"ready","title":"Original","details":"First details"}""";

    /// <summary>The ready activity JSON used by custom resolver tests.</summary>
    private const string ReadyActivityPayloadJson = """{"status":"ready"}""";

    /// <summary>The invalid payload hash used by integrity tests.</summary>
    private const string InvalidPayloadHash = "sha256-invalid";

    /// <summary>The activity title property name.</summary>
    private const string TitlePropertyName = "title";

    /// <summary>The activity details property name.</summary>
    private const string DetailsPropertyName = "details";

    /// <summary>The original activity title value.</summary>
    private const string OriginalTitle = "Original";

    /// <summary>The replacement activity title value.</summary>
    private const string ReplacementTitle = "Replacement";

    /// <summary>The original activity details value.</summary>
    private const string FirstDetails = "First details";

    /// <summary>The replacement activity details value.</summary>
    private const string SecondDetails = "Second details";

    /// <summary>The activity JSON patch that replaces title with an empty string.</summary>
    private const string EmptyTitlePatchJson = """{"status":"ready","title":""}""";

    /// <summary>The activity JSON patch that replaces details with whitespace.</summary>
    private const string WhitespaceDetailsPatchJson = """{"status":"ready","details":"   "}""";

    /// <summary>The whitespace details value preserved by custom merge.</summary>
    private const string WhitespaceDetails = "   ";

    /// <summary>The deterministic operation timestamp used by resolver tests.</summary>
    private static readonly DateTimeOffset OperationTimestampUtc = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The deterministic server write timestamp used by resolver tests.</summary>
    private static readonly DateTimeOffset ServerCommittedAtUtc = new(2026, 1, 1, 0, 0, 1, TimeSpan.Zero);

    /// <summary>Verifies the example registers the custom stream and all built-in CRDT families.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateAllRegistersCustomStreamAndEveryCrdtFamily()
    {
        var registrations = CollaborationStreamRegistrations.CreateAll();
        var streams = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < registrations.Count; index++)
        {
            registrations[index].Validate();
            _ = streams.Add(registrations[index].StreamId.Value);
        }

        await Assert.That(streams.Contains(CollaborationStreamRegistrations.ActivityStream.Value)).IsTrue();
        await Assert.That(streams.Contains(CollaborationStreamRegistrations.GCounterStream.Value)).IsTrue();
        await Assert.That(streams.Contains(CollaborationStreamRegistrations.PNCounterStream.Value)).IsTrue();
        await Assert.That(streams.Contains(CollaborationStreamRegistrations.ORSetStream.Value)).IsTrue();
        await Assert.That(streams.Contains(CollaborationStreamRegistrations.LwwRegisterStream.Value)).IsTrue();
        await Assert.That(HasCrdtRegistration(registrations, CrdtKind.GCounter)).IsTrue();
        await Assert.That(HasCrdtRegistration(registrations, CrdtKind.PNCounter)).IsTrue();
        await Assert.That(HasCrdtRegistration(registrations, CrdtKind.ORSet)).IsTrue();
        await Assert.That(HasCrdtRegistration(registrations, CrdtKind.LwwRegister)).IsTrue();
    }

    /// <summary>Verifies the custom activity stream demonstrates custom conflict behavior instead of aliasing every policy to LWW.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationUsesDistinctCustomConflictBehavior()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());

        await Assert.That(ReferenceEquals(registration.LastWriterWinsResolver, registration.MergeResolver)).IsFalse();
        await Assert.That(ReferenceEquals(registration.LastWriterWinsResolver, registration.CustomResolver)).IsFalse();
        await Assert.That(registration.DomainHandler).IsTypeOf<ActivityDomainHandler>();
    }

    /// <summary>Verifies the registered custom resolver canonicalizes valid activity payloads.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the resolver does not produce a canonical payload.</exception>
    [Test]
    public async Task ActivityRegistrationCustomResolverProducesCanonicalConflictPayload()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        var operation = CreateActivityOperation();
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialActivityVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialActivityVersion));
        var context = new ConflictContext(
            current,
            [operation],
            new(ClientId, TenantHint),
            new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } });

        var result = await registration.CustomResolver.ResolveAsync(context, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(result.AcceptedOperations).Count().IsEqualTo(1);
        await Assert.That(result.Conflicts).Count().IsEqualTo(1);
        var payload = result.Conflicts[0].ResolvedPayload
            ?? throw new InvalidOperationException("The custom resolver should produce a canonical payload.");
        var json = Encoding.UTF8.GetString(payload.Payload.Span);
        await Assert.That(json.Contains("serverAcceptedUtc", StringComparison.Ordinal)).IsTrue();
        await Assert.That(json.Contains("acceptedClientId", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies the registered resolver accepts a valid typed-client update with trusted server provenance.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the resolver does not produce a canonical payload.</exception>
    [Test]
    public async Task ActivityRegistrationCustomResolverAcceptsTypedClientUpdate()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        var operation = CreateActivityOperation() with { Type = SyncOperationType.Update };
        var context = new ConflictContext(
            CreateInitialState(),
            [operation],
            new(ClientId, TenantHint),
            CreateServerContext(operation));

        var result = await registration.CustomResolver.ResolveAsync(context, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(result.AcceptedOperations).Count().IsEqualTo(1);
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(0);
        await Assert.That(result.Conflicts).Count().IsEqualTo(1);
        var payload = result.Conflicts[0].ResolvedPayload
            ?? throw new InvalidOperationException("The update resolver should produce a canonical payload.");
        using var document = JsonDocument.Parse(payload.Payload.ToArray());
        await Assert.That(document.RootElement.GetProperty("acceptedClientId").GetString()).IsEqualTo(ClientId);
        await Assert.That(document.RootElement.GetProperty("status").GetString()).IsEqualTo("ready");
    }

    /// <summary>Verifies corrupted current canonical state is not merged into new activity state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationRejectsInvalidCurrentCanonicalPayload()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        var operation = CreateActivityOperation();
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialActivityVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialActivityVersion) with { PayloadHash = InvalidPayloadHash });
        var context = new ConflictContext(
            current,
            [operation],
            new(ClientId, TenantHint),
            new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } });

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() =>
            registration.CustomResolver.ResolveAsync(context, CancellationToken.None).AsTask());
    }

    /// <summary>Verifies client-shaped JSON cannot be used as current canonical state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationRejectsClientShapeCurrentCanonicalPayload()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        var operation = CreateActivityOperation();
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialActivityVersion,
            ActivityPayloads.Create(ReadyActivityPayloadJson));
        var context = new ConflictContext(
            current,
            [operation],
            new(ClientId, TenantHint),
            new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } });

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() =>
            registration.CustomResolver.ResolveAsync(context, CancellationToken.None).AsTask());
    }

    /// <summary>Verifies stale custom activity patches preserve fields omitted by the later operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationMergesStaleDisjointPatchesThroughServerHub()
    {
        await using var hub = ServerStreamHub.CreateInMemory(CreateActivityHubOptions());
        var first = CreateActivityOperation(
            InitialClientSequence,
            null,
            InitialActivityPayloadJson);
        var second = CreateActivityOperation(
            StalePatchClientSequence,
            InitialActivityVersion,
            """{"status":"ready","details":"Second details"}""");

        _ = await hub.ApplyOperationsAsync(
            new(Guid.NewGuid(), [first]),
            CreateAuthenticatedClient(),
            CancellationToken.None);
        var result = await hub.ApplyOperationsAsync(
            new(Guid.NewGuid(), [second]),
            CreateAuthenticatedClient(),
            CancellationToken.None);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.Result.Operations[0].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(1);
        await Assert.That(result.ProducedEvents[0].CausedByOperationId).IsEqualTo(second.OperationId);
        using var document = JsonDocument.Parse(result.ProducedEvents[0].Payload.Payload.ToArray());
        var root = document.RootElement;
        await Assert.That(root.GetProperty(TitlePropertyName).GetString()).IsEqualTo(OriginalTitle);
        await Assert.That(root.GetProperty(DetailsPropertyName).GetString()).IsEqualTo(SecondDetails);
    }

    /// <summary>Verifies explicit null clears an optional activity field while omitted fields preserve.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationClearsExplicitNullPatchThroughServerHub()
    {
        await using var hub = ServerStreamHub.CreateInMemory(CreateActivityHubOptions());
        var first = CreateActivityOperation(
            InitialClientSequence,
            null,
            InitialActivityPayloadJson);
        var second = CreateActivityOperation(
            StalePatchClientSequence,
            InitialActivityVersion,
            """{"status":"ready","title":null}""");

        _ = await hub.ApplyOperationsAsync(
            new(Guid.NewGuid(), [first]),
            CreateAuthenticatedClient(),
            CancellationToken.None);
        var result = await hub.ApplyOperationsAsync(
            new(Guid.NewGuid(), [second]),
            CreateAuthenticatedClient(),
            CancellationToken.None);

        using var document = JsonDocument.Parse(result.ProducedEvents[0].Payload.Payload.ToArray());
        var root = document.RootElement;
        await Assert.That(root.GetProperty(TitlePropertyName).ValueKind).IsEqualTo(JsonValueKind.Null);
        await Assert.That(root.GetProperty(DetailsPropertyName).GetString()).IsEqualTo(FirstDetails);
    }

    /// <summary>Verifies string patches replace one optional field while omitted sibling fields preserve.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationReplacesStringPatchThroughServerHub()
    {
        await using var hub = ServerStreamHub.CreateInMemory(CreateActivityHubOptions());
        var first = CreateActivityOperation(
            InitialClientSequence,
            null,
            InitialActivityPayloadJson);
        var second = CreateActivityOperation(
            StalePatchClientSequence,
            InitialActivityVersion,
            """{"status":"ready","title":"Replacement"}""");

        _ = await hub.ApplyOperationsAsync(
            new(Guid.NewGuid(), [first]),
            CreateAuthenticatedClient(),
            CancellationToken.None);
        var result = await hub.ApplyOperationsAsync(
            new(Guid.NewGuid(), [second]),
            CreateAuthenticatedClient(),
            CancellationToken.None);

        using var document = JsonDocument.Parse(result.ProducedEvents[0].Payload.Payload.ToArray());
        var root = document.RootElement;
        await Assert.That(root.GetProperty(TitlePropertyName).GetString()).IsEqualTo(ReplacementTitle);
        await Assert.That(root.GetProperty(DetailsPropertyName).GetString()).IsEqualTo(FirstDetails);
    }

    /// <summary>Verifies empty string patches replace optional fields without normalization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationPreservesEmptyStringPatchThroughServerHub()
    {
        await using var hub = ServerStreamHub.CreateInMemory(CreateActivityHubOptions());
        var first = CreateActivityOperation(InitialClientSequence, null, InitialActivityPayloadJson);
        var second = CreateActivityOperation(StalePatchClientSequence, InitialActivityVersion, EmptyTitlePatchJson);

        _ = await hub.ApplyOperationsAsync(new(Guid.NewGuid(), [first]), CreateAuthenticatedClient(), CancellationToken.None);
        var result = await hub.ApplyOperationsAsync(new(Guid.NewGuid(), [second]), CreateAuthenticatedClient(), CancellationToken.None);

        using var document = JsonDocument.Parse(result.ProducedEvents[0].Payload.Payload.ToArray());
        var root = document.RootElement;
        await Assert.That(root.GetProperty(TitlePropertyName).GetString()).IsEqualTo(string.Empty);
        await Assert.That(root.GetProperty(DetailsPropertyName).GetString()).IsEqualTo(FirstDetails);
    }

    /// <summary>Verifies whitespace string patches replace optional fields without normalization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationPreservesWhitespaceStringPatchThroughServerHub()
    {
        await using var hub = ServerStreamHub.CreateInMemory(CreateActivityHubOptions());
        var first = CreateActivityOperation(InitialClientSequence, null, InitialActivityPayloadJson);
        var second = CreateActivityOperation(StalePatchClientSequence, InitialActivityVersion, WhitespaceDetailsPatchJson);

        _ = await hub.ApplyOperationsAsync(new(Guid.NewGuid(), [first]), CreateAuthenticatedClient(), CancellationToken.None);
        var result = await hub.ApplyOperationsAsync(new(Guid.NewGuid(), [second]), CreateAuthenticatedClient(), CancellationToken.None);

        using var document = JsonDocument.Parse(result.ProducedEvents[0].Payload.Payload.ToArray());
        var root = document.RootElement;
        await Assert.That(root.GetProperty(TitlePropertyName).GetString()).IsEqualTo(OriginalTitle);
        await Assert.That(root.GetProperty(DetailsPropertyName).GetString()).IsEqualTo(WhitespaceDetails);
    }

    /// <summary>Verifies the activity resolver rejects batches containing more than one incoming operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationRejectsMultipleIncomingOperations()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        var first = CreateActivityOperation();
        var second = CreateActivityOperation() with { ClientSequence = StalePatchClientSequence };
        var context = new ConflictContext(
            CreateInitialState(),
            [first, second],
            new(ClientId, TenantHint),
            CreateServerContext(first));

        var result = await registration.CustomResolver.ResolveAsync(context, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(result.AcceptedOperations).Count().IsEqualTo(0);
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(1);
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("activity-operation-count-mismatch");
    }

    /// <summary>Verifies the activity resolver rejects writes without trusted server provenance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationRejectsMissingServerProvenance()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        var operation = CreateActivityOperation();
        var context = new ConflictContext(CreateInitialState(), [operation], new(ClientId, TenantHint));

        var result = await registration.CustomResolver.ResolveAsync(context, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(result.AcceptedOperations).Count().IsEqualTo(0);
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(1);
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("activity-missing-server-provenance");
    }

    /// <summary>Verifies the activity resolver rejects unsupported operation types.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationRejectsUnsupportedOperationTypes()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        foreach (var operationType in new[] { SyncOperationType.Append, SyncOperationType.Delete })
        {
            var operation = CreateActivityOperation() with { Type = operationType };
            var context = new ConflictContext(
                CreateInitialState(),
                [operation],
                new(ClientId, TenantHint),
                CreateServerContext(operation));

            var result = await registration.CustomResolver.ResolveAsync(context, CancellationToken.None).ConfigureAwait(false);

            await Assert.That(result.AcceptedOperations).Count().IsEqualTo(0);
            await Assert.That(result.RejectedOperations).Count().IsEqualTo(1);
            await Assert.That(result.RejectedOperations[0].OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("activity-operation-type-mismatch");
        }
    }

    /// <summary>Verifies the activity resolver rejects malformed activity input payloads.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityRegistrationRejectsInvalidActivityPayload()
    {
        var registration = FindActivityRegistration(CollaborationStreamRegistrations.CreateAll());
        var operation = CreateActivityOperation() with { Payload = ActivityPayloadTestFactory.CreateEnvelope("""{"status":42}""") };
        var context = new ConflictContext(
            CreateInitialState(),
            [operation],
            new(ClientId, TenantHint),
            CreateServerContext(operation));

        var result = await registration.CustomResolver.ResolveAsync(context, CancellationToken.None).ConfigureAwait(false);

        await Assert.That(result.AcceptedOperations).Count().IsEqualTo(0);
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(1);
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("activity-status-type");
    }

    /// <summary>Checks whether the registrations contain the expected CRDT stream for the requested kind.</summary>
    /// <param name="registrations">The registrations to inspect.</param>
    /// <param name="kind">The CRDT kind to find.</param>
    /// <returns><see langword="true"/> when the expected registration exists.</returns>
    private static bool HasCrdtRegistration(IReadOnlyList<ServerConflictStreamRegistration> registrations, CrdtKind kind)
    {
        var expected = kind switch
        {
            CrdtKind.GCounter => CollaborationStreamRegistrations.GCounterStream,
            CrdtKind.PNCounter => CollaborationStreamRegistrations.PNCounterStream,
            CrdtKind.ORSet => CollaborationStreamRegistrations.ORSetStream,
            CrdtKind.LwwRegister => CollaborationStreamRegistrations.LwwRegisterStream,
            _ => default,
        };

        for (var index = 0; index < registrations.Count; index++)
        {
            if (registrations[index].StreamId == expected)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Finds the custom activity stream registration.</summary>
    /// <param name="registrations">The registrations to inspect.</param>
    /// <returns>The activity stream registration.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the activity registration is missing.</exception>
    private static ServerConflictStreamRegistration FindActivityRegistration(IReadOnlyList<ServerConflictStreamRegistration> registrations)
    {
        for (var index = 0; index < registrations.Count; index++)
        {
            if (registrations[index].StreamId == CollaborationStreamRegistrations.ActivityStream)
            {
                return registrations[index];
            }
        }

        throw new InvalidOperationException("The activity stream registration was not found.");
    }

    /// <summary>Creates the initial activity server state.</summary>
    /// <returns>The initial server state.</returns>
    private static ServerState CreateInitialState() =>
        new(
            CollaborationStreamRegistrations.ActivityStream,
            InitialActivityVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialActivityVersion));

    /// <summary>Creates trusted server provenance for one activity operation.</summary>
    /// <param name="operation">The operation being resolved.</param>
    /// <returns>The trusted server conflict context.</returns>
    private static ConflictServerContext CreateServerContext(SyncOperation operation) =>
        new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } };

    /// <summary>Creates a hub that uses the example activity stream registrations.</summary>
    /// <returns>The configured hub options.</returns>
    private static ServerStreamHubOptions CreateActivityHubOptions() =>
        new()
        {
            AuthorizationPolicy = new ConfiguredIdentityAuthorizationPolicy(),
            ConflictHandler = new() { Streams = CollaborationStreamRegistrations.CreateAll() },
            TimeProvider = new FixedTimeProvider(ServerCommittedAtUtc),
        };

    /// <summary>Creates the authenticated activity test client.</summary>
    /// <returns>The authenticated client.</returns>
    private static ServerAuthenticatedClient CreateAuthenticatedClient() =>
        new(TenantHint, ClientId);

    /// <summary>Creates a valid custom activity operation.</summary>
    /// <returns>The custom activity operation.</returns>
    private static SyncOperation CreateActivityOperation() =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = CollaborationStreamRegistrations.ActivityStream,
            ClientSequence = InitialClientSequence,
            TimestampUtc = OperationTimestampUtc,
            Type = SyncOperationType.Custom,
            Payload = ActivityPayloads.Create(ReadyActivityPayloadJson),
        };

    /// <summary>Creates a custom activity operation with the requested client sequence and base version.</summary>
    /// <param name="clientSequence">The client operation sequence.</param>
    /// <param name="baseVersion">The optional stale base version.</param>
    /// <param name="json">The activity JSON patch.</param>
    /// <returns>The custom activity operation.</returns>
    private static SyncOperation CreateActivityOperation(
        long clientSequence,
        string? baseVersion,
        string json) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = CollaborationStreamRegistrations.ActivityStream,
            ClientSequence = clientSequence,
            TimestampUtc = OperationTimestampUtc,
            BaseVersion = baseVersion,
            Type = SyncOperationType.Custom,
            Payload = ActivityPayloads.Create(json),
            Policy = SyncOperation.DefaultPolicy with { ConflictPolicy = ConflictPolicy.Custom },
        };

    /// <summary>Provides deterministic UTC time to the real server hub.</summary>
    /// <param name="utcNow">The fixed UTC time.</param>
    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => utcNow;

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            new FixedTimer();
    }

    /// <summary>Timer used by the deterministic test time provider.</summary>
    private sealed class FixedTimer : ITimer
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        /// <inheritdoc/>
        public void Dispose()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
