// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="ActivityDomainHandler"/>.</summary>
public sealed class ActivityDomainHandlerTests
{
    /// <summary>The version used for the current activity state.</summary>
    private const string InitialVersion = "activity-v0";

    /// <summary>The version expected after accepting an activity operation.</summary>
    private const string AcceptedVersion = "activity-v1";

    /// <summary>The client identifier used by apply-context tests.</summary>
    private const string ClientId = "client-a";

    /// <summary>The tenant identifier used by apply-context tests.</summary>
    private const string TenantId = "tenant-a";

    /// <summary>The activity payload contract version used by tests.</summary>
    private const int ActivityPayloadContractVersion = 1;

    /// <summary>The unsupported activity payload contract version used by rejection tests.</summary>
    private const int InvalidActivityPayloadContractVersion = ActivityPayloadContractVersion + 1;

    /// <summary>The initial client sequence used by activity operations.</summary>
    private const long InitialClientSequence = 1;

    /// <summary>The ready JSON payload used by activity tests.</summary>
    private const string ReadyPayloadJson = """{"status":"ready"}""";

    /// <summary>The canonical activity title used by optional-field preservation tests.</summary>
    private const string ExistingTitle = "existing-title";

    /// <summary>The canonical activity details used by optional-field preservation tests.</summary>
    private const string ExistingDetails = "existing-details";

    /// <summary>The ready JSON payload with an unknown field.</summary>
    private const string UnknownFieldPayloadJson = """{"status":"ready","unexpected":true}""";

    /// <summary>The ready JSON payload with a duplicate status field.</summary>
    private const string DuplicateStatusPayloadJson = """{"status":"ready","status":"again"}""";

    /// <summary>The ready JSON payload with an invalid optional field type.</summary>
    private const string InvalidOptionalTypePayloadJson = """{"status":"ready","title":42}""";

    /// <summary>The activity JSON payload with an invalid required status field type.</summary>
    private const string InvalidStatusTypePayloadJson = """{"status":42}""";

    /// <summary>The ready JSON payload with an invalid details field type.</summary>
    private const string InvalidDetailsTypePayloadJson = """{"status":"ready","details":42}""";

    /// <summary>The ready JSON payload with duplicate title fields.</summary>
    private const string DuplicateTitlePayloadJson = """{"status":"ready","title":"one","title":"two"}""";

    /// <summary>The ready JSON payload with duplicate details fields.</summary>
    private const string DuplicateDetailsPayloadJson = """{"status":"ready","details":"one","details":"two"}""";

    /// <summary>The ready JSON payload with explicit null optional fields.</summary>
    private const string NullOptionalFieldsPayloadJson = """{"status":"ready","title":null,"details":null}""";

    /// <summary>The JSON payload without a required activity status.</summary>
    private const string MissingStatusPayloadJson = """{"title":"ready"}""";

    /// <summary>The non-object JSON payload used by parser rejection tests.</summary>
    private const string ArrayPayloadJson = """["ready"]""";

    /// <summary>The malformed JSON payload used by parser rejection tests.</summary>
    private const string MalformedPayloadJson = "{";

    /// <summary>The invalid content type used by envelope rejection tests.</summary>
    private const string InvalidContentType = "application/json";

    /// <summary>The payload hash used for mismatch tests.</summary>
    private const string InvalidPayloadHash = "sha256-invalid";

    /// <summary>The activity conflict resolution code used by resolver tests.</summary>
    private const string ActivityResolutionCode = "activity.custom.canonicalized";

    /// <summary>The compact GUID format used by canonical payload tests.</summary>
    private const string GuidCompactFormat = "N";

    /// <summary>The round-trip timestamp format used by canonical payload tests.</summary>
    private const string RoundTripDateTimeFormat = "O";

    /// <summary>The character used to build oversize payloads.</summary>
    private const char OversizePayloadCharacter = 'a';

    /// <summary>The oversize activity payload target byte count.</summary>
    private const int OversizePayloadCharacters = 4097;

    /// <summary>The too-long optional text payload target character count.</summary>
    private const int TooLongOptionalTextCharacters = 257;

    /// <summary>The deterministic operation timestamp used by apply-context tests.</summary>
    private static readonly DateTimeOffset OperationTimestampUtc = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The deterministic server write timestamp used by apply-context tests.</summary>
    private static readonly DateTimeOffset ServerCommittedAtUtc = new(2026, 1, 1, 0, 0, 1, TimeSpan.Zero);

    /// <summary>Verifies the custom activity domain rejects payloads from a different contract.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsInvalidContract()
    {
        var operation = CreateOperation(new(
            "example.invalid",
            ActivityPayloadContractVersion,
            ActivityPayloads.ContentType,
            """{"status":"ready"}"""u8.ToArray(),
            InvalidPayloadHash));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects payloads from a different schema version.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsInvalidSchemaVersion()
    {
        var operation = CreateOperation(CreateEnvelope(ReadyPayloadJson) with { SchemaVersion = InvalidActivityPayloadContractVersion });
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects payloads with the wrong content type.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsInvalidContentType()
    {
        var operation = CreateOperation(CreateEnvelope(ReadyPayloadJson) with { ContentType = InvalidContentType });
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects invalid JSON before it is persisted as canonical state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsInvalidJson()
    {
        var operation = CreateOperation(CreateEnvelope(MalformedPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects JSON payloads that are not objects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsNonObjectJson()
    {
        var operation = CreateOperation(CreateEnvelope(ArrayPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects client payloads with unknown fields.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsUnknownInputField()
    {
        var operation = CreateOperation(CreateEnvelope(UnknownFieldPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects duplicate known client payload fields.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsDuplicateKnownInputField()
    {
        var operation = CreateOperation(CreateEnvelope(DuplicateStatusPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects duplicate optional title fields.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsDuplicateTitleField()
    {
        var operation = CreateOperation(CreateEnvelope(DuplicateTitlePayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects duplicate optional details fields.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsDuplicateDetailsField()
    {
        var operation = CreateOperation(CreateEnvelope(DuplicateDetailsPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects invalid required status field types.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsInvalidStatusPropertyType()
    {
        var operation = CreateOperation(CreateEnvelope(InvalidStatusTypePayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects invalid optional field types.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsInvalidOptionalPropertyType()
    {
        var operation = CreateOperation(CreateEnvelope(InvalidOptionalTypePayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects invalid details field types.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsInvalidDetailsPropertyType()
    {
        var operation = CreateOperation(CreateEnvelope(InvalidDetailsTypePayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects oversize payloads.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsOversizePayload()
    {
        var operation = CreateOperation(CreateEnvelope(new(OversizePayloadCharacter, OversizePayloadCharacters)));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects missing required status text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsMissingRequiredStatus()
    {
        var operation = CreateOperation(CreateEnvelope(MissingStatusPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects required status text beyond the documented bound.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsTooLongRequiredStatus()
    {
        var longStatus = new string(OversizePayloadCharacter, TooLongOptionalTextCharacters);
        var operation = CreateOperation(CreateEnvelope($$"""{"status":"{{longStatus}}"}"""));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects optional field text beyond the documented bound.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsTooLongOptionalText()
    {
        var longTitle = new string(OversizePayloadCharacter, TooLongOptionalTextCharacters);
        var operation = CreateOperation(CreateEnvelope($$"""{"status":"ready","title":"{{longTitle}}"}"""));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies the custom activity domain rejects payload hash mismatches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsPayloadHashMismatch()
    {
        var operation = CreateOperation(CreateEnvelope(ReadyPayloadJson) with { PayloadHash = InvalidPayloadHash });
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() => ApplyAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies accepted custom activity writes add server-owned metadata to the canonical state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerProducesCustomCanonicalState()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var handler = new ActivityDomainHandler();

        var result = await ApplyAsync(handler, operation).ConfigureAwait(false);

        var json = Encoding.UTF8.GetString(result.NewState.State.Payload.Span);
        await Assert.That(json.Contains("serverAcceptedUtc", StringComparison.Ordinal)).IsTrue();
        await Assert.That(result.Events).Count().IsEqualTo(1);
    }

    /// <summary>Verifies explicit null optional fields clear canonical activity text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerAcceptsExplicitNullOptionalFields()
    {
        var operation = CreateOperation(ActivityPayloads.Create(NullOptionalFieldsPayloadJson));
        var handler = new ActivityDomainHandler();

        var result = await ApplyAsync(handler, operation).ConfigureAwait(false);

        var json = Encoding.UTF8.GetString(result.NewState.State.Payload.Span);
        await Assert.That(json.Contains("\"title\":null", StringComparison.Ordinal)).IsTrue();
        await Assert.That(json.Contains("\"details\":null", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies omitted optional fields preserve the current canonical activity text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerPreservesOmittedOptionalFields()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var current = CreateCanonicalEnvelope(
            operation,
            ServerCommittedAtUtc,
            ClientId,
            InitialVersion,
            ExistingTitle,
            ExistingDetails);
        var handler = new ActivityDomainHandler();

        var result = await ApplyWithCurrentStateAsync(handler, operation, current).ConfigureAwait(false);

        var json = Encoding.UTF8.GetString(result.NewState.State.Payload.Span);
        await Assert.That(json.Contains($"\"title\":\"{ExistingTitle}\"", StringComparison.Ordinal)).IsTrue();
        await Assert.That(json.Contains($"\"details\":\"{ExistingDetails}\"", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies canonical server timestamps require trusted server write provenance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsUntrustedTimestampContext()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => ApplyWithoutServerWriteAsync(handler, operation).AsTask());
    }

    /// <summary>Verifies resolved canonical payloads still require trusted server write provenance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsResolvedPayloadWithoutTrustedTimestampContext()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var resolved = CreateCanonicalEnvelope(operation, ServerCommittedAtUtc);
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            ApplyResolvedWithoutServerWriteAsync(handler, operation, resolved).AsTask());
    }

    /// <summary>Verifies resolved canonical payload timestamps must match trusted server write provenance.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsResolvedPayloadWithCallerTimestamp()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var resolved = CreateCanonicalEnvelope(operation, OperationTimestampUtc);
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            ApplyResolvedWithServerWriteAsync(handler, operation, resolved).AsTask());
    }

    /// <summary>Verifies resolved payload lookup skips conflict entries for other operations before selecting the matching one.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerSkipsResolvedConflictsForOtherOperations()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var resolved = CreateCanonicalEnvelope(operation, ServerCommittedAtUtc);
        var otherConflict = new ResolvedConflict(OperationId.New(), ActivityResolutionCode, null);
        var matchingConflict = new ResolvedConflict(operation.OperationId, ActivityResolutionCode, resolved);
        var handler = new ActivityDomainHandler();

        var result = await ApplyResolvedConflictsWithServerWriteAsync(
            handler,
            operation,
            [otherConflict, matchingConflict]).ConfigureAwait(false);

        await Assert.That(result.NewState.State).IsEqualTo(resolved);
    }

    /// <summary>Verifies resolved activity conflicts must include canonical payload bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsResolvedConflictWithoutPayload()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            ApplyResolvedWithServerWriteAsync(handler, operation, null).AsTask());
    }

    /// <summary>Verifies resolved activity conflicts must contain valid canonical JSON.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsMalformedResolvedCanonicalJson()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var resolved = CreateEnvelope(MalformedPayloadJson);
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<ArgumentException>(() =>
            ApplyResolvedWithServerWriteAsync(handler, operation, resolved).AsTask());
    }

    /// <summary>Verifies resolved activity payloads cannot claim a different accepted client.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsResolvedPayloadWithMismatchedClient()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var resolved = CreateCanonicalEnvelope(operation, ServerCommittedAtUtc, "other-client", AcceptedVersion);
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            ApplyResolvedWithServerWriteAsync(handler, operation, resolved).AsTask());
    }

    /// <summary>Verifies resolved activity payloads cannot claim a different server version.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActivityDomainHandlerRejectsResolvedPayloadWithMismatchedVersion()
    {
        var operation = CreateOperation(ActivityPayloads.Create(ReadyPayloadJson));
        var resolved = CreateCanonicalEnvelope(operation, ServerCommittedAtUtc, ClientId, "activity-v9");
        var handler = new ActivityDomainHandler();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            ApplyResolvedWithServerWriteAsync(handler, operation, resolved).AsTask());
    }

    /// <summary>Applies an operation to the activity domain handler with a deterministic conflict context.</summary>
    /// <param name="handler">The activity domain handler under test.</param>
    /// <param name="operation">The operation to apply.</param>
    /// <returns>The domain apply result.</returns>
    private static ValueTask<ServerDomainApplyResult> ApplyAsync(ActivityDomainHandler handler, SyncOperation operation)
    {
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialVersion));
        var conflict = new ConflictContext(
            current,
            [operation],
            new(ClientId, TenantId),
            new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } });
        var context = new ServerDomainApplyContext
        {
            Client = new(ClientId, TenantId),
            Operation = operation,
            Conflict = conflict,
            Resolution = new([operation.OperationId], [], [], [], AcceptedVersion),
        };

        return handler.ApplyAsync(context, CancellationToken.None);
    }

    /// <summary>Applies an operation with no trusted server write stamp.</summary>
    /// <param name="handler">The activity domain handler under test.</param>
    /// <param name="operation">The operation to apply.</param>
    /// <returns>The domain apply result.</returns>
    private static ValueTask<ServerDomainApplyResult> ApplyWithoutServerWriteAsync(
        ActivityDomainHandler handler,
        SyncOperation operation)
    {
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialVersion));
        var conflict = new ConflictContext(current, [operation], new(ClientId, TenantId));
        var context = new ServerDomainApplyContext
        {
            Client = new(ClientId, TenantId),
            Operation = operation,
            Conflict = conflict,
            Resolution = new([operation.OperationId], [], [], [], AcceptedVersion),
        };

        return handler.ApplyAsync(context, CancellationToken.None);
    }

    /// <summary>Applies an operation with an explicit current canonical state.</summary>
    /// <param name="handler">The activity domain handler under test.</param>
    /// <param name="operation">The operation to apply.</param>
    /// <param name="currentState">The current canonical state.</param>
    /// <returns>The domain apply result.</returns>
    private static ValueTask<ServerDomainApplyResult> ApplyWithCurrentStateAsync(
        ActivityDomainHandler handler,
        SyncOperation operation,
        PayloadEnvelope currentState)
    {
        var current = new ServerState(CollaborationStreamRegistrations.ActivityStream, InitialVersion, currentState);
        var conflict = new ConflictContext(
            current,
            [operation],
            new(ClientId, TenantId),
            new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } });
        var context = new ServerDomainApplyContext
        {
            Client = new(ClientId, TenantId),
            Operation = operation,
            Conflict = conflict,
            Resolution = new([operation.OperationId], [], [], [], AcceptedVersion),
        };

        return handler.ApplyAsync(context, CancellationToken.None);
    }

    /// <summary>Applies an operation with a custom resolved-conflict list and trusted server write stamp.</summary>
    /// <param name="handler">The activity domain handler under test.</param>
    /// <param name="operation">The operation to apply.</param>
    /// <param name="conflicts">The resolved conflict entries.</param>
    /// <returns>The domain apply result.</returns>
    private static ValueTask<ServerDomainApplyResult> ApplyResolvedConflictsWithServerWriteAsync(
        ActivityDomainHandler handler,
        SyncOperation operation,
        IReadOnlyList<ResolvedConflict> conflicts)
    {
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialVersion));
        var conflict = new ConflictContext(
            current,
            [operation],
            new(ClientId, TenantId),
            new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } });
        var context = new ServerDomainApplyContext
        {
            Client = new(ClientId, TenantId),
            Operation = operation,
            Conflict = conflict,
            Resolution = new([operation.OperationId], [], conflicts, [], AcceptedVersion),
        };

        return handler.ApplyAsync(context, CancellationToken.None);
    }

    /// <summary>Applies an operation with a resolved payload and no trusted server write stamp.</summary>
    /// <param name="handler">The activity domain handler under test.</param>
    /// <param name="operation">The operation to apply.</param>
    /// <param name="resolved">The resolved payload.</param>
    /// <returns>The domain apply result.</returns>
    private static ValueTask<ServerDomainApplyResult> ApplyResolvedWithoutServerWriteAsync(
        ActivityDomainHandler handler,
        SyncOperation operation,
        PayloadEnvelope resolved)
    {
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialVersion));
        var conflict = new ConflictContext(current, [operation], new(ClientId, TenantId));
        var context = new ServerDomainApplyContext
        {
            Client = new(ClientId, TenantId),
            Operation = operation,
            Conflict = conflict,
            Resolution = new(
                [operation.OperationId],
                [],
                [new(operation.OperationId, ActivityResolutionCode, resolved)],
                [],
                AcceptedVersion),
        };

        return handler.ApplyAsync(context, CancellationToken.None);
    }

    /// <summary>Applies an operation with a resolved payload and trusted server write stamp.</summary>
    /// <param name="handler">The activity domain handler under test.</param>
    /// <param name="operation">The operation to apply.</param>
    /// <param name="resolved">The resolved payload.</param>
    /// <returns>The domain apply result.</returns>
    private static ValueTask<ServerDomainApplyResult> ApplyResolvedWithServerWriteAsync(
        ActivityDomainHandler handler,
        SyncOperation operation,
        PayloadEnvelope? resolved)
    {
        var current = new ServerState(
            CollaborationStreamRegistrations.ActivityStream,
            InitialVersion,
            ActivityPayloadTestFactory.CreateInitialState(InitialVersion));
        var conflict = new ConflictContext(
            current,
            [operation],
            new(ClientId, TenantId),
            new() { CandidateWrite = new() { ClientId = ClientId, CommittedAtUtc = ServerCommittedAtUtc, OperationId = operation.OperationId } });
        var context = new ServerDomainApplyContext
        {
            Client = new(ClientId, TenantId),
            Operation = operation,
            Conflict = conflict,
            Resolution = new(
                [operation.OperationId],
                [],
                [new(operation.OperationId, ActivityResolutionCode, resolved)],
                [],
                AcceptedVersion),
        };

        return handler.ApplyAsync(context, CancellationToken.None);
    }

    /// <summary>Creates a payload envelope for direct handler validation tests.</summary>
    /// <param name="json">The JSON text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreateEnvelope(string json)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        return new(ActivityPayloads.ContractId, ActivityPayloadContractVersion, ActivityPayloads.ContentType, bytes, CreateHash(bytes));
    }

    /// <summary>Creates a resolved canonical payload envelope for direct handler validation tests.</summary>
    /// <param name="operation">The operation represented by the canonical payload.</param>
    /// <param name="acceptedUtc">The accepted timestamp written into the payload.</param>
    /// <returns>The canonical payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreateCanonicalEnvelope(SyncOperation operation, DateTimeOffset acceptedUtc) =>
        CreateCanonicalEnvelope(operation, acceptedUtc, ClientId, AcceptedVersion);

    /// <summary>Creates a resolved canonical payload envelope for direct handler validation tests.</summary>
    /// <param name="operation">The operation represented by the canonical payload.</param>
    /// <param name="acceptedUtc">The accepted timestamp written into the payload.</param>
    /// <param name="acceptedClientId">The accepted client identifier written into the payload.</param>
    /// <param name="acceptedVersion">The accepted server version written into the payload.</param>
    /// <returns>The canonical payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreateCanonicalEnvelope(
        SyncOperation operation,
        DateTimeOffset acceptedUtc,
        string acceptedClientId,
        string acceptedVersion) =>
        CreateCanonicalEnvelope(operation, acceptedUtc, acceptedClientId, acceptedVersion, null, null);

    /// <summary>Creates a resolved canonical payload envelope for direct handler validation tests.</summary>
    /// <param name="operation">The operation represented by the canonical payload.</param>
    /// <param name="acceptedUtc">The accepted timestamp written into the payload.</param>
    /// <param name="acceptedClientId">The accepted client identifier written into the payload.</param>
    /// <param name="acceptedVersion">The accepted server version written into the payload.</param>
    /// <param name="title">The optional canonical title.</param>
    /// <param name="details">The optional canonical details.</param>
    /// <returns>The canonical payload envelope.</returns>
    private static PayloadEnvelope CreateCanonicalEnvelope(
        SyncOperation operation,
        DateTimeOffset acceptedUtc,
        string acceptedClientId,
        string acceptedVersion,
        string? title,
        string? details)
    {
        var acceptedOperationId = operation.OperationId.Value.ToString(GuidCompactFormat);
        var acceptedAt = acceptedUtc.ToString(RoundTripDateTimeFormat, CultureInfo.InvariantCulture);
        var optionalFields = CreateOptionalCanonicalFields(title, details);
        var json = $$"""
            {
              "status":"ready",{{optionalFields}}
              "acceptedClientId":"{{acceptedClientId}}",
              "acceptedOperationId":"{{acceptedOperationId}}",
              "acceptedVersion":"{{acceptedVersion}}",
              "serverAcceptedUtc":"{{acceptedAt}}"
            }
            """;
        return CreateEnvelope(json);
    }

    /// <summary>Creates canonical JSON fields for optional activity text.</summary>
    /// <param name="title">The optional title.</param>
    /// <param name="details">The optional details.</param>
    /// <returns>The optional JSON field fragment.</returns>
    private static string CreateOptionalCanonicalFields(string? title, string? details)
    {
        var builder = new StringBuilder();
        if (title is not null)
        {
            _ = builder.Append(CultureInfo.InvariantCulture, $"\n  \"title\":\"{title}\",");
        }

        if (details is not null)
        {
            _ = builder.Append(CultureInfo.InvariantCulture, $"\n  \"details\":\"{details}\",");
        }

        return builder.ToString();
    }

    /// <summary>Creates a SHA-256 payload hash.</summary>
    /// <param name="bytes">The bytes to hash.</param>
    /// <returns>The payload hash string.</returns>
    private static string CreateHash(byte[] bytes) =>
        $"sha256-{Convert.ToBase64String(SHA256.HashData(bytes))}";

    /// <summary>Creates a custom activity sync operation for the supplied payload.</summary>
    /// <param name="payload">The payload to attach to the operation.</param>
    /// <returns>The sync operation.</returns>
    private static SyncOperation CreateOperation(PayloadEnvelope payload) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = CollaborationStreamRegistrations.ActivityStream,
            ClientSequence = InitialClientSequence,
            TimestampUtc = OperationTimestampUtc,
            Type = SyncOperationType.Custom,
            Payload = payload,
        };
}
