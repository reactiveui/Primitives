// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>SQLite quarantine boundary tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The maximum retained quarantine evidence prefix byte count.</summary>
    private const int SqliteQuarantineBoundaryEvidenceBytes = 4096;

    /// <summary>A caller supplied value that exceeds the evidence boundary.</summary>
    private const int SqliteOversizedQuarantineBoundaryBytes = SqliteQuarantineBoundaryEvidenceBytes + 904;

    /// <summary>The number of bytes in a kibibyte.</summary>
    private const int KibibyteBytes = 1024;

    /// <summary>The number of bytes in a mebibyte.</summary>
    private const int MebibyteBytes = KibibyteBytes * KibibyteBytes;

    /// <summary>The large corrupt SQLite value size used to detect unbounded managed allocation.</summary>
    private const int LargeCorruptSqliteValueBytes = 16 * MebibyteBytes;

    /// <summary>The valid payload size that exceeds the normal reopened worker capacity.</summary>
    private const int ReopenCapacityRegressionPayloadBytes = 512 * KibibyteBytes;

    /// <summary>The maximum managed allocation allowed while quarantining a large corrupt SQLite value.</summary>
    private const long MaximumLargeCorruptRecoveryAllocationBytes = 8L * MebibyteBytes;

    /// <summary>The worker byte capacity used to write rows larger than the default test adapter budget.</summary>
    private const long LargeWriteWorkerBytes = 64L * MebibyteBytes;

    /// <summary>The expected quarantine marker missing message.</summary>
    private const string ExpectedQuarantineMarkerMessage = "Expected quarantine marker.";

    /// <summary>The test payload contract identifier.</summary>
    private const string BoundaryPayloadContractId = "reading";

    /// <summary>The test payload content type.</summary>
    private const string BoundaryPayloadContentType = "application/json";

    /// <summary>The SQLite payload hash parameter name.</summary>
    private const string PayloadHashParameterName = "$payloadHash";

    /// <summary>The UTF-8 byte count for each generated emoji scalar.</summary>
    private const int EmojiUtf8ByteCount = 4;

    /// <summary>The UTF-16 code-unit count in one surrogate pair.</summary>
    private const int SurrogatePairCodeUnitCount = 2;

    /// <summary>An invalid enum value used for request validation.</summary>
    private const int InvalidQuarantineEnumValue = -1;

    /// <summary>Gets the Base64 payload character count in a canonical SHA-256 hash.</summary>
    private const int CanonicalSha256HashPayloadCharacters = 44;

    /// <summary>Verifies caller supplied evidence is normalized before SQLite retains a marker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    public async Task WhenSuppliedQuarantineEvidenceExceedsBounds_ThenStoredMarkerIsBounded()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var request = CreateQuarantineRequest(subscriptionId, operation) with
        {
            Evidence = CreateOversizedSqliteQuarantineBoundaryEvidence(SqliteOversizedQuarantineBoundaryBytes),
            Envelope = null,
        };

        _ = await adapter.QuarantinePayloadAsync(request, CancellationToken.None);
        var marker = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(marker.Evidence.PayloadLength).IsEqualTo(SqliteOversizedQuarantineBoundaryBytes);
        await Assert.That(marker.Evidence.PayloadPrefix.Length).IsEqualTo(SqliteQuarantineBoundaryEvidenceBytes);
        await Assert.That(marker.Evidence.PayloadPrefix.ToArray().SequenceEqual(CreateExpectedSqliteBoundaryPrefix())).IsTrue();
        await AssertSqliteBoundedUtf8Async(marker.Evidence.ContractId);
        await AssertSqliteBoundedUtf8Async(marker.Evidence.ContentType);
        await AssertSqliteBoundedUtf8Async(marker.Evidence.PayloadHash);
    }

    /// <summary>Verifies impossible caller evidence cannot poison the SQLite stream.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSuppliedQuarantineEvidencePrefixExceedsPayloadLength_ThenWriteRejectsWithoutPoisoning()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var request = CreateQuarantineRequest(subscriptionId, operation) with
        {
            Evidence = new(BoundaryPayloadContractId, SchemaVersion, BoundaryPayloadContentType, 1, "hash", "xx"u8.ToArray()),
            Envelope = null,
        };
        Func<Task> quarantine = () => adapter.QuarantinePayloadAsync(request, CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<ArgumentException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies oversized valid Unicode evidence metadata is bounded without splitting a surrogate pair.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker or metadata is missing.</exception>
    [Test]
    public async Task WhenSuppliedUnicodeEvidenceMetadataExceedsBounds_ThenStoredMarkerKeepsSurrogateBoundary()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var unicode = CreateOversizedSqliteUnicodeEvidenceText();
        var request = CreateQuarantineRequest(subscriptionId, operation) with
        {
            Evidence = new(unicode, SchemaVersion, unicode, FirstClientSequence, unicode, "x"u8.ToArray()),
            Envelope = null,
        };

        _ = await adapter.QuarantinePayloadAsync(request, CancellationToken.None);
        var marker = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await AssertSqliteUnicodeBoundaryAsync(marker.Evidence.ContractId);
        await AssertSqliteUnicodeBoundaryAsync(marker.Evidence.ContentType);
        await AssertSqliteUnicodeBoundaryAsync(marker.Evidence.PayloadHash);
    }

    /// <summary>Verifies overlong and malformed caller request metadata is rejected rather than truncated.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineRequestMetadataIsInvalid_ThenWriteRejectsWithoutPoisoning()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> overlongReasonCode = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { ReasonCode = new('r', SqliteOversizedQuarantineBoundaryBytes) },
            CancellationToken.None).AsTask();
        Func<Task> malformedCursor = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { Cursor = "\uD800" },
            CancellationToken.None).AsTask();

        await Assert.That(overlongReasonCode).ThrowsExactly<ArgumentException>();
        await Assert.That(malformedCursor).ThrowsExactly<ArgumentException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies supplied subscription identities must match durable SQLite binding before poisoning.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineSubscriptionDoesNotMatchDurableBinding_ThenWriteRejectsWithoutPoisoning()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> quarantine = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                SubscriptionId = SubscriptionId.New(),
                Evidence = CreateSqliteRawEvidence(),
                Envelope = null,
            },
            CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies a supplied operation id must belong to the request stream before poisoning.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineOperationBelongsToAnotherStream_ThenWriteRejectsWithoutPoisoning()
    {
        using var database = TempDatabase.Create();
        var otherStream = new StreamId("sensor/humidity");
        var operation = CreateOperation(FirstClientSequence);
        var otherOperation = CreateOperation(FirstClientSequence) with { StreamId = otherStream };
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(otherStream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(otherOperation, CreateSnapshotMutation(otherStream, 0), CancellationToken.None);
        Func<Task> quarantine = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                OperationId = otherOperation.OperationId,
                Evidence = CreateSqliteRawEvidence(),
                Envelope = null,
            },
            CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(otherStream, CancellationToken.None)).IsNull();
        var lease = await ReadSingleLeaseAsync(adapter, new(otherStream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(otherOperation.OperationId);
    }

    /// <summary>Verifies idempotent SQLite requests still validate caller identity before returning an existing marker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExistingMarkerReceivesMismatchedIdempotentRequest_ThenWriteRejects()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var first = await adapter.QuarantinePayloadAsync(CreateQuarantineRequest(subscriptionId, operation), CancellationToken.None);
        Func<Task> idempotentMismatch = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { SubscriptionId = SubscriptionId.New() },
            CancellationToken.None).AsTask();

        await Assert.That(idempotentMismatch).ThrowsExactly<InvalidOperationException>();
        var marker = await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None);
        await Assert.That(marker?.QuarantineId).IsEqualTo(first.Record.QuarantineId);
    }

    /// <summary>Verifies invalid SQLite quarantine request identifiers and classifications are rejected before poisoning.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineRequestIdentityOrClassificationIsInvalid_ThenWriteRejectsWithoutPoisoning()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var evidence = CreateSqliteRawEvidence();
        Func<Task> emptySubscription = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { SubscriptionId = new(Guid.Empty), Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> emptyOperation = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { OperationId = new(Guid.Empty), Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> emptyEvent = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with { EventId = Guid.Empty, Evidence = evidence, Envelope = null },
            CancellationToken.None).AsTask();
        Func<Task> invalidSource = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                Source = (LocalPayloadQuarantineSource)InvalidQuarantineEnumValue,
                Evidence = evidence,
                Envelope = null,
            },
            CancellationToken.None).AsTask();
        Func<Task> invalidReason = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                Reason = (LocalPayloadQuarantineReason)InvalidQuarantineEnumValue,
                Evidence = evidence,
                Envelope = null,
            },
            CancellationToken.None).AsTask();

        await Assert.That(emptySubscription).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyOperation).ThrowsExactly<ArgumentException>();
        await Assert.That(emptyEvent).ThrowsExactly<ArgumentException>();
        await Assert.That(invalidSource).ThrowsExactly<ArgumentException>();
        await Assert.That(invalidReason).ThrowsExactly<ArgumentException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies SQLite rejects an unregistered operation id before writing a quarantine marker.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineOperationIsUnregistered_ThenWriteRejectsWithoutPoisoning()
    {
        using var database = TempDatabase.Create();
        var operation = CreateOperation(FirstClientSequence);
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        Func<Task> quarantine = () => adapter.QuarantinePayloadAsync(
            CreateQuarantineRequest(subscriptionId, operation) with
            {
                OperationId = OperationId.New(),
                Evidence = CreateSqliteRawEvidence(),
                Envelope = null,
            },
            CancellationToken.None).AsTask();

        await Assert.That(quarantine).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        var lease = await ReadSingleLeaseAsync(adapter, new(Stream, FirstAttempt, NormalWorkerBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies an oversized envelope-only quarantine request is rejected by admission before poisoning.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenQuarantineEnvelopeExceedsWorkerBudget_ThenAdmissionRejectsBeforePoisoning()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = TinyWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);
        await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        LocalPayloadQuarantineRequest request = new()
        {
            StreamId = Stream,
            Source = LocalPayloadQuarantineSource.Snapshot,
            Reason = LocalPayloadQuarantineReason.SchemaRejected,
            Envelope = CreatePayload(new('q', OversizedPayloadLength)),
            ObservedAtUtc = DateTimeOffset.UnixEpoch,
        };
        Func<Task> quarantine = () => adapter.QuarantinePayloadAsync(request, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(quarantine);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(await adapter.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies an empty persisted payload hash is quarantined before payload materialization.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    public async Task WhenPersistedSnapshotPayloadHashIsEmpty_ThenRecoveryQuarantinesRawEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        }

        CorruptSnapshotPayloadHash(database.Path, string.Empty);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker.Evidence.PayloadHash).IsEqualTo(string.Empty);
    }

    /// <summary>Verifies malformed SQLite TEXT hash bytes are captured as unreadable evidence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    public async Task WhenPersistedSnapshotPayloadHashHasMalformedUtf8_ThenRecoveryQuarantinesUnreadableEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        }

        CorruptSnapshotPayloadHashBytesAsText(database.Path, "FF");
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker.Evidence.PayloadHash).IsNull();
    }

    /// <summary>Verifies exact-length malformed SHA-256 hash metadata is quarantined before payload materialization.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    public async Task WhenAuthoritativePayloadHashHasMalformedCanonicalShape_ThenRecoveryQuarantinesRawEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        await using (var adapter = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes))
        {
            var snapshotMutation = CreateSnapshotMutation(0) with { AuthoritativeState = CreateHashedPayload((byte)'c', FirstClientSequence) };
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), snapshotMutation, CancellationToken.None);
        }

        CorruptAuthoritativeSnapshotPayloadHashTo(database.Path, $"sha256-{new string('!', CanonicalSha256HashPayloadCharacters)}");
        await using var reopened = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker.Evidence.PayloadHash).StartsWith("sha256-");
    }

    /// <summary>Verifies malformed SQLite TEXT metadata bytes are captured without manufacturing invalid Unicode evidence.</summary>
    /// <param name="metadataHex">The raw metadata bytes to store in a TEXT column.</param>
    /// <param name="expectedContractId">The expected retained contract identifier.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    [Arguments("C0", null)]
    [Arguments("C2", "")]
    [Arguments("C2A2", "\u00A2")]
    [Arguments("E2", "")]
    [Arguments("E08080", null)]
    [Arguments("E282AC", "\u20AC")]
    [Arguments("EDA080", null)]
    [Arguments("E24180", null)]
    [Arguments("F0", "")]
    [Arguments("F0808080", null)]
    [Arguments("F1808080", "\U00040000")]
    [Arguments("F48FBFBF", "\U0010FFFF")]
    [Arguments("F4908080", null)]
    [Arguments("F09F9880", "\U0001F600")]
    public async Task WhenPersistedSnapshotMetadataHasUtf8BoundaryBytes_ThenRecoveryQuarantinesBoundedEvidence(
        string metadataHex,
        string? expectedContractId)
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), CreateSnapshotMutation(0), CancellationToken.None);
        }

        CorruptSnapshotPayloadContractBytesAndSchema(database.Path, metadataHex);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker.Evidence.ContractId).IsEqualTo(expectedContractId);
    }

    /// <summary>Verifies SQLite raw provider metadata corruption is retained as bounded evidence.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    public async Task WhenPersistedSnapshotMetadataTextIsOversized_ThenRecoveryQuarantinesBoundedRawEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        var snapshotMutation = CreateSnapshotMutation(0);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, snapshotMutation, CancellationToken.None);
        }

        CorruptSnapshotPayloadSchemaVersionAndOversizedMetadata(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker.Evidence.SchemaVersion).IsEqualTo(0);
        await Assert.That(marker.Evidence.PayloadLength).IsEqualTo(snapshotMutation.State.PayloadLength);
        await AssertSqliteBoundedUtf8Async(marker.Evidence.ContractId);
        await AssertSqliteBoundedUtf8Async(marker.Evidence.ContentType);
        await AssertSqliteBoundedUtf8Async(marker.Evidence.PayloadHash);
    }

    /// <summary>Verifies a large wrong-storage payload is quarantined without materializing the corrupt value.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    [NotInParallel]
    public async Task WhenPersistedSnapshotPayloadStorageIsOversizedText_ThenRecoveryUsesBoundedRawEvidence()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        }

        CorruptSnapshotPayloadWithLargeTextStorage(database.Path);
        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(allocated).IsLessThan(MaximumLargeCorruptRecoveryAllocationBytes);
        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker.Evidence.PayloadLength).IsEqualTo(LargeCorruptSqliteValueBytes);
        await Assert.That(marker.Evidence.PayloadPrefix.Length).IsEqualTo(SqliteQuarantineBoundaryEvidenceBytes);
    }

    /// <summary>Verifies shrinking the current recovery budget does not misclassify a valid stored row as corrupt.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected recovered snapshot is missing.</exception>
    [Test]
    public async Task WhenValidSnapshotWasWrittenWithLargerCapacity_ThenSmallerReopenRejectsWithoutPoisoning()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var snapshotMutation = new SnapshotMutation(
            Stream,
            CreateHashedPayload((byte)'s', ReopenCapacityRegressionPayloadBytes),
            FormatVersion: 1,
            ExpectedRevision: 0);
        await using (var adapter = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), snapshotMutation, CancellationToken.None);
        }

        await using (var reopened = CreateAdapter(database.Path))
        {
            await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

            await Assert.That(recover).ThrowsExactly<QueueCapacityExceededException>();
            await Assert.That(await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
        }

        await using var larger = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes);
        await larger.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovered = await larger.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var snapshot = recovered.Snapshot ?? throw new InvalidOperationException("Expected recovered snapshot.");

        await Assert.That(recovered.SubscriptionId).IsEqualTo(subscriptionId);
        await Assert.That(snapshot.StreamId).IsEqualTo(Stream);
        await Assert.That(snapshot.Revision).IsEqualTo(FirstClientSequence);
        await Assert.That(snapshot.State.ContractId).IsEqualTo(snapshotMutation.State.ContractId);
        await Assert.That(snapshot.State.SchemaVersion).IsEqualTo(snapshotMutation.State.SchemaVersion);
        await Assert.That(snapshot.State.ContentType).IsEqualTo(snapshotMutation.State.ContentType);
        await Assert.That(snapshot.State.PayloadHash).IsEqualTo(snapshotMutation.State.PayloadHash);
        await Assert.That(snapshot.State.Payload.ToArray().SequenceEqual(snapshotMutation.State.Payload.ToArray())).IsTrue();
        await Assert.That(await larger.GetPayloadQuarantineAsync(Stream, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies large valid-storage metadata with a bad hash shape is rejected before full text allocation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    [NotInParallel]
    public async Task WhenAuthoritativePayloadHashMetadataIsHuge_ThenRecoveryQuarantinesWithoutFullMetadataAllocation()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes))
        {
            var snapshotMutation = CreateSnapshotMutation(0) with { AuthoritativeState = CreateHashedPayload((byte)'a', FirstClientSequence) };
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, snapshotMutation, CancellationToken.None);
        }

        CorruptAuthoritativeSnapshotPayloadHashWithLargeText(database.Path);
        await using var reopened = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(allocated).IsLessThan(MaximumLargeCorruptRecoveryAllocationBytes);
        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await AssertSqliteBoundedUtf8Async(marker.Evidence.PayloadHash);
    }

    /// <summary>Verifies a large BLOB with a mismatched hash is hashed before the payload is materialized.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    /// <exception cref="InvalidOperationException">The expected marker is missing.</exception>
    [Test]
    [NotInParallel]
    public async Task WhenAuthoritativePayloadBlobHashMismatches_ThenRecoveryQuarantinesWithoutFullPayloadAllocation()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes))
        {
            var snapshotMutation = CreateSnapshotMutation(0) with
            {
                AuthoritativeState = CreateHashedPayload((byte)'b', LargeCorruptSqliteValueBytes),
            };
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, snapshotMutation, CancellationToken.None);
        }

        CorruptAuthoritativeSnapshotPayloadHash(database.Path);
        await using var reopened = CreateSqliteBoundaryAdapter(database.Path, LargeWriteWorkerBytes);
        await reopened.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> recover = () => reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None).AsTask();

        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        var marker = await reopened.GetPayloadQuarantineAsync(Stream, CancellationToken.None)
            ?? throw new InvalidOperationException(ExpectedQuarantineMarkerMessage);

        await Assert.That(allocated).IsLessThan(MaximumLargeCorruptRecoveryAllocationBytes);
        await Assert.That(marker.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(marker.Reason).IsEqualTo(LocalPayloadQuarantineReason.PersistedRecordCorrupt);
        await Assert.That(marker.Evidence.PayloadLength).IsEqualTo(LargeCorruptSqliteValueBytes);
        await Assert.That(marker.Evidence.PayloadPrefix.Length).IsEqualTo(SqliteQuarantineBoundaryEvidenceBytes);
    }

    /// <summary>Creates oversized supplied evidence for SQLite boundary tests.</summary>
    /// <param name="payloadLength">The advertised payload length.</param>
    /// <returns>The oversized evidence.</returns>
    private static LocalPayloadQuarantineEvidence CreateOversizedSqliteQuarantineBoundaryEvidence(int payloadLength)
    {
        var prefix = new byte[SqliteOversizedQuarantineBoundaryBytes];
        Array.Fill(prefix, (byte)'x');
        return new(
            new('c', SqliteOversizedQuarantineBoundaryBytes),
            SchemaVersion,
            new('t', SqliteOversizedQuarantineBoundaryBytes),
            payloadLength,
            new('h', SqliteOversizedQuarantineBoundaryBytes),
            prefix);
    }

    /// <summary>Creates an adapter with a boundary-test worker byte capacity.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="workerBytes">The worker byte capacity.</param>
    /// <returns>The configured adapter.</returns>
    private static SqliteLocalStoreAdapter CreateSqliteBoundaryAdapter(string path, long workerBytes) =>
        new(path, new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = workerBytes });

    /// <summary>Creates a payload envelope with a valid SHA-256 hash.</summary>
    /// <param name="fill">The byte value used to fill the payload.</param>
    /// <param name="payloadBytes">The payload byte count.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreateHashedPayload(byte fill, int payloadBytes)
    {
        var payload = new byte[payloadBytes];
        Array.Fill(payload, fill);
        return new(BoundaryPayloadContractId, SchemaVersion, BoundaryPayloadContentType, payload, ComputeSha256PayloadHash(payload));
    }

    /// <summary>Computes the canonical SHA-256 payload hash used by SQLite authoritative validation.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The formatted payload hash.</returns>
    private static string ComputeSha256PayloadHash(byte[] payload) =>
        $"sha256-{Convert.ToBase64String(SHA256.HashData(payload))}";

    /// <summary>Creates raw evidence for SQLite quarantine tests.</summary>
    /// <returns>The evidence.</returns>
    private static LocalPayloadQuarantineEvidence CreateSqliteRawEvidence() =>
        new(BoundaryPayloadContractId, SchemaVersion, BoundaryPayloadContentType, FirstClientSequence, "hash", "x"u8.ToArray());

    /// <summary>Creates the expected retained evidence prefix.</summary>
    /// <returns>The expected prefix.</returns>
    private static byte[] CreateExpectedSqliteBoundaryPrefix()
    {
        var prefix = new byte[SqliteQuarantineBoundaryEvidenceBytes];
        Array.Fill(prefix, (byte)'x');
        return prefix;
    }

    /// <summary>Asserts the supplied text fits the quarantine metadata evidence boundary.</summary>
    /// <param name="value">The value to inspect.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    /// <exception cref="InvalidOperationException">The expected metadata is missing.</exception>
    private static async Task AssertSqliteBoundedUtf8Async(string? value)
    {
        if (value is null)
        {
            throw new InvalidOperationException("Expected retained evidence metadata.");
        }

        await Assert.That(System.Text.Encoding.UTF8.GetByteCount(value)).IsLessThanOrEqualTo(SqliteQuarantineBoundaryEvidenceBytes);
    }

    /// <summary>Creates valid oversized Unicode evidence metadata.</summary>
    /// <returns>The oversized Unicode string.</returns>
    private static string CreateOversizedSqliteUnicodeEvidenceText()
    {
        var builder = new System.Text.StringBuilder();
        for (var i = 0; i < (SqliteOversizedQuarantineBoundaryBytes / EmojiUtf8ByteCount) + 1; i++)
        {
            _ = builder.Append("\U0001F600");
        }

        return builder.ToString();
    }

    /// <summary>Asserts that retained Unicode metadata is bounded and ends on a surrogate-pair boundary.</summary>
    /// <param name="value">The retained value.</param>
    /// <returns>A task representing the asynchronous assertion.</returns>
    /// <exception cref="InvalidOperationException">The expected metadata is missing.</exception>
    private static async Task AssertSqliteUnicodeBoundaryAsync(string? value)
    {
        if (value is null)
        {
            throw new InvalidOperationException("Expected retained evidence metadata.");
        }

        await Assert.That(System.Text.Encoding.UTF8.GetByteCount(value)).IsEqualTo(SqliteQuarantineBoundaryEvidenceBytes);
        await Assert.That(char.IsSurrogatePair(value, value.Length - SurrogatePairCodeUnitCount)).IsTrue();
    }

    /// <summary>Corrupts the persisted snapshot schema and metadata with oversized text.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptSnapshotPayloadSchemaVersionAndOversizedMetadata(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_contract_id = $contractId,
                payload_schema_version = 0,
                payload_content_type = $contentType,
                payload_hash = $payloadHash
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$contractId", new string('c', SqliteOversizedQuarantineBoundaryBytes));
        _ = command.Parameters.AddWithValue("$contentType", new string('t', SqliteOversizedQuarantineBoundaryBytes));
        _ = command.Parameters.AddWithValue(PayloadHashParameterName, new string('h', SqliteOversizedQuarantineBoundaryBytes));
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot payload hash.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="payloadHash">The payload hash to store.</param>
    private static void CorruptSnapshotPayloadHash(string path, string payloadHash)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_hash = $payloadHash
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(PayloadHashParameterName, payloadHash);
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot payload hash with raw bytes stored as TEXT.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="payloadHashHex">The raw payload hash bytes to store as TEXT.</param>
    private static void CorruptSnapshotPayloadHashBytesAsText(string path, string payloadHashHex)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_hash = CAST($payloadHashBytes AS TEXT)
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.Add("$payloadHashBytes", SqliteType.Blob);
        command.Parameters["$payloadHashBytes"].Value = Convert.FromHexString(payloadHashHex);
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted authoritative snapshot payload hash to a supplied value.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="payloadHash">The payload hash to store.</param>
    private static void CorruptAuthoritativeSnapshotPayloadHashTo(string path, string payloadHash)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshot_authoritative_states
            SET payload_hash = $payloadHash
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(PayloadHashParameterName, payloadHash);
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot contract bytes and schema version.</summary>
    /// <param name="path">The SQLite database path.</param>
    /// <param name="metadataHex">The raw metadata bytes to store as TEXT.</param>
    private static void CorruptSnapshotPayloadContractBytesAndSchema(string path, string metadataHex)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload_contract_id = CAST($metadataBytes AS TEXT),
                payload_schema_version = 0
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.Add("$metadataBytes", SqliteType.Blob);
        command.Parameters["$metadataBytes"].Value = Convert.FromHexString(metadataHex);
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the persisted snapshot payload storage with oversized text.</summary>
    /// <param name="path">The SQLite database path.</param>
    private static void CorruptSnapshotPayloadWithLargeTextStorage(string path)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshots
            SET payload = replace(hex(zeroblob($payloadBytes)), '00', 'p')
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$payloadBytes", LargeCorruptSqliteValueBytes);
        AddStreamParameters(command);
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Corrupts the authoritative snapshot payload hash with a huge invalid text value.</summary>
    /// <param name="path">The SQLite database path.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CorruptAuthoritativeSnapshotPayloadHashWithLargeText(string path) =>
        CorruptAuthoritativeSnapshotPayloadHashTo(path, $"sha256-{new string('h', LargeCorruptSqliteValueBytes)}");

    /// <summary>Corrupts the authoritative snapshot payload hash with a well-formed mismatched SHA-256 value.</summary>
    /// <param name="path">The SQLite database path.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CorruptAuthoritativeSnapshotPayloadHash(string path) =>
        CorruptAuthoritativeSnapshotPayloadHashTo(path, ComputeSha256PayloadHash("mismatch"u8.ToArray()));
}
