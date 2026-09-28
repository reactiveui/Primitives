// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Authoritative snapshot state tests.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The first authoritative payload text.</summary>
    private const string AuthoritativeInitialText = "auth-0";

    /// <summary>The replacement authoritative payload text.</summary>
    private const string AuthoritativeRemoteText = "auth-7";

    /// <summary>The changed authoritative payload text.</summary>
    private const string AuthoritativeChangedText = "auth-changed";

    /// <summary>A mismatched canonical SHA-256 payload hash.</summary>
    private const string TamperedCanonicalPayloadHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    /// <summary>A malformed canonical SHA-256 payload hash.</summary>
    private const string MalformedCanonicalPayloadHash = "sha256-malformed";

    /// <summary>The first optimistic payload text.</summary>
    private const string OptimisticInitialText = "optimistic-21";

    /// <summary>The second optimistic payload text.</summary>
    private const string OptimisticLocalText = "optimistic-22";

    /// <summary>The remote optimistic payload text.</summary>
    private const string OptimisticRemoteText = "optimistic-12";

    /// <summary>The first client sequence.</summary>
    private const int FirstClientSequence = 1;

    /// <summary>The second snapshot revision.</summary>
    private const int SecondSnapshotRevision = 2;

    /// <summary>Verifies a local authoritative checkpoint is durable and remains independent from optimistic state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeStateIsCommittedAndStoreReopens_ThenDistinctSnapshotHalvesRecover()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText);

        _ = store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies recovery rejects a tampered authoritative payload hash.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeSnapshotHashIsTampered_ThenRecoveryRejectsIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticInitialText), FormatVersion: 1, ExpectedRevision: 0)
        {
            AuthoritativeState = CreateCanonicalPayload(AuthoritativeInitialText),
        };
        _ = store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None);
        SetAuthoritativeSnapshotHash(database.Path, TamperedCanonicalPayloadHash);

        using var reopened = CreateInitializedStore(database.Path);
        var recover = new Action(() => reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None));

        await Assert.That(recover).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies authoritative mutations with invalid canonical hashes are rejected before commit.</summary>
    /// <param name="payloadHash">The invalid canonical payload hash.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments(MalformedCanonicalPayloadHash)]
    [Arguments(TamperedCanonicalPayloadHash)]
    public async Task WhenAuthoritativeMutationHashIsInvalid_ThenCommitRejectsIt(string payloadHash)
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        _ = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticInitialText), FormatVersion: 1, ExpectedRevision: 0)
        {
            AuthoritativeState = CreatePayloadWithHash(AuthoritativeInitialText, payloadHash),
        };

        var commit = new Action(() => store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None));

        await Assert.That(commit).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies non-SHA opaque authoritative hashes are not reinterpreted during recovery.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeOpaqueHashHasCanonicalLength_ThenRecoveryAcceptsIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var snapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticInitialText), FormatVersion: 1, ExpectedRevision: 0)
        {
            AuthoritativeState = CreatePayloadWithHash(AuthoritativeInitialText, new('x', TamperedCanonicalPayloadHash.Length)),
        };
        _ = store.CommitLocalOperation(CreateOperation(FirstClientSequence), snapshot, CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies local commits without authoritative state preserve the previously stored authoritative checkpoint.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLocalPublishOmitsAuthoritativeState_ThenPreviousAuthoritativeStateIsPreserved()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(
            CreateOperation(FirstClientSequence),
            CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText),
            CancellationToken.None);

        _ = store.CommitLocalOperation(
            CreateOperation(SecondClientSequence),
            CreateMutation(expectedRevision: 1, OptimisticLocalText, authoritativeText: null),
            CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticLocalText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies remote apply replaces authoritative and optimistic state in one durable transaction.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRemoteApplySuppliesAuthoritativeState_ThenAtomicSnapshotPairRecovers()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(
            CreateOperation(FirstClientSequence),
            CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText),
            CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);

        var result = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [remoteEvent]),
            CreateMutation(expectedRevision: 1, OptimisticRemoteText, AuthoritativeRemoteText),
            CancellationToken.None);

        using var reopened = CreateInitializedStore(database.Path);
        var recovery = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(result.SnapshotRevision).IsEqualTo(SecondSnapshotRevision);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticRemoteText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeRemoteText);
        await Assert.That(recovery.Snapshot?.ServerCursor).IsEqualTo(FirstRemoteCursor);
    }

    /// <summary>Verifies stale and aborted writes leave both snapshot halves unchanged.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeSnapshotWriteFails_ThenBothSnapshotHalvesRemainUnchanged()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = store.CommitLocalOperation(
            CreateOperation(FirstClientSequence),
            CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText),
            CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(FirstRemoteCursor);

        Action stale = () => store.CommitLocalOperation(
            CreateOperation(SecondClientSequence),
            CreateMutation(expectedRevision: 0, "optimistic-stale", "auth-stale"),
            CancellationToken.None);
        Action staleRemote = () => store.ApplyRemoteBatch(
            CreateRemoteBatch("wrong-cursor", FirstRemoteCursor, [remoteEvent]),
            CreateMutation(expectedRevision: 1, "optimistic-remote", "auth-remote"),
            CancellationToken.None);

        await Assert.That(stale).ThrowsExactly<InvalidOperationException>();
        await Assert.That(staleRemote).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var unapplied = store.GetUnappliedEventIds(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies duplicate local operation intent includes authoritative mutation presence and content.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDuplicateOperationChangesAuthoritativeMutation_ThenOriginalIntentRejectsIt()
    {
        using var database = TempDatabase.Create();
        using var store = CreateInitializedStore(database.Path);
        var subscriptionId = store.GetOrCreateSubscriptionId(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateMutation(expectedRevision: 0, OptimisticInitialText, AuthoritativeInitialText);
        var first = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        _ = store.ApplyRemoteBatch(
            CreateRemoteBatch(null, FirstRemoteCursor, [CreateRemoteEvent(FirstRemoteCursor)]),
            CreateMutation(expectedRevision: 1, OptimisticRemoteText, AuthoritativeRemoteText),
            CancellationToken.None);

        var replay = store.CommitLocalOperation(operation, snapshot, CancellationToken.None);
        Action changedAuthoritative = () => store.CommitLocalOperation(
            operation,
            snapshot with { AuthoritativeState = CreatePayload(AuthoritativeChangedText) },
            CancellationToken.None);

        await Assert.That(replay).IsEqualTo(first);
        await Assert.That(changedAuthoritative).ThrowsExactly<InvalidOperationException>();
        Action omittedAuthoritative = () => store.CommitLocalOperation(
            operation,
            snapshot with { AuthoritativeState = null },
            CancellationToken.None);
        Action changedOptimistic = () => store.CommitLocalOperation(
            operation,
            snapshot with { State = CreatePayload(OptimisticLocalText) },
            CancellationToken.None);
        await Assert.That(omittedAuthoritative).ThrowsExactly<InvalidOperationException>();
        await Assert.That(changedOptimistic).ThrowsExactly<InvalidOperationException>();
        var recovery = store.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeRemoteText);
    }

    /// <summary>Creates a snapshot mutation.</summary>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <param name="optimisticText">The optimistic payload text.</param>
    /// <param name="authoritativeText">The authoritative payload text.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateMutation(long expectedRevision, string optimisticText, string? authoritativeText)
    {
        var authoritativeState = authoritativeText is null ? null : CreatePayload(authoritativeText);
        return new(Stream, CreatePayload(optimisticText), FormatVersion: 1, expectedRevision) { AuthoritativeState = authoritativeState };
    }

    /// <summary>Creates a canonical SHA-256 payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreateCanonicalPayload(string text)
    {
        var payload = System.Text.Encoding.UTF8.GetBytes(text);
        var hash = $"sha256-{Convert.ToBase64String(System.Security.Cryptography.SHA256.HashData(payload))}";
        return CreatePayloadWithHash(payload, hash);
    }

    /// <summary>Creates a payload envelope with an explicit hash.</summary>
    /// <param name="text">The payload text.</param>
    /// <param name="payloadHash">The payload hash.</param>
    /// <returns>The payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreatePayloadWithHash(string text, string payloadHash) =>
        CreatePayloadWithHash(System.Text.Encoding.UTF8.GetBytes(text), payloadHash);

    /// <summary>Creates a payload envelope with explicit payload bytes and hash.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="payloadHash">The payload hash.</param>
    /// <returns>The payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreatePayloadWithHash(byte[] payload, string payloadHash) =>
        new("reading", 1, "application/json", payload, payloadHash);

    /// <summary>Tampers with the current authoritative snapshot payload hash.</summary>
    /// <param name="path">The database path.</param>
    /// <param name="payloadHash">The payload hash.</param>
    /// <exception cref="InvalidOperationException">The authoritative snapshot sidecar is missing.</exception>
    private static void SetAuthoritativeSnapshotHash(string path, string payloadHash)
    {
        using var connection = OpenRawConnection(path);
        using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE oc_snapshot_authoritative_states
            SET payload_hash = $payloadHash
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue("$payloadHash", payloadHash);
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, StoreIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, Stream.Value);
        if (command.ExecuteNonQuery() == 1)
        {
            return;
        }

        throw new InvalidOperationException("The authoritative snapshot sidecar was not found.");
    }

    /// <summary>Reads payload text for test assertions.</summary>
    /// <param name="payload">The optional payload.</param>
    /// <returns>The decoded payload text.</returns>
    private static string? PayloadText(PayloadEnvelope? payload) =>
        payload is null ? null : System.Text.Encoding.UTF8.GetString(payload.Payload.Span);
}
