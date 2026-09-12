// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="InMemoryLocalStoreAdapter"/>.</summary>
/// <content>Authoritative snapshot state tests.</content>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The first authoritative payload text.</summary>
    private const string AuthoritativeInitialText = "auth-0";

    /// <summary>The replacement authoritative payload text.</summary>
    private const string AuthoritativeRemoteText = "auth-7";

    /// <summary>The first optimistic payload text.</summary>
    private const string OptimisticInitialText = "optimistic-21";

    /// <summary>The second optimistic payload text.</summary>
    private const string OptimisticLocalText = "optimistic-22";

    /// <summary>The remote optimistic payload text.</summary>
    private const string OptimisticRemoteText = "optimistic-12";

    /// <summary>The changed authoritative payload text.</summary>
    private const string AuthoritativeChangedText = "auth-changed";

    /// <summary>Verifies local and remote snapshot mutations preserve or replace authoritative state explicitly.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeStateIsPersisted_ThenRecoveryKeepsItSeparateFromOptimisticState()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var authoritative0 = CreatePayload(AuthoritativeInitialText);
        var firstSnapshot = CreateSnapshotMutation(expectedRevision: 0, payloadText: OptimisticInitialText) with { AuthoritativeState = authoritative0 };
        _ = await store.CommitLocalOperationAsync(CreateOperation(FirstClientSequence), firstSnapshot, CancellationToken.None);
        var preservingSnapshot = CreateSnapshotMutation(expectedRevision: 1, payloadText: OptimisticLocalText);
        _ = await store.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), preservingSnapshot, CancellationToken.None);
        var preserved = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(PayloadText(preserved.Snapshot?.State)).IsEqualTo(OptimisticLocalText);
        await Assert.That(PayloadText(preserved.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
        var authoritative7 = CreatePayload(AuthoritativeRemoteText);
        var remoteSnapshot = new SnapshotMutation(Stream, CreatePayload(OptimisticRemoteText), FormatVersion: 1, ExpectedRevision: 2) { AuthoritativeState = authoritative7 };

        _ = await store.ApplyRemoteBatchAsync(CreateRemoteBatch(null, RemoteCursor, [CreateRemoteEvent(RemoteCursor)]), remoteSnapshot, CancellationToken.None);
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);

        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticRemoteText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeRemoteText);
        await Assert.That(PayloadText(authoritative0)).IsEqualTo(AuthoritativeInitialText);
    }

    /// <summary>Verifies failed writes leave the optimistic and authoritative snapshot pair unchanged.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenAuthoritativeMutationFails_ThenStoredSnapshotPairIsUnchanged()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        _ = await store.CommitLocalOperationAsync(
            CreateOperation(FirstClientSequence),
            CreateSnapshotMutation(expectedRevision: 0, payloadText: OptimisticInitialText) with { AuthoritativeState = CreatePayload(AuthoritativeInitialText) },
            CancellationToken.None);
        var remoteEvent = CreateRemoteEvent(RemoteCursor);

        Func<Task> staleLocal = async () => await store.CommitLocalOperationAsync(
            CreateOperation(SecondClientSequence),
            CreateSnapshotMutation(expectedRevision: 0, payloadText: "optimistic-stale") with { AuthoritativeState = CreatePayload("auth-stale") },
            CancellationToken.None);
        Func<Task> staleRemote = async () => await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch("wrong-cursor", RemoteCursor, [remoteEvent]),
            new(Stream, CreatePayload("optimistic-remote"), FormatVersion: 1, ExpectedRevision: 1) { AuthoritativeState = CreatePayload("auth-remote") },
            CancellationToken.None);

        await Assert.That(staleLocal).ThrowsExactly<InvalidOperationException>();
        await Assert.That(staleRemote).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [remoteEvent.EventId], CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.State)).IsEqualTo(OptimisticInitialText);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeInitialText);
        await Assert.That(unapplied.Count).IsEqualTo(1);
    }

    /// <summary>Verifies duplicate local operation intent includes the original authoritative mutation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenDuplicateOperationChangesAuthoritativeIntent_ThenCommitIsRejected()
    {
        await using var store = await CreateInitializedStoreAsync();
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        var snapshot = CreateSnapshotMutation(expectedRevision: 0) with { AuthoritativeState = CreatePayload(AuthoritativeInitialText) };
        var first = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);
        _ = await store.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, RemoteCursor, [CreateRemoteEvent(RemoteCursor)]),
            new(Stream, CreatePayload("optimistic-remote"), FormatVersion: 1, ExpectedRevision: 1) { AuthoritativeState = CreatePayload(AuthoritativeRemoteText) },
            CancellationToken.None);
        var replay = await store.CommitLocalOperationAsync(operation, snapshot, CancellationToken.None);

        Func<Task> changedAuthoritative = async () => await store.CommitLocalOperationAsync(
            operation,
            snapshot with { AuthoritativeState = CreatePayload(AuthoritativeChangedText) },
            CancellationToken.None);

        await Assert.That(replay).IsEqualTo(first);
        await Assert.That(changedAuthoritative).ThrowsExactly<InvalidOperationException>();
        Func<Task> omittedAuthoritative = async () => await store.CommitLocalOperationAsync(
            operation,
            snapshot with { AuthoritativeState = null },
            CancellationToken.None);
        await Assert.That(omittedAuthoritative).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(PayloadText(recovery.Snapshot?.AuthoritativeState)).IsEqualTo(AuthoritativeRemoteText);
    }

    /// <summary>Reads payload text for test assertions.</summary>
    /// <param name="payload">The optional payload.</param>
    /// <returns>The decoded payload text.</returns>
    private static string? PayloadText(PayloadEnvelope? payload) =>
        payload is null ? null : System.Text.Encoding.UTF8.GetString(payload.Payload.Span);
}
