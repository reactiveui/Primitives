// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Durable inbox and capability coverage tests for <see cref="ILocalStoreAdapter"/> implementations.</summary>
public sealed partial class ILocalStoreAdapterTests
{
    /// <summary>The third remote cursor.</summary>
    private const string ThirdRemoteCursor = "remote-cursor-3";

    /// <summary>The third snapshot revision.</summary>
    private const int ThirdSnapshotRevision = 3;

    /// <summary>The duplicate count when two already applied events are replayed after restart.</summary>
    private const int ReplayedDuplicateCount = 2;

    /// <summary>Maps each local store capability to the tests that prove its behavior.</summary>
    private static readonly (LocalStoreCapabilities Capability, string Test)[] CapabilitySuites =
    [
        (LocalStoreCapabilities.AtomicLocalCommit, nameof(AtomicLocalCommitCapabilityCommitsOperationSequenceAndSnapshotTogether)),
        (LocalStoreCapabilities.AtomicRemoteApply, nameof(AtomicRemoteApplyCapabilityCommitsInboxCursorAndSnapshotTogether)),
        (LocalStoreCapabilities.DurableInbox, nameof(DurableInboxCapabilityDeduplicatesEventsBeforeAndAfterRestart)),
        (LocalStoreCapabilities.LeasedOutbox, nameof(LeasedOutboxCapabilityExcludesRenewsExpiresAndReclaimsOwnership)),
        (LocalStoreCapabilities.DurableLocalCommit, nameof(SqliteDurableCapabilitiesReopenOperationInboxCursorSnapshotAndStatusState)),
        (LocalStoreCapabilities.ClientIdentityBinding, nameof(ClientIdentityBindingCapabilityAcceptsSameClientAndRejectsDifferentClientWithoutMutation)),
        (LocalStoreCapabilities.AtomicSnapshotRecovery, nameof(AtomicSnapshotRecoveryCapabilityAcceptsValidRecoveryAndRejectsInvalidMutations)),
    ];

    /// <summary>Verifies a durable inbox reports duplicate events once, before and after the store reopens.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task DurableInboxCapabilityDeduplicatesEventsBeforeAndAfterRestart(int kind)
    {
        await using (var probe = await CreateFixtureAsync(kind))
        {
            Skip.Unless(
                HasCapability(probe.Store, LocalStoreCapabilities.DurableInbox),
                "The adapter does not advertise DurableInbox; the restart deduplication suite does not apply.");
        }

        var directory = SqliteTestDirectory.Create("rxui-oc-durable-inbox-");
        var databasePath = Path.Combine(directory.FullName, "local.db");
        try
        {
            var first = CreateRemoteEvent(FirstRemoteCursor, "inbox-first");
            var second = CreateRemoteEvent(SecondRemoteCursor, "inbox-second");
            var third = CreateRemoteEvent(ThirdRemoteCursor, "inbox-third");
            await using (var fixture = await CreateInitializedFixtureAsync(kind, databasePath: databasePath))
            {
                _ = await fixture.Store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
                var applied = await fixture.Store.ApplyRemoteBatchAsync(
                    CreateRemoteBatch(null, FirstRemoteCursor, [first]),
                    CreateSnapshotMutation(0, "inbox-first-snapshot"),
                    CancellationToken.None);
                await Assert.That(applied.AppliedCount).IsEqualTo(1);
            }

            await using (var reopened = await CreateInitializedFixtureAsync(kind, databasePath: databasePath))
            {
                var unapplied = await reopened.Store.GetUnappliedEventIdsAsync(Stream, [first.EventId, second.EventId], CancellationToken.None);
                var receipt = await reopened.Store.ApplyRemoteBatchAsync(
                    CreateRemoteBatch(FirstRemoteCursor, SecondRemoteCursor, [first, second]),
                    CreateSnapshotMutation(FirstSnapshotRevision, "inbox-second-snapshot"),
                    CancellationToken.None);
                await Assert.That(unapplied.Count).IsEqualTo(1);
                await Assert.That(unapplied[0]).IsEqualTo(second.EventId);
                await Assert.That(receipt.AppliedCount).IsEqualTo(1);
                await Assert.That(receipt.DuplicateCount).IsEqualTo(1);
            }

            await using var restarted = await CreateInitializedFixtureAsync(kind, databasePath: databasePath);
            var replay = await restarted.Store.ApplyRemoteBatchAsync(
                CreateRemoteBatch(SecondRemoteCursor, ThirdRemoteCursor, [first, second, third]),
                CreateSnapshotMutation(SecondSnapshotRevision, "inbox-third-snapshot"),
                CancellationToken.None);
            var remaining = await restarted.Store.GetUnappliedEventIdsAsync(Stream, [first.EventId, second.EventId, third.EventId], CancellationToken.None);

            await Assert.That(replay.AppliedCount).IsEqualTo(1);
            await Assert.That(replay.DuplicateCount).IsEqualTo(ReplayedDuplicateCount);
            await Assert.That(replay.SnapshotRevision).IsEqualTo(ThirdSnapshotRevision);
            await Assert.That(remaining.Count).IsEqualTo(0);
        }
        finally
        {
            if (Directory.Exists(directory.FullName))
            {
                Directory.Delete(directory.FullName, recursive: true);
            }
        }
    }

    /// <summary>Verifies every advertised local store capability maps to at least one behavioral test.</summary>
    /// <param name="kind">The adapter kind.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(InMemoryAdapterKind)]
    [Arguments(SqliteAdapterKind)]
    public async Task EveryAdvertisedCapabilityHasBehavioralTest(int kind)
    {
        await using var fixture = await CreateFixtureAsync(kind);
        var covered = LocalStoreCapabilities.None;
        foreach (var (capability, test) in CapabilitySuites)
        {
            await Assert.That(typeof(ILocalStoreAdapterTests).GetMethod(test)).IsNotNull();
            covered |= capability;
        }

        await Assert.That(fixture.Store.Capabilities & ~covered).IsEqualTo(LocalStoreCapabilities.None);
    }
}
