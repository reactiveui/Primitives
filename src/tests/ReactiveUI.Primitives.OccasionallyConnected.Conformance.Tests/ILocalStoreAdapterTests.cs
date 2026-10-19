// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Runs the same public store assertions against each shipped provider.</summary>
[NotInParallel]
public sealed partial class ILocalStoreAdapterTests
{
    /// <summary>Checks local commit fencing and atomicity.</summary>
    /// <param name="provider">The shipped provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(EncryptedSqliteProvider)]
#if NET10_0_OR_GREATER
    [Arguments(5)]
#endif
    public async Task AtomicLocalCommitPreservesFailedRevision(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await LocalStoreConformance.AtomicLocalCommitAsync(store);
    }

    /// <summary>Checks canceled commits preserve every state component.</summary>
    /// <param name="provider">The shipped provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(EncryptedSqliteProvider)]
#if NET10_0_OR_GREATER
    [Arguments(5)]
#endif
    public async Task CanceledLocalCommitPreservesState(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await LocalStoreConformance.CanceledCommitAsync(store);
    }

    /// <summary>Checks remote cursor fences and inbox duplicate handling.</summary>
    /// <param name="provider">The shipped provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(EncryptedSqliteProvider)]
#if NET10_0_OR_GREATER
    [Arguments(5)]
#endif
    public async Task AtomicRemoteApplyPreservesRejectedCursorAndDeduplicates(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await LocalStoreConformance.AtomicRemoteApplyAsync(store);
    }

    /// <summary>Checks lease ownership and complete-result validation.</summary>
    /// <param name="provider">The shipped provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(EncryptedSqliteProvider)]
#if NET10_0_OR_GREATER
    [Arguments(5)]
#endif
    public async Task LeaseOwnershipAndResultValidationPreserveReplay(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await LocalStoreConformance.LeasedOutboxAsync(store);
    }

    /// <summary>Checks client binding and stable subscription mappings.</summary>
    /// <param name="provider">The shipped provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(EncryptedSqliteProvider)]
#if NET10_0_OR_GREATER
    [Arguments(5)]
#endif
    public async Task ClientIdentityAndSubscriptionCannotBeRebound(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await LocalStoreConformance.ClientIdentityAsync(store);
    }

    /// <summary>Checks native durable stores preserve local and remote state on reopen.</summary>
    /// <param name="provider">The native durable provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(EncryptedSqliteProvider)]
    public async Task DurableStateSurvivesReopen(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        SubscriptionId subscription;
        (OperationId Operation, Guid Event) committed;
        await using (var store = await fixture.OpenAsync())
        {
            subscription = await store.GetOrCreateSubscriptionIdAsync(LocalStoreConformance.Stream, null, CancellationToken.None);
            committed = await LocalStoreConformance.SeedDurableAsync(store);
        }

        await using var reopened = await fixture.OpenAsync();
        await LocalStoreConformance.AssertDurableAsync(reopened, subscription, committed.Operation, committed.Event);
    }

    /// <summary>Checks every recovery-capable provider rejects stale fences before replacing state.</summary>
    /// <param name="provider">The recovery-capable provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(EncryptedSqliteProvider)]
    public async Task SnapshotRecoveryPreservesInvalidFencesAndCommitsCheckpoint(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await LocalStoreConformance.SnapshotRecoveryAsync(store);
    }

    /// <summary>Checks renewal and expiry against controlled time on every provider.</summary>
    /// <param name="provider">The shipped provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(FileSystemProvider)]
    [Arguments(LiteDbProvider)]
    [Arguments(BliteDbProvider)]
    [Arguments(EncryptedSqliteProvider)]
#if NET10_0_OR_GREATER
    [Arguments(IndexedDbProvider)]
#endif
    public async Task RenewedLeaseExcludesThenExpiresForReclaim(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await LocalStoreConformance.LeaseExpiryAsync(store, fixture.Clock.Advance);
    }

    /// <summary>Checks no provider advertises a capability without its positive evidence suite.</summary>
    /// <param name="provider">The shipped provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(FileSystemProvider)]
    [Arguments(LiteDbProvider)]
    [Arguments(BliteDbProvider)]
    [Arguments(EncryptedSqliteProvider)]
#if NET10_0_OR_GREATER
    [Arguments(IndexedDbProvider)]
#endif
    public async Task AdvertisedCapabilitiesHavePositiveEvidence(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        var covered = LocalStoreCapabilities.AtomicLocalCommit
            | LocalStoreCapabilities.AtomicRemoteApply
            | LocalStoreCapabilities.LeasedOutbox
            | LocalStoreCapabilities.ClientIdentityBinding;
        if (provider != 0)
        {
            covered |= LocalStoreCapabilities.DurableLocalCommit | LocalStoreCapabilities.DurableInbox;
        }

        if (provider is 0 or 1 or EncryptedSqliteProvider)
        {
            covered |= LocalStoreCapabilities.AtomicSnapshotRecovery;
        }

        if (provider == EncryptedSqliteProvider)
        {
            covered |= LocalStoreCapabilities.AuthenticatedEncryptionAtRest;
        }

        await Assert.That(store.Capabilities & ~covered).IsEqualTo(LocalStoreCapabilities.None);
        await Assert.That((store.Capabilities & LocalStoreCapabilities.MultiProcessCoordination) != 0).IsFalse();
    }

    /// <summary>Checks unsupported encryption requirements fail explicitly instead of using plaintext.</summary>
    /// <param name="provider">The provider without record encryption.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(FileSystemProvider)]
    [Arguments(LiteDbProvider)]
    [Arguments(BliteDbProvider)]
#if NET10_0_OR_GREATER
    [Arguments(IndexedDbProvider)]
#endif
    public async Task MissingEncryptionCapabilityRejectsRequiredInitialization(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using var store = await fixture.OpenAsync();
        await Assert.That(() => store.InitializeAsync(
                new(LocalStoreConformance.Identity, 1, true) { ClientId = LocalStoreConformance.Client },
                CancellationToken.None).AsTask())
            .Throws<NotSupportedException>();
    }
}
