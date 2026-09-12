// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies schema and lease ownership boundaries.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>The primary client identity used for partition binding.</summary>
    private const string ClientId = "client-a";

    /// <summary>The secondary client identity used for partition binding conflicts.</summary>
    private const string OtherClientId = "client-b";

    /// <summary>The first invalid client identity length above the UTF-16 limit.</summary>
    private const int ClientIdLengthAboveLimit = 257;

    /// <summary>Verifies ending enumeration leaves the durable lease available to its caller.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task StoppingEnumerationDoesNotReleaseReturnedLease()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, OperationPayloadText);
        var request = new OutboxLeaseRequest(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1));
        using var requestCancellation = new CancellationTokenSource();
        using var enumerationCancellation = new CancellationTokenSource();
        Guid leaseId;
        await using (var iterator = store.LeasePendingOperationsAsync(request, requestCancellation.Token).GetAsyncEnumerator(enumerationCancellation.Token))
        {
            await Assert.That(await iterator.MoveNextAsync()).IsTrue();
            leaseId = iterator.Current.LeaseId;
        }

        await Assert.That(await LeaseSingleBatchAsync(store, request)).IsNull();
        await Assert.That((await store.TryBeginRemoteAttemptAsync(leaseId, operation.OperationId, 1, CancellationToken.None)).MaySend).IsTrue();
        await store.ReleaseLeaseAsync(leaseId, CancellationToken.None);
    }

    /// <summary>Verifies unsupported schema initialization cannot bind the store identity.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task UnsupportedSchemaDoesNotInitializeStore()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        Func<Task> unsupported = async () => await store.InitializeAsync(new(StoreIdentity, SchemaVersion + 1, false), CancellationToken.None);
        await Assert.That(unsupported).ThrowsExactly<NotSupportedException>();
        await store.InitializeAsync(new(OtherStoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        await Assert.That(subscription.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies only an initialized store can create stream state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InvalidSchemaAndUninitializedAccessPreserveStore()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        Func<Task> invalid = async () => await store.InitializeAsync(new(StoreIdentity, 0, false), CancellationToken.None);
        Func<Task> uninitialized = async () => _ = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        await Assert.That(invalid).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(uninitialized).ThrowsExactly<InvalidOperationException>();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        await Assert.That(await store.GetOrCreateSubscriptionIdAsync(Stream, subscription, CancellationToken.None)).IsEqualTo(subscription);
        Func<Task> conflict = async () => _ = await store.GetOrCreateSubscriptionIdAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        Func<Task> wrongRecovery = async () => _ = await store.RecoverStreamAsync(Stream, SubscriptionId.New(), CancellationToken.None);
        await Assert.That(conflict).ThrowsExactly<InvalidOperationException>();
        await Assert.That(wrongRecovery).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a lease cannot authorize another stream's operation or repeat an attempt.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task LeaseOwnershipAndAttemptSequencePreventUnauthorizedSends()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, "first");
        var other = await CommitOperationAsync(store, OtherStream, 1, "other");
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        Func<Task> wrongOwner = async () => _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, other.OperationId, 1, CancellationToken.None);
        await Assert.That(wrongOwner).ThrowsExactly<InvalidOperationException>();
        await Assert.That((await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None)).MaySend).IsTrue();
        await Assert.That((await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None)).MaySend).IsFalse();
        await Assert.That((await store.GetOperationStatusAsync(other.OperationId, CancellationToken.None))?.Attempt).IsEqualTo(0);
        await Assert.That(await store.GetRetryStateAsync(OperationId.New(), CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies a bound in-memory partition accepts only the same ordinal client identity.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task BoundClientIdentityAcceptsSameClientAndRejectsNullOrDifferentClient()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);

        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        Func<Task> nullClient = async () => await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        Func<Task> differentClient = async () => await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = OtherClientId }, CancellationToken.None);

        await Assert.That((store.Capabilities & LocalStoreCapabilities.ClientIdentityBinding) != 0).IsTrue();
        await Assert.That(nullClient).ThrowsExactly<InvalidOperationException>();
        await Assert.That(differentClient).ThrowsExactly<InvalidOperationException>();
        await Assert.That(await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None)).IsEqualTo(subscription);
    }

    /// <summary>Verifies unbound pending work cannot be reassigned to a first client identity.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ExistingUnboundPendingWorkRejectsFirstClientBindingAndPreservesState()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = await CommitOperationAsync(store, Stream, FirstClientSequence, OperationPayloadText);

        Func<Task> bind = async () => await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);

        await Assert.That(bind).ThrowsExactly<InvalidOperationException>();
        var recovery = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies empty subscription mappings alone do not prevent first client binding.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task EmptySubscriptionMappingsRemainPristineForFirstClientBinding()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);

        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);

        await Assert.That(await store.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None)).IsEqualTo(subscription);
    }

    /// <summary>Verifies malformed and oversized client identities fail before the store is initialized.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task InvalidClientIdentityDoesNotInitializeStore()
    {
        await using var malformed = new InMemoryLocalStoreAdapter();
        await using var oversized = new InMemoryLocalStoreAdapter();

        Func<Task> malformedClient = async () => await malformed.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = new('\uD800', 1) }, CancellationToken.None);
        Func<Task> oversizedClient = async () => await oversized.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = new('a', ClientIdLengthAboveLimit) }, CancellationToken.None);

        await Assert.That(malformedClient).ThrowsExactly<ArgumentException>();
        await Assert.That(oversizedClient).ThrowsExactly<ArgumentException>();
        await malformed.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        await oversized.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
    }

    /// <summary>Verifies client identity binding capacity is checked before mutating in-memory binding state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ClientIdentityBindingCapacityRejectsBeforeMutation()
    {
        await using var store = new InMemoryLocalStoreAdapter(maximumRecordCount: 1, maximumEncodedBytes: 1);
        await store.InitializeAsync(new("a", SchemaVersion, false), CancellationToken.None);

        Func<Task> bind = async () => await store.InitializeAsync(new("a", SchemaVersion, false) { ClientId = "b" }, CancellationToken.None);

        await Assert.That(bind).ThrowsExactly<QueueCapacityExceededException>();
        await store.InitializeAsync(new("a", SchemaVersion, false), CancellationToken.None);
    }

    /// <summary>Verifies ordinal client identity comparison does not normalize equivalent-looking Unicode.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ClientIdentityBindingUsesOrdinalTextWithoutNormalization()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = "caf\u00E9" }, CancellationToken.None);

        Func<Task> decomposed = async () => await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = "cafe\u0301" }, CancellationToken.None);

        await Assert.That(decomposed).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies cancellation before first binding leaves an unbound pristine partition bindable later.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task CanceledClientIdentityBindingDoesNotMutatePartition()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        Func<Task> canceled = async () => await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, cancellation.Token);

        await Assert.That(canceled).ThrowsExactly<OperationCanceledException>();
        await store.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = OtherClientId }, CancellationToken.None);
    }

    /// <summary>Verifies renewal extends the original deadline and expiry invalidates ownership.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RenewedLeaseRemainsExclusiveUntilExtendedDeadline()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var operation = await CommitOperationAsync(store, Stream, 1, "first");
        var request = new OutboxLeaseRequest(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1));
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, request));
        await store.RenewLeaseAsync(lease.LeaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(1));
        await Assert.That(await LeaseSingleBatchAsync(store, request)).IsNull();
        clock.Advance(TimeSpan.FromMinutes(1));
        Func<Task> expired = async () => _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);
        await Assert.That(expired).ThrowsExactly<InvalidOperationException>();
        var replacement = RequireBatch(await LeaseSingleBatchAsync(store, request));
        await Assert.That(replacement.LeaseId).IsNotEqualTo(lease.LeaseId);
        await Assert.That(replacement.Operations[0].OperationId).IsEqualTo(operation.OperationId);
    }
}
