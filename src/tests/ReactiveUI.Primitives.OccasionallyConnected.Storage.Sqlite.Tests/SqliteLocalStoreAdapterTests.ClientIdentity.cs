// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Client identity binding tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The primary client identity used for partition binding.</summary>
    private const string ClientId = "client-a";

    /// <summary>The secondary client identity used for partition binding conflicts.</summary>
    private const string OtherClientId = "client-b";

    /// <summary>The maximum valid client identity length in UTF-16 code units.</summary>
    private const int MaximumClientIdLength = 256;

    /// <summary>The first invalid client identity length above the UTF-16 limit.</summary>
    private const int ClientIdLengthAboveLimit = MaximumClientIdLength + 1;

    /// <summary>Verifies a SQLite partition reopens only for the same ordinal client identity after binding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BoundClientIdentityReopensForSameClientAndRejectsNullOrDifferentClient()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        }

        await using (var sameClient = CreateAdapter(database.Path))
        {
            await sameClient.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
            await Assert.That(await sameClient.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None)).IsEqualTo(subscriptionId);
        }

        await using (var nullClient = CreateAdapter(database.Path))
        {
            Func<Task> initialize = () => nullClient.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None).AsTask();
            await Assert.That(initialize).ThrowsExactly<InvalidOperationException>();
        }

        await using var differentClient = CreateAdapter(database.Path);
        Func<Task> differentClientInitialize = () => differentClient.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = OtherClientId }, CancellationToken.None).AsTask();
        await Assert.That(differentClientInitialize).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies unbound pending durable work cannot be reassigned to a first client identity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ExistingUnboundPendingWorkRejectsFirstClientBindingAndPreservesState()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var operation = CreateOperation(FirstClientSequence);
        await using (var adapter = CreateAdapter(database.Path))
        {
            await adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(expectedRevision: 0), CancellationToken.None);
        }

        await using (var rejected = CreateAdapter(database.Path))
        {
            Func<Task> initialize = () => rejected.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None).AsTask();
            await Assert.That(initialize).ThrowsExactly<InvalidOperationException>();
        }

        await using var legacy = CreateAdapter(database.Path);
        await legacy.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        var recovery = await legacy.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies empty subscription mappings alone remain pristine for first client binding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EmptySubscriptionMappingsRemainPristineForFirstClientBinding()
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        await using (var legacy = CreateAdapter(database.Path))
        {
            await legacy.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
            subscriptionId = await legacy.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        }

        await using var bound = CreateAdapter(database.Path);
        await bound.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        await Assert.That(await bound.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None)).IsEqualTo(subscriptionId);
    }

    /// <summary>Verifies different store identity partitions keep separate client bindings in one SQLite database.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DifferentStoreIdentityPartitionsHaveIsolatedClientBindings()
    {
        using var database = TempDatabase.Create();
        await using (var alpha = CreateAdapter(database.Path))
        {
            await alpha.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, CancellationToken.None);
        }

        await using (var beta = CreateAdapter(database.Path))
        {
            await beta.InitializeAsync(new("store-beta", SchemaVersion, false) { ClientId = OtherClientId }, CancellationToken.None);
        }

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new("store-beta", SchemaVersion, false) { ClientId = OtherClientId }, CancellationToken.None);
        var subscriptionId = await reopened.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        await Assert.That(subscriptionId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies invalid client identities are rejected before SQLite file mutation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task InvalidClientIdentityRejectsBeforeSQLiteMutation()
    {
        using var malformedDatabase = TempDatabase.Create();
        using var oversizedDatabase = TempDatabase.Create();
        await using var malformed = CreateAdapter(malformedDatabase.Path);
        await using var oversized = CreateAdapter(oversizedDatabase.Path);

        Func<Task> malformedClient = () => malformed.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = new('\uD800', 1) }, CancellationToken.None).AsTask();
        Func<Task> oversizedClient = () => oversized.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = new('a', ClientIdLengthAboveLimit) }, CancellationToken.None).AsTask();

        await Assert.That(malformedClient).ThrowsExactly<ArgumentException>();
        await Assert.That(oversizedClient).ThrowsExactly<ArgumentException>();
        await Assert.That(File.Exists(malformedDatabase.Path)).IsFalse();
        await Assert.That(File.Exists(oversizedDatabase.Path)).IsFalse();
    }

    /// <summary>Verifies client identity sizing participates in adapter admission.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ClientIdentityInputExceedsWorkerBytesBeforeSQLiteMutation()
    {
        using var database = TempDatabase.Create();
        SqliteLocalStoreAdapterOptions options = new() { WorkerCapacity = TwoWorkerCommands, WorkerCapacityBytes = TinyWorkerBytes };
        await using var adapter = new SqliteLocalStoreAdapter(database.Path, options);

        Func<Task> initialize = () => adapter.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = new('x', MaximumClientIdLength) }, CancellationToken.None).AsTask();

        var exception = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(initialize);

        await Assert.That(exception?.CanFitWhenEmpty).IsFalse();
        await Assert.That(File.Exists(database.Path)).IsFalse();
    }

    /// <summary>Verifies cancellation before first binding leaves an unbound SQLite partition bindable later.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CanceledClientIdentityInitializationDoesNotBindPartition()
    {
        using var database = TempDatabase.Create();
        await using (var legacy = CreateAdapter(database.Path))
        {
            await legacy.InitializeAsync(new(StoreIdentity, SchemaVersion, false), CancellationToken.None);
        }

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await using var canceled = CreateAdapter(database.Path);
        Func<Task> initialize = () => canceled.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = ClientId }, cancellation.Token).AsTask();

        await Assert.That(initialize).ThrowsExactly<OperationCanceledException>();
        await using var rebound = CreateAdapter(database.Path);
        await rebound.InitializeAsync(new(StoreIdentity, SchemaVersion, false) { ClientId = OtherClientId }, CancellationToken.None);
    }
}
