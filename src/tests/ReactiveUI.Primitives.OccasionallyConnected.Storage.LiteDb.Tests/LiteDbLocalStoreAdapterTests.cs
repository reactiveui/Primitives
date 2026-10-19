// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb.Tests;

/// <summary>Tests for the transactional LiteDB store.</summary>
public sealed partial class LiteDbLocalStoreAdapterTests
{
    /// <summary>The default store identity used by tests.</summary>
    private const string StoreIdentity = "store";

    /// <summary>The default client identity used by tests.</summary>
    private const string ClientIdentity = "client";

    /// <summary>The test stream name.</summary>
    private const string TemperatureStreamName = "sensor/temperature";

    /// <summary>The initial snapshot payload value.</summary>
    private const string SnapshotPayload = "snapshot";

    /// <summary>The first event cursor used by remote batch tests.</summary>
    private const string FirstCursor = "cursor-1";

    /// <summary>The second event cursor used by remote batch tests.</summary>
    private const string SecondCursor = "cursor-2";

    /// <summary>The standard lease byte limit.</summary>
    private const int LeaseBytes = 1024;

    /// <summary>The initial sequence number.</summary>
    private const int FirstSequence = 0;

    /// <summary>The next sequence number.</summary>
    private const int SecondSequence = 1;

    /// <summary>The initial snapshot revision.</summary>
    private const int InitialRevision = 0;

    /// <summary>The first committed snapshot revision.</summary>
    private const int FirstRevision = 1;

    /// <summary>The second committed snapshot revision.</summary>
    private const int SecondRevision = 2;

    /// <summary>The third client sequence used by tests.</summary>
    private const int ThirdSequence = 2;

    /// <summary>The fourth client sequence used by tests.</summary>
    private const int FourthSequence = 3;

    /// <summary>The third committed snapshot revision.</summary>
    private const int ThirdRevision = 3;

    /// <summary>The fourth committed snapshot revision.</summary>
    private const int FourthRevision = 4;

    /// <summary>The fifth committed snapshot revision.</summary>
    private const int FifthRevision = 5;

    /// <summary>The deterministic timestamp used by time-provider tests.</summary>
    private static readonly DateTimeOffset FixedTimestamp = new(2032, 4, 5, 6, 7, 8, TimeSpan.Zero);

    /// <summary>Verifies committed state survives closing and reopening the adapter.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CommitIsRecoveredAfterReopen()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId(TemperatureStreamName);
        var operation = CreateOperation(stream, FirstSequence);
        var mutation = CreateSnapshotMutation(stream, SnapshotPayload, InitialRevision);
        var initialization = CreateInitialization();
        SubscriptionId subscriptionId;

        await using (var adapter = new LiteDbLocalStoreAdapter(directory.DatabasePath))
        {
            await adapter.InitializeAsync(initialization, CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
            var result = await adapter.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
            await Assert.That(result.OperationId).IsEqualTo(operation.OperationId);
            await Assert.That(adapter.Capabilities).IsEqualTo(
                LocalStoreCapabilities.AtomicLocalCommit
                | LocalStoreCapabilities.AtomicRemoteApply
                | LocalStoreCapabilities.DurableInbox
                | LocalStoreCapabilities.LeasedOutbox
                | LocalStoreCapabilities.DurableLocalCommit
                | LocalStoreCapabilities.ClientIdentityBinding);
        }

        await using var reopened = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await reopened.InitializeAsync(initialization, CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations).HasSingleItem();
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(recovered.Snapshot).IsNotNull();
        await Assert.That(recovered.Snapshot!.Revision).IsEqualTo(FirstRevision);
    }

    /// <summary>Verifies durable timestamps use the configured time provider.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TimeProviderControlsDurableTimestamps()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId(TemperatureStreamName);
        var clock = new FixedTimeProvider(FixedTimestamp);
        var operation = CreateOperation(stream, FirstSequence);
        var mutation = CreateSnapshotMutation(stream, SnapshotPayload, InitialRevision);

        await using var adapter = new LiteDbLocalStoreAdapter(directory.DatabasePath, clock);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        var subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        var result = await adapter.CommitLocalOperationAsync(operation, mutation, CancellationToken.None);
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        var recovered = await adapter.RecoverStreamAsync(stream, subscriptionId, CancellationToken.None);

        await Assert.That(result.CommittedAtUtc).IsEqualTo(FixedTimestamp);
        await Assert.That(status!.ChangedAtUtc).IsEqualTo(FixedTimestamp);
        await Assert.That(recovered.Snapshot!.SavedAtUtc).IsEqualTo(FixedTimestamp);
    }

    /// <summary>Verifies concurrent and repeated disposal completes safely.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentDisposeAsyncCallsComplete()
    {
        using var directory = new TestDirectory();
        var adapter = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);
        await Task.WhenAll(adapter.DisposeAsync().AsTask(), adapter.DisposeAsync().AsTask());
        await adapter.DisposeAsync();
    }

    /// <summary>Verifies unsupported encryption is rejected explicitly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EncryptionRequirementIsRejectedRatherThanAdvertised()
    {
        using var directory = new TestDirectory();
        await using var adapter = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await Assert.That(() => adapter.InitializeAsync(
            new(StoreIdentity, 1, true) { ClientId = ClientIdentity },
            CancellationToken.None).AsTask()).Throws<NotSupportedException>();
    }

    /// <summary>Verifies subscription and local commit preconditions reject invalid state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IdentityAndCommitPreconditionsRejectInvalidState()
    {
        using var directory = new TestDirectory();
        var stream = new StreamId("preconditions");
        await using var adapter = new LiteDbLocalStoreAdapter(directory.DatabasePath);
        await Assert.That(() => adapter.GetOrCreateSubscriptionIdAsync(
            stream,
            new SubscriptionId(Guid.Empty),
            CancellationToken.None).AsTask()).Throws<ArgumentException>();
        await adapter.InitializeAsync(CreateInitialization(), CancellationToken.None);

        await Assert.That(() => IgnoreResultAsync(adapter.RecoverStreamAsync(
            stream,
            new(Guid.NewGuid()),
            CancellationToken.None))).Throws<InvalidOperationException>();

        var operation = CreateOperation(stream, FirstSequence);
        await Assert.That(() => IgnoreResultAsync(adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(stream, SnapshotPayload, InitialRevision),
            CancellationToken.None))).Throws<InvalidOperationException>();

        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(stream, null, CancellationToken.None);
        await Assert.That(() => IgnoreResultAsync(adapter.GetOrCreateSubscriptionIdAsync(
            stream,
            new(Guid.NewGuid()),
            CancellationToken.None))).Throws<InvalidOperationException>();
        await adapter.CommitLocalOperationAsync(
            operation,
            CreateSnapshotMutation(stream, SnapshotPayload, InitialRevision),
            CancellationToken.None);

        await Assert.That(() => IgnoreResultAsync(adapter.CommitLocalOperationAsync(
            CreateOperation(stream, SecondSequence),
            CreateSnapshotMutation(stream, SnapshotPayload, InitialRevision),
            CancellationToken.None))).Throws<InvalidOperationException>();

        await Assert.That(subscription.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Creates the standard initialization request.</summary>
    /// <returns>The initialization request.</returns>
    private static LocalStoreInitialization CreateInitialization() =>
        new(StoreIdentity, 1, false) { ClientId = ClientIdentity, };

    /// <summary>Creates a deterministic test operation.</summary>
    /// <param name="stream">The target stream.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(StreamId stream, long sequence) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = stream,
            ClientSequence = sequence,
            TimestampUtc = FixedTimestamp.AddMinutes(sequence),
            Type = SyncOperationType.Custom,
            Payload = CreatePayload($"operation-{sequence}"),
            Metadata = new Dictionary<string, string> { ["sequence"] = sequence.ToString(System.Globalization.CultureInfo.InvariantCulture), },
        };

    /// <summary>Creates a deterministic payload envelope.</summary>
    /// <param name="value">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        var hash = Convert.ToHexString(SHA256.HashData(bytes));
        return new("test.payload", 1, "application/json", bytes, hash);
    }

    /// <summary>Creates a snapshot mutation for the given stream.</summary>
    /// <param name="stream">The stream identifier.</param>
    /// <param name="state">The snapshot payload text.</param>
    /// <param name="expectedRevision">The expected durable revision.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(StreamId stream, string state, long expectedRevision) =>
        new(stream, CreatePayload(state), 1, expectedRevision);

    /// <summary>Reads the next lease batch, if one exists.</summary>
    /// <param name="adapter">The adapter under test.</param>
    /// <param name="request">The lease request.</param>
    /// <returns>The next batch, or null.</returns>
    private static async ValueTask<LeasedOperationBatch?> ReadOptionalLeaseAsync(
        LiteDbLocalStoreAdapter adapter,
        OutboxLeaseRequest request)
    {
        await using var enumerator = adapter.LeasePendingOperationsAsync(request, CancellationToken.None).GetAsyncEnumerator();
        return await enumerator.MoveNextAsync() ? enumerator.Current : null;
    }

    /// <summary>Ignores a value task result while preserving failures.</summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="task">The task to await.</param>
    /// <returns>The task.</returns>
    private static async Task IgnoreResultAsync<T>(ValueTask<T> task) => _ = await task;

    /// <summary>Provides a deterministic time provider for tests.</summary>
    /// <param name="utcNow">The current timestamp.</param>
    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current timestamp returned by the provider.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <summary>Advances the current time.</summary>
        /// <param name="delta">The duration to add.</param>
        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => _utcNow;
    }

    /// <summary>Creates and cleans up a unique test directory.</summary>
    private sealed class TestDirectory : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="TestDirectory"/> class.</summary>
        public TestDirectory()
        {
            RootPath = Path.Combine(AppContext.BaseDirectory, "litedb-test-data", Guid.NewGuid().ToString("N"));
            _ = Directory.CreateDirectory(RootPath);
        }

        /// <summary>Gets the root directory path.</summary>
        public string RootPath { get; }

        /// <summary>Gets the store database path.</summary>
        public string DatabasePath => Path.Combine(RootPath, "store.db");

        /// <inheritdoc />
        public void Dispose()
        {
            if (Directory.Exists(RootPath))
            {
                Directory.Delete(RootPath, recursive: true);
            }
        }
    }
}
