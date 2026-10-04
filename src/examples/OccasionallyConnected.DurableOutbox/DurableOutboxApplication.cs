// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Implements the durable outbox teaching workflow over the public SQLite store adapter.</summary>
internal sealed partial class DurableOutboxApplication
{
    /// <summary>The maximum pending operations admitted by the sample before synchronization is demonstrated.</summary>
    internal const int MaximumPendingOperations = 4;

    /// <summary>The maximum payload bytes admitted to the SQLite adapter worker.</summary>
    internal const long WorkerCapacityBytes = 1024L * 1024L;

    /// <summary>The maximum operations leased in one simulated upload batch.</summary>
    internal const int MaximumLeaseOperations = 1;

    /// <summary>The sample's single stream identifier.</summary>
    internal static readonly StreamId TemperatureStream = new("sensor/temperature");

    /// <summary>The required local store schema version.</summary>
    private const int RequiredSchemaVersion = 1;

    /// <summary>The snapshot format version used by the example projection.</summary>
    private const int SnapshotFormatVersion = 1;

    /// <summary>The payload schema version used by both sample contracts.</summary>
    private const int PayloadSchemaVersion = 1;

    /// <summary>The worker queue capacity passed to the SQLite adapter.</summary>
    private const int WorkerCapacity = 8;

    /// <summary>The maximum payload bytes accepted by the serializer.</summary>
    private const int SerializerPayloadBytes = 16_384;

    /// <summary>The maximum payload bytes leased for one simulated batch.</summary>
    private const long LeaseCapacityBytes = 64L * 1024L;

    /// <summary>The simulated lease duration in minutes.</summary>
    private const int LeaseDurationMinutes = 5;

    /// <summary>The first deterministic demo reading.</summary>
    private const double FirstDemoReading = 21.25;

    /// <summary>The second deterministic demo reading.</summary>
    private const double SecondDemoReading = 22.0;

    /// <summary>The local store identity for this sample database.</summary>
    private const string StoreIdentity = "durable-outbox-example";

    /// <summary>The local client identity for this sample database.</summary>
    private const string ClientId = "durable-outbox-client";

    /// <summary>The payload contract for reading operations.</summary>
    private const string ReadingContract = "example.temperature-reading";

    /// <summary>The payload contract for snapshots.</summary>
    private const string SnapshotContract = "example.temperature-snapshot";

    /// <summary>The operation metadata key that names the source.</summary>
    private const string MetadataSourceKey = "source";

    /// <summary>The operation metadata value that names this sample.</summary>
    private const string MetadataSourceValue = "durable-outbox-example";

    /// <summary>The operation metadata key that records the requested guarantee.</summary>
    private const string MetadataGuaranteeKey = "guarantee";

    /// <summary>The durable reason code used when an at-most-once response is ambiguous.</summary>
    private const string AtMostOnceAmbiguousReason = "OC.AttemptAmbiguous";

    /// <summary>The durable reason code used by a simulated rejection.</summary>
    private const string SampleRejectedReason = "OC.SampleRejected";

    /// <summary>The fake server version label stored by local simulations.</summary>
    private const string SampleServerVersion = "sample-server-version";

    /// <summary>The shared label for operation output lines.</summary>
    private const string OperationLabel = "operation";

    /// <summary>The shared label for operation state output lines.</summary>
    private const string StateLabel = "state";

    /// <summary>The text printed for missing durable status.</summary>
    private const string UnknownText = "unknown";

    /// <summary>The message printed when an at-most-once attempt is ambiguous.</summary>
    private const string AtMostOnceAmbiguousMessage =
        "AtMostOnce ambiguous operations are not retried; inspect the status and resolve manually.";

    /// <summary>The message printed when recovered pending work has no durable status row.</summary>
    private const string MissingDurableStatusMessage = "The pending operation has no durable status.";

    /// <summary>The message printed when rejected recovered work has no durable snapshot.</summary>
    private const string MissingRejectionSnapshotMessage =
        "The rejected operation cannot rebuild a snapshot because no durable snapshot exists.";

    /// <summary>The message printed when rejected recovered work has no authoritative checkpoint.</summary>
    private const string MissingRejectionAuthoritativeSnapshotMessage =
        "The rejected operation cannot rebuild a snapshot because no authoritative checkpoint exists.";

    /// <summary>The message printed when the next durable send attempt cannot be represented.</summary>
    private const string AttemptOverflowMessage = "The next durable attempt number would overflow.";

    /// <summary>The payload serializer configured with source-generated contracts.</summary>
    private readonly JsonPayloadSerializer _serializer;

    /// <summary>The clock used by sample timestamps and SQLite adapter timestamps.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>Creates local store adapter instances for this sample application.</summary>
    private readonly Func<string, SqliteLocalStoreAdapterOptions, ILocalStoreAdapter> _storeFactory;

    /// <summary>Initializes a new instance of the <see cref="DurableOutboxApplication"/> class.</summary>
    internal DurableOutboxApplication()
        : this(TimeProvider.System)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DurableOutboxApplication"/> class.</summary>
    /// <param name="timeProvider">The clock used for sample timestamps and SQLite store timestamps.</param>
    internal DurableOutboxApplication(TimeProvider timeProvider)
        : this(timeProvider, static (databasePath, options) => new SqliteLocalStoreAdapter(databasePath, options))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DurableOutboxApplication"/> class.</summary>
    /// <param name="timeProvider">The clock used for sample timestamps and SQLite store timestamps.</param>
    /// <param name="storeFactory">The adapter factory used to open durable local store sessions.</param>
    internal DurableOutboxApplication(
        TimeProvider timeProvider,
        Func<string, SqliteLocalStoreAdapterOptions, ILocalStoreAdapter> storeFactory)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(storeFactory);

        _timeProvider = timeProvider;
        _storeFactory = storeFactory;
        var schemaRegistry = new SchemaRegistry()
            .Register(ReadingContract, PayloadSchemaVersion, DurableOutboxJsonContext.Default.TemperatureReading)
            .Register(SnapshotContract, PayloadSchemaVersion, DurableOutboxJsonContext.Default.TemperatureSnapshot);
        _serializer = new(schemaRegistry, SerializerPayloadBytes);
    }

    /// <summary>Runs a parsed command and converts expected command failures to process-style output.</summary>
    /// <param name="command">The command to run.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    internal async ValueTask<OutboxCommandResult> RunCommandAsync(IOutboxCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);

        try
        {
            return await command.ExecuteAsync(this, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException exception)
        {
            return Error(exception.Message, exitCode: 2);
        }
        catch (NotSupportedException exception)
        {
            return Error(exception.Message, exitCode: 2);
        }
        catch (ArgumentException exception)
        {
            return Error(exception.Message, exitCode: 2);
        }
    }

    /// <summary>Appends a reading to the durable local outbox.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="deviceId">The source device identifier.</param>
    /// <param name="value">The reading value.</param>
    /// <param name="guarantee">The requested delivery guarantee.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    internal async ValueTask<OutboxCommandResult> AppendReadingAsync(
        string databasePath,
        string deviceId,
        double value,
        DeliveryGuarantee guarantee,
        CancellationToken cancellationToken)
    {
        if (guarantee == DeliveryGuarantee.ExactlyOnce)
        {
            return Error(
                "ExactlyOnce effect requires a server idempotency ledger and atomic apply-plus-ack. This sample has only a local SQLite durable store.",
                exitCode: 2);
        }

        var policy = new OperationPolicy(guarantee, OperationDurability.Durable, Priority: 0, ConflictPolicy.Merge);
        policy.Validate();
        await using var session = await OpenSessionAsync(databasePath, cancellationToken).ConfigureAwait(false);
        if (session.Recovered.PendingOperations.Count >= MaximumPendingOperations)
        {
            return Error(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"The sample outbox already has {session.Recovered.PendingOperations.Count} pending operations; capacity is {MaximumPendingOperations}."),
                exitCode: 2);
        }

        var currentSnapshot = await ReadSnapshotAsync(session.Recovered.Snapshot, cancellationToken).ConfigureAwait(false);
        var reading = new TemperatureReading(deviceId, value, "C", _timeProvider.GetUtcNow());
        var nextSnapshot = currentSnapshot.Apply(reading);
        var payload = await _serializer.SerializeAsync(ReadingContract, PayloadSchemaVersion, reading, cancellationToken)
            .ConfigureAwait(false);
        var snapshotPayload = await _serializer.SerializeAsync(
            SnapshotContract,
            PayloadSchemaVersion,
            nextSnapshot,
            cancellationToken).ConfigureAwait(false);
        var authoritativePayload = session.Recovered.Snapshot?.AuthoritativeState
            ?? await _serializer.SerializeAsync(
                SnapshotContract,
                PayloadSchemaVersion,
                TemperatureSnapshot.Empty,
                cancellationToken).ConfigureAwait(false);
        var operation = CreateOperation(session, reading, payload, policy, guarantee);
        var expectedRevision = session.Recovered.Snapshot?.Revision ?? 0;
        var mutation = CreateSnapshotMutation(snapshotPayload, authoritativePayload, expectedRevision);
        var commit = await session.Store.CommitLocalOperationAsync(operation, mutation, cancellationToken).ConfigureAwait(false);
        PublishReceipt receipt = new(
            commit.OperationId,
            commit.ClientSequence,
            SyncOperationState.QueuedForUpload,
            commit.CommittedAtUtc);
        var output = FormatLines(
            string.Create(CultureInfo.InvariantCulture, $"{OperationLabel}: {receipt.OperationId.Value}"),
            string.Create(CultureInfo.InvariantCulture, $"client-sequence: {receipt.ClientSequence}"),
            string.Create(CultureInfo.InvariantCulture, $"{StateLabel}: {receipt.State}"),
            string.Create(CultureInfo.InvariantCulture, $"saved-at: {receipt.SavedAtUtc:O}"),
            string.Create(CultureInfo.InvariantCulture, $"snapshot-revision: {commit.SnapshotRevision}"),
            "local receipt persisted before any server acknowledgement");
        return Ok(output, receipt.OperationId);
    }

    /// <summary>Inspects durable SQLite state after reopening the store.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="view">The view to print.</param>
    /// <param name="take">The bounded number of subscription entries to print.</param>
    /// <param name="operationId">The optional operation id whose retained status should be printed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    internal async ValueTask<OutboxCommandResult> InspectAsync(
        string databasePath,
        InspectView view,
        int take,
        OperationId? operationId,
        CancellationToken cancellationToken)
    {
        if (take <= 0)
        {
            return Error("The subscription take count must be positive.", exitCode: 2);
        }

        await using var session = await OpenSessionAsync(databasePath, cancellationToken).ConfigureAwait(false);
        List<string> lines = [];
        AppendSubscription(lines, session);
        if (view == InspectView.Subscription)
        {
            await AppendSubscriptionViewAsync(lines, session, take, cancellationToken).ConfigureAwait(false);
        }

        if (view is InspectView.Status or InspectView.Pending)
        {
            await AppendPendingAsync(lines, session, cancellationToken).ConfigureAwait(false);
        }

        if (view == InspectView.Status && operationId.HasValue)
        {
            await AppendOperationStatusAsync(lines, session, operationId.Value, cancellationToken).ConfigureAwait(false);
        }

        if (view is InspectView.Status or InspectView.Snapshot)
        {
            await AppendSnapshotAsync(lines, session, cancellationToken).ConfigureAwait(false);
        }

        AppendCapacity(lines);
        lines.Add("local receipt persisted before any server acknowledgement");
        return Ok(FormatLines(lines), default);
    }

    /// <summary>Runs a bounded deterministic demonstration in an owned temporary directory.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The command result.</returns>
    internal ValueTask<OutboxCommandResult> RunDemoAsync(CancellationToken cancellationToken)
    {
        DurableOutboxDemo demo = new(
            Path.Combine(TemporaryDirectory.GetTemporaryDirectory(), "oc-durable-outbox-demo"),
            [
                async (databasePath, stageCancellationToken) => await AppendReadingAsync(
                    databasePath,
                    "device-demo",
                    FirstDemoReading,
                    DeliveryGuarantee.AtLeastOnce,
                    stageCancellationToken).ConfigureAwait(false),
                async (databasePath, stageCancellationToken) => await SimulateAttemptAsync(
                    databasePath,
                    default,
                    SimulatedAttemptOutcome.Accepted,
                    stageCancellationToken).ConfigureAwait(false),
                async (databasePath, stageCancellationToken) => await InspectAsync(
                    databasePath,
                    InspectView.Status,
                    take: 1,
                    operationId: null,
                    stageCancellationToken).ConfigureAwait(false),
                async (databasePath, stageCancellationToken) => await AppendReadingAsync(
                    databasePath,
                    "device-demo",
                    SecondDemoReading,
                    DeliveryGuarantee.AtMostOnce,
                    stageCancellationToken).ConfigureAwait(false),
                async (databasePath, stageCancellationToken) => await SimulateAttemptAsync(
                    databasePath,
                    default,
                    SimulatedAttemptOutcome.LostResponse,
                    stageCancellationToken).ConfigureAwait(false),
            ]);
        return demo.RunAsync(cancellationToken);
    }

    /// <summary>Deserializes the current local snapshot.</summary>
    /// <param name="snapshot">The stored snapshot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The sample snapshot.</returns>
    private async ValueTask<TemperatureSnapshot> ReadSnapshotAsync(LocalSnapshot? snapshot, CancellationToken cancellationToken)
    {
        if (snapshot is null)
        {
            return TemperatureSnapshot.Empty;
        }

        var value = await _serializer.DeserializeAsync(snapshot.State, typeof(TemperatureSnapshot), cancellationToken)
            .ConfigureAwait(false);
        return (TemperatureSnapshot)value;
    }

    /// <summary>Opens, initializes, and recovers a SQLite store session.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovered store session.</returns>
    private async ValueTask<StoreSession> OpenSessionAsync(string databasePath, CancellationToken cancellationToken)
    {
        var options = CreateStoreOptions();
        var store = _storeFactory(databasePath, options);
        try
        {
            await store.InitializeAsync(
                new(StoreIdentity, RequiredSchemaVersion, RequireAuthenticatedEncryptionAtRest: false) { ClientId = ClientId },
                cancellationToken).ConfigureAwait(false);
            var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(TemperatureStream, null, cancellationToken)
                .ConfigureAwait(false);
            var recovered = await store.RecoverStreamAsync(TemperatureStream, subscriptionId, cancellationToken)
                .ConfigureAwait(false);
            return new(store, subscriptionId, recovered);
        }
        catch (Exception exception)
        {
            await FailedOpenCleanup.DisposeAsync(store).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            return await Task.FromException<StoreSession>(exception).ConfigureAwait(false);
        }
    }

    /// <summary>Creates SQLite options for the sample store.</summary>
    /// <returns>The configured SQLite adapter options.</returns>
    private SqliteLocalStoreAdapterOptions CreateStoreOptions() =>
        new() { WorkerCapacity = WorkerCapacity, WorkerCapacityBytes = WorkerCapacityBytes, TimeProvider = _timeProvider };

    /// <summary>Disposes stores after failed open attempts without replacing the original open failure.</summary>
    private static class FailedOpenCleanup
    {
        /// <summary>Disposes a store after open failure without replacing the original failure.</summary>
        /// <param name="store">The store to dispose.</param>
        /// <returns>A task that captures synchronous and asynchronous cleanup failures.</returns>
        internal static async Task DisposeAsync(ILocalStoreAdapter store) =>
            await store.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Holds an initialized store and its recovered durable stream view.</summary>
    private sealed class StoreSession : IAsyncDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="StoreSession"/> class.</summary>
        /// <param name="store">The SQLite store adapter.</param>
        /// <param name="subscriptionId">The durable subscription id.</param>
        /// <param name="recovered">The recovered stream state.</param>
        internal StoreSession(ILocalStoreAdapter store, SubscriptionId subscriptionId, RecoveredStream recovered)
        {
            Store = store;
            SubscriptionId = subscriptionId;
            Recovered = recovered;
        }

        /// <summary>Gets the SQLite store adapter.</summary>
        internal ILocalStoreAdapter Store { get; }

        /// <summary>Gets the durable subscription id.</summary>
        internal SubscriptionId SubscriptionId { get; }

        /// <summary>Gets the recovered stream state.</summary>
        internal RecoveredStream Recovered { get; }

        /// <summary>Disposes the SQLite store adapter.</summary>
        /// <returns>The dispose operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => Store.DisposeAsync();
    }
}
