// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Builds the public in-memory hub, loopback transport and CRDT values shared by the smaller lab scenarios.</summary>
internal static class ResilienceLabLoopback
{
    /// <summary>The trusted tenant identifier used by every lab client.</summary>
    internal const string TenantId = "resilience-lab";

    /// <summary>The finite call, batch and subscription bound used by the lab.</summary>
    internal const int Capacity = 16;

    /// <summary>The finite logical batch byte bound used by the lab.</summary>
    internal const int BatchBytes = 8192;

    /// <summary>The finite retained event bound used by the lab.</summary>
    internal const int MaximumEvents = 32;

    /// <summary>The scenario guard timeout in seconds.</summary>
    internal const int GuardTimeoutSeconds = 10;

    /// <summary>The finite retained journal byte bound.</summary>
    private const int JournalBytes = 65_536;

    /// <summary>The finite retained stream bound.</summary>
    private const int MaximumStreams = 8;

    /// <summary>The finite CRDT component and element bound.</summary>
    private const int CrdtComponents = 8;

    /// <summary>The finite CRDT element and register byte bound.</summary>
    private const int CrdtElementBytes = 64;

    /// <summary>The finite CRDT encoded payload byte bound.</summary>
    private const int CrdtEncodedBytes = 2048;

    /// <summary>The server idempotency retention in minutes.</summary>
    private const int IdempotencyRetentionMinutes = 30;

    /// <summary>The deterministic GUID prefix used by the lab.</summary>
    private const string GuidPrefix = "00000000-0000-0000-0000-";

    /// <summary>The number of hex digits in the deterministic GUID tail.</summary>
    private const string GuidSeedFormat = "x12";

    /// <summary>Gets the deterministic initial clock value shared by the lab scenarios.</summary>
    internal static DateTimeOffset InitialTime { get; } = DateTimeOffset.Parse(
        "2026-09-26T00:00:00Z",
        CultureInfo.InvariantCulture,
        DateTimeStyles.AssumeUniversal);

    /// <summary>Gets the finite CRDT bounds shared by the lab scenarios.</summary>
    internal static CrdtBounds Bounds { get; } = new()
    {
        MaximumCounterComponents = CrdtComponents,
        MaximumDotBindings = CrdtComponents,
        MaximumTombstones = CrdtComponents,
        MaximumElements = CrdtComponents,
        MaximumElementBytes = CrdtElementBytes,
        MaximumRegisterBytes = CrdtElementBytes,
        MaximumEncodedBytes = CrdtEncodedBytes,
        MaximumClientIdUtf8Bytes = CrdtElementBytes,
    };

    /// <summary>Gets the guard timeout that bounds every lab wait.</summary>
    internal static TimeSpan GuardTimeout { get; } = TimeSpan.FromSeconds(GuardTimeoutSeconds);

    /// <summary>Creates in-memory hub options with one G-counter registration per stream.</summary>
    /// <param name="clock">The server clock.</param>
    /// <param name="journalLimits">The journal limits.</param>
    /// <param name="streams">The G-counter streams to register.</param>
    /// <returns>The hub options.</returns>
    internal static ServerStreamHubOptions CreateHubOptions(
        TimeProvider clock,
        ServerCommitJournalLimits journalLimits,
        params StreamId[] streams)
    {
        var registrations = new ServerConflictStreamRegistration[streams.Length];
        for (var index = 0; index < streams.Length; index++)
        {
            registrations[index] = CrdtServerStreamRegistration.Create(
                new() { StreamId = streams[index], Kind = CrdtKind.GCounter, Bounds = Bounds });
        }

        return new()
        {
            AuthorizationPolicy = new ResilienceLabAuthorizationPolicy(TenantId),
            ConflictHandler = new() { Streams = registrations, MaximumProducedEvents = MaximumEvents },
            TimeProvider = clock,
            MaximumActiveCalls = Capacity,
            MaximumActiveSubscriptions = Capacity,
            MaximumBatchOperations = Capacity,
            MaximumBatchLogicalBytes = BatchBytes,
            MaximumReceiveGroups = MaximumEvents,
            MaximumReceiveEvents = MaximumEvents,
            MaximumReceiveLogicalBytes = BatchBytes,
            EmptyPollDelay = GuardTimeout,
            JournalLimits = journalLimits,
        };
    }

    /// <summary>Creates finite journal limits with the supplied operation retention.</summary>
    /// <param name="operationRetention">The operation and event retention.</param>
    /// <returns>The journal limits.</returns>
    internal static ServerCommitJournalLimits CreateJournalLimits(TimeSpan operationRetention) =>
        new()
        {
            MaximumStreams = MaximumStreams,
            MaximumLedgerEntries = MaximumEvents,
            MaximumEvents = MaximumEvents,
            MaximumLogicalBytes = JournalBytes,
            MaximumOperationCaptureCount = MaximumEvents,
            MaximumEntryEventCount = MaximumEvents,
            MaximumSubscriptions = Capacity,
            MaximumSubscriptionOffers = Capacity,
            OperationRetention = operationRetention,
            SubscriptionRetention = TimeSpan.FromMinutes(IdempotencyRetentionMinutes),
        };

    /// <summary>Creates loopback adapter options for one trusted client and one peer feature set.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="clientId">The trusted client identifier.</param>
    /// <param name="features">The peer features the loopback advertises.</param>
    /// <returns>The loopback adapter options.</returns>
    internal static LoopbackTransportAdapterOptions CreateLoopbackOptions(
        IServerStreamHub hub,
        string clientId,
        RemoteTransportCapabilities features) =>
        new()
        {
            Hub = hub,
            AuthenticatedClient = new(TenantId, clientId),
            PeerCapabilities = new(
                new(1, 0),
                features,
                Capacity,
                BatchBytes,
                TimeSpan.FromMinutes(IdempotencyRetentionMinutes),
                null),
            MaximumConcurrentRequests = Capacity,
            MaximumConcurrentAcknowledgements = Capacity,
            MaximumConcurrentSubscriptions = Capacity,
            MaximumReceiveEvents = MaximumEvents,
            MaximumCompletedOperations = MaximumEvents,
            MaximumLogicalBatchBytes = BatchBytes,
            MaximumMetadataEntries = MaximumStreams,
            MaximumStringBytes = CrdtEncodedBytes,
        };

    /// <summary>Creates a trusted client connect request for one delivery guarantee.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="guarantee">The required delivery guarantee.</param>
    /// <returns>The connect request.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static TransportConnectRequest CreateConnectRequest(string clientId, DeliveryGuarantee guarantee) =>
        new(new(new(1, 0), new(1, 0)), new(clientId), [guarantee]);

    /// <summary>Creates a one-operation CRDT batch.</summary>
    /// <param name="batchSeed">The deterministic batch seed.</param>
    /// <param name="operationSeed">The deterministic operation seed.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="sequence">The client stream sequence.</param>
    /// <param name="mutation">The CRDT mutation.</param>
    /// <returns>The synchronization batch.</returns>
    internal static SyncBatch CreateBatch(
        int batchSeed,
        int operationSeed,
        StreamId streamId,
        long sequence,
        CrdtMutation mutation) =>
        new(
            CreateGuid(batchSeed),
            [
                new SyncOperation
                {
                    OperationId = new(CreateGuid(operationSeed)),
                    StreamId = streamId,
                    ClientSequence = sequence,
                    TimestampUtc = InitialTime,
                    Type = SyncOperationType.Update,
                    Payload = CrdtServerPayloads.CreateInput(CrdtInput.ForMutation(mutation), Bounds),
                    Policy = OperationPolicy.Default,
                },
            ]);

    /// <summary>Reads the first available receive page, bounded by the supplied token.</summary>
    /// <param name="pages">The receive page sequence.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The first receive page.</returns>
    /// <exception cref="InvalidOperationException">The sequence ended before a page arrived.</exception>
    internal static async ValueTask<RemoteEventBatch> ReadFirstPageAsync(
        IAsyncEnumerable<RemoteEventBatch> pages,
        CancellationToken cancellationToken)
    {
        await using var enumerator = pages.GetAsyncEnumerator(cancellationToken);
        return await enumerator.MoveNextAsync().ConfigureAwait(false)
            ? enumerator.Current
            : throw new InvalidOperationException("The receive sequence ended before a page arrived.");
    }

    /// <summary>Creates an expected-versus-actual invariant case.</summary>
    /// <param name="name">The case name.</param>
    /// <param name="expected">The expected value.</param>
    /// <param name="actual">The actual value.</param>
    /// <returns>The invariant case.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ResilienceLabCaseResult Case(string name, object expected, object actual) =>
        new(name, expected, actual, Equals(expected, actual));

    /// <summary>Creates a deterministic GUID from a small positive seed.</summary>
    /// <param name="seed">The seed.</param>
    /// <returns>The deterministic GUID.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static Guid CreateGuid(int seed) =>
        new(GuidPrefix + seed.ToString(GuidSeedFormat, CultureInfo.InvariantCulture));
}
