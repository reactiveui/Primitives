// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class CrdtLoopbackScenario
{
    /// <summary>Creates finite CRDT bounds for the lab.</summary>
    /// <returns>The CRDT bounds.</returns>
    private static CrdtBounds CreateBounds() =>
        new()
        {
            MaximumCounterComponents = MaximumCounterComponents,
            MaximumDotBindings = MaximumDots,
            MaximumTombstones = MaximumDots,
            MaximumElements = MaximumElements,
            MaximumElementBytes = MaximumElementBytes,
            MaximumRegisterBytes = MaximumRegisterBytes,
            MaximumEncodedBytes = MaximumEncodedBytes,
            MaximumClientIdUtf8Bytes = MaximumClientIdBytes,
        };

    /// <summary>Creates the real in-memory server hub options.</summary>
    /// <param name="timeProvider">The deterministic server clock.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The hub options.</returns>
    private static ServerStreamHubOptions CreateHubOptions(TimeProvider timeProvider, CrdtBounds bounds) =>
        new()
        {
            AuthorizationPolicy = new LabAuthorizationPolicy(TenantId),
            ConflictHandler = new()
            {
                Streams =
                [
                    CreateRegistration(GCounterStream, CrdtKind.GCounter, bounds),
                    CreateRegistration(PNCounterStream, CrdtKind.PNCounter, bounds),
                    CreateRegistration(ORSetStream, CrdtKind.ORSet, bounds),
                    CreateRegistration(LwwStream, CrdtKind.LwwRegister, bounds),
                    CreateRegistration(AckProbeClientAStream, CrdtKind.GCounter, bounds),
                    CreateRegistration(AckProbeClientBStream, CrdtKind.GCounter, bounds),
                ],
                MaximumProducedEvents = MaximumEvents,
            },
            TimeProvider = timeProvider,
            MaximumActiveCalls = MaximumBatchOperations,
            MaximumActiveSubscriptions = MaximumSubscriptions,
            MaximumBatchOperations = MaximumBatchOperations,
            MaximumBatchLogicalBytes = MaximumBatchBytes,
            MaximumReceiveGroups = MaximumLedgerEntries,
            MaximumReceiveEvents = CrdtLoopbackScenario.MaximumReceiveEvents,
            MaximumReceiveLogicalBytes = MaximumBatchBytes,
            EmptyPollDelay = TimeSpan.FromSeconds(ScenarioTimeoutSeconds),
            JournalLimits = new()
            {
                MaximumStreams = MaximumStreams,
                MaximumLedgerEntries = MaximumLedgerEntries,
                MaximumEvents = MaximumEvents,
                MaximumLogicalBytes = MaximumJournalBytes,
                MaximumOperationCaptureCount = MaximumOperationCaptureCount,
                MaximumEntryEventCount = MaximumEvents,
                MaximumSubscriptions = MaximumSubscriptions,
                MaximumSubscriptionOffers = MaximumSubscriptions,
                OperationRetention = TimeSpan.FromMinutes(OperationRetentionMinutes),
                SubscriptionRetention = TimeSpan.FromMinutes(SubscriptionRetentionMinutes),
            },
        };

    /// <summary>Creates one CRDT stream registration.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="kind">The CRDT kind.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The server stream registration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServerConflictStreamRegistration CreateRegistration(
        StreamId streamId,
        CrdtKind kind,
        CrdtBounds bounds) =>
        CrdtServerStreamRegistration.Create(new() { StreamId = streamId, Kind = kind, Bounds = bounds });

    /// <summary>Creates loopback adapter options for a trusted client.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="client">The trusted client identity.</param>
    /// <returns>The loopback adapter options.</returns>
    private static LoopbackTransportAdapterOptions CreateLoopbackOptions(
        IServerStreamHub hub,
        ServerAuthenticatedClient client) =>
        new()
        {
            Hub = hub,
            AuthenticatedClient = client,
            PeerCapabilities = CreateCapabilities(),
            MaximumConcurrentRequests = MaximumBatchOperations,
            MaximumConcurrentAcknowledgements = MaximumBatchOperations,
            MaximumConcurrentSubscriptions = MaximumSubscriptions,
            MaximumReceiveEvents = CrdtLoopbackScenario.MaximumReceiveEvents,
            MaximumCompletedOperations = CrdtLoopbackScenario.MaximumCompletedOperations,
            MaximumLogicalBatchBytes = MaximumBatchBytes,
            MaximumMetadataEntries = MaximumStreams,
            MaximumStringBytes = MaximumEncodedBytes,
        };

    /// <summary>Creates finite negotiated loopback capabilities.</summary>
    /// <returns>The negotiated capabilities.</returns>
    private static NegotiatedCapabilities CreateCapabilities() =>
        new(
            new(1, 0),
            CrdtLoopbackScenarioShape.VolatileLoopbackCapabilities,
            MaximumBatchOperations,
            MaximumBatchBytes,
            TimeSpan.FromMinutes(ServerIdempotencyRetentionMinutes),
            TimeSpan.FromMinutes(ClientInboxRetentionMinutes));

    /// <summary>Creates a trusted client connect request.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <returns>The connect request.</returns>
    private static TransportConnectRequest CreateConnectRequest(string clientId) =>
        new(new(new(1, 0), new(1, 0)), new(clientId), [DeliveryGuarantee.AtLeastOnce]);
}
