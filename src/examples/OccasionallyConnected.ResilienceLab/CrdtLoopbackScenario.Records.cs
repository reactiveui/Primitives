// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the bounded in-memory CRDT loopback demonstration.</summary>
internal static partial class CrdtLoopbackScenario
{
    /// <summary>Holds decoded values from one client's received authoritative streams.</summary>
    /// <param name="GCounter">The G-counter value.</param>
    /// <param name="PNCounter">The PN-counter value.</param>
    /// <param name="ORSet">The OR-set value.</param>
    /// <param name="Lww">The LWW register value.</param>
    private sealed record ReceivedValueSnapshot(
        int GCounter,
        int PNCounter,
        string ORSet,
        string Lww);

    /// <summary>Holds inputs used to build runner case results.</summary>
    /// <param name="ClientAStates">The client A received streams.</param>
    /// <param name="ClientBStates">The client B received streams.</param>
    /// <param name="ClientAValues">The client A decoded values.</param>
    /// <param name="ClientBValues">The client B decoded values.</param>
    /// <param name="ClientAAcknowledged">Whether client A passed the ACK proof.</param>
    /// <param name="ClientBAcknowledged">Whether client B passed the ACK proof.</param>
    /// <param name="DuplicateDelta">The duplicate operation effect delta.</param>
    /// <param name="Bounds">The CRDT bounds.</param>
    private sealed record CaseBuildContext(
        CrdtLoopbackReceivedStates ClientAStates,
        CrdtLoopbackReceivedStates ClientBStates,
        ReceivedValueSnapshot ClientAValues,
        ReceivedValueSnapshot ClientBValues,
        bool ClientAAcknowledged,
        bool ClientBAcknowledged,
        int DuplicateDelta,
        CrdtBounds Bounds);

    /// <summary>Describes one deterministic same-subscription ACK probe.</summary>
    private sealed record AckProbeDescriptor
    {
        /// <summary>Gets the dedicated probe stream identifier.</summary>
        public required StreamId StreamId { get; init; }

        /// <summary>Gets the trusted client identifier.</summary>
        public required string ClientId { get; init; }

        /// <summary>Gets the deterministic subscription seed.</summary>
        public required int SubscriptionSeed { get; init; }

        /// <summary>Gets the first deterministic batch seed.</summary>
        public required int FirstBatchSeed { get; init; }

        /// <summary>Gets the second deterministic batch seed.</summary>
        public required int SecondBatchSeed { get; init; }

        /// <summary>Gets the first deterministic operation seed.</summary>
        public required int FirstOperationSeed { get; init; }

        /// <summary>Gets the second deterministic operation seed.</summary>
        public required int SecondOperationSeed { get; init; }
    }
}
