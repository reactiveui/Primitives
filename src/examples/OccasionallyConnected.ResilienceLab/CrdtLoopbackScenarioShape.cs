// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Defines the public API surface the CRDT loopback scenario must exercise.</summary>
public static class CrdtLoopbackScenarioShape
{
    /// <summary>Gets the CRDT loopback scenario name.</summary>
    public static string ScenarioName { get; } = "crdt-loopback";

    /// <summary>Gets the volatile loopback capability set used by the in-memory demonstration.</summary>
    public static RemoteTransportCapabilities VolatileLoopbackCapabilities { get; } =
        RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>Gets the expected CRDT families covered by the scenario.</summary>
    public static IReadOnlyList<CrdtKind> CoveredKinds { get; } =
        [CrdtKind.GCounter, CrdtKind.PNCounter, CrdtKind.ORSet, CrdtKind.LwwRegister];
}
