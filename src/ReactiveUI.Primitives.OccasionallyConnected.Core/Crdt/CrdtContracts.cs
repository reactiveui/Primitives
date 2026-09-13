// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Contains the built-in CRDT payload contract identifiers.</summary>
public static class CrdtContracts
{
    /// <summary>Gets the CRDT state contract identifier.</summary>
    public static string StateContractId { get; } = "reactiveui.oc.crdt.state";

    /// <summary>Gets the CRDT input contract identifier.</summary>
    public static string InputContractId { get; } = "reactiveui.oc.crdt.input";

    /// <summary>Gets the current CRDT contract schema version.</summary>
    public static int SchemaVersion { get; } = 1;
}
