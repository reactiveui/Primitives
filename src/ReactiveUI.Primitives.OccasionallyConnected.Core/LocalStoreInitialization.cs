// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes local store initialization requirements.</summary>
/// <param name="StoreIdentity">The stable store identity.</param>
/// <param name="RequiredSchemaVersion">The minimum schema version the adapter must support.</param>
/// <param name="RequireAuthenticatedEncryptionAtRest">Whether authenticated encryption at rest is required.</param>
[System.Diagnostics.DebuggerDisplay("{StoreIdentity,nq}")]
public sealed record LocalStoreInitialization(
    string StoreIdentity,
    int RequiredSchemaVersion,
    bool RequireAuthenticatedEncryptionAtRest)
{
    /// <summary>Gets the optional client identity bound to this local store partition.</summary>
    public string? ClientId { get; init; }

    /// <summary>Gets optional global capacity for unresolved outgoing operations.</summary>
    public OutboxOptions? Outbox { get; init; }
}
