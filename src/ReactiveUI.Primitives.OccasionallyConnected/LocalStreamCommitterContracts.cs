// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes payload and snapshot contracts used by a local stream committer.</summary>
internal sealed record LocalStreamCommitterContracts
{
    /// <summary>Gets the local input payload contract identifier.</summary>
    public required string InputContractId { get; init; }

    /// <summary>Gets the local input payload schema version.</summary>
    public required int InputSchemaVersion { get; init; }

    /// <summary>Gets the local state snapshot payload contract identifier.</summary>
    public required string StateContractId { get; init; }

    /// <summary>Gets the local state snapshot payload schema version.</summary>
    public required int StateSchemaVersion { get; init; }

    /// <summary>Gets the durable snapshot format version.</summary>
    public required int SnapshotFormatVersion { get; init; }

    /// <summary>Validates this contract set.</summary>
    /// <exception cref="InvalidOperationException">The contract set is malformed.</exception>
    internal void Validate()
    {
        ValidateContractId(InputContractId, nameof(InputContractId));
        ValidateContractId(StateContractId, nameof(StateContractId));
        ValidateVersion(InputSchemaVersion, nameof(InputSchemaVersion));
        ValidateVersion(StateSchemaVersion, nameof(StateSchemaVersion));
        ValidateVersion(SnapshotFormatVersion, nameof(SnapshotFormatVersion));
    }

    /// <summary>Validates a contract identifier.</summary>
    /// <param name="contractId">The contract identifier.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="InvalidOperationException">The contract identifier is malformed.</exception>
    private static void ValidateContractId(string? contractId, string parameterName)
    {
        if (!string.IsNullOrWhiteSpace(contractId))
        {
            return;
        }

        throw new InvalidOperationException($"{parameterName} must be non-empty.");
    }

    /// <summary>Validates a positive version number.</summary>
    /// <param name="version">The version number.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="InvalidOperationException">The version is not positive.</exception>
    private static void ValidateVersion(int version, string parameterName)
    {
        if (version > 0)
        {
            return;
        }

        throw new InvalidOperationException($"{parameterName} must be positive.");
    }
}
