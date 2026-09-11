// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Converts one payload contract schema version to a later contiguous version.</summary>
public interface IPayloadUpcaster
{
    /// <summary>Gets the contract identifier this upcaster handles.</summary>
    string ContractId { get; }

    /// <summary>Gets the source schema version.</summary>
    int FromVersion { get; }

    /// <summary>Gets the target schema version.</summary>
    int ToVersion { get; }

    /// <summary>Converts a payload envelope to the target schema version.</summary>
    /// <param name="source">The source payload envelope.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The upcast payload envelope.</returns>
    ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken);
}
