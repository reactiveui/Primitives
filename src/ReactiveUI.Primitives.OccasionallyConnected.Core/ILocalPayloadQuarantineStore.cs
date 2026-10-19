// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Persists stream-level quarantine markers for invalid local payload data.</summary>
public interface ILocalPayloadQuarantineStore
{
    /// <summary>Persists or returns a durable quarantine marker for an affected stream.</summary>
    /// <param name="request">The quarantine request.</param>
    /// <param name="cancellationToken">The cancellation token observed before persistence commits.</param>
    /// <returns>The persisted marker and whether the call created it.</returns>
    ValueTask<LocalPayloadQuarantineResult> QuarantinePayloadAsync(
        LocalPayloadQuarantineRequest request,
        CancellationToken cancellationToken);

    /// <summary>Reads the durable quarantine marker for a stream, when present.</summary>
    /// <param name="streamId">The affected stream identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The quarantine marker, or <see langword="null"/> when the stream is not quarantined.</returns>
    ValueTask<LocalPayloadQuarantineRecord?> GetPayloadQuarantineAsync(
        StreamId streamId,
        CancellationToken cancellationToken);
}
