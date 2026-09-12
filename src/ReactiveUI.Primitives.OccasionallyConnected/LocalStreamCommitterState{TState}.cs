// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the current local stream state held by the committer.</summary>
/// <typeparam name="TState">The projected local state type.</typeparam>
/// <param name="StreamId">The stream identifier.</param>
/// <param name="SubscriptionId">The stable logical subscription identifier.</param>
/// <param name="State">The projected local state.</param>
/// <param name="Revision">The current snapshot revision.</param>
/// <param name="NextClientSequence">The next client sequence to assign.</param>
/// <param name="ServerCursor">The recovered server cursor.</param>
internal sealed record LocalStreamCommitterState<TState>(
    StreamId StreamId,
    SubscriptionId SubscriptionId,
    TState State,
    long Revision,
    long NextClientSequence,
    string? ServerCursor)
{
    /// <summary>Gets the immutable payload used to prepare isolated projection state.</summary>
    internal PayloadEnvelope? MaterializedPayload { get; init; }

    /// <summary>Gets the authoritative payload, or null when historical authoritative state is unknown.</summary>
    internal PayloadEnvelope? AuthoritativePayload { get; init; }
}
