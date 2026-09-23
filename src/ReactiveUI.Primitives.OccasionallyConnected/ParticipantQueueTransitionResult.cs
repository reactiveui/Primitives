// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes queue diagnostics emitted by a participant-owned durable transition.</summary>
/// <param name="QueueSnapshot">The authoritative queue snapshot, or null when the participant has no queue aggregate.</param>
internal readonly record struct ParticipantQueueTransitionResult(QueueDiagnosticSnapshot? QueueSnapshot)
{
    /// <summary>Gets an empty transition result for participants without queue authority.</summary>
    internal static ParticipantQueueTransitionResult None { get; } = new(null);
}
