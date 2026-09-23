// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a participant snapshot recovery transition visible to the engine.</summary>
/// <param name="Acknowledgement">The remote acknowledgement that confirms the recovered cursor.</param>
/// <param name="QueueSnapshot">The participant-owned queue aggregate after recovery.</param>
internal readonly record struct ParticipantSnapshotRecoveryTransitionResult(
    ReceiveAcknowledgement Acknowledgement,
    QueueDiagnosticSnapshot? QueueSnapshot);
