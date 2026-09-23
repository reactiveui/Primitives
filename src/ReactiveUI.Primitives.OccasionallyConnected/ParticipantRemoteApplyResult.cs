// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a participant-owned remote apply receipt and optional queue diagnostics.</summary>
/// <param name="Receipt">The durable remote apply receipt.</param>
/// <param name="QueueSnapshot">The authoritative queue snapshot, or null when the participant has no queue aggregate.</param>
/// <param name="CursorAdvanced">Whether the participant advanced its durable receive cursor.</param>
internal readonly record struct ParticipantRemoteApplyResult(
    RemoteApplyResult Receipt,
    QueueDiagnosticSnapshot? QueueSnapshot,
    bool CursorAdvanced);
