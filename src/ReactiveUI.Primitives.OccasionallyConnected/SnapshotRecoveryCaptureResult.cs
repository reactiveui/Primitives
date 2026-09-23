// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores the local capture and remote request used for one recovery attempt.</summary>
/// <param name="Capture">The local capture.</param>
/// <param name="Request">The remote recovery request.</param>
internal readonly record struct SnapshotRecoveryCaptureResult(
    LocalSnapshotRecoveryCapture Capture,
    RemoteSnapshotRecoveryRequest Request);
