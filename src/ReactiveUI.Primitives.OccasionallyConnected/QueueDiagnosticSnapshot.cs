// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores a bounded per-stream queue diagnostic aggregate.</summary>
/// <param name="PendingOperations">The pending operation count.</param>
/// <param name="PendingBytes">The retained pending bytes.</param>
/// <param name="Revision">The monotonic diagnostic revision that produced this aggregate.</param>
internal readonly record struct QueueDiagnosticSnapshot(long PendingOperations, long PendingBytes, long Revision);
