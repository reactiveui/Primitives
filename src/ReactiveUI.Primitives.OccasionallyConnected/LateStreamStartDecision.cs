// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores a late stream start admission decision.</summary>
/// <param name="CanStart">Whether the stream start can be admitted.</param>
/// <param name="Generation">The generation that owns the late stream start.</param>
internal readonly record struct LateStreamStartDecision(bool CanStart, StartGeneration? Generation);
