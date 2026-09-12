// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Contains a bounded FIFO prefix and its full encoded byte count.</summary>
/// <param name="Kind">The reason the prefix is ready, waiting, or blocked.</param>
/// <param name="PrefixCount">The number of FIFO candidates in the planned prefix.</param>
/// <param name="EncodedBytes">The encoded bytes for the prefix, including the envelope.</param>
internal readonly record struct BatchSelectionResult(BatchSelectionResultKind Kind, int PrefixCount, long EncodedBytes);
