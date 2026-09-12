// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes one FIFO candidate using transport-computed encoded bytes.</summary>
/// <param name="ClientSequence">The client-assigned sequence used to preserve FIFO order.</param>
/// <param name="EncodedBytes">The complete caller-computed encoded byte contribution.</param>
internal readonly record struct BatchSelectionItem(long ClientSequence, long EncodedBytes);
