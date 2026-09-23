// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Bounded recovered upload scheduling metadata.</summary>
/// <param name="Priority">The recovered pending upload priority.</param>
/// <param name="NotBeforeUtc">The optional UTC time before which the head is known not to be leaseable.</param>
internal readonly record struct RecoveredUploadHead(int Priority, DateTimeOffset? NotBeforeUtc);
