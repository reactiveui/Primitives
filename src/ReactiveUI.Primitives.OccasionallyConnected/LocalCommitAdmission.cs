// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies one bounded local commit admission reservation.</summary>
/// <param name="StreamId">The admitted stream identity.</param>
/// <param name="RetainedBytes">The retained bytes reserved for the admission.</param>
/// <param name="SignalId">The capacity signal identity that accepted the admission.</param>
internal readonly record struct LocalCommitAdmission(StreamId StreamId, long RetainedBytes, Guid SignalId);
