// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Describes one participant's independently received value and authoritative cursor.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="Value">The independently received value.</param>
/// <param name="Cursor">The authoritative receive cursor.</param>
[System.Diagnostics.DebuggerDisplay("{Value,nq}; Cursor={Cursor,nq}")]
internal sealed record CrdtLoopbackConvergenceParticipant<T>(T Value, string Cursor);
