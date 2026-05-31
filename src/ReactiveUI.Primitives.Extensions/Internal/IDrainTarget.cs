// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Implemented by sinks that drive a <see cref="ScheduledDrainState{T}"/>. The state helper invokes
/// <see cref="Drain"/> once per scheduled burst via a static scheduler callback, so passing the sink
/// as an <see cref="IDrainTarget"/> keeps the scheduled action allocation-free (no captured closure).
/// </summary>
internal interface IDrainTarget
{
    /// <summary>Drains the queued notifications on the scheduler thread.</summary>
    void Drain();
}
