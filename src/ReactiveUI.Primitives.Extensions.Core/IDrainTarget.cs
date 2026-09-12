// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Receives the drain callback a <c>ScheduledDrainState&lt;T&gt;</c> raises once per scheduled pass.</summary>
public interface IDrainTarget
{
    /// <summary>Drains the queued notifications on the scheduler thread.</summary>
    void Drain();
}
