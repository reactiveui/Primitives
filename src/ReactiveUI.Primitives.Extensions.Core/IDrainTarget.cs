// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Receives the callback that delivers queued notifications, from a delivery gate or a scheduled drain.</summary>
public interface IDrainTarget
{
    /// <summary>Delivers every queued notification that has not been delivered yet.</summary>
    void Drain();
}
