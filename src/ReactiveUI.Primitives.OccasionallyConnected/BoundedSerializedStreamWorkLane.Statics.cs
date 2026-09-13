// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Static helpers for <see cref="BoundedSerializedStreamWorkLane"/>.</summary>
internal sealed partial class BoundedSerializedStreamWorkLane
{
    /// <summary>Queues lane work on the thread pool.</summary>
    /// <param name="action">The action to run.</param>
    private static void QueueWork(Action action) => _ = Task.Run(action);
}
