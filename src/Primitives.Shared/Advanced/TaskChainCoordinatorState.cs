// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>State transitions for <see cref="TaskChainCoordinator{T}"/>.</summary>
internal static class TaskChainCoordinatorState
{
    /// <summary>Marks the active inner source complete and drains the coordinator unless it has stopped.</summary>
    /// <typeparam name="T">The task result type.</typeparam>
    /// <param name="gate">The coordinator state gate.</param>
    /// <param name="done">Whether a terminal notification has been emitted.</param>
    /// <param name="active">Whether an inner source is active.</param>
    /// <param name="coordinator">The coordinator to drain.</param>
    internal static void OnInnerCompleted<T>(
        Lock gate,
        ref bool done,
        ref bool active,
        TaskChainCoordinator<T> coordinator)
    {
        lock (gate)
        {
            if (done)
            {
                return;
            }

            active = false;
        }

        coordinator.Drain();
    }
}
