// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Concurrency;
#else
namespace ReactiveUI.Primitives.Concurrency;
#endif

/// <summary>Disposal helpers shared by sequencer work items.</summary>
internal static class SequencerWorkItemDisposal
{
    /// <summary>Publishes the action's disposable into the shared slot, disposing it when disposal won the race.</summary>
    /// <param name="slot">The disposable slot shared with the work item's disposal.</param>
    /// <param name="disposable">The disposable returned by the scheduled action.</param>
    /// <remarks>
    /// Disposal swaps a non-null sentinel into the slot, so a non-null exchange result means disposal already
    /// owns (or will own) the slot and the freshly produced disposable must be released here. A null result
    /// means this caller published first and disposal releases the slot later. This keeps the in-flight dispose
    /// race correct with a single compare-exchange instead of a re-check loop.
    /// </remarks>
    internal static void Publish(ref IDisposable? slot, IDisposable disposable)
    {
        if (Interlocked.CompareExchange(ref slot, disposable, null) is null)
        {
            return;
        }

        disposable.Dispose();
    }
}
