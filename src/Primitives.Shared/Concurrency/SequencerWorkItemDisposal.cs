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
    /// <summary>Publishes the disposable or releases it if disposal owns the slot.</summary>
    /// <param name="slot">The disposable slot shared with the work item's disposal.</param>
    /// <param name="disposable">The disposable returned by the scheduled action.</param>
    internal static void Publish(ref IDisposable? slot, IDisposable disposable)
    {
        if (Interlocked.CompareExchange(ref slot, disposable, null) is null)
        {
            return;
        }

        disposable.Dispose();
    }
}
