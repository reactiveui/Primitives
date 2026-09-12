// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Arms the single active timer held by an operator's cancellation slot.</summary>
public static class TimerSlot
{
    /// <summary>Publishes slot ownership before scheduling, then fills it with the scheduled handle.</summary>
    /// <param name="slot">The slot holding the operator's currently armed timer.</param>
    /// <param name="sequencer">The sequencer that runs the callback.</param>
    /// <param name="delay">The delay before the callback runs.</param>
    /// <param name="tick">The timer callback.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    /// <remarks>A callback that runs during scheduling can rearm the slot without its successor being canceled.</remarks>
    public static void Arm(SingleReplaceableDisposable slot, ISequencer sequencer, TimeSpan delay, Action tick)
    {
        ArgumentExceptionHelper.ThrowIfNull(slot);

        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        ArgumentExceptionHelper.ThrowIfNull(tick);

        SingleDisposable pending = new();
        slot.Create(pending);
        pending.Create(sequencer.Schedule(delay, tick));
    }
}
