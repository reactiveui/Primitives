// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Subscription handle for an <see cref="AfterSignal"/> timer.</summary>
[System.Diagnostics.DebuggerDisplay("AfterSubscription: Current = {Current}, DueTime = {DueTime}, Period = {Period}")]
public sealed class AfterSubscription : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="AfterSubscription"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="scheduler">The scheduler used to emit ticks.</param>
    /// <param name="dueTime">The delay before the first tick.</param>
    /// <param name="period">The period between subsequent ticks, or <see langword="null"/> for one-shot timers.</param>
    public AfterSubscription(IObserver<long> observer, ISequencer scheduler, TimeSpan dueTime, TimeSpan? period)
    {
        Observer = observer;
        Scheduler = scheduler;
        DueTime = dueTime;
        Period = period;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<long> Observer { get; }

    /// <summary>Gets the scheduler used to emit ticks.</summary>
    private ISequencer Scheduler { get; }

    /// <summary>Gets the delay before the first tick.</summary>
    private TimeSpan DueTime { get; }

    /// <summary>Gets the period between subsequent ticks.</summary>
    private TimeSpan? Period { get; }

    /// <summary>Gets the scheduled work slot.</summary>
    private SingleReplaceableDisposable Slot { get; } = new();

    /// <summary>Gets or sets the next tick value.</summary>
    private long Current { get; set; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Slot.Dispose();

    /// <summary>Schedules the first tick.</summary>
    /// <returns>The subscription handle.</returns>
    public AfterSubscription Run()
    {
        TimerSlot.Arm(Slot, Scheduler, Sequencer.Normalize(DueTime), Tick);
        return this;
    }

    /// <summary>Emits one tick and reschedules when this is a periodic timer.</summary>
    private void Tick()
    {
        var tick = Current;
        Current++;
        Observer.OnNext(tick);
        if (Slot.IsDisposed)
        {
            return;
        }

        if (Period is not { } period)
        {
            Observer.OnCompleted();
            return;
        }

        TimerSlot.Arm(Slot, Scheduler, period, Tick);
    }
}
