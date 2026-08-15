// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits timer ticks for the <c>After</c> factory overloads.</summary>
[System.Diagnostics.DebuggerDisplay("DueTime = {_dueTime}, Period = {_period}")]
public sealed class AfterSignal : IRequireCurrentThread<long>
{
    /// <summary>The delay before the single tick.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The recurring period after the first tick, when this is a periodic timer.</summary>
    private readonly TimeSpan? _period;

    /// <summary>The sequencer used to schedule the tick.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="AfterSignal"/> class.</summary>
    /// <param name="dueTime">The delay before the single tick.</param>
    /// <param name="scheduler">The sequencer used to schedule the tick.</param>
    public AfterSignal(TimeSpan dueTime, ISequencer scheduler)
    {
        _dueTime = dueTime;
        _scheduler = scheduler;
    }

    /// <summary>Initializes a new instance of the <see cref="AfterSignal"/> class.</summary>
    /// <param name="dueTime">The delay before the first tick.</param>
    /// <param name="period">The period between subsequent ticks.</param>
    /// <param name="scheduler">The sequencer used to schedule ticks.</param>
    public AfterSignal(TimeSpan dueTime, TimeSpan period, ISequencer scheduler)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        _dueTime = dueTime;
        _period = period;
        _scheduler = scheduler;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<long> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return Run(observer);
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(
            (Self: this, subscription, observer),
            static (_, s) =>
            {
                s.subscription.Create(s.Self.Run(s.observer));
                return EmptyDisposable.Instance;
            });
        return subscription;
    }

    /// <summary>Schedules the timer on the sequencer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that cancels the pending timer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private AfterSubscription Run(IObserver<long> observer) =>
        new AfterSubscription(observer, _scheduler, _dueTime, _period).Run();
}
